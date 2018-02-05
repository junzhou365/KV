package shardkv

import (
	"sync"
	"time"
)

type MigrationReq struct {
	Table      map[string]string
	Duplicates map[int]Op
	GID        int
	done       chan interface{}
}

func (kv *ShardKV) Migrate(args *MigrateArgs, reply *MigrateReply) {
	DTPrintf("%d-%d: Server [Migrate], args: %+v\n", kv.gid, kv.me, args)
	defer DTPrintf("%d-%d: Server [Migrate], reply: %+v\n", kv.gid, kv.me, reply)

	req := MigrationReq{
		Table:      args.Table,
		Duplicates: args.Duplicates,
		GID:        args.GID,
		done:       make(chan interface{})}

	kv.rpcCh <- &req
	<-req.done

}

func (kv *ShardKV) receiveMigration(op Op, index int, term int) {
	// The second condition could happend when none of the servers agreed.
	if op.PrevGID == 0 || op.PrevGID == kv.gid {
		return
	}

	DTPrintf("%d-%d: receive Migration, op: %+v\n", kv.gid, kv.me, op)
	defer DTPrintf("%d-%d: done Migration, op: %+v\n", kv.gid, kv.me, op)

	// wait for migration req from leaving shards
	req := <-kv.rpcCh
	defer close(req.done)

	if op.PrevGID != req.GID {
		DTPrintf("%d-%d: Migration op %+v mismatch with %d\n", kv.gid, kv.me, op, req.GID)
	}

	kv.state.rw.Lock()
	for k, v := range req.Table {
		kv.state.Table[k] = v
	}
	kv.state.rw.Unlock()

	// must take snapshot here because we don't have the log for these kv states
	if kv.maxraftstate != -1 {
		kv.takeSnapshot(index, term)
		go kv.rf.DiscardLogEnries(index, term)
	}

	kv.state.rw.Lock()
	for id, op := range req.Duplicates {
		if origOp, ok := kv.state.Duplicates[id]; !ok {
			kv.state.Duplicates[id] = op
		} else {
			// client seq increases monotonically.
			if op.Seq > origOp.Seq {
				kv.state.Duplicates[id] = op
			}
		}
	}
	kv.state.rw.Unlock()
}

// used for servers that lost shard
func (kv *ShardKV) sendMigration(shard int, names []string) {
	if _, isLeader := kv.rf.GetState(); !isLeader {
		return
	}

	DTPrintf("%d-%d: [LEAVE] send Migration for shard %v to names: %v\n", kv.gid, kv.me, shard, names)
	defer DTPrintf("%d-%d: [LEAVE] done Migration for shard %v to names: %v\n", kv.gid, kv.me, shard, names)

	var args MigrateArgs
	table := make(map[string]string)
	kv.state.rw.RLock()
	for key, value := range kv.state.Table {
		if key2shard(key) == shard {
			table[key] = value
		}
	}
	args = MigrateArgs{
		Table:      table,
		Duplicates: kv.state.Duplicates,
		Shard:      shard,
		GID:        kv.gid}
	kv.state.rw.RUnlock()

	var wg sync.WaitGroup
	for _, server := range names {
		wg.Add(1)
		go func(server string) {
			defer wg.Done()
			for {
				reply := MigrateReply{}
				srv := kv.make_end(server)
				ok := srv.Call("ShardKV.Migrate", &args, &reply)
				if ok {
					return
				}
				time.Sleep(100 * time.Millisecond)
			}
		}(server)
	}
	wg.Wait()
}
