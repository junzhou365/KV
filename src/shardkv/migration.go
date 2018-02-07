package shardkv

import (
	"fmt"
	"time"
)

func (kv *ShardKV) Migrate(args *MigrateArgs, reply *MigrateReply) {
	kv.KVLeaderPrintf("Server [Migrate] from %d, args: %+v", args.GID, args)
	//defer DTPrintf("%d-%d: Server [Migrate] from %d, reply: %+v\n", kv.gid, kv.me, args.GID, reply)

	ret := kv.commitOperation(Op{
		Type:    "JOIN",
		Shard:   args.Shard,
		PrevGID: args.GID,

		Table:      args.Table,
		Duplicates: args.Duplicates})

	switch ret.(type) {
	case bool:
		reply.WrongLeader = true
	case Err:
		reply.Err = ret.(Err)
	default:
		reply.Err = Err(OK)
	}
}

func (kv *ShardKV) receiveMigration(op Op, index int, term int) {
	// The second condition could happend when none of the servers agreed.
	if op.PrevGID == 0 || op.PrevGID == kv.gid {
		return
	}

	//kv.DTPrintf("%d-%d: receive Migration, op: %+v\n", kv.gid, kv.me, op)
	//defer DTPrintf("%d-%d: done Migration, op: %+v\n", kv.gid, kv.me, op)

	kv.state.rw.Lock()
	//DTPrintf("%d-%d: receive Migration, table: %v, duplicates: %v\n",
	//kv.gid, kv.me, *op.Table, *op.Duplicates)
	for k, v := range op.Table {
		if _, ok := kv.state.Table[k]; ok {
			panic(fmt.Sprintf("%d-%d: Key %v exists", kv.gid, kv.me, k))
		}
		kv.state.Table[k] = v
	}

	for id, op := range op.Duplicates {
		if origOp, ok := kv.state.Duplicates[id]; !ok {
			kv.state.Duplicates[id] = op
		} else {
			// client seq increases monotonically.
			if op.Seq > origOp.Seq {
				kv.state.Duplicates[id] = op
			}
		}
	}
	kv.KVPrintf("MigrateDone, table: %v, dup: %v, shards: %v",
		kv.state.Table, kv.state.Duplicates, kv.state.Shards)
	kv.state.rw.Unlock()

	// must take snapshot here because we don't have the log for these kv states
	//if kv.maxraftstate != -1 {
	//kv.takeSnapshot(index, term)
	//go kv.rf.DiscardLogEnries(index, term)
	//kv.KVPrintf("after snapshot, table: %v, dup: %v, shards: %v",
	//kv.state.Table, kv.state.Duplicates, kv.state.Shards)
	//}
}

// used for servers that lost shard
func (kv *ShardKV) updateAndSendMigration(shard int, names []string) {
	table := make(map[string]string)
	duplicates := make(map[int]Op)

	kv.state.rw.Lock()
	for key, value := range kv.state.Table {
		if key2shard(key) == shard {
			table[key] = value
		}
	}

	for k := range table {
		delete(kv.state.Table, k)
	}

	kv.KVPrintf("[LEAVE] for shard %v update table: %v, arg table: %v",
		shard, kv.state.Table, table)

	for k, v := range kv.state.Duplicates {
		duplicates[k] = v
	}

	kv.state.rw.Unlock()

	if _, isLeader := kv.rf.GetState(); !isLeader {
		return
	}

	//DTPrintf("%d-%d: [LEAVE] send Migration for shard %v to names: %v\n", kv.gid, kv.me, shard, names)
	//defer DTPrintf("%d-%d: [LEAVE] done Migration for shard %v to names: %v\n", kv.gid, kv.me, shard, names)

	var args MigrateArgs
	args = MigrateArgs{
		Table:      table,
		Duplicates: duplicates,
		Shard:      shard,
		GID:        kv.gid,
		Index:      kv.me}

	resCh := make(chan bool)
	done := make(chan interface{})
	defer close(done)

	for _, server := range names {
		go func(server string) {
			for {
				reply := MigrateReply{}
				srv := kv.make_end(server)
				ok := srv.Call("ShardKV.Migrate", &args, &reply)
				if ok && reply.Err == OK {
					resCh <- true
					return
				}

				select {
				case <-done:
					return
				default:
				}

				time.Sleep(100 * time.Millisecond)
			}
		}(server)
	}

	<-resCh
}
