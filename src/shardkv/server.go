package shardkv

import "shardmaster"
import "labrpc"
import "raft"
import "sync"
import "encoding/gob"
import "time"

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Type     string
	Seq      uint // for de-duplicate reqs
	ClientId int
	Key      string
	Value    string
	GID      int
	Names    []string
	Shard    int
	PrevGID  int
}

type Request struct {
	resCh chan interface{}
	op    *Op
	index int
	term  int
}

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	masters      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	mck           *shardmaster.Clerk
	lastestConfig shardmaster.Config
	shards        [shardmaster.NShards]int // the shards state ShardKV has
	rpcCh         chan *MigrationReq

	persister  *raft.Persister
	stateDelta int

	state        KVState
	liveRequests map[int]*Request
	interval     time.Duration
}

func (kv *ShardKV) getRequest(i int) (*Request, bool) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	req, ok := kv.liveRequests[i]
	return req, ok
}

func (kv *ShardKV) putRequest(i int, req *Request) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	kv.liveRequests[i] = req
}

func (kv *ShardKV) delRequest(i int) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	req, ok := kv.liveRequests[i]
	if !ok {
		panic("req doesn't exist")
	}
	close(req.resCh)
	delete(kv.liveRequests, i)
}

func (kv *ShardKV) getConfig() shardmaster.Config {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	return kv.lastestConfig
}

func (kv *ShardKV) setConfig(config shardmaster.Config) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	kv.lastestConfig = config
}

func (kv *ShardKV) setShard(shard int, gid int) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	kv.shards[shard] = gid
}

func (kv *ShardKV) getShard(shard int) (gid int) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	gid = kv.shards[shard]
	return gid
}

func (kv *ShardKV) isShardOwned(shard int) bool {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	return kv.gid == kv.shards[shard]
}

func (kv *ShardKV) shardFetchLoop() {
	for {
		select {
		case <-time.After(50 * time.Millisecond):
			config := kv.mck.Query(-1)
			prevConfig := kv.getConfig()
			// The requests with affected key range should be rejected immediately
			kv.setConfig(config)
			// kv.shards is updated after servers agree on re-configing
			for shard, newGID := range config.Shards {
				if newGID == kv.gid && !kv.isShardOwned(shard) {
					kv.rf.Start(Op{
						Type:    "JOIN",
						Shard:   shard,
						PrevGID: prevConfig.Shards[shard]})
				}

				if newGID != kv.gid && kv.isShardOwned(shard) {
					kv.rf.Start(Op{
						Type:  "LEAVE",
						Shard: shard,
						GID:   newGID,
						Names: config.Groups[newGID]})
				}
			}
		}
	}
}

func (kv *ShardKV) run() {

	clearRequests := func() {
		kv.mu.Lock()
		defer kv.mu.Unlock()
		for _, req := range kv.liveRequests {
			close(req.resCh)
		}

		kv.liveRequests = make(map[int]*Request)
	}

	processMsg := func(msg raft.ApplyMsg) {
		req, ok := kv.getRequest(msg.Index)

		if !ok {
			// No req for this msg
			return
		}

		switch {
		case msg.Term == req.term:
			req.resCh <- msg.Command
			kv.delRequest(req.index)

		case msg.Term > req.term:
			kv.delRequest(req.index)
		}
	}

	// get a msg
	// 1. found in requests:
	//    a. msg.Term == req.Term. Succeed
	//    b. msg.Term < req.Term. msg is lagged
	//    c. msg.Term > req.Term. req was old and not cleared. Clear it. On the
	//       commitOperation side, req might also lag.
	// 2. not found: pass
	//
	// waiting for a msg
	// 1. msg might lose. Polling the leadership state, 20 ms
	//    a. keep waiting if we are leader
	//    b. clear all requests if we lost leadershp
LOOP:
	for {
		select {
		case msg := <-kv.applyCh:
			if index := kv.state.getLastIncludedIndex(); msg.Index > 0 && msg.Index <= index {
				DTPrintf("%d: skip snapshotted msg %d\n", kv.me, msg.Index)
				continue LOOP
			}

			if msg.Snapshot != nil {
				kv.saveOrRestoreSnapshot(msg.Snapshot, msg.UseSnapshot)
				close(msg.SavedCh)
				continue LOOP
			}

			DTPrintf("%d-%d: new msg %d, command %+v\n", kv.gid, kv.me, msg.Index, msg.Command)
			op := msg.Command.(Op)
			switch op.Type {
			case "LEAVE":
				kv.sendMigration(op.Shard, op.Names)
				kv.setShard(op.Shard, op.GID)
				continue LOOP

			case "JOIN":
				kv.receiveMigration(op, msg.Index, msg.Term)
				kv.setShard(op.Shard, kv.gid)
				continue LOOP
			}

			if !kv.isShardOwned(key2shard(op.Key)) {
				msg.Command = Err(ErrWrongGroup)
			} else {
				newOp := kv.changeState(op)
				msg.Command = newOp
			}

			processMsg(msg)

			kv.checkForTakingSnapshot(msg)

		case <-time.After(kv.interval):
			if _, isLeader := kv.rf.GetState(); !isLeader {
				clearRequests()
			}
		}
	}
}

func (kv *ShardKV) changeState(op Op) Op {
	// Duplicate Op
	if resOp, ok := kv.state.getDup(op.ClientId); ok && resOp.Seq == op.Seq {
		return resOp
	}

	switch op.Type {
	case "GET":
		// nonexistent value is ok
		value, _ := kv.state.getValue(op.Key)
		op.Value = value

	case "PUT":
		kv.state.setValue(op.Key, op.Value)

	case "APPEND":
		kv.state.appendValue(op.Key, op.Value)
	}

	kv.state.setDup(op.ClientId, op)
	return op
}

// invoke the agreement on operation.
// return applied Op and WrongLeader
func (kv *ShardKV) commitOperation(op Op) interface{} {
	if config := kv.getConfig(); config.Shards[key2shard(op.Key)] != kv.gid {
		return Err(ErrWrongGroup)
	}

	index, term, isLeader := kv.rf.Start(op)
	if !isLeader {
		return true
	}

	req := &Request{
		resCh: make(chan interface{}),
		op:    &op,
		index: index,
		term:  term}

	if _, ok := kv.getRequest(index); ok {
		kv.delRequest(index)
	}

	kv.putRequest(index, req)
	cmd := <-req.resCh

	if cmd == nil {
		return Err(ErrNotLeader)
	}
	return cmd
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	DTPrintf("%d-%d: Server [Get], args: %+v\n", kv.gid, kv.me, args)

	op := Op{
		Seq:      args.State.Seq,
		Type:     "GET",
		ClientId: args.State.Id,
		Key:      args.Key}

	ret := kv.commitOperation(op)

	switch ret.(type) {
	case bool:
		reply.WrongLeader = true
	case Err:
		reply.Err = ret.(Err)
	default:
		reply.Value = ret.(Op).Value
		reply.Err = OK
	}

	DTPrintf("%d-%d: Server [Get], for key %s, reply: %+v\n", kv.gid, kv.me, args.Key, reply)
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	DTPrintf("%d-%d: Server [PutAppend], args: %+v\n", kv.gid, kv.me, args)
	opType := "PUT"
	if args.Op == "Append" {
		opType = "APPEND"
	}
	op := Op{
		Seq:      args.State.Seq,
		Type:     opType,
		ClientId: args.State.Id,
		Key:      args.Key,
		Value:    args.Value}

	ret := kv.commitOperation(op)
	switch ret.(type) {
	case bool:
		reply.WrongLeader = true
	case Err:
		reply.Err = ret.(Err)
	default:
		reply.Err = OK
	}

	DTPrintf("%d-%d: Server [PutAppend], reply: %+v\n", kv.gid, kv.me, reply)
}

//
// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *ShardKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
}

//
// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots with
// persister.SaveSnapshot(), and Raft should save its state (including
// log) with persister.SaveRaftState().
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardmaster.
//
// pass masters[] to shardmaster.MakeClerk() so you can send
// RPCs to the shardmaster.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use masters[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, masters []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call gob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	gob.Register(Op{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.masters = masters

	// Your initialization code here.
	kv.stateDelta = 20
	kv.interval = 10 * time.Millisecond
	kv.state = KVState{
		Table:      make(map[string]string),
		Duplicates: make(map[int]Op)}
	kv.persister = persister
	kv.rpcCh = make(chan *MigrationReq)

	// Use something like this to talk to the shardmaster:
	kv.mck = shardmaster.MakeClerk(kv.masters)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	go kv.run()
	go kv.shardFetchLoop()
	DTPrintf("%d-%d: is created\n", kv.gid, kv.me)

	return kv
}
