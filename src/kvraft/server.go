package raftkv

import (
	"bytes"
	"encoding/gob"
	"labrpc"
	"raft"
	"sync"
	"time"
)

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Seq      uint // for de-duplicate reqs
	Type     int
	Key      string
	Value    string
	ClientId int
}

type Request struct {
	resCh chan interface{}
	op    *Op
	index int
	term  int
}

type RaftKV struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	maxraftstate int // snapshot if log grows this big
	persister    *raft.Persister
	stateDelta   int

	// Your definitions here.
	state        KVState
	reqs         chan *Request
	liveRequests map[int]*Request
	interval     time.Duration
}

func (kv *RaftKV) getRequest(i int) (*Request, bool) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	req, ok := kv.liveRequests[i]
	return req, ok
}

func (kv *RaftKV) putRequest(i int, req *Request) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	kv.liveRequests[i] = req
}

func (kv *RaftKV) delRequest(i int) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	req, ok := kv.liveRequests[i]
	if !ok {
		panic("req doesn't exist")
	}
	close(req.resCh)
	delete(kv.liveRequests, i)
}

func (kv *RaftKV) run() {

	clearRequests := func() {
		kv.mu.Lock()
		defer kv.mu.Unlock()
		for _, req := range kv.liveRequests {
			//DTPrintf("%d: drain the req %+v\n", kv.me, req)
			close(req.resCh)
		}

		kv.liveRequests = make(map[int]*Request)
	}

	processMsg := func(msg raft.ApplyMsg) {
		req, ok := kv.getRequest(msg.Index)

		if !ok {
			// No req for this msg
			//DTPrintf("%d: no req for the msg %d\n", kv.me, msg.Index)
			return
		}

		switch {
		case msg.Term == req.term:
			req.resCh <- msg.Command
			kv.delRequest(req.index)
			//DTPrintf("%d: req %+v succeed\n", kv.me, req)

		case msg.Term > req.term:
			kv.delRequest(req.index)
			//DTPrintf("%d: req %+v was lagged\n", kv.me, req)

		default:
			//DTPrintf("%d: msg %d is lagged for req %+v\n", kv.me, msg.Index, req)
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
	for {
		select {
		case msg := <-kv.applyCh:
			if index := kv.state.getLastIncludedIndex(); msg.Index > 0 && msg.Index <= index {
				DTPrintf("%d: skip snapshotted msg %d\n", kv.me, msg.Index)
				continue
			}

			if msg.Snapshot != nil {
				kv.saveOrRestoreSnapshot(msg.Snapshot, msg.UseSnapshot)
				close(msg.SavedCh)
				continue
			}

			DTPrintf("%d: new msg %d\n", kv.me, msg.Index)

			newOp := kv.changeState(msg.Command.(Op))
			msg.Command = newOp

			processMsg(msg)

			kv.checkForTakingSnapshot(msg)

		case <-time.After(kv.interval):
			if _, isLeader := kv.rf.GetState(); !isLeader {
				clearRequests()
			}
		}
	}
}

func (kv *RaftKV) changeState(op Op) Op {
	// Duplicate Op
	if resOp, ok := kv.state.getDup(op.ClientId); ok && resOp.Seq == op.Seq {
		return resOp
		//DTPrintf("%d: duplicate op: %+v\n", kv.me, resOp)
	}

	switch op.Type {
	case OP_GET:
		// nonexistent value is ok
		value, _ := kv.state.getValue(op.Key)
		op.Value = value
	case OP_PUT:
		kv.state.setValue(op.Key, op.Value)
	case OP_APPEND:
		kv.state.appendValue(op.Key, op.Value)
	}

	kv.state.setDup(op.ClientId, op)
	return op
}

// invoke the agreement on operation.
// return applied Op and WrongLeader
func (kv *RaftKV) commitOperation(op Op) interface{} {
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
		//DTPrintf("%d: outdated req %+v was not cleared\n", kv.me, oldReq)
		kv.delRequest(index)
	}

	kv.putRequest(index, req)
	cmd := <-req.resCh

	//DTPrintf("%d: cmd from commitOperation is %+v\n", kv.me, cmd)
	if cmd == nil {
		return Err("Leader role lost")
	}
	return cmd
}

func (kv *RaftKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	//DTPrintf("%d: Server [Get], args: %+v\n", kv.me, args)

	op := Op{
		Seq:      args.State.Seq,
		Type:     OP_GET,
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
	}

	//DTPrintf("%d: Server [Get], for key %s, reply: %+v\n", kv.me, args.Key, reply)
}

func (kv *RaftKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	//DTPrintf("%d: Server [PutAppend], args: %+v\n", kv.me, args)

	opType := OP_PUT
	if args.Op == "Append" {
		opType = OP_APPEND
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
	}

	//DTPrintf("%d: Server [PutAppend], reply: %+v\n", kv.me, reply)
}

//
// the tester calls Kill() when a RaftKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *RaftKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots with persister.SaveSnapshot(),
// and Raft should save its state (including log) with persister.SaveRaftState().
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *RaftKV {
	// call gob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	gob.Register(Op{})

	kv := new(RaftKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.stateDelta = 20
	kv.interval = 10 * time.Millisecond

	// You may need initialization code here.
	kv.state = KVState{
		Table:      make(map[string]string),
		Duplicates: make(map[int]Op)}

	kv.persister = persister

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.reqs = make(chan *Request)

	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.saveOrRestoreSnapshot(nil, true)

	// You may need initialization code here.
	go kv.run()

	DTPrintf("%d: is created\n", kv.me)

	return kv
}

func (kv *RaftKV) checkForTakingSnapshot(msg raft.ApplyMsg) {
	if kv.maxraftstate == -1 {
		return
	}
	//defer DTPrintf("%d: check for taking snapshot done for msg: %+v\n", kv.me, msg)

	//DTPrintf("%d: check for taking snapshot for msg: %+v\n", kv.me, msg)

	takeSnapshot := func(lastIndex int, lastTerm int) {
		//DTPrintf("%d: taking snapshot at %d\n", kv.me, lastIndex)
		//defer DTPrintf("%d: taking snapshot done at %d\n", kv.me, lastIndex)

		w := new(bytes.Buffer)
		e := gob.NewEncoder(w)

		kv.state.rw.Lock()
		kv.state.lastIncludedIndex = lastIndex
		kv.state.lastIncludedTerm = lastTerm

		e.Encode(kv.state.lastIncludedIndex)
		e.Encode(kv.state.lastIncludedTerm)
		e.Encode(kv.state.Table)
		e.Encode(kv.state.Duplicates)
		kv.state.rw.Unlock()

		snapshot := w.Bytes()
		// Protected by Serialize
		kv.persister.SaveSnapshot(snapshot)
	}

	// The raft size might be reduced by raft taking snapshots. But it's ok.
	if kv.maxraftstate-kv.persister.RaftStateSize() <= kv.stateDelta {
		if msg.Index <= kv.state.getLastIncludedIndex() {
			DTPrintf("%d: new snapshot was given for %+v. Just return\n", kv.me, msg)
			return
			//if _, isLeader := kv.rf.GetState(); !isLeader {
			//DTPrintf("%d: new snapshot was given for %d. Just return\n", kv.me, lastIndex)
			//return
			//} else {
			//DTPrintf("%d: snapshot was lost for %d. Fatal\n", kv.me, lastIndex)
			//log.Fatal("snapshot lost")
			//}
		}

		DTPrintf("%d: take snapshot\n", kv.me)
		// we must first save snapshot
		takeSnapshot(msg.Index, msg.Term)
		go kv.rf.DiscardLogEnries(msg.Index, msg.Term)
	}
}
