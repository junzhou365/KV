package raftkv

import (
	"encoding/gob"
	"labrpc"
	"log"
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

type RequestValue struct {
	index int
	term  int
}

type Request struct {
	resCh chan interface{}
	valCh chan RequestValue
	op    *Op
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
	state       KVState
	queue       chan *Request
	reqs        chan *Request
	notifyCh    chan raft.ApplyMsg
	maxRequests int
	interval    time.Duration
}

func (kv *RaftKV) run() {
	go kv.serve()

	drainQueue := func() {
		for {
			select {
			case req := <-kv.queue:
				DTPrintf("%d: not leader, drain the req %+v\n", kv.me, req)
				<-req.valCh
				req.resCh <- nil
			default:
				return
			}
		}
	}

	msgStream := kv.msgStream()
	for {
		select {
		case msg := <-msgStream:

			DTPrintf("%d: new raw msg, its index %d\n", kv.me, msg.Index)
			kv.checkForTakingSnapshot(msg.Index)

			// If we were leader but lost, even if we've applied change. Just
			// retry. If we were follower but gained leadership, there'd be no
			// reqs. Use the highestSentTerm to avoid this situation
			if term, isLeader := kv.rf.GetState(); !isLeader || term > kv.state.getHighestSentTerm() {
				drainQueue()
				DTPrintf("%d: drained the queue for index %d because we are no longer leader\n",
					kv.me, msg.Index)
				continue
			}

			select {
			// msg must see the req that caused the msg
			case req := <-kv.queue:
				DTPrintf("%d: get new req %+v\n", kv.me, req)
				reqVal := <-req.valCh
				DTPrintf("%d: new req index %d, msg index is %d\n", kv.me, reqVal.index, msg.Index)

				switch term, _ := kv.rf.GetState(); {
				// Leader role was lost
				case term != reqVal.term || reqVal.index > msg.Index:
					DTPrintf("%d: drain the req.Index %d\n", kv.me, reqVal.index)
					req.resCh <- nil

				case reqVal.index == msg.Index:
					req.resCh <- msg.Command

				default:
					// unsynced req
					DTPrintf("%d: req.Index %d < msg.index %d\n", kv.me, reqVal.index, msg.Index)
					log.Fatal("req.Index < index")
				}
			}

		case <-time.After(kv.interval):
			if _, isLeader := kv.rf.GetState(); !isLeader {
				drainQueue()
			}
		}
	}
}

// long-time running loop for server applying commands received from Raft
func (kv *RaftKV) msgStream() <-chan raft.ApplyMsg {
	msgStream := make(chan raft.ApplyMsg)

	go func() {
		defer close(msgStream)
		for msg := range kv.applyCh {

			if index, _ := kv.rf.GetLastIndexAndTerm(); msg.Index > 0 && msg.Index <= index {
				DTPrintf("%d: skip snapshotted msg %d\n", kv.me, msg.Index)
				continue
			}

			if msg.Snapshot != nil {
				kv.saveOrRestoreSnapshot(msg.Snapshot, msg.UseSnapshot)
				close(msg.SavedCh)
				continue
			}

			op := msg.Command.(Op)
			DTPrintf("%d: new msg. msg.Index: %d\n", kv.me, msg.Index)

			// Duplicate Op
			if resOp, ok := kv.state.getDup(op.ClientId); ok && resOp.Seq == op.Seq {
				msg.Command = resOp
				//DTPrintf("%d: duplicate op: %+v\n", kv.me, resOp)
				msgStream <- msg
				continue
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
			msg.Command = op
			//DTPrintf("%d: new op: %+v, updated msg is %+v\n", kv.me, op, msg)
			msgStream <- msg
		}
	}()

	return msgStream
}

func (kv *RaftKV) serve() {
	for req := range kv.reqs {
		index, term, isLeader := kv.rf.Start(*req.op)
		if !isLeader {
			req.resCh <- true
			DTPrintf("%d: not leader for op %+v. return from serve()\n", kv.me, *req.op)
			continue
		}

		kv.state.setHighestSentTerm(term)

		DTPrintf("%d: Server handles op: %+v. The req index is %d\n",
			kv.me, *req.op, index)
		// we must serialize this feeding req because otherwise reqs are in
		// wrong order
		kv.queue <- req
		DTPrintf("%d: put new req %+v\n", kv.me, req)

		// provide the value in the future
		go func(req *Request) {
			val := RequestValue{index: index, term: term}
			req.valCh <- val
			DTPrintf("%d: val %+v is put to req %+v\n", kv.me, val, req)
		}(req)

	}
}

// invoke the agreement on operation.
// return applied Op and WrongLeader
func (kv *RaftKV) commitOperation(op Op) interface{} {
	// The Start() and putting req to the queue must be done atomically
	req := &Request{
		resCh: make(chan interface{}),
		valCh: make(chan RequestValue),
		op:    &op}

	DTPrintf("%d: new FUCK req %+v is created\n", kv.me, req)
	kv.reqs <- req

	cmd := <-req.resCh
	DTPrintf("%d: cmd from commitOperation is %+v\n", kv.me, cmd)
	if cmd == nil {
		return Err("Leader role lost")
	}
	return cmd
}

func (kv *RaftKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	DTPrintf("%d: Server [Get], args: %+v\n", kv.me, args)

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

	DTPrintf("%d: Server [Get], for key %s, reply: %+v\n", kv.me, args.Key, reply)
}

func (kv *RaftKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	DTPrintf("%d: Server [PutAppend], args: %+v\n", kv.me, args)

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

	DTPrintf("%d: Server [PutAppend], reply: %+v\n", kv.me, reply)
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
	kv.maxRequests = 1000
	kv.interval = 10 * time.Millisecond

	// You may need initialization code here.
	kv.state = KVState{
		Table:      make(map[string]string),
		Duplicates: make(map[int]Op)}

	kv.persister = persister

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.notifyCh = make(chan raft.ApplyMsg)
	kv.queue = make(chan *Request, kv.maxRequests)
	kv.reqs = make(chan *Request)

	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.saveOrRestoreSnapshot(kv.readSnapshot(), true)

	// You may need initialization code here.
	go kv.run()

	DTPrintf("%d: is created\n", kv.me)

	return kv
}

func (kv *RaftKV) checkForTakingSnapshot(lastIndex int) {
	defer DTPrintf("%d: check for taking snapshot done for index: %d\n", kv.me, lastIndex)
	kv.state.rw.RLock()
	defer kv.state.rw.RUnlock()

	// RaftStateSize should have a consistent view
	rw := kv.rf.GetPersisterLock()
	rw.Lock()
	defer rw.Unlock()

	DTPrintf("%d: check for taking snapshot for index: %d\n", kv.me, lastIndex)

	if kv.maxraftstate-kv.persister.RaftStateSize() <= kv.stateDelta {
		if !kv.rf.IndexValidWithNoLock(lastIndex) {
			if _, isLeader := kv.rf.GetStateWithNoLock(); isLeader {
				log.Fatal("leader lost snapshots")
			}
			DTPrintf("%d: new snapshot was given. Just return\n", kv.me)
			return
		}

		DTPrintf("%d: take snapshot\n", kv.me)
		lastTerm := kv.rf.GetLogEntryTermWithNoLock(lastIndex)
		// we must first save snapshot
		kv.takeSnapshotWithNoLock(lastIndex, lastTerm)
		kv.rf.DiscardLogEnriesWithNoLock(lastIndex)
	}
}
