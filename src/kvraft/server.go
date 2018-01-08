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

type Request struct {
	resCh chan interface{}
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
	state       KVState
	queue       chan *Request
	notifyCh    chan raft.ApplyMsg
	maxRequests int
	interval    time.Duration
}

func (kv *RaftKV) serve() {
	msgStream := kv.msgStream()
	for {
		select {
		case msg := <-msgStream:
			select {
			case req := <-kv.queue:
				index := msg.Index
				DTPrintf("%d: new req %+v, msg is %+v\n", kv.me, req, msg)

				// Leader role was lost
				switch term, _ := kv.rf.GetState(); {
				case term != req.term || req.index > index:
					DTPrintf("%d: drain the index %d\n", kv.me, req.index)
					req.resCh <- nil
				case req.index == index:
					req.resCh <- msg.Command
				default:
					// unsynced req
					DTPrintf("%d: req.index %d < msg.index %d\n", kv.me, req.index, index)
					log.Fatal("req.index < index")
				}
			default:
				DTPrintf("%d: drain the msg %d\n", kv.me, msg.Index)
			}
		case <-time.After(kv.interval):
			if _, isLeader := kv.rf.GetState(); !isLeader {
			LOOP:
				for {
					select {
					case req := <-kv.queue:
						DTPrintf("%d: not leader, drain the index %d\n", kv.me, req.index)
						req.resCh <- nil
					default:
						break LOOP
					}
				}
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
				continue
			}

			switch {
			case msg.Snapshot != nil:
				kv.persister.SaveSnapshot(msg.Snapshot)
				close(msg.SavedCh)
				continue

			case msg.UseSnapshot:
				kv.restoreSnapshot()
				close(msg.SavedCh)
				continue
			}

			op := msg.Command.(Op)
			DTPrintf("%d: new msg. msg.Index: %d\n", kv.me, msg.Index)

			// Duplicate Op
			if resOp, ok := kv.state.getDup(op.ClientId); ok && resOp.Seq == op.Seq {
				msg.Command = resOp
				kv.rf.CheckForTakingSnapshot(
					msg.Index, kv.maxraftstate, kv.stateDelta, kv.takeSnapshot)
				DTPrintf("%d: duplicate op: %+v\n", kv.me, resOp)
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
			kv.rf.CheckForTakingSnapshot(
				msg.Index, kv.maxraftstate, kv.stateDelta, kv.takeSnapshot)
			msg.Command = op
			DTPrintf("%d: new op: %+v, updated msg is %+v\n", kv.me, op, msg)
			msgStream <- msg
		}
	}()

	return msgStream
}

// invoke the agreement on operation.
// return applied Op and WrongLeader
func (kv *RaftKV) commitOperation(op Op) interface{} {
	// The Start() and putting req to the queue must be done atomically
	kv.mu.Lock()
	index, term, isLeader := kv.rf.Start(op)

	if !isLeader {
		kv.mu.Unlock()
		return true
	}

	DTPrintf("%d: Server handles op: %+v. The req index is %d\n",
		kv.me, op, index)
	req := &Request{resCh: make(chan interface{}), index: index, term: term}
	kv.queue <- req
	kv.mu.Unlock()

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

	// You may need initialization code here.
	kv.state = KVState{
		Table:      make(map[string]string),
		Duplicates: make(map[int]Op)}

	kv.persister = persister
	kv.restoreSnapshot()

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.stateDelta = 20
	kv.notifyCh = make(chan raft.ApplyMsg)
	kv.maxRequests = 1000
	kv.interval = 10 * time.Millisecond
	kv.queue = make(chan *Request, kv.maxRequests)

	// You may need initialization code here.
	go kv.serve()

	DTPrintf("%d: is created\n", kv.me)

	return kv
}
