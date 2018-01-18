package raftkv

import (
	"bytes"
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
	maxRequests int
	interval    time.Duration
}

type MsgOp struct {
	msg  raft.ApplyMsg
	done chan interface{}
}

func (kv *RaftKV) run() {
	go kv.serve()

	drainQueue := func() {
		for {
			select {
			case req := <-kv.queue:
				DTPrintf("%d: drain the req %+v\n", kv.me, req)
				<-req.valCh
				req.resCh <- nil
			default:
				return
			}
		}
	}

	msgStream := kv.msgStream()

	getNextReq := func(index int) (req *Request) {
		// msg must see the req that caused the msg
		term, ok := kv.rf.GetLogEntryTerm(index)
		if !ok {
		}

		for {
			req = <-kv.queue
			reqVal := <-req.valCh
			DTPrintf("%d: new req index %d, msg index is %d\n", kv.me, reqVal.index, index)
			switch {
			case reqVal.index > index:
				// Not the right msg. This msg was lagged
				DTPrintf("%d: lagged msg %d\n", kv.me, index)

			case reqVal.index == index && reqVal.term == term:
				return req

			default:
				// unsynced req
				DTPrintf("%d: req.Index %d < msg.index %d\n", kv.me, reqVal.index, index)
				log.Fatal("req.Index < index")
			}
		}
	}

	for {
		select {
		case msgOp := <-msgStream:

			msg := msgOp.msg

			DTPrintf("%d: new raw msg, its index %d\n", kv.me, msg.Index)

			// If we were leader but lost, even if we've applied change. Just
			// retry. If we were follower but just gained leadership, there
			// might be no reqs.
			switch term, isLeader := kv.rf.GetState(); {
			case !isLeader:

				DTPrintf("%d: try to drain the queue for index %d because we are no longer leader\n",
					kv.me, msg.Index)
				drainQueue()
				DTPrintf("%d: drained the queue for index %d because we are no longer leader\n",
					kv.me, msg.Index)

			// new leader, lagged msg
			case term > kv.rf.GetLogEntryTerm(msg.Index):
				// pass through

			default:
				req := getNextReq(msg.Index)
				req.resCh <- msg.Command
			}

			close(msgOp.done)
			DTPrintf("%d: raw msg %d is done\n", kv.me, msg.Index)

		case <-time.After(kv.interval):
			if _, isLeader := kv.rf.GetState(); !isLeader {
				drainQueue()
			}
		}
	}
}

// long-time running loop for server applying commands received from Raft
func (kv *RaftKV) msgStream() <-chan MsgOp {
	msgStream := make(chan MsgOp)

	go func() {
		defer close(msgStream)
		for msg := range kv.applyCh {

			if index, _ := kv.rf.GetLastIndexAndTerm(); msg.Index > 0 && msg.Index <= index {
				DTPrintf("%d: skip snapshotted msg %d\n", kv.me, msg.Index)
				continue
			}

			if msg.Snapshot != nil {
				kv.saveOrRestoreSnapshot(msg.Snapshot, msg.UseSnapshot)
				//close(msg.SavedCh)
				continue
			}

			DTPrintf("%d: new msg %+v\n", kv.me, msg)
			op := msg.Command.(Op)
			DTPrintf("%d: new msg. msg.Index: %d\n", kv.me, msg.Index)

			// Duplicate Op
			if resOp, ok := kv.state.getDup(op.ClientId); ok && resOp.Seq == op.Seq {
				msg.Command = resOp
				//DTPrintf("%d: duplicate op: %+v\n", kv.me, resOp)
			} else {

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
			}

			DTPrintf("%d: msg.Index: %d FUCK THIS SHIT\n", kv.me, msg.Index)

			kv.checkForTakingSnapshot(msg.Index)
			msgOp := MsgOp{msg: msg, done: make(chan interface{})}
			msgStream <- msgOp
			<-msgOp.done
			DTPrintf("%d: msgOp %d is done\n", kv.me, msg.Index)
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
	kv.queue = make(chan *Request, kv.maxRequests)
	kv.reqs = make(chan *Request)

	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.saveOrRestoreSnapshot(nil, true)

	// You may need initialization code here.
	go kv.run()

	DTPrintf("%d: is created\n", kv.me)

	return kv
}

func (kv *RaftKV) checkForTakingSnapshot(lastIndex int) {
	defer DTPrintf("%d: check for taking snapshot done for index: %d\n", kv.me, lastIndex)
	// Take kv lock first

	DTPrintf("%d: check for taking snapshot for index: %d\n", kv.me, lastIndex)

	takeSnapshot := func(lastIndex int, lastTerm int) {
		DTPrintf("%d: taking snapshot at %d\n", kv.me, lastIndex)
		defer DTPrintf("%d: taking snapshot done at %d\n", kv.me, lastIndex)

		w := new(bytes.Buffer)
		e := gob.NewEncoder(w)

		e.Encode(lastIndex)
		e.Encode(lastTerm)

		kv.state.rw.RLock()
		e.Encode(kv.state.Table)
		e.Encode(kv.state.Duplicates)
		kv.state.rw.RUnlock()

		snapshot := w.Bytes()
		// Protected by Serialize
		kv.persister.SaveSnapshot(snapshot)
	}

	// It's okay to get raftstatesize without lock because msg is serialized
	if kv.maxraftstate-kv.persister.RaftStateSize() <= kv.stateDelta {
		if !kv.rf.IndexValid(lastIndex) {
			DTPrintf("%d: new snapshot was given for %d. Just return\n", kv.me, lastIndex)
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
		lastTerm := kv.rf.GetLogEntryTerm(lastIndex)
		// we must first save snapshot
		takeSnapshot(lastIndex, lastTerm)
		kv.rf.DiscardLogEnries(lastIndex)
	}
}
