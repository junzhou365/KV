package raftkv

import (
	"encoding/gob"
	"labrpc"
	"raft"
	"sync"
)

const (
	OP_GET = iota
	OP_PUT
	OP_APPEND
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

type RaftKV struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	state          KVState
	notifyLeaderCh chan Op
}

// long-time running loop for server applying commands received from Raft
func (kv *RaftKV) appliedLoop() {
	for {
		// wait for the op to be committed
		// XXX: might wait forever, or get nil
		msg := <-kv.applyCh
		op := msg.Command.(Op)

		if resOp, ok := kv.state.getDup(op.ClientId); ok && resOp.Seq == op.Seq {
			// Duplicate Op
			if _, isLeader := kv.rf.GetState(); isLeader {
				kv.notifyLeaderCh <- resOp
			}
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
		if _, isLeader := kv.rf.GetState(); isLeader {
			kv.notifyLeaderCh <- op
		}
	}
}

// invoke the agreement on operation.
// return applied Op and WrongLeader
func (kv *RaftKV) commitOperation(op Op) (Op, bool) {
	_, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		return Op{}, true
	}

	resOp := <-kv.notifyLeaderCh
	return resOp, false
}

func (kv *RaftKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	op := Op{
		Seq:      args.State.Seq,
		Type:     OP_GET,
		ClientId: args.State.Id,
		Key:      args.Key}

	resOp, wrongLeader := kv.commitOperation(op)
	if wrongLeader {
		reply.WrongLeader = true
	} else {
		reply.Value = resOp.Value
	}
}

func (kv *RaftKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	opType := OP_PUT
	if args.Op == "Append" {
		opType = OP_APPEND
	}

	op := Op{
		Seq:      args.State.Seq,
		Type:     opType,
		ClientId: args.State.Id,
		Key:      args.Key}

	_, wrongLeader := kv.commitOperation(op)
	if wrongLeader {
		reply.WrongLeader = true
	}
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
	kv.state = KVState{}

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.notifyLeaderCh = make(chan Op)
	go kv.appliedLoop()

	return kv
}
