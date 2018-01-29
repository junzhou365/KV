package shardmaster

import "raft"
import "labrpc"
import "sync"
import "encoding/gob"
import "time"
import "fmt"

type ShardMaster struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.
	configNum    int
	reqs         chan *Request
	liveRequests map[int]*Request
	interval     time.Duration

	duplicates map[int]Op
	configs    []Config // indexed by config num
}

func (sm *ShardMaster) getDup(id int) (Op, bool) {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	op, ok := sm.duplicates[id]
	return op, ok
}

func (sm *ShardMaster) setDup(id int, op Op) {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	sm.duplicates[id] = op
}

// get a copy of last config
func (sm *ShardMaster) getLastConfigCopyWOLOCK() Config {
	if len(sm.configs) == 1 {
		panic("No valid config available")
	}

	lastConfig := sm.configs[len(sm.configs)-1]
	copy := Config{Num: lastConfig.Num}
	for i := 0; i < NShards; i++ {
		copy.Shards[i] = lastConfig.Shards[i]
	}
	copy.Groups = make(map[int][]string)
	for k, v := range lastConfig.Groups {
		copy.Groups[k] = v
	}

	return copy
}

type Op struct {
	// Your data here.
	Type     string
	Seq      uint // for de-duplicate reqs
	ClientId int

	Servers map[int][]string
	GIDs    []int
	Shard   int
	Num     int
}

type Request struct {
	resCh chan interface{}
	op    *Op
	index int
	term  int
}

func (sm *ShardMaster) getRequest(i int) (*Request, bool) {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	req, ok := sm.liveRequests[i]
	return req, ok
}

func (sm *ShardMaster) putRequest(i int, req *Request) {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	sm.liveRequests[i] = req
}

func (sm *ShardMaster) delRequest(i int) {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	req, ok := sm.liveRequests[i]
	if !ok {
		panic("req doesn't exist")
	}
	close(req.resCh)
	delete(sm.liveRequests, i)
}

func (sm *ShardMaster) run() {

	clearRequests := func() {
		sm.mu.Lock()
		defer sm.mu.Unlock()
		for _, req := range sm.liveRequests {
			close(req.resCh)
		}

		sm.liveRequests = make(map[int]*Request)
	}

	processMsg := func(msg raft.ApplyMsg) {
		req, ok := sm.getRequest(msg.Index)

		if !ok {
			// No req for this msg
			return
		}

		switch {
		case msg.Term == req.term:
			req.resCh <- msg.Command
			sm.delRequest(req.index)

		case msg.Term > req.term:
			sm.delRequest(req.index)

		default:
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
		case msg := <-sm.applyCh:
			if msg.Snapshot != nil {
				panic("Should not use snapshot!")
			}

			DTPrintf("%d: new msg %d\n", sm.me, msg.Index)

			newOp := sm.changeState(msg.Command.(Op))
			msg.Command = newOp

			processMsg(msg)

		case <-time.After(sm.interval):
			if _, isLeader := sm.rf.GetState(); !isLeader {
				clearRequests()
			}
		}
	}
}
func distributeShards(groups map[int][]string) [NShards]int {
	gids := []int{}
	for k, _ := range groups {
		gids = append(gids, k)
	}
	shards := [NShards]int{}
	for i := 0; i < NShards; i++ {
		// Simply distribute gids using mod. May not be evenly
		// distributed.
		shards[i] = gids[i%len(gids)]
	}

	return shards
}

func (sm *ShardMaster) changeState(op Op) Op {

	// Duplicate Op
	if resOp, ok := sm.getDup(op.ClientId); ok && resOp.Seq == op.Seq {
		return resOp
	}

	sm.mu.Lock()
	newConfig := Config{
		Num:    len(sm.configs),
		Groups: make(map[int][]string)}

	switch op.Type {
	case "Join":
		// deep copy of map
		for gid, servers := range op.Servers {
			newConfig.Groups[gid] = servers
		}

		newConfig.Shards = distributeShards(newConfig.Groups)

	case "Leave":
		// update groups
		oldConfig := sm.getLastConfigCopyWOLOCK()
	Remove_LOOP:
		for gid, servers := range oldConfig.Groups {
			for _, toRemoveGid := range op.GIDs {
				if gid == toRemoveGid {
					continue Remove_LOOP
				}
			}
			newConfig.Groups[gid] = servers
		}
		newConfig.Shards = distributeShards(newConfig.Groups)

	case "Move":
		// shallow the original newConfig
		newConfig = sm.getLastConfigCopyWOLOCK()
		newConfig.Shards[op.Shard] = op.GIDs[0]
	}

	sm.configs = append(sm.configs, newConfig)
	DTPrintf("%d: The new config is %v\n", sm.me, newConfig)
	sm.mu.Unlock()

	sm.setDup(op.ClientId, op)
	return op
}

// invoke the agreement on operation.
// return applied Op and WrongLeader
func (sm *ShardMaster) commitOperation(op Op) interface{} {
	index, term, isLeader := sm.rf.Start(op)
	if !isLeader {
		return true
	}

	req := &Request{
		resCh: make(chan interface{}),
		op:    &op,
		index: index,
		term:  term}

	if _, ok := sm.getRequest(index); ok {
		//DTPrintf("%d: outdated req %+v was not cleared\n", sm.me, oldReq)
		sm.delRequest(index)
	}

	sm.putRequest(index, req)
	config := <-req.resCh

	//DTPrintf("%d: config from commitOperation is %+v\n", sm.me, config)
	if config == nil {
		return Err("Leader role lost")
	}
	return config
}

func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	op := Op{
		Type:     "Join",
		Seq:      args.State.Seq,
		ClientId: args.State.Id,
		Servers:  args.Servers}

	ret := sm.commitOperation(op)
	switch ret.(type) {
	case bool:
		reply.WrongLeader = true
	case Err:
		reply.Err = ret.(Err)
	default:
		panic(fmt.Sprintf("Wrong ret type %v", ret))
	}
}

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	op := Op{
		Type:     "Leave",
		Seq:      args.State.Seq,
		ClientId: args.State.Id,
		GIDs:     args.GIDs}

	ret := sm.commitOperation(op)
	switch ret.(type) {
	case bool:
		reply.WrongLeader = true
	case Err:
		reply.Err = ret.(Err)
	default:
		panic(fmt.Sprintf("Wrong ret type %v", ret))
	}
}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	op := Op{
		Type:     "Move",
		Seq:      args.State.Seq,
		ClientId: args.State.Id,
		Shard:    args.Shard,
		GIDs:     []int{args.GID}}

	ret := sm.commitOperation(op)
	switch ret.(type) {
	case bool:
		reply.WrongLeader = true
	case Err:
		reply.Err = ret.(Err)
	default:
		panic(fmt.Sprintf("Wrong ret type %v", ret))
	}
}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
}

//
// the tester calls Kill() when a ShardMaster instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (sm *ShardMaster) Kill() {
	sm.rf.Kill()
	// Your code here, if desired.
}

// needed by shardkv tester
func (sm *ShardMaster) Raft() *raft.Raft {
	return sm.rf
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant shardmaster service.
// me is the index of the current server in servers[].
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardMaster {
	sm := new(ShardMaster)
	sm.me = me

	sm.configs = make([]Config, 1)
	sm.configs[0].Groups = map[int][]string{}

	gob.Register(Op{})
	sm.applyCh = make(chan raft.ApplyMsg)
	sm.rf = raft.Make(servers, me, persister, sm.applyCh)

	// Your code here.

	return sm
}
