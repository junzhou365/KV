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
	Config  Config
	// this is needed because if server just restarts and receives a request
	// with n = 1, then the server will return the lastest config, not the
	// config with n = 1 because replaying of previous configs.
	Num int
}

type Request struct {
	resCh chan interface{}
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

func distributeShards(config Config) [NShards]int {
	prevShards := config.Shards
	groups := config.Groups
	// create tickets for each gid
	// e.g. ngroups: 3
	// avg = 10 / 3 = 3.
	// first 10 % ngroups = 1 share the extra ticket
	ngroups := len(groups)
	avgTickes := NShards / ngroups
	extraTickets := NShards % ngroups

	usage := map[int]int{}
	updateUse := func(i int, gid int) bool {
		// new gid that hasn't been used
		use, ok := usage[gid]
		if !ok {
			usage[gid] = 1
			return true
		}

		if use < avgTickes || (use == avgTickes && extraTickets > 0) {
			if use == avgTickes {
				extraTickets--
			}
			usage[gid]++
			return true
		}

		return false
	}

	gids := []int{}
	for gid, _ := range config.Groups {
		gids = append(gids, gid)
	}
	nextGIDIndex := 0
	getNextGID := func() int {
		for nextGIDIndex < len(gids) {
			gid := gids[nextGIDIndex]
			use, ok := usage[gid]
			if !ok || use < avgTickes || (use == avgTickes && extraTickets > 0) {
				//fmt.Printf("new gid: %v\n", gids[nextGIDIndex])
				return gids[nextGIDIndex]
			}
			nextGIDIndex++
			//fmt.Printf("usage: %v\n", usage)
		}

		panic("not reached")
	}

	//fmt.Printf("prev shards: %v\n", prevShards)
	shards := [NShards]int{}
	// first pass. Update all remaining gids' usage
	for i := 0; i < NShards; i++ {
		gid := prevShards[i]
		if _, ok := config.Groups[gid]; ok {
			if updateUse(i, gid) {
				shards[i] = gid
			}
		}
	}

	//fmt.Printf("usage after first: %v\n", usage)
	//fmt.Printf("shards after first: %v\n", shards)
	//fmt.Printf("gids after first: %v\n", gids)
	//fmt.Printf("avg: %v, extra: %v\n", avgTickes, extraTickets)

	// second pass. Update all gids
	for i := 0; i < NShards; i++ {
		gid := shards[i]
		if gid != 0 {
			continue
		}

		if _, ok := config.Groups[gid]; !ok {
			gid = getNextGID()
		}

		for {
			if updateUse(i, gid) {
				shards[i] = gid
				break
			} else {
				gid = getNextGID()
			}
		}
		//fmt.Printf("shards in second: %v\n", shards)
	}

	//fmt.Printf("shards after second: %v\n", shards)

	return shards
}

func (sm *ShardMaster) changeState(op Op) Op {
	defer DTPrintf("%d: op %+v is applied to state\n", sm.me, op)

	// Duplicate Op
	if resOp, ok := sm.getDup(op.ClientId); ok && resOp.Seq == op.Seq {
		return resOp
	}

	sm.mu.Lock()

	switch op.Type {
	case "Join":
		newConfig := sm.getLastConfigCopyWOLOCK()
		newConfig.Num = len(sm.configs)
		for gid, servers := range op.Servers {
			if _, ok := newConfig.Groups[gid]; ok {
				//panic(fmt.Sprintf("%d: gid %v must be new", sm.me, gid))
			}
			newConfig.Groups[gid] = servers
		}

		newConfig.Shards = distributeShards(newConfig)
		sm.configs = append(sm.configs, newConfig)
		DTPrintf("%d: after join, configs: %v\n", sm.me, sm.configs)

	case "Leave":
		newConfig := sm.getLastConfigCopyWOLOCK()
		newConfig.Num = len(sm.configs)
		// update groups
		for _, gid := range op.GIDs {
			if _, ok := newConfig.Groups[gid]; ok {
				delete(newConfig.Groups, gid)
			}
		}

		newConfig.Shards = distributeShards(newConfig)
		sm.configs = append(sm.configs, newConfig)

	case "Move":
		newConfig := sm.getLastConfigCopyWOLOCK()
		newConfig.Num = len(sm.configs)
		newConfig.Shards[op.Shard] = op.GIDs[0]
		sm.configs = append(sm.configs, newConfig)

	case "Query":
		DTPrintf("%d: before query, op: %v\n", sm.me, op)
		if op.Num == -1 || op.Num >= len(sm.configs) {
			op.Config = sm.getLastConfigCopyWOLOCK()
		} else {
			op.Config = sm.configs[op.Num]
		}
		DTPrintf("%d: after query, configs: %v\n", sm.me, sm.configs)
		DTPrintf("%d: after query, config: %v\n", sm.me, op.Config)
	}

	sm.mu.Unlock()

	sm.setDup(op.ClientId, op)
	return op
}

// invoke the agreement on operation.
// return applied Op and WrongLeader
func (sm *ShardMaster) commitOperation(op Op) interface{} {
	req := &Request{resCh: make(chan interface{})}

	putReq := func(index int, term int) {
		req.index = index
		req.term = term
		if _, ok := sm.getRequest(req.index); ok {
			//DTPrintf("%d: outdated req %+v was not cleared\n", sm.me, oldReq)
			sm.delRequest(req.index)
		}

		sm.putRequest(req.index, req)
	}

	_, _, isLeader := sm.rf.StartWithFunc(op, putReq)
	if !isLeader {
		return true
	}

	config := <-req.resCh

	DTPrintf("%d: config from commitOperation is %+v\n", sm.me, config)
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
	}
}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	DTPrintf("%d: Server [Move], args: %+v\n", sm.me, args)
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
	}
}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	DTPrintf("%d: Server [Query], args: %+v\n", sm.me, args)
	sm.mu.Lock()
	DTPrintf("configs: %v\n", sm.configs)
	switch {
	case args.Num >= 0 && args.Num < len(sm.configs):
		reply.Config = sm.configs[args.Num]
		sm.mu.Unlock()
		return
	case args.Num < -1:
		sm.mu.Unlock()
		return
	}
	sm.mu.Unlock()

	op := Op{
		Type:     "Query",
		Seq:      args.State.Seq,
		ClientId: args.State.Id,
		Num:      args.Num}

	ret := sm.commitOperation(op)
	switch ret.(type) {
	case bool:
		reply.WrongLeader = true
	case Err:
		reply.Err = ret.(Err)
	case Op:
		reply.Config = ret.(Op).Config
		DTPrintf("%d: Query result: %v\n", sm.me, reply.Config)
	default:
		panic(fmt.Sprintf("Wrong ret type %v", ret))
	}
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

	sm.duplicates = make(map[int]Op)
	sm.liveRequests = make(map[int]*Request)

	gob.Register(Op{})
	sm.applyCh = make(chan raft.ApplyMsg)
	sm.rf = raft.Make(servers, me, persister, sm.applyCh)

	// Your code here.
	go sm.run()
	DTPrintf("shardmaster %d is created\n", sm.me)

	return sm
}
