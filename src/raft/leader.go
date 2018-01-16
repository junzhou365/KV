package raft

import (
	"log"
	"time"
)

func (rf *Raft) runLeader() {
	rf.state.initializeMatchAndNext()
	term := rf.state.getCurrentTerm()

	heartbeatTimer := time.After(0)
	last_seen_index := 0

	respCh := make(chan rpcResp)

	// cannot use defer close(done) here because the "done" will be bound to a
	// future closed channel
	done := make(chan interface{})
	defer close(done)

	//rf.sendChs = make([]chan sendJob, len(rf.peers))
	//for i := 0; i < len(rf.peers); i++ {
	//rf.sendChs[i] = make(chan sendJob)
	//go rf.sendLoop(done, rf.sendChs[i])
	//}

	rf.appendReplyCh = make(chan bool)
	signalCh := make(chan int, 1000)

	go rf.sendLoop(done, signalCh, term)

	for {
		select {
		case rf.rpcCh <- respCh:
			if r := <-respCh; r.toFollower {
				DTPrintf("%d: exit leader loop\n", rf.me)
				return
			}

		case <-heartbeatTimer:
			heartbeatTimer = time.After(rf.heartbeatInterval)
			signalCh <- -1

		case index := <-rf.newEntry:
			if index > last_seen_index {
				signalCh <- index
				last_seen_index = index
			}

		case <-rf.appendReplyCh:
			DTPrintf("%d: exit leader loop\n", rf.me)
			return
		}
	}
}

func (rf *Raft) sendLoop(done <-chan interface{}, signalCh <-chan int, term int) {
	for {
		select {
		case <-done:
			return
		case index := <-signalCh:
			if index == -1 {
				rf.appendNewEntries(done, true, term, -1)
			} else {
				rf.appendNewEntries(done, false, term, index)
			}
		}
	}
}

func (rf *Raft) appendNewEntries(
	done <-chan interface{}, heartbeat bool, term int, newIndex int) {

	// save before a change
	rf.state.persist(rf.persister)

	if newIndex == -1 {
		newIndex = rf.state.getLogLen() - 1
	}

	//DTPrintf("%d: starts sending Append RPCs heartbeat: %t.\n", rf.me, heartbeat)
	for i := 0; i < len(rf.peers); i++ {
		next := rf.state.getNextIndex(i)
		if !heartbeat && next > newIndex {
			//DTPrintf("%d: follower %d's next %d than the new index\n", rf.me, i, next)
			continue
		}

		if i == rf.me {
			continue
		}

		go rf.sendAppend(done, i, heartbeat, term, next, newIndex)

		//go func(i int) {
		//job := sendJob{
		//i:         i,
		//heartbeat: heartbeat,
		//term:      term,
		//next:      next,
		//newIndex:  newIndex}

		//DTPrintf("%d: send job %+v\n", rf.me, job)

		//rf.sendChs[i] <- job

		//DTPrintf("%d: sent job %+v\n", rf.me, job)
		//}(i)
	}
}

func (rf *Raft) sendAppend(done <-chan interface{}, i int, heartbeat bool, term int,
	next int, newIndex int) {

	//DTPrintf("%d: begins to send Append to %d\n", rf.me, i)

	newNext := next
	oldNext := newNext

LOOP:
	for {
		// If unable to send AppendRPC due to [next:lastIndex] are gone, send
		// Snapshot instead.
		oldNext = newNext
		newNext = rf.sendSnapshot(done, i, term, newNext)

		if newNext < 0 || (!heartbeat && newNext > newIndex) {
			DTPrintf("%d: for %d, newNext: %d, oldNext: %d, newIndex: %d. returning!!\n",
				rf.me, i, newNext, oldNext, newIndex)
			return
		}

		args := rf.getAppendArgs(newNext, newIndex, term, heartbeat)
		if args == nil {
			continue
		}

		reply := new(AppendEntriesReply)
		//DTPrintf("%d sends Append RPC to %d for term %d. Args: %+v\n", rf.me, i, term, args)

		ok := rf.peers[i].Call("Raft.AppendEntries", args, reply)

		select {
		case <-done:
			return
		default:
		}

		// Retry. Failed reply makes other cases meaningless
		if !ok {
			continue LOOP
		}

		shouldReturn, newEntries := false, false
		shouldReturn, newEntries, newIndex, newNext =
			rf.processAppendReply(done, reply, heartbeat, i, args, term, newIndex, newNext)

		if newEntries {
			go rf.sendAppend(done, i, false, term, newIndex, newIndex)
		}

		//DTPrintf("%d: [UNLOCK] for %d finish processing Append reply\n", rf.me, i)
		if shouldReturn {
			return
		}
	}
}

//type sendJob struct {
//i         int
//heartbeat bool
//term      int
//next      int
//newIndex  int
//}

//func (rf *Raft) sendLoop(done <-chan interface{}, sendCh <-chan sendJob) {
//for {
//select {
//case <-done:
//return
//case job := <-sendCh:
//rf.sendAppend(done, job.i, job.heartbeat, job.term, job.next, job.newIndex)
//DTPrintf("%d: finish job %+v\n", rf.me, job)
//}
//}
//}

// return
// -1: leadership lost
func (rf *Raft) sendSnapshot(done <-chan interface{}, i int, term int, next int) int {
LOOP:
	for {
		args := rf.getSnapshotArgs(term, next)

		if args == nil {
			return next
		}

		reply := &InstallSnapshotReply{}
		DTPrintf("%d: send Snapshot to %d, lastIndex: %d\n",
			rf.me, i, args.LastIncludedEntryIndex)

		ok := rf.peers[i].Call("Raft.InstallSnapshot", args, reply)

		select {
		case <-done:
			return -1
		default:
		}

		if !ok {
			DTPrintf("%d: send Snapshot failed\n", rf.me)
			continue LOOP
		}

		return rf.processSnapshotReply(done, reply, i, args)
	}
}

func (rf *Raft) getAppendArgs(newNext int, newIndex int, term int,
	heartbeat bool) (args *AppendEntriesArgs) {

	req := StateRequest{done: make(chan interface{})}
	rf.state.queue <- req
	defer close(req.done)

	if !rf.state.indexExist(newNext) {
		DTPrintf("%d: [WARNING] snapshot was taken again. newNext %d\n", rf.me, newNext)
		return nil
	}

	args = &AppendEntriesArgs{
		Term:         term,
		PrevLogIndex: newNext - 1,
		PrevLogTerm:  rf.state.getLogEntryTerm(newNext - 1),
		LeaderCommit: rf.state.getCommitIndex()}

	if !heartbeat {
		args.Entries = append(args.Entries,
			rf.state.getLogRange(newNext, newIndex+1)...)
	}

	return args
}

func (rf *Raft) processAppendReply(done <-chan interface{}, reply *AppendEntriesReply,
	heartbeat bool, i int, args *AppendEntriesArgs, term int, newIndex int, newNext int) (
	shouldReturn bool, newEntries bool, nnewIndex int, nnewNext int) {

	req := StateRequest{done: make(chan interface{})}
	rf.state.queue <- req
	defer close(req.done)

	shouldReturn, newEntries = false, false
	nnewIndex, nnewNext = newIndex, newNext

	if !heartbeat && !rf.state.indexExist(newIndex+1) {
		DTPrintf("%d: [WARNING] snapshot was taken again. newNext %d\n", rf.me, newNext)
		shouldReturn = true
	}

	switch {
	case shouldReturn:
		// Pass through

	case !reply.Success && reply.Term > term:
		rf.state.setCurrentTerm(reply.Term)
		rf.state.setRole(FOLLOWER)

		select {
		case rf.appendReplyCh <- true:
		case <-done:
		}

	case !reply.Success && heartbeat:
		//DTPrintf("%d: heartbeat discovered %d's conflict logs\n", rf.me, i)
		newEntries = true
		//go rf.sendAppend(done, i, false, term, newNext, newIndex)
		shouldReturn = true

	case !reply.Success && reply.ConflictTerm == -1:
		// Follower doesn't have the entry with that term. len of
		// follower's log is shorter than the leader's.
		nnewNext = reply.ConflictIndex
		//DTPrintf("%d: conflict index: %d\n", rf.me, newNext)

	case !reply.Success && reply.ConflictTerm != -1:
		// Find the last entry of the conflicting term. The conflicting
		// server will delete all entries after this new newNext
		j := newIndex
		// j > 0 is safe here because two logs are the same at lastIncludedEntryIndex
		for ; j > 0; j-- {
			if rf.state.getLogEntryTerm(j) == reply.ConflictTerm {
				break
			}
		}

		nnewNext = j + 1
		//DTPrintf("%d: conflict term %d. new index: %d\n", rf.me, reply.ConflictTerm, newNext)

	case reply.Success && heartbeat:
		nnewIndex = rf.state.getLogLen() - 1
		nnewNext = rf.state.getNextIndex(i)
		if nnewIndex >= nnewNext {
			//DTPrintf("%d: heartbeat discovered %d's new log entries\n", rf.me, i)
			//go rf.sendAppend(done, i, false, term, nnewNe, nnewIndex)
			newEntries = true
		}
		shouldReturn = true

	case reply.Success:
		iMatch := args.PrevLogIndex + len(args.Entries)
		rf.state.setMatchIndex(i, iMatch)
		//DTPrintf("%d: update %d's match %d\n", rf.me, i, iMatch)

		iNext := newNext + len(args.Entries)
		rf.state.setNextIndex(i, iNext)

		rf.state.updateCommitIndex(term, rf.commit)

		shouldReturn = true

	default:
		DTPrintf("!!! reply: %v, term: %d\n", reply, term)
		log.Fatal("!!! Incorrect Append RPC reply")
	}

	return shouldReturn, newEntries, nnewIndex, nnewNext
}

func (rf *Raft) getSnapshotArgs(term int, next int) (args *InstallSnapshotArgs) {

	req := StateRequest{done: make(chan interface{})}
	rf.state.queue <- req
	defer close(req.done)

	if rf.state.indexExist(next) {
		DTPrintf("%d: next exists\n", rf.me)
		return nil
	}

	lastIndex := rf.state.getLastIndex()
	lastTerm := rf.state.getLastTerm()

	args = &InstallSnapshotArgs{
		Term: term,
		LastIncludedEntryIndex: lastIndex,
		LastIncludedEntryTerm:  lastTerm,
		Data: rf.state.readSnapshot(rf.persister)}

	return args
}

func (rf *Raft) processSnapshotReply(done <-chan interface{}, reply *InstallSnapshotReply,
	i int, args *InstallSnapshotArgs) (ret int) {

	req := StateRequest{done: make(chan interface{})}
	rf.state.queue <- req
	defer close(req.done)

	ret = -1

	switch {
	case reply.Term > args.Term:
		rf.state.setCurrentTerm(reply.Term)
		rf.state.setRole(FOLLOWER)

		select {
		case rf.appendReplyCh <- true:
		case <-done:
		}

	default:
		// If succeeded, it means follower has the same state at least up to lastIncludedEntryIndex.
		rf.state.setMatchIndex(i, args.LastIncludedEntryIndex)

		iMatch := rf.state.getMatchIndex(i)
		rf.state.setNextIndex(i, iMatch+1)

		ret = iMatch + 1
		DTPrintf("%d: in snapshot set next to %d\n", rf.me, rf.state.getNextIndex(i))
	}

	return ret

}
