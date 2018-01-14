package raft

import (
	"log"
	"time"
)

func (rf *Raft) runLeader() {
	// Initiliaze next and match indexes
	logLen := rf.state.getLogLen()
	rf.state.rw.Lock()
	rf.state.nextIndexes = make([]int, len(rf.peers))
	for i := 0; i < len(rf.peers); i++ {
		rf.state.nextIndexes[i] = logLen
	}
	rf.state.matchIndexes = make([]int, len(rf.peers))
	rf.state.rw.Unlock()

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

	DTPrintf("%d: begins to send Append to %d\n", rf.me, i)

	newNext := next
	oldNext := newNext

	for {
		// If unable to send AppendRPC due to [next:lastIndex] are gone, send
		// Snapshot instead.
		oldNext = newNext
		newNext = rf.sendSnapshot(done, i, term, newNext)

		rf.state.rw.RLock()
		//DTPrintf("%d: [RLOCK] for %d start processing Append req\n", rf.me, i)

		if newNext < 0 || (!heartbeat && newNext > newIndex) {
			DTPrintf("%d: for %d, newNext: %d, oldNext: %d, newIndex: %d. returning!!\n",
				rf.me, i, newNext, oldNext, newIndex)

			rf.state.rw.RUnlock()
			//DTPrintf("%d: for %d [RUNLOCK] finish processing Append req\n", rf.me, i)
			return
		}

		//DTPrintf("%d: for %d [RLOCK] start processing Append req\n", rf.me, i)

		if !rf.state.indexExistWithNoLock(newNext) {
			DTPrintf("%d: [WARNING] snapshot was taken again. newNext %d\n", rf.me, newNext)
			rf.state.rw.RUnlock()
			//DTPrintf("%d: for %d [RUNLOCK] finish processing Append req\n", rf.me, i)
			continue
		}

		args := AppendEntriesArgs{
			Term:         term,
			PrevLogIndex: newNext - 1,
			PrevLogTerm:  rf.state.getLogEntryTermWithNoLock(newNext - 1),
			LeaderCommit: rf.state.getCommitIndexWithNoLock()}

		//DTPrintf("%d: logLen: %d, prevLogIndex: %d, newIndex: %d for %d\n", rf.me,
		//rf.state.getLogLenWithNoLock(), newNext-1, newIndex, i)

		if !heartbeat {
			args.Entries = append(args.Entries,
				rf.state.getLogRangeWithNoLock(newNext, newIndex+1)...)
		}

		rf.state.rw.RUnlock()
		//DTPrintf("%d: for %d [RUNLOCK] finish processing Append req\n", rf.me, i)

		reply := new(AppendEntriesReply)
		//DTPrintf("%d sends Append RPC to %d for term %d. Args: %+v\n", rf.me, i, term, args)

		ok := rf.peers[i].Call("Raft.AppendEntries", &args, reply)

		select {
		case <-done:
			return
		default:
		}

		rf.state.rw.Lock()
		//DTPrintf("%d: [LOCK] for %d start processing Append reply\n", rf.me, i)

		shouldReturn := false

		if !heartbeat && !rf.state.indexExistWithNoLock(newIndex+1) {
			shouldReturn = true
		}

		switch {
		// Retry. Failed reply makes other cases meaningless
		case !ok:
			//DTPrintf("%d: Append RPC failed for %d\n", rf.me, i)

		case shouldReturn:
			DTPrintf("%d: [WARNING] snapshot was taken again. newNext %d\n", rf.me, newNext)

		case !reply.Success && reply.Term > term:
			rf.state.CurrentTerm = reply.Term
			rf.state.role = FOLLOWER
			//DTPrintf("%d: discovered new term\n", rf.me)
			//DTPrintf("%d: is appendCh nil? %v\n", rf.me, rf.appendReplyCh)
			rf.state.rw.Unlock()
			//DTPrintf("%d: [UNLOCK] for %d finish processing Append reply\n", rf.me, i)
			select {
			case rf.appendReplyCh <- true:
			case <-done:
			}
			return

		case !reply.Success && heartbeat:
			//DTPrintf("%d: heartbeat discovered %d's conflict logs\n", rf.me, i)
			go rf.sendAppend(done, i, false, term, newNext, newIndex)
			shouldReturn = true

		case !reply.Success && reply.ConflictTerm == -1:
			// Follower doesn't have the entry with that term. len of
			// follower's log is shorter than the leader's.
			newNext = reply.ConflictIndex
			//DTPrintf("%d: conflict index: %d\n", rf.me, newNext)

		case !reply.Success && reply.ConflictTerm != -1:
			// Find the last entry of the conflicting term. The conflicting
			// server will delete all entries after this new newNext
			j := newIndex
			// j > 0 is safe here because two logs are the same at lastIncludedEntryIndex
			for ; j > 0; j-- {
				if rf.state.getLogEntryTermWithNoLock(j) == reply.ConflictTerm {
					break
				}
			}

			newNext = j + 1
			//DTPrintf("%d: conflict term %d. new index: %d\n", rf.me, reply.ConflictTerm, newNext)

		case reply.Success && heartbeat:
			nnewIndex := rf.state.getLogLenWithNoLock() - 1
			nnextIndex := rf.state.getNextIndexWithNoLock(i)
			if nnewIndex >= nnextIndex {
				//DTPrintf("%d: heartbeat discovered %d's new log entries\n", rf.me, i)
				go rf.sendAppend(done, i, false, term, nnextIndex, nnewIndex)
			}
			shouldReturn = true

		case reply.Success:
			iMatch := args.PrevLogIndex + len(args.Entries)
			rf.state.setMatchIndexWithNoLock(i, iMatch)
			//DTPrintf("%d: update %d's match %d\n", rf.me, i, iMatch)

			iNext := newNext + len(args.Entries)
			rf.state.setNextIndexWithNoLock(i, iNext)

			rf.updateCommitIndexWithNoLock(term)

			shouldReturn = true

		default:
			DTPrintf("!!! reply: %v, term: %d\n", reply, term)
			log.Fatal("!!! Incorrect Append RPC reply")
		}

		rf.state.rw.Unlock()
		//DTPrintf("%d: [UNLOCK] for %d finish processing Append reply\n", rf.me, i)
		if shouldReturn {
			return
		}
	}
}

func (rf *Raft) updateCommitIndexWithNoLock(term int) {
	//rf.state.rw.Lock()
	//DTPrintf("%d: [LOCK] start processing update commit\n", rf.me)
	//defer DTPrintf("%d: [UNLOCK] finish processing update commit\n", rf.me)
	//defer rf.state.rw.Unlock()

	//DTPrintf("%d: try update commitIndex for term %d. orig commitIndex: %d\n",
	//rf.me, term, rf.state.commitIndex)

	logLen := rf.state.getLogLenWithNoLock()
	for n := logLen - 1; n > rf.state.commitIndex; n-- {
		count := 1
		for j := 0; j < len(rf.peers); j++ {
			if j != rf.me && rf.state.matchIndexes[j] >= n {
				//DTPrintf("%d: find %d's match %d >= n %d", rf.me, j, rf.state.matchIndexes[j], n)
				count++
			}
		}
		//DTPrintf("%d: the count is %d, n is %d", rf.me, count, n)
		if count > len(rf.peers)/2 && n > rf.state.commitIndex &&
			term == rf.state.getLogEntryTermWithNoLock(n) {
			rf.state.commitIndex = n
			go func() { rf.commit <- true }()
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
		rf.state.rw.RLock()
		//DTPrintf("%d: for %d [RLOCK] start processing Snapshot req\n", rf.me, i)

		select {
		case <-done:
			rf.state.rw.RUnlock()
			//DTPrintf("%d: for %d [RUNLOCK] finish processing Snapshot req\n", rf.me, i)
			return -1
		default:
		}

		if rf.state.indexExistWithNoLock(next) {
			DTPrintf("%d: next exists\n", rf.me)
			rf.state.rw.RUnlock()
			//DTPrintf("%d: for %d [RUNLOCK] finish processing Snapshot req\n", rf.me, i)
			return next
		}

		lastIndex := rf.state.getLastIndexWithNoLock()
		lastTerm := rf.state.getLastTermWithNoLock()
		args := &InstallSnapshotArgs{
			Term: term,
			LastIncludedEntryIndex: lastIndex,
			LastIncludedEntryTerm:  lastTerm,
			Data: rf.persister.ReadSnapshot()}

		reply := &InstallSnapshotReply{}

		DTPrintf("%d: send Snapshot to %d, lastIndex: %d\n",
			rf.me, i, args.LastIncludedEntryIndex)

		rf.state.rw.RUnlock()
		//DTPrintf("%d: for %d [RUNLOCK] finish processing Snapshot req\n", rf.me, i)

		ok := rf.peers[i].Call("Raft.InstallSnapshot", args, reply)

		select {
		case <-done:
			return -1
		default:
		}

		if !ok {
			continue LOOP
		}

		ret := -1

		rf.state.rw.Lock()
		//DTPrintf("%d: for %d [LOCK] start processing Snapshot reply\n", rf.me, i)

		select {
		case <-done:
			rf.state.rw.Unlock()
			//DTPrintf("%d: for %d [UNLOCK] finish processing Snapshot reply\n", rf.me, i)
			return -1
		default:
		}

		switch {
		case reply.Term > term:
			rf.state.CurrentTerm = reply.Term
			rf.state.role = FOLLOWER
			//DTPrintf("%d: discovered new term\n", rf.me)
			//DTPrintf("%d: is appendCh nil? %v\n", rf.me, rf.appendReplyCh)
			rf.state.rw.Unlock()
			//DTPrintf("%d: for %d [UNLOCK] finish processing Snapshot reply\n", rf.me, i)
			select {
			case rf.appendReplyCh <- true:
				//DTPrintf("%d: FUCK!!\n", rf.me)
			case <-done:
			}

			return ret

		default:
			// If succeeded, it means follower has the same state at least up to lastIncludedEntryIndex.
			rf.state.setMatchIndexWithNoLock(i, lastIndex)

			iMatch := rf.state.getMatchIndexWithNoLock(i)
			rf.state.setNextIndexWithNoLock(i, iMatch+1)

			ret = iMatch + 1
		}

		rf.state.rw.Unlock()
		//DTPrintf("%d: for %d [UNLOCK] finish processing Snapshot reply\n", rf.me, i)
		return ret
	}
}
