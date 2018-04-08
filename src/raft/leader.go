package raft

import (
	"fmt"
	"log"
	"time"
)

func (rf *Raft) runLeader() {
	state := &rf.state
	state.initializeMatchAndNext()

	term := state.getCurrentTerm()

	respCh := make(chan rpcResp)

	done := make(chan interface{})
	defer close(done)

	rf.exitLeaderCh = make(chan int)
	// send heartbeat immediately
	heartbeatTimer := time.After(0)

	for {
		select {
		case rf.rpcCh <- respCh:
			if r := <-respCh; r.toFollower {
				return
			}

		case <-heartbeatTimer:
			go rf.appendNewEntries(done, true, term)
			heartbeatTimer = time.After(rf.heartbeatInterval)

		case <-rf.newEntry:
			go rf.appendNewEntries(done, false, term)

		case term := <-rf.exitLeaderCh:
			rf.state.setCurrentTerm(term)
			rf.state.setRole(FOLLOWER)
			DTPrintf("%d: exit leader loop\n", rf.me)
			return

		case <-rf.shutDown:
			return
		}
	}
}

func (rf *Raft) appendNewEntries(
	done <-chan interface{}, heartbeat bool, term int) {
	// save before a change
	rf.state.persist(rf.persister)

	for i := range rf.peersIndexes() {
		go rf.sendAppend(done, i, heartbeat, term)
	}
}

func (rf *Raft) sendAppend(
	done <-chan interface{}, i int, heartbeat bool, term int) {

	//defer DTPrintf("%d: finished sending Append to %d with first: %d, lastNewIndex: %d\n",
	//rf.me, i, firstNewIndex, lastNewIndex)
	//DTPrintf("%d: starts sending Append to %d with first: %d, lastNewIndex: %d\n",
	//rf.me, i, firstNewIndex, lastNewIndex)
	first := rf.state.getNextIndex(i)
	last := rf.state.getLogLen() - 1

	if !heartbeat && last < first {
		return
	}

LOOP:
	for {
		// If unable to send AppendRPC due to log[first:some index] are
		// trimmed, send Snapshot instead.
		// heartbeat also needs this because prev = first - 1 is needed.
		if rf.state.indexTrimmed(first) {
			first = rf.sendSnapshot(done, i, term, first)
			//DTPrintf("%d: After sending snapshot, the first: %d\n", rf.me, first)
		}

		args := rf.getAppendArgs(first, term, heartbeat)
		if args == nil {
			continue
		}

		reply := new(AppendEntriesReply)
		DTPrintf("%d: sends Append to %d with next: %d\n",
			rf.me, i, first)

		ok := rf.peers[i].Call("Raft.AppendEntries", args, reply)

		select {
		case <-done:
			return
		default:
		}

		// Retry. Failed reply makes other cases meaningless
		if !ok {
			time.Sleep(rf.retryInterval)
			continue LOOP
		}

		first = rf.processAppendReply(reply, heartbeat, i, args)

		switch {
		case first <= 0:
			return
		default:
			heartbeat = false
		}
	}
}

func (rf *Raft) sendSnapshot(done <-chan interface{}, i int, term int, next int) int {
LOOP:
	for {

		lastIndex, lastTerm, data := rf.state.readSnapshot(rf.persister)

		args := &InstallSnapshotArgs{
			Term: term,
			LastIncludedEntryIndex: lastIndex,
			LastIncludedEntryTerm:  lastTerm,
			Data: data}

		reply := &InstallSnapshotReply{}
		DTPrintf("%d: sends Snapshot to %d, lastIndex: %d\n",
			rf.me, i, args.LastIncludedEntryIndex)

		ok := rf.peers[i].Call("Raft.InstallSnapshot", args, reply)

		select {
		case <-done:
			return -1
		default:
		}

		if !ok {
			DTPrintf("%d: send Snapshot failed. Retry. role: %d, term: %d\n",
				rf.me, rf.state.getRole(), rf.state.getCurrentTerm())
			time.Sleep(rf.retryInterval)
			continue LOOP
		}

		return rf.processSnapshotReply(done, reply, i, args)
	}
}

// first: first new entry
func (rf *Raft) getAppendArgs(first int, term int, heartbeat bool) (
	args *AppendEntriesArgs) {

	state := &rf.state
	state.rw.RLock()
	defer state.rw.RUnlock()

	if state.baseIndexTrimmedWithNoLock(first - 1) {
		DTPrintf("%d: [WARNING] snapshot was taken again. first %d\n", rf.me, first)
		return nil
	}

	args = &AppendEntriesArgs{
		Term:         term,
		PrevLogIndex: first - 1,
		PrevLogTerm:  state.getLogEntryTermWithNoLock(first - 1),
		LeaderCommit: state.getCommitIndexWithNoLock()}

	if !heartbeat {
		end := state.getLogLenWithNoLock()
		newEntries, ok := state.getLogRangeWithNoLock(first, end)
		if !ok {
			panic(fmt.Sprintf("%d: logs lost. first: %d, end: %d\n",
				rf.me, first, end))
		}
		args.Entries = append(args.Entries, newEntries...)
	}

	return args
}

// return values:
// 0: normal return
// -1: found new term
// positive: inconsistent log index. Could be conflicted or short follower's log
func (rf *Raft) processAppendReply(reply *AppendEntriesReply,
	heartbeat bool, i int, args *AppendEntriesArgs) (divgIndex int) {

	//defer DTPrintf("%d: process the AppendReply done %+v, heartbeat: %t\n",
	//rf.me, reply, heartbeat)

	state := &rf.state
	state.rw.Lock()
	defer state.rw.Unlock()

	//DTPrintf("%d: process the AppendReply %+v, heartbeat: %t\n",
	//rf.me, reply, heartbeat)

	divgIndex = 0

	switch {

	case !reply.Success && reply.Term > args.Term:
		DTPrintf("%d: Append discovered higher term: %d\n",
			rf.me, reply.Term)
		go func() { rf.exitLeaderCh <- reply.Term }()
		return -1

	case !reply.Success && heartbeat:
		divgIndex = reply.ConflictIndex
		DTPrintf("%d: heartbeat discovered %d's conflict entries start at: %d\n",
			rf.me, i, divgIndex)

	case !reply.Success && reply.ConflictTerm == -1:
		// Follower doesn't have the entry with that term. len of
		// follower's log is shorter than the leader's.
		divgIndex = reply.ConflictIndex

	case !reply.Success && reply.ConflictTerm != -1:
		// Find the last entry of the conflicting term. The conflicting
		// server will delete all entries after this new start
		j := state.getLogLenWithNoLock() - 1
		// Notice here we don't use baseIndexTrimmed
		found := false
		for ; !state.indexTrimmedWithNoLock(j); j-- {
			entryTerm := state.getLogEntryTermWithNoLock(j)
			if entryTerm == reply.ConflictTerm {
				found = true
				break
			}
		}

		if !found {
			divgIndex = reply.ConflictIndex
		} else {
			divgIndex = j + 1
		}
		DTPrintf("%d: conflict term %d. index: %d\n",
			rf.me, reply.ConflictTerm, divgIndex)

	case reply.Success && heartbeat:
		last := state.getLogLenWithNoLock() - 1
		first := state.getNextIndexWithNoLock(i)
		if last >= first {
			DTPrintf("%d: heartbeat discovered %d's new log entries\n", rf.me, i)
			divgIndex = first
		}

	case reply.Success:
		match := args.PrevLogIndex + len(args.Entries)
		if match > state.getMatchIndexWithNoLock(i) {
			state.setMatchIndexWithNoLock(i, match)
		}

		next := match + 1
		last := state.getLogLenWithNoLock() - 1

		state.setNextIndexWithNoLock(i, next)

		if last >= next {
			DTPrintf("%d: heartbeat discovered %d's new log entries at %d\n",
				rf.me, i, next)
			divgIndex = next
		}

		commitIndex := state.updateCommitIndexWithNoLock(args.Term)
		if commitIndex != -1 {
			go func() { rf.commit <- commitIndex }()
		}

	default:
		log.Fatal("!!! Incorrect Append RPC reply: %v for term %d",
			reply, args.Term)
	}

	return divgIndex
}

// return
// -1: fails
// new first (might exceed log range)
func (rf *Raft) processSnapshotReply(done <-chan interface{}, reply *InstallSnapshotReply,
	i int, args *InstallSnapshotArgs) (ret int) {

	jobDone := rf.Serialize("processSnapshotReply")
	defer close(jobDone)

	ret = -1

	switch {
	case reply.Term > args.Term:
		go func() { rf.exitLeaderCh <- reply.Term }()
		rf.state.setCurrentTerm(reply.Term)
		rf.state.setRole(FOLLOWER)

	default:
		// succeeded, it means follower has the same state at least up to lastIncludedEntryIndex.
		if args.LastIncludedEntryIndex > rf.state.getMatchIndex(i) {
			rf.state.setMatchIndex(i, args.LastIncludedEntryIndex)
		}

		iMatch := rf.state.getMatchIndex(i)
		rf.state.setNextIndex(i, iMatch+1)

		ret = iMatch + 1
		DTPrintf("%d: in snapshot set next to %d\n", rf.me, rf.state.getNextIndex(i))
	}

	return ret

}
