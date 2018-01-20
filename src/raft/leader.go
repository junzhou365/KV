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

	rf.appendReplyCh = make(chan bool)

	for {
		select {
		case rf.rpcCh <- respCh:
			if r := <-respCh; r.toFollower {
				DTPrintf("%d: exit leader loop\n", rf.me)
				return
			}

		case <-heartbeatTimer:
			heartbeatTimer = time.After(rf.heartbeatInterval)
			rf.appendNewEntries(done, true, term, -1)

		case index := <-rf.newEntry:
			if index > last_seen_index {
				rf.appendNewEntries(done, false, term, index)
				last_seen_index = index
			}

		case <-rf.appendReplyCh:
			DTPrintf("%d: exit leader loop\n", rf.me)
			return
		}
	}
}

func (rf *Raft) appendNewEntries(
	done <-chan interface{}, heartbeat bool, term int, lastNewIndex int) {

	// save before a change
	rf.state.persist(rf.persister)

	if lastNewIndex == -1 {
		lastNewIndex = rf.state.getLogLen() - 1
	}

	DTPrintf("%d: starts sending Append RPCs heartbeat: %t.\n", rf.me, heartbeat)
	for i := 0; i < len(rf.peers); i++ {
		firstNewIndex := rf.state.getNextIndex(i)
		if !heartbeat && firstNewIndex > lastNewIndex {
			DTPrintf("%d: follower %d's firstNewIndex %d than the new index\n", rf.me, i, firstNewIndex)
			continue
		}

		if i == rf.me {
			continue
		}

		go rf.sendAppend(done, i, heartbeat, term, firstNewIndex, lastNewIndex)
	}
}

func (rf *Raft) sendAppend(done <-chan interface{}, i int, heartbeat bool, term int,
	firstNewIndex int, lastNewIndex int) {

	//defer DTPrintf("%d: finished sending Append to %d with next: %d, lastNewIndex: %d\n", rf.me, i, next, lastNewIndex)
	//DTPrintf("%d: begins to send Append to %d with next: %d, newIndex: %d\n", rf.me, i, next, newIndex)

	first := firstNewIndex
	last := lastNewIndex

LOOP:
	for {
		// If unable to send AppendRPC due to [next:lastIndex] are gone, send
		// Snapshot instead.
		first = rf.sendSnapshot(done, i, term, first)
		DTPrintf("%d: After sending snapshot, the first: %d\n", rf.me, first)

		if first < 0 || (!heartbeat && first > last) {
			return
		}

		args := rf.getAppendArgs(first, last, term, heartbeat)
		if args == nil {
			continue
		}

		reply := new(AppendEntriesReply)
		DTPrintf("%d: sends Append to %d with next: %d, last: %d\n", rf.me, i, first, last)

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
		shouldReturn, newEntries, first, last =
			rf.processAppendReply(done, reply, heartbeat, i, args)

		if newEntries {
			go rf.sendAppend(done, i, false, term, last, last)
		}

		//DTPrintf("%d: [UNLOCK] for %d finish processing Append reply\n", rf.me, i)
		if shouldReturn {
			return
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
			DTPrintf("%d: send Snapshot failed\n", rf.me)
			continue LOOP
		}

		return rf.processSnapshotReply(done, reply, i, args)
	}
}

// first: first new entry
// last: last new entry
func (rf *Raft) getAppendArgs(first int, last int, term int,
	heartbeat bool) (args *AppendEntriesArgs) {

	jobDone := rf.Serialize("getAppendArgs")
	defer close(jobDone)

	DTPrintf("%d: try to get prev Term for %d\n", rf.me, first-1)
	if rf.state.baseIndexTrimmed(first - 1) {
		DTPrintf("%d: [WARNING] snapshot was taken again. first %d\n", rf.me, first)
		return nil
	}
	prevTerm := rf.state.getLogEntryTerm(first - 1)

	args = &AppendEntriesArgs{
		Term:         term,
		PrevLogIndex: first - 1,
		PrevLogTerm:  prevTerm,
		LeaderCommit: rf.state.getCommitIndex()}

	if !heartbeat {
		DTPrintf("%d: try to get log range with first: %d, last: %d\n",
			rf.me, first, last)
		newEntries, ok := rf.state.getLogRange(first, last+1)
		if !ok {
			DTPrintf("%d: [WARNING] snapshot was taken again. first %d\n", rf.me, first)
			return nil
		}
		args.Entries = append(args.Entries, newEntries...)
	}

	return args
}

func (rf *Raft) processAppendReply(done <-chan interface{}, reply *AppendEntriesReply,
	heartbeat bool, i int, args *AppendEntriesArgs) (
	shouldReturn bool, newEntries bool, first int, last int) {

	defer DTPrintf("%d: process the AppendReply done %+v, heartbeat: %t\n",
		rf.me, reply, heartbeat)

	jobDone := rf.Serialize("processAppendReply")
	defer close(jobDone)

	DTPrintf("%d: process the AppendReply %+v, heartbeat: %t\n", rf.me, reply, heartbeat)

	shouldReturn, newEntries = false, false
	origFirst := args.PrevLogIndex + 1
	origLast := args.PrevLogIndex + len(args.Entries)
	first, last = origFirst, origLast

	switch {

	case !reply.Success && reply.Term > args.Term:
		rf.state.setCurrentTerm(reply.Term)
		rf.state.setRole(FOLLOWER)

		select {
		case rf.appendReplyCh <- true:
		case <-done:
		}

	case !reply.Success && heartbeat:
		newEntries = true
		shouldReturn = true

	case !reply.Success && reply.ConflictTerm == -1:
		// Follower doesn't have the entry with that term. len of
		// follower's log is shorter than the leader's.
		first = reply.ConflictIndex

	case !reply.Success && reply.ConflictTerm != -1:
		// Find the last entry of the conflicting term. The conflicting
		// server will delete all entries after this new start
		j := last
		// Notice here we don't use baseIndexTrimmed
		for ; rf.state.indexTrimmed(j); j-- {
			entryTerm := rf.state.getLogEntryTerm(j)
			if entryTerm == reply.ConflictTerm {
				break
			}
		}

		first = j + 1
		DTPrintf("%d: conflict term %d. new index: %d\n", rf.me, reply.ConflictTerm, first)

	case reply.Success && heartbeat:
		last = rf.state.getLogLen() - 1
		first = rf.state.getNextIndex(i)
		if last >= first {
			DTPrintf("%d: heartbeat discovered %d's new log entries\n", rf.me, i)
			newEntries = true
		}
		shouldReturn = true

	case reply.Success:
		iMatch := args.PrevLogIndex + len(args.Entries)
		if iMatch > rf.state.getMatchIndex(i) {
			rf.state.setMatchIndex(i, iMatch)
			DTPrintf("%d: update %d's match %d\n", rf.me, i, iMatch)
		}

		iNext := last + 1
		rf.state.setNextIndex(i, iNext)

		commitIndex := rf.state.updateCommitIndex(args.Term)
		if commitIndex != -1 {
			rf.commit <- commitIndex
		}

		shouldReturn = true

	default:
		DTPrintf("!!! reply: %v, term: %d\n", reply, args.Term)
		log.Fatal("!!! Incorrect Append RPC reply")
	}

	return shouldReturn, newEntries, first, last
}

func (rf *Raft) processSnapshotReply(done <-chan interface{}, reply *InstallSnapshotReply,
	i int, args *InstallSnapshotArgs) (ret int) {

	jobDone := rf.Serialize("processSnapshotReply")
	defer close(jobDone)

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
