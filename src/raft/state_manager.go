package raft

func (rs *RaftState) stateLoop() {
	for req := range rs.queue {
		<-req.done
	}
}

func (rs *RaftState) getAppendArgs(newNext int, newIndex int, term int,
	heartbeat bool) (args *AppendEntriesArgs) {

	req := StateRequest{done: make(chan interface{})}
	rs.queue <- req
	defer close(req.done)

	rs.rw.RLock()
	defer rs.rw.RUnlock()
	//DTPrintf("%d: for %d [RLOCK] start processing Append req\n", rf.me, i)

	if !rs.indexExistWithNoLock(newNext) {
		DTPrintf("%d: [WARNING] snapshot was taken again. newNext %d\n", rs.me, newNext)
		//DTPrintf("%d: for %d [RUNLOCK] finish processing Append req\n", rf.me, i)
		return nil
	}

	args = &AppendEntriesArgs{
		Term:         term,
		PrevLogIndex: newNext - 1,
		PrevLogTerm:  rs.getLogEntryTermWithNoLock(newNext - 1),
		LeaderCommit: rs.getCommitIndexWithNoLock()}

	//DTPrintf("%d: logLen: %d, prevLogIndex: %d, newIndex: %d for %d\n", rf.me,
	//rs.getLogLenWithNoLock(), newNext-1, newIndex, i)

	if !heartbeat {
		args.Entries = append(args.Entries,
			rs.getLogRangeWithNoLock(newNext, newIndex+1)...)
	}

	return args
}
