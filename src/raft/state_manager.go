package raft

// Mainly responsible for serializing accessing to log related structures
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

	// XXX: will be removed
	rs.rw.RLock()
	defer rs.rw.RUnlock()

	if !rs.indexExistWithNoLock(newNext) {
		DTPrintf("%d: [WARNING] snapshot was taken again. newNext %d\n", rs.me, newNext)
		return nil
	}

	args = &AppendEntriesArgs{
		Term:         term,
		PrevLogIndex: newNext - 1,
		PrevLogTerm:  rs.getLogEntryTermWithNoLock(newNext - 1),
		LeaderCommit: rs.getCommitIndexWithNoLock()}

	if !heartbeat {
		args.Entries = append(args.Entries,
			rs.getLogRangeWithNoLock(newNext, newIndex+1)...)
	}

	return args
}

func (rs *RaftState) getAppendArgs(newNext int, newIndex int, term int,
	heartbeat bool) (args *AppendEntriesArgs) {

	req := StateRequest{done: make(chan interface{})}
	rs.queue <- req
	defer close(req.done)

	// XXX: will be removed
	rs.rw.RLock()
	defer rs.rw.RUnlock()

	if !rs.indexExistWithNoLock(newNext) {
		DTPrintf("%d: [WARNING] snapshot was taken again. newNext %d\n", rs.me, newNext)
		return nil
	}

	args = &AppendEntriesArgs{
		Term:         term,
		PrevLogIndex: newNext - 1,
		PrevLogTerm:  rs.getLogEntryTermWithNoLock(newNext - 1),
		LeaderCommit: rs.getCommitIndexWithNoLock()}

	if !heartbeat {
		args.Entries = append(args.Entries,
			rs.getLogRangeWithNoLock(newNext, newIndex+1)...)
	}

	return args
}
