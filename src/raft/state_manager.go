package raft

// Mainly responsible for serializing accessing to log related structures
func (rs *RaftState) stateLoop() {
	for req := range rs.queue {
		<-req.done
	}
}
