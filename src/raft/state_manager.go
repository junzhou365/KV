package raft

type StateRequest struct {
	done chan interface{}
	name string
}

// Mainly responsible for serializing accessing to log related structures
func (rs *RaftState) stateLoop() {
	for req := range rs.queue {
		<-req.done
		DTPrintf("%d align: %s is done\n", rs.me, req.name)
	}
}

func (rf *Raft) Serialize(name string) chan interface{} {
	req := StateRequest{done: make(chan interface{}), name: name}
	rf.state.queue <- req
	DTPrintf("%d align: %s's req is acquired\n", rf.me, name)
	return req.done
}
