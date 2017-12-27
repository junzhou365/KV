package raft

import (
	"time"
)

func (rf *Raft) runFollower() {
	respCh := make(chan rpcResp)

	select {
	case rf.rpcCh <- respCh:
		<-respCh

	case <-time.After(getElectionTimeout()):
		rf.state.setRole(CANDIDATE)
		return
	}
}
