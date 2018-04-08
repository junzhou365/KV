package raft

import (
	"time"
)

func (rf *Raft) runFollower() {
	respCh := make(chan rpcResp)

	for {
		select {
		case rf.rpcCh <- respCh:
			<-respCh

		case <-time.After(getElectionTimeout()):
			rf.state.setRole(CANDIDATE)
			return

		case <-rf.shutDown:
			return
		}
	}
}
