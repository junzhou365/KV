package raft

import (
	"time"
)

func (rf *Raft) runFollower() {
	for {
		select {
		case <-rf.rpcCh:

		case <-time.After(getElectionTimeout()):
			rf.state.setRole(CANDIDATE)
			return

		case <-rf.shutDown:
			return
		}
	}
}
