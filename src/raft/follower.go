package raft

import (
	"time"
)

func (rf *Raft) runFollower() {
	select {
	case <-time.After(getElectionTimeout()):
		rf.state.setRole(CANDIDATE)
		return
	}
}
