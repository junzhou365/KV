package raft

import (
	"time"
)

func (rf *Raft) runFollower() {
	respCh := make(chan rpcResp)

	startElection := false

LOOP:
	for {
		select {
		case rf.rpcCh <- respCh:
			r := <-respCh
			DTPrintf("%d: follower: %t\n", rf.me, r)

		case <-time.After(getElectionTimeout()):
			startElection = true
			break LOOP
		}
	}
	if startElection {
		rf.state.setRole(CANDIDATE)
	}
}
