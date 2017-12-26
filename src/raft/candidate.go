package raft

import (
	"log"
	"time"
)

func (rf *Raft) runCandidate() {
	electionDone := make(chan RaftState)
	electionTimeoutTimer := time.After(getElectionTimeout())
	rf.elect(electionDone)
	for {
		select {
		case r := <-electionDone:
			electionDone = nil
			rf.state.setRole(r.role)
			rf.state.setCurrentTerm(r.CurrentTerm)
			if r.role == LEADER {
				DTPrintf("======= %d become a LEADER for term %d\n", rf.me, r.CurrentTerm)
				return
			}
		case <-electionTimeoutTimer:
			// restart election
			return
		}
	}
}

type voteResult struct {
	role int
	term int
}

func (rf *Raft) elect(electionDone chan RaftState) {
	DTPrintf("<<<< Triggered, node: %d >>>>\n", rf.me)

	if rf.state.getRole() == LEADER {
		DTPrintf("%d: Wrong rf role %d\n", rf.me, rf.state.role)
		log.Fatal("In electionLoop, Wrong Raft Role. Dead!!!")
	}
	term := rf.state.getCurrentTerm()
	term++
	rf.state.setCurrentTerm(term)
	DTPrintf("%d becomes candidate for term %d\n", rf.me, term)
	rf.state.setVotedFor(rf.me)

	args := new(RequestVoteArgs)
	args.Term = term
	args.CandidateId = rf.me
	args.LastLogIndex = rf.state.getLogLen() - 1
	args.LastLogTerm = rf.state.getLogEntry(args.LastLogIndex).Term

	go func(vDone chan RaftState) {
		term := rf.state.getCurrentTerm()
		replyCh := make(chan RequestVoteReply)
		for i := 0; i < len(rf.peers); i++ {
			if i == rf.me {
				// Simulate that the server responds to its own RPC.
				rf.persist()
				continue
			}
			go func(i int) {
				DTPrintf("%d send requestVote to %d for term %d\n", rf.me, i, term)
				reply := RequestVoteReply{index: i}
				for ok := false; !ok; {
					ok = rf.sendRequestVote(i, args, &reply)
					if !ok {
						DTPrintf("%d sent RequestVote RPC; it failed at %d for term %d. Retry...", rf.me, i, term)
					}
				}
				replyCh <- reply
			}(i)
		}

		yays, neys := 0, 0
		for i := 0; i < len(rf.peers)-1; i++ {
			reply := <-replyCh
			DTPrintf("%d got requestVote with reply %v from %d for term %d\n", rf.me, reply, reply.index, term)
			switch {
			case reply.Term > term:
				vDone <- RaftState{role: FOLLOWER, CurrentTerm: reply.Term}
				return
			case reply.VoteGranted:
				yays++
			case reply.VoteGranted == false:
				neys++
			}

			switch {
			case yays+1 > len(rf.peers)/2:
				vDone <- RaftState{role: LEADER, CurrentTerm: term}
				return
			case neys > (len(rf.peers)+1)/2-1:
				vDone <- RaftState{role: FOLLOWER, CurrentTerm: term}
				return
			}
		}
		vDone <- RaftState{role: FOLLOWER, CurrentTerm: term}
	}(electionDone)
}
