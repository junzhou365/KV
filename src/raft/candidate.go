package raft

import (
	"log"
	"time"
)

func (rf *Raft) runCandidate() {
	if rf.state.getRole() != CANDIDATE {
		DTPrintf("%d: Wrong rf role %d\n", rf.me, rf.state.role)
		log.Fatal("In runCandidate, Wrong Raft Role. Dead!!!")
	}

	term := rf.state.getCurrentTerm()
	term++
	rf.state.setCurrentTerm(term)
	rf.state.setVotedFor(rf.me)

	electionResCh := rf.elect(term)

	electionTimeoutTimer := time.After(getElectionTimeout())
	DTPrintf("%d becomes candidate for term %d\n", rf.me, term)

	respCh := make(chan rpcResp)
	votes := 1
	for {
		select {
		case rf.rpcCh <- respCh:
			r := <-respCh
			DTPrintf("%d: the fucking candidate r is %v!\n", rf.me, r)
			if r.toFollower {
				return
			}

		case reply := <-electionResCh:
			DTPrintf("%d got requestVote with reply %v from %d for term %d\n",
				rf.me, reply, reply.index, reply.Term)
			switch {
			case reply.Term > term:
				rf.state.setRole(FOLLOWER)
				return
			case reply.VoteGranted:
				votes++
			}

			if votes > len(rf.peers)/2 {
				rf.state.setRole(LEADER)
				DTPrintf("======= %d become a LEADER for term %d\n", rf.me, term)
				return
			}

		case <-electionTimeoutTimer:
			// restart election
			return
		}
	}
}

func (rf *Raft) elect(term int) chan RequestVoteReply {
	DTPrintf("<<<< Triggered, node: %d >>>>\n", rf.me)

	args := new(RequestVoteArgs)
	args.Term = term
	args.CandidateId = rf.me
	args.LastLogIndex = rf.state.getLogLen() - 1
	args.LastLogTerm = rf.state.getLogEntry(args.LastLogIndex).Term

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
			}

			replyCh <- reply
		}(i)
	}
	return replyCh
}
