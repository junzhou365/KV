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

	done := make(chan interface{})
	defer close(done)

	term := rf.state.getCurrentTerm()
	term++
	rf.state.setCurrentTerm(term)
	rf.state.setVotedFor(rf.me)

	electionResCh := rf.elect(done, term)

	electionTimeoutTimer := time.After(getElectionTimeout())
	DTPrintf("%d collects votes for term %d\n", rf.me, term)

	respCh := make(chan rpcResp)
	votes := 1

	for {
		select {
		case rf.rpcCh <- respCh:
			r := <-respCh
			if r.toFollower {
				return
			}

		case reply := <-electionResCh:
			DTPrintf("%d got requestVote with reply %+v from %d for term %d\n",
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
				DTPrintf("======= %d become a LEADER for term %d, log len: %d\n",
					rf.me, term, rf.state.getLogLen())
				return
			}

		case <-electionTimeoutTimer:
			// restart election
			return
		}
	}
}

func (rf *Raft) elect(done <-chan interface{}, term int) chan RequestVoteReply {
	DTPrintf("<<<< Triggered, node: %d >>>>\n", rf.me)

	args := new(RequestVoteArgs)
	args.Term = term
	args.CandidateId = rf.me
	args.LastLogIndex = rf.state.getLogLen() - 1
	args.LastLogTerm = rf.state.getLogEntry(args.LastLogIndex).Term

	replyCh := make(chan RequestVoteReply)

	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}

		go func(i int) {
			DTPrintf("%d send requestVote to %d for term %d\n", rf.me, i, term)
			reply := RequestVoteReply{index: i}
			for ok := false; !ok; {
				select {
				case <-done:
					return
				default:
				}
				ok = rf.sendRequestVote(i, args, &reply)
			}

			select {
			case replyCh <- reply:
			case <-done:
			}
		}(i)
	}
	return replyCh
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}
