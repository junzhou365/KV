package raft

import (
	"log"
	"time"
)

func (rf *Raft) runCandidate() {
	if rf.state.getRole() != CANDIDATE {
		log.Fatal("In runCandidate, Wrong Raft Role. Dead!!!")
	}

	var term int
	var args *RequestVoteArgs
	state := &rf.state // Make sure it's pointer

	state.rw.Lock() // Lock

	state.CurrentTerm++
	state.VotedFor = rf.me
	term = state.CurrentTerm

	args = &RequestVoteArgs{
		Term:         term,
		CandidateId:  rf.me,
		LastLogIndex: state.getLogLenWithNoLock() - 1,
		LastLogTerm: state.getLogEntryTermWithNoLock(
			state.getLogLenWithNoLock() - 1)}

	state.rw.Unlock() // Unlock

	DTPrintf("%d collects votes for term %d\n", rf.me, term)

	done := make(chan interface{})
	defer close(done)

	electionResCh := rf.elect(done, args)

	respCh := make(chan rpcResp)
	votes := 1

	for {
		select {
		case rf.rpcCh <- respCh:
			r := <-respCh
			if r.toFollower {
				state.setRole(FOLLOWER)
				return
			}

		case reply := <-electionResCh:
			switch {
			case reply.Term > term:
				state.setRole(FOLLOWER)
				return

			case reply.VoteGranted:
				votes++
			}

			if votes > len(rf.peers)/2 {
				state.setRole(LEADER)
				DTPrintf("======= %d become a LEADER for term %d\n", rf.me, term)
				return
			}

		case <-time.After(getElectionTimeout()):
			DTPrintf("%d: Restart election\n", rf.me)
			return

		case <-rf.shutDown:
			return
		}
	}

}

func (rf *Raft) elect(done <-chan interface{}, args *RequestVoteArgs) chan RequestVoteReply {
	replyCh := make(chan RequestVoteReply)
	send := func(i int) {
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
	}

	for i := range rf.peersIndexes() {
		go send(i)
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
