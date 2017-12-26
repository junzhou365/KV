package raft

import (
	"log"
)

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

// Should run in order
func (rf *Raft) appendNewEntries(appendDone chan AppendEntriesReply) {
	var llog []RaftLogEntry
	var localNextIndexes []int
	rf.state.rw.RLock()
	llog = append(llog, rf.state.Log...)
	localNextIndexes = append(localNextIndexes, rf.state.nextIndexes...)
	rf.state.rw.RUnlock()
	DTPrintf("%d: starts sending Append RPCs.\nCurrent log len is %d\n", rf.me, len(llog))
	curTerm := rf.state.getCurrentTerm()
	role := rf.state.getRole()
	leaderCommit := rf.state.getCommitIndex()

	if role != LEADER {
		return
	}

	replyCh := make(chan AppendEntriesReply)
	for i, next := range localNextIndexes {
		if next > len(llog)-1 {
			DTPrintf("%d: follower %d's next %d than last log index\n", rf.me, i, next)
			continue
		}

		if i == rf.me {
			// Simulate that the server responds to its own RPC.
			rf.persist()
			continue
		}
		go func(i int, next int) {
			for {
				args := AppendEntriesArgs{}
				args.Term = curTerm
				args.PrevLogIndex = next - 1
				args.PrevLogTerm = llog[args.PrevLogIndex].Term
				args.Entries = llog[next:]
				args.LeaderCommit = leaderCommit

				reply := new(AppendEntriesReply)
				DTPrintf("%d sends Append RPC to %d for term %d\n", rf.me, i, curTerm)
				for ok := false; !ok; {
					ok = rf.peers[i].Call("Raft.AppendEntries", &args, reply)
				}
				switch {
				case !reply.Success && reply.Term > curTerm:
					replyCh <- *reply
					return
				case !reply.Success && reply.ConflictTerm == -1:
					// Follower doesn't have the entry with that term. len of
					// follower's log is shorter than the leader's.
					next = reply.ConflictIndex
					DTPrintf("%d resend Append RPC to %d with conflicted index %d\n", rf.me, i, next)
				case !reply.Success && reply.ConflictTerm != -1:
					j := len(llog) - 1
					for ; j > 0; j-- {
						if llog[j].Term == reply.ConflictTerm {
							break
						}
					}
					next = j + 1
					DTPrintf("%d resend Append RPC to %d with decremented index %d\n", rf.me, i, next)
				case reply.Success:
					rf.state.setNextIndex(i, next+len(args.Entries))
					rf.state.setMatchIndex(i, args.PrevLogIndex+len(args.Entries))
					reply.index = i
					DTPrintf("%d: got Append RPC from %d for term %d\n", rf.me, i, curTerm)
					replyCh <- *reply
					return
				default:
					DTPrintf("!!! reply: %v, term: %d\n", reply, curTerm)
					log.Fatal("!!! Incorrect Append RPC reply")
				}
			}
		}(i, next)
	}

	received := 1
	for i := 0; i < len(rf.peers)-1; i++ {
		r := <-replyCh
		if !r.Success {
			appendDone <- r
			return
		}

		received++
		if received <= len(rf.peers)/2 {
			continue
		}

		count := 1
		rf.state.rw.RLock()
		for n := len(rf.state.Log) - 1; n > rf.state.commitIndex; n-- {
			//DTPrintf("%d: try to find n. matches: %v, n: %d, commit: %d\n", rf.me, rf.state.matchIndexes, n, rf.state.commitIndex)
			for j := 0; j < len(rf.peers); j++ {
				if j != rf.me && rf.state.matchIndexes[j] >= n {
					count++
				}
			}
			if count > len(rf.peers)/2 && n > 0 && curTerm == rf.state.Log[n].Term {
				DTPrintf("%d: commitIndex %d is updated to %d\n", rf.me, rf.state.commitIndex, n)
				rf.state.commitIndex = n
				go func() { rf.commit <- true }()
				break
			}
		}
		rf.state.rw.RUnlock()
	}

	appendDone <- AppendEntriesReply{Term: curTerm, Success: true}
}

func (rf *Raft) sendHB() chan RaftState {
	term := rf.state.getCurrentTerm()
	leaderCommit := rf.state.getCommitIndex()
	prevLogIndex := rf.state.getLogLen() - 1
	prevLogTerm := rf.state.getLogEntry(prevLogIndex).Term

	DTPrintf("%d starts sending HB for term %d\n", rf.me, term)

	heartbeatDone := make(chan RaftState) // the channel is only used for this term
	go func(hDone chan RaftState) {
		//replies := make([]AppendEntriesReply, len(rf.peers))
		replyCh := make(chan AppendEntriesReply)
		for i, _ := range rf.peers {
			if i == rf.me {
				continue
			}
			go func(i int) {
				args := AppendEntriesArgs{}
				args.Term = term
				args.PrevLogIndex = prevLogIndex
				args.PrevLogTerm = prevLogTerm
				args.LeaderCommit = leaderCommit
				reply := AppendEntriesReply{}
				//DTPrintf("%d sends heartbeat to %d for term %d with args %v\n", rf.me, i, term, args)
				for ok := false; !ok; {
					ok = rf.peers[i].Call("Raft.AppendEntries", &args, &reply)
				}
				//DTPrintf("%d got heartbeat reply %v from %d for term %d\n", rf.me, reply, i, term)
				replyCh <- reply
			}(i)
		}

		for i := 0; i < len(rf.peers)-1; i++ {
			reply := <-replyCh
			if reply.Term > term {
				hDone <- RaftState{role: FOLLOWER, CurrentTerm: reply.Term}
				return
			}
		}
		hDone <- RaftState{role: LEADER, CurrentTerm: term}
	}(heartbeatDone)

	return heartbeatDone
}
