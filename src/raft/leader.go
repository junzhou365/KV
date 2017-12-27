package raft

import (
	"log"
	"time"
)

func (rf *Raft) runLeader() {
	// Initiliaze next and match indexes
	rf.state.rw.Lock()
	rf.state.nextIndexes = make([]int, len(rf.peers))
	for i := 0; i < len(rf.peers); i++ {
		rf.state.nextIndexes[i] = len(rf.state.Log)
	}
	rf.state.matchIndexes = make([]int, len(rf.peers))
	rf.state.rw.Unlock()

	term := rf.state.getCurrentTerm()

	heartbeatTimer := time.After(0)
	var heartbeatReplyCh chan AppendEntriesReply
	var appendReplyCh chan AppendEntriesReply
	// nei: new entry index
	last_seen_nei := 0

	respCh := make(chan rpcResp)
	appended := 1

	for {
		select {
		case rf.rpcCh <- respCh:
			if r := <-respCh; r.toFollower {
				DTPrintf("%d: leader to follower!\n", rf.me)
				return
			}

		case <-heartbeatTimer:
			heartbeatTimer = time.After(rf.heartbeatInterval)
			heartbeatReplyCh = rf.sendHeartbeat()

		case r := <-heartbeatReplyCh:
			if r.Term > term {
				rf.state.setRole(FOLLOWER)
				rf.state.setCurrentTerm(r.Term)
				DTPrintf("%d discovered new leader for term %d, switch to follower",
					rf.me, r.Term)
				return
			}

		case nei := <-rf.newEntry:
			if nei > last_seen_nei {
				DTPrintf("%d: newEntry at %d\n", rf.me, nei)
				appendReplyCh = make(chan AppendEntriesReply)
				appended = 1
				go rf.appendNewEntries(term, appendReplyCh)
				last_seen_nei = nei
			}

		case reply := <-appendReplyCh:
			if !reply.Success && reply.Term > term {
				rf.state.setCurrentTerm(reply.Term)
				rf.state.setRole(FOLLOWER)
				DTPrintf("%d discovered new leader for term %d, switch to follower",
					rf.me, reply.Term)
				return
			}
			appended++
			if appended > len(rf.peers)/2 {
				rf.updateNextAndMatchIndexes(term)
			}
		}
	}
}
func (rf *Raft) updateNextAndMatchIndexes(term int) {
	rf.state.rw.RLock()
	count := 1
	for n := len(rf.state.Log) - 1; n > rf.state.commitIndex; n-- {
		for j := 0; j < len(rf.peers); j++ {
			if j != rf.me && rf.state.matchIndexes[j] >= n {
				count++
			}
		}
		if count > len(rf.peers)/2 && n > 0 && term == rf.state.Log[n].Term {
			rf.state.commitIndex = n
			go func() { rf.commit <- true }()
			break
		}
	}
	rf.state.rw.RUnlock()
}

// Should run in order
func (rf *Raft) appendNewEntries(term int, appendReplyCh chan AppendEntriesReply) {
	var llog []RaftLogEntry
	var localNextIndexes []int
	rf.state.rw.RLock()
	llog = append(llog, rf.state.Log...)
	localNextIndexes = append(localNextIndexes, rf.state.nextIndexes...)
	rf.state.rw.RUnlock()
	DTPrintf("%d: starts sending Append RPCs.\nCurrent log len is %d\n", rf.me, len(llog))
	leaderCommit := rf.state.getCommitIndex()

	sendAppend := func(i int, next int) {
		for rf.state.getCurrentTerm() == term {
			args := AppendEntriesArgs{}
			args.Term = term
			args.PrevLogIndex = next - 1
			args.PrevLogTerm = llog[args.PrevLogIndex].Term
			args.Entries = llog[next:]
			args.LeaderCommit = leaderCommit

			reply := new(AppendEntriesReply)
			DTPrintf("%d sends Append RPC to %d for term %d\n", rf.me, i, term)
			for ok := false; !ok; {
				ok = rf.peers[i].Call("Raft.AppendEntries", &args, reply)
			}
			switch {
			case !reply.Success && reply.Term > term:
				appendReplyCh <- *reply
				return
			case !reply.Success && reply.ConflictTerm == -1:
				// Follower doesn't have the entry with that term. len of
				// follower's log is shorter than the leader's.
				next = reply.ConflictIndex
				DTPrintf("%d try to resend Append RPC to %d with conflicted index %d\n",
					rf.me, i, next)
			case !reply.Success && reply.ConflictTerm != -1:
				j := len(llog) - 1
				for ; j > 0; j-- {
					if llog[j].Term == reply.ConflictTerm {
						break
					}
				}
				next = j + 1
				DTPrintf("%d try to resend Append RPC to %d with decremented index %d\n",
					rf.me, i, next)
			case reply.Success:
				rf.state.setNextIndex(i, next+len(args.Entries))
				rf.state.setMatchIndex(i, args.PrevLogIndex+len(args.Entries))
				reply.index = i
				DTPrintf("%d: got Append RPC from %d for term %d\n", rf.me, i, term)
				appendReplyCh <- *reply
				return
			default:
				DTPrintf("!!! reply: %v, term: %d\n", reply, term)
				log.Fatal("!!! Incorrect Append RPC reply")
			}
		}
	}

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
		go sendAppend(i, next)
	}

}

func (rf *Raft) sendHeartbeat() chan AppendEntriesReply {
	term := rf.state.getCurrentTerm()
	leaderCommit := rf.state.getCommitIndex()
	prevLogIndex := rf.state.getLogLen() - 1
	prevLogTerm := rf.state.getLogEntry(prevLogIndex).Term

	DTPrintf("%d starts sending HB for term %d\n", rf.me, term)
	// the channel is only used for this term
	heartbeatReplyCh := make(chan AppendEntriesReply)

	sendAppend := func(i int) {
		args := AppendEntriesArgs{}
		args.Term = term
		args.PrevLogIndex = prevLogIndex
		args.PrevLogTerm = prevLogTerm
		args.LeaderCommit = leaderCommit
		reply := AppendEntriesReply{}
		DTPrintf("%d sends heartbeat to %d for term %d with args %v\n",
			rf.me, i, term, args)
		// Retry indefinitely and do not send outdated request
		for ok := false; !ok && term == rf.state.getCurrentTerm(); {
			ok = rf.peers[i].Call("Raft.AppendEntries", &args, &reply)
		}
		//DTPrintf("%d got heartbeat reply %v from %d for term %d\n",
		//rf.me, reply, i, term)
		heartbeatReplyCh <- reply
	}

	go func() {
		for i, _ := range rf.peers {
			if i == rf.me {
				continue
			}
			go sendAppend(i)
		}
	}()

	return heartbeatReplyCh
}
