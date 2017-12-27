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
	last_seen_index := 0

	respCh := make(chan rpcResp)
	appended := 1

	for {
		select {
		case rf.rpcCh <- respCh:
			if r := <-respCh; r.toFollower {
				return
			}

		case <-heartbeatTimer:
			heartbeatTimer = time.After(rf.heartbeatInterval)
			heartbeatReplyCh = rf.sendHeartbeat()

		case r := <-heartbeatReplyCh:
			if r.Term > term {
				rf.state.setRole(FOLLOWER)
				rf.state.setCurrentTerm(r.Term)
				return
			}

		case index := <-rf.newEntry:
			if index > last_seen_index {
				appendReplyCh = make(chan AppendEntriesReply)
				appended = 1
				go rf.appendNewEntries(term, index, appendReplyCh)
				last_seen_index = index
			}

		case reply := <-appendReplyCh:
			if !reply.Success && reply.Term > term {
				rf.state.setCurrentTerm(reply.Term)
				rf.state.setRole(FOLLOWER)
				return
			}
			appended++
			if appended > len(rf.peers)/2 {
				rf.updateCommitIndex(term)
			}
		}
	}
}
func (rf *Raft) updateCommitIndex(term int) {
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
func (rf *Raft) appendNewEntries(
	term int, new_index int, appendReplyCh chan AppendEntriesReply) {

	sendAppend := func(i int, next int) {
		for {
			args := AppendEntriesArgs{}
			args.Term = term
			args.PrevLogIndex = next - 1
			args.PrevLogTerm = rf.state.getLogEntry(args.PrevLogIndex).Term
			args.LeaderCommit = rf.state.getCommitIndex()

			rf.state.rw.RLock()
			args.Entries = append(args.Entries, rf.state.Log[next:new_index+1]...)
			rf.state.rw.RUnlock()
			reply := new(AppendEntriesReply)
			DTPrintf("%d sends Append RPC to %d for term %d. Args: pli: %d, plt: %d\n",
				rf.me, i, term, args.PrevLogIndex, args.PrevLogTerm)

			ok := rf.peers[i].Call("Raft.AppendEntries", &args, reply)
			switch {
			// term changed
			case term != rf.state.getCurrentTerm():
				return

			// Retry. Failed reply makes other cases meaningless
			case !ok:
				continue

			case !reply.Success && reply.Term > term:
				appendReplyCh <- *reply
				return

			case !reply.Success && reply.ConflictTerm == -1:
				// Follower doesn't have the entry with that term. len of
				// follower's log is shorter than the leader's.
				next = reply.ConflictIndex

			case !reply.Success && reply.ConflictTerm != -1:
				j := new_index
				for ; j > 0; j-- {
					if rf.state.getLogEntry(j).Term == reply.ConflictTerm {
						break
					}
				}
				next = j + 1

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

	DTPrintf("%d: starts sending Append RPCs.\n", rf.me)
	for i := 0; i < len(rf.peers); i++ {
		next := rf.state.getNextIndex(i)
		if next > new_index {
			DTPrintf("%d: follower %d's next %d than the new index\n", rf.me, i, next)
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

	// the channel is only used for this term
	heartbeatReplyCh := make(chan AppendEntriesReply)

	sendAppend := func(i int) {
		args := AppendEntriesArgs{}
		args.Term = term
		args.PrevLogIndex = prevLogIndex
		args.PrevLogTerm = prevLogTerm
		args.LeaderCommit = leaderCommit
		reply := AppendEntriesReply{}
		// Retry indefinitely and do not send outdated request
		for ok := false; !ok && term == rf.state.getCurrentTerm(); {
			ok = rf.peers[i].Call("Raft.AppendEntries", &args, &reply)
		}
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
