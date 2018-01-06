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
	var appendReplyCh <-chan AppendEntriesReply
	last_seen_index := 0

	respCh := make(chan rpcResp)

	done := make(chan interface{})
	// cannot use defer close(done) here because the "done" will be bound to a
	// future closed channel

	for {
		select {
		case rf.rpcCh <- respCh:
			if r := <-respCh; r.toFollower {
				close(done)
				return
			}

		case <-heartbeatTimer:
			close(done)
			done = make(chan interface{})
			heartbeatTimer = time.After(rf.heartbeatInterval)
			appendReplyCh = rf.appendNewEntries(done, true, term, rf.state.getLogLen()-1)

		case index := <-rf.newEntry:
			if index > last_seen_index {
				close(done)
				done = make(chan interface{})
				appendReplyCh = rf.appendNewEntries(done, false, term, index)
				last_seen_index = index
			}

		case reply := <-appendReplyCh:
			if !reply.Success && reply.Term > term {
				rf.state.setCurrentTerm(reply.Term)
				rf.state.setRole(FOLLOWER)
				close(done)
				return
			}
			rf.updateCommitIndex(term)
		}
	}
}
func (rf *Raft) updateCommitIndex(term int) {
	rf.state.rw.Lock()
	defer rf.state.rw.Unlock()

	getLogLen := func() int {
		return len(rf.state.Log) + rf.state.lastIncludedEntryIndex
	}

	getLogEntryTerm := func(i int) int {
		return rf.state.Log[i-rf.state.lastIncludedEntryIndex].Term
	}

	for n := getLogLen() - 1; n > rf.state.commitIndex; n-- {
		count := 1
		for j := 0; j < len(rf.peers); j++ {
			if j != rf.me && rf.state.matchIndexes[j] >= n {
				//DTPrintf("%d: find %d's match %d >= n %d", rf.me, j, rf.state.matchIndexes[j], n)
				count++
			}
		}
		DTPrintf("%d: the count is %d, n is %d", rf.me, count, n)
		if count > len(rf.peers)/2 && n > 0 && term == getLogEntryTerm(n) {
			rf.state.commitIndex = n
			go func() { rf.commit <- true }()
			return
		}
	}
}

func (rf *Raft) sendAppend(done <-chan interface{},
	appendReplyCh chan AppendEntriesReply, i int, heartbeat bool, term int,
	next int, new_index int) {
	for {
		args := AppendEntriesArgs{}
		args.Term = term
		args.PrevLogIndex = next - 1
		DTPrintf("%d: logLen: %d, prevLogIndex: %d\n", rf.me, rf.state.getLogLen(), next-1)
		args.PrevLogTerm = rf.state.getLogEntryTerm(args.PrevLogIndex)
		args.LeaderCommit = rf.state.getCommitIndex()

		if !heartbeat {
			rf.state.rw.RLock()
			args.Entries = append(args.Entries, rf.state.Log[next:new_index+1]...)
			rf.state.rw.RUnlock()
		}
		reply := new(AppendEntriesReply)
		DTPrintf("%d sends Append RPC to %d for term %d. Args: pli: %d, plt: %d, enries: %v\n",
			rf.me, i, term, args.PrevLogIndex, args.PrevLogTerm, args.Entries)

		ok := rf.peers[i].Call("Raft.AppendEntries", &args, reply)

		select {
		case <-done:
			return
		default:
		}

		switch {
		// Retry. Failed reply makes other cases meaningless
		case !ok:
			continue

		case !reply.Success && reply.Term > term:
			select {
			case appendReplyCh <- *reply:
			case <-done:
			}
			return

		case !reply.Success && reply.ConflictTerm == -1:
			// Follower doesn't have the entry with that term. len of
			// follower's log is shorter than the leader's.
			next = reply.ConflictIndex

		case !reply.Success && reply.ConflictTerm != -1:
			j := new_index
			// Find the last entry of the conflicting term. The conflicting
			// server will delete all entries after this new next
			// XXX: might need update
			for ; j > 0; j-- {
				if rf.state.getLogEntry(j).Term == reply.ConflictTerm {
					break
				}
			}
			next = j + 1

		case reply.Success:
			iNext := next + len(args.Entries)
			iMatch := args.PrevLogIndex + len(args.Entries)
			DTPrintf("%d: update %d's next %d and match %d\n", rf.me, i,
				iNext, iMatch)
			rf.state.setNextIndex(i, iNext)
			rf.state.setMatchIndex(i, iMatch)
			if rf.state.getLogLen()-1 >= iNext {
				DTPrintf("%d: append new logs after truncating %d's logs\n", rf.me, i)
				rf.sendAppend(done, appendReplyCh, i, false, term, iNext, new_index)
			}
			if !heartbeat {
				select {
				case appendReplyCh <- *reply:
				case <-done:
				}
			}
			return

		default:
			DTPrintf("!!! reply: %v, term: %d\n", reply, term)
			log.Fatal("!!! Incorrect Append RPC reply")
		}
	}
}

// Should run in order
func (rf *Raft) appendNewEntries(done <-chan interface{}, heartbeat bool,
	term int, new_index int) <-chan AppendEntriesReply {

	// save before a change
	rf.persist()

	appendReplyCh := make(chan AppendEntriesReply)

	DTPrintf("%d: starts sending Append RPCs heartbeat: %t.\n", rf.me, heartbeat)
	for i := 0; i < len(rf.peers); i++ {
		next := rf.state.getNextIndex(i)
		if !heartbeat && next > new_index {
			DTPrintf("%d: follower %d's next %d than the new index\n", rf.me, i, next)
			continue
		}

		if i == rf.me {
			continue
		}
		go rf.sendAppend(done, appendReplyCh, i, heartbeat, term, next, new_index)
	}

	return appendReplyCh
}
