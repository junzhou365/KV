package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"bytes"
	"encoding/gob"
	"labrpc"
	"log"
	"math/rand"
	"sync"
	"time"
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
//
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	state             RaftState
	rpcCh             chan chan rpcResp
	heartbeatInterval time.Duration
	commit            chan bool
	applyCh           chan ApplyMsg
	newEntry          chan int
}

type rpcResp struct {
	toFollower bool
}

const (
	RES_VOTE = iota
	RES_APPEND
)

const (
	FOLLOWER = iota
	CANDIDATE
	LEADER
)

func (rf *Raft) DiscardLogEntries(newIndex int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.state.discardLogEnries(newIndex)
}

func (rf *Raft) GetLogEntryTerm(index int) int {
	return rf.state.getLogEntryTerm(index)
}

// return CurrentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// Your code here (2A).
	return rf.state.getCurrentTerm(), rf.state.getRole() == LEADER
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := gob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	e.Encode(rf.state.getCurrentTerm())
	e.Encode(rf.state.getVotedFor())

	rf.state.rw.RLock()
	e.Encode(rf.state.Log)
	rf.state.rw.RUnlock()

	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := gob.NewDecoder(r)
	// d.Decode(&rf.xxx)
	// d.Decode(&rf.yyy)
	if data == nil || len(data) < 1 { // bootstrap without any state?
		rf.state.setCurrentTerm(0)
		rf.state.setVotedFor(-1)
		rf.state.rw.Lock()
		rf.state.Log = make([]RaftLogEntry, 1)
		rf.state.rw.Unlock()
	} else {
		r := bytes.NewBuffer(data)
		d := gob.NewDecoder(r)
		var term int
		d.Decode(&term)
		rf.state.setCurrentTerm(term)

		var votedFor int
		d.Decode(&votedFor)
		rf.state.setVotedFor(votedFor)

		rf.state.rw.Lock()
		d.Decode(&rf.state.Log)
		rf.state.rw.Unlock()
	}
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term        int
	CandidateId int

	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
	index       int // not needed for RPC
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	resCh := <-rf.rpcCh

	curTerm := rf.state.getCurrentTerm()
	curVotedFor := rf.state.getVotedFor()

	resp := rpcResp{toFollower: false}
	// update term
	if args.Term > curTerm {
		curTerm = args.Term
		rf.state.setCurrentTerm(curTerm)
		rf.state.setRole(FOLLOWER)
		curVotedFor = -1
		rf.state.setVotedFor(curVotedFor)
		resp.toFollower = true
	}

	resCh <- resp

	isLeaderTermValid := args.Term == curTerm
	isVotedForValid := curVotedFor == -1 || curVotedFor == args.CandidateId
	isEntryUpToDate := false
	// Is it most up-to-date?
	logLen := rf.state.getLogLen()
	lastLogIndex := logLen - 1
	lastLogEntry := rf.state.getLogEntry(lastLogIndex)
	DTPrintf("%d received RequestVote RPC req %+v | votedFor: %d, lastLogIndex: %d, logLen: %d\n",
		rf.me, args, curVotedFor, lastLogIndex, logLen)
	if logLen == 1 || args.LastLogTerm > lastLogEntry.Term ||
		args.LastLogTerm == lastLogEntry.Term && args.LastLogIndex >= lastLogIndex {
		isEntryUpToDate = true
	}

	if isLeaderTermValid && isVotedForValid && isEntryUpToDate {
		reply.VoteGranted = true
		// XXX: VotedFor is implicitly updated in args.Term > term case
		rf.state.setVotedFor(args.CandidateId)
	} else {
		reply.VoteGranted = false
	}

	reply.Term = curTerm

	DTPrintf("%d voted back to %d with reply %v\n", rf.me, args.CandidateId, reply)
}

type AppendEntriesArgs struct {
	Term int

	PrevLogIndex int
	PrevLogTerm  int
	Entries      []RaftLogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term          int
	Success       bool
	index         int // no need for RPC
	ConflictIndex int // optimization
	ConflictTerm  int // optimization
}

func min(a int, b int) int {
	if a < b {
		return a
	}
	return b
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	var resCh chan rpcResp
	resCh = <-rf.rpcCh

	curTerm := rf.state.getCurrentTerm()
	resp := rpcResp{toFollower: false}
	switch {
	case args.Term > curTerm:
		curTerm = args.Term
		rf.state.setCurrentTerm(curTerm)
		rf.state.setRole(FOLLOWER)
		resp.toFollower = true
	case args.Term < curTerm:
		reply.Term = curTerm
		reply.Success = false
		//DTPrintf("%d received reset for term %d from stale leader\n", rf.me, args.Term)
		// Did not receive RPC from *current* leader
		resp.toFollower = false
		resCh <- resp
		return
	}

	// At this point the role could only be FOLLOWER and CANDIDATE.
	if rf.state.getRole() == LEADER {
		DTPrintf("Term %d Leader %d got Append RPC from another same term leader\n",
			args.Term, rf.me)
		log.Fatal("In AppendEntriesReply Dead!")
	}

	rf.state.setRole(FOLLOWER)
	resCh <- resp

	reply.Term = curTerm
	logLen := rf.state.getLogLen()

	switch {
	case args.PrevLogIndex > logLen-1:
		reply.Success = false
		reply.ConflictIndex = logLen
		reply.ConflictTerm = -1

	case rf.state.getLogEntryTerm(args.PrevLogIndex) != args.PrevLogTerm:
		reply.Success = false
		reply.ConflictTerm = rf.state.getLogEntryTerm(args.PrevLogIndex)
		rf.state.rw.RLock()
		// find the first entry that has the conflicting term
		for i, l := range rf.state.Log {
			if l.Term == reply.ConflictTerm {
				reply.ConflictIndex = i
				break
			}
		}
		rf.state.rw.RUnlock()

	default:
		reply.Success = true
		deleteIndex := args.PrevLogIndex + 1
		for _, entry := range args.Entries {
			switch {
			case deleteIndex == logLen ||
				rf.state.getLogEntryTerm(deleteIndex) != entry.Term:
				// delete conflicted entries
				rf.state.rw.Lock()
				rf.state.Log = append(rf.state.Log[:deleteIndex],
					args.Entries[deleteIndex-args.PrevLogIndex-1:]...)
				rf.state.rw.Unlock()
				break
			default:
				deleteIndex++
			}
		}

		lastNewEntryIndex := args.PrevLogIndex + len(args.Entries)
		if args.LeaderCommit > rf.state.getCommitIndex() {
			rf.state.setCommitIndex(min(args.LeaderCommit, lastNewEntryIndex))
			go func() { rf.commit <- true }()
		}
	}
	DTPrintf("%d received Append for term %d from leader\n", rf.me, args.Term)
}

type InstallSnapshotArgs struct {
	Term                   int
	LastIncludedEntryIndex int
	LastIncludedEntryTerm  int
	Data                   []byte
}

type InstallSnapshotReply struct {
	Term int
}

func (rf *Raft) InstallSnapshot(
	args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	var resCh chan rpcResp
	resCh = <-rf.rpcCh

	resp := rpcResp{toFollower: false}
	if rf.state.getCurrentTerm() < args.Term {
		rf.state.setCurrentTerm(args.Term)
		rf.state.setRole(FOLLOWER)
		resp.toFollower = true
		resCh <- resp

		reply.Term = args.Term
		return
	}

	resCh <- resp

	if rf.state.getLogLen()-1 >= args.LastIncludedEntryIndex &&
		args.LastIncludedEntryTerm == rf.state.getLogEntryTerm(args.LastIncludedEntryIndex) {
		rf.applyCh <- ApplyMsg{UseSnapshot: false, Snapshot: args.Data}
		rf.state.discardLogEnries(args.LastIncludedEntryIndex)
		return
	}

	rf.state.discardLogEnries(rf.state.getLogLen() - 1)
	// reply snapshot
	rf.applyCh <- ApplyMsg{UseSnapshot: true, Snapshot: args.Data}

}

func (rf *Raft) commitLoop() {
	for {
		select {
		case <-rf.commit:
			commitIndex := rf.state.getCommitIndex()
			lastApplied := rf.state.getLastApplied()
			for commitIndex > lastApplied {
				lastApplied++
				rf.state.setLastApplied(lastApplied)
				command := rf.state.getLogEntry(lastApplied).Command
				rf.applyCh <- ApplyMsg{Index: lastApplied, Command: command}
				DTPrintf("%d: applied the command %v at log index %d\n",
					rf.me, command, lastApplied)
			}
		}
	}
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).
	if rf.state.getRole() != LEADER {
		isLeader = false
	} else {
		term = rf.state.getCurrentTerm()
		DTPrintf("%d: Start is called with command %+v\n", rf.me, command)
		rf.state.appendLogEntry(RaftLogEntry{Term: term, Command: command})
		index = rf.state.getLogLen() - 1
		go func() { rf.newEntry <- index }()
		DTPrintf("%d: Start call on leader with command %v finished\n", rf.me, command)
	}

	return index, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
}

// Long time main running goroutine
func (rf *Raft) run() {
	for {
		switch role := rf.state.getRole(); role {
		case FOLLOWER:
			rf.runFollower()
		case CANDIDATE:
			rf.runCandidate()
		case LEADER:
			rf.runLeader()
		}
	}
}

// returns a duration in [300, 450) millisecond
func getElectionTimeout() time.Duration {
	s := rand.NewSource(time.Now().UnixNano())
	r := rand.New(s)
	return time.Duration(r.Intn(150))*time.Millisecond + 300*time.Millisecond
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	rf.state.setRole(FOLLOWER)

	rf.heartbeatInterval = 100 * time.Millisecond // max number test permits
	rf.rpcCh = make(chan chan rpcResp)
	rf.newEntry = make(chan int)

	rf.commit = make(chan bool)
	rf.applyCh = applyCh

	go rf.run()
	go rf.commitLoop()
	DTPrintf("%d is created, log len: %d\n", rf.me, rf.state.getLogLen())

	return rf
}
