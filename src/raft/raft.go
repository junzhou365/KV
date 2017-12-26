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
	resourceCh        chan chan resourceRes
	electionProcess   *electionProcess
	leaderProcess     *leaderProcess
	heartbeatInterval time.Duration
	commit            chan bool
	applyCh           chan ApplyMsg
}

type resourceRes struct {
	kind  int
	reset bool
}

const (
	RES_VOTE = iota
	RES_APPEND
)

type electionProcess struct {
	start chan bool
}

type leaderProcess struct {
	start    chan bool
	newEntry chan int
}

const (
	FOLLOWER = iota
	CANDIDATE
	LEADER
)

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

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	DTPrintf("%d enter RequestVote, the arg: %+v\n", rf.me, args)
	// Your code here (2A, 2B).
	var resCh chan resourceRes
	resCh = <-rf.resourceCh
	rf.persist()

	curTerm := rf.state.getCurrentTerm()
	curVotedFor := rf.state.getVotedFor()

	// update term
	if args.Term > curTerm {
		curTerm = args.Term
		rf.state.setCurrentTerm(curTerm)
		rf.state.setRole(FOLLOWER)
		curVotedFor = -1
		rf.state.setVotedFor(curVotedFor)
		go func() { rf.electionProcess.start <- true }()
	}

	isLeaderTermValid := args.Term == curTerm
	isVotedForValid := curVotedFor == -1 || curVotedFor == args.CandidateId
	isEntryUpToDate := false
	// Is it most up-to-date?
	logLen := rf.state.getLogLen()
	lastLogIndex := logLen - 1
	lastLogEntry := rf.state.getLogEntry(lastLogIndex)
	DTPrintf("%d received RequestVote RPC req | votedFor: %d, lastLogIndex: %d, logLen: %d\n",
		rf.me, curVotedFor, lastLogIndex, logLen)
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
	resCh <- resourceRes{reset: reply.VoteGranted, kind: RES_VOTE}
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
	var resCh chan resourceRes
	resCh = <-rf.resourceCh
	rf.persist()

	//DTPrintf("%d received Append RPC req for term %d. Its term is %d, commit is %d, log len is %d\n",
	//rf.me, args.Term, rf.state.CurrentTerm, rf.state.commitIndex, len(rf.state.Log))

	role := rf.state.getRole()
	curTerm := rf.state.getCurrentTerm()
	switch {
	case args.Term > curTerm:
		curTerm = args.Term
		rf.state.setCurrentTerm(curTerm)
		switch {
		case role == CANDIDATE:
			DTPrintf("%d switched to follower\n", rf.me)
		case role == LEADER:
			DTPrintf("%d switched to follower\n", rf.me)
		case role == FOLLOWER:
			DTPrintf("%d update its follower term\n", rf.me)
		}
		role = FOLLOWER
		rf.state.setRole(role)
	case args.Term < curTerm:
		reply.Term = curTerm
		reply.Success = false
		DTPrintf("%d received reset from stale leader\n", rf.me)
		// Did not receive RPC from *current* leader
		resCh <- resourceRes{reset: false, kind: RES_APPEND}
		return
	}

	if role == LEADER {
		DTPrintf("Term %d Leader %d got Append RPC from another same term leader\n", args.Term, rf.me)
		log.Fatal("In AppendEntriesReply Dead!")
	}

	reply.Term = curTerm

	logLen := rf.state.getLogLen()

	switch {
	case args.PrevLogIndex > logLen-1:
		reply.Success = false
		reply.ConflictIndex = logLen
		reply.ConflictTerm = -1
	case rf.state.getLogEntry(args.PrevLogIndex).Term != args.PrevLogTerm:
		reply.Success = false
		reply.ConflictTerm = rf.state.getLogEntry(args.PrevLogIndex).Term
		rf.state.rw.RLock()
		for i, l := range rf.state.Log {
			if l.Term == reply.ConflictTerm {
				reply.ConflictIndex = i
				break
			}
		}
		rf.state.rw.RUnlock()
	default:
		reply.Success = true
		if len(args.Entries) > 0 {
			deleteIndex := args.PrevLogIndex + 1
			for _, entry := range args.Entries {
				switch {
				case deleteIndex == logLen ||
					rf.state.getLogEntry(deleteIndex).Term != entry.Term:
					rf.state.rw.Lock()
					rf.state.Log = append(rf.state.Log[:deleteIndex],
						args.Entries[deleteIndex-args.PrevLogIndex-1:]...)
					rf.state.rw.Unlock()
					break
				default:
					deleteIndex++
				}
			}
			DTPrintf("%d Append RPC done Success: %t for term %d, the c_index is %d, c_term is %d\n",
				rf.me, reply.Success, args.Term, reply.ConflictIndex, reply.ConflictTerm)
			DTPrintf("%d: loglen: %d\n", rf.me, rf.state.getLogLen())
		}
		if args.LeaderCommit > rf.state.getCommitIndex() {
			rf.state.setCommitIndex(min(args.LeaderCommit, logLen-1))
			go func() { rf.commit <- true }()
		}
	}

	resCh <- resourceRes{reset: true, kind: RES_APPEND}
}

func (rf *Raft) commitLoop() {
	for {
		select {
		case <-rf.commit:
			commitIndex := rf.state.getCommitIndex()
			lastApplied := rf.state.getLastApplied()
			DTPrintf("%d: try to apply with commitIndex: %d, lastApplied: %d\n", rf.me, commitIndex, lastApplied)
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
		DTPrintf("%d: Start is called with command %v\n", rf.me, command)
		rf.state.appendLogEntry(RaftLogEntry{Term: term, Command: command})
		index = rf.state.getLogLen() - 1
		go func() { rf.leaderProcess.newEntry <- index }()
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

type voteResult struct {
	role int
	term int
}

func (rf *Raft) elect() (<-chan time.Time, chan RaftState) {
	DTPrintf("<<<< Triggered, node: %d >>>>\n", rf.me)

	role := rf.state.getRole()
	if role == LEADER {
		DTPrintf("%d: Wrong rf role %d\n", rf.me, rf.state.role)
		log.Fatal("In electionLoop, Wrong Raft Role. Dead!!!")
	}
	role = CANDIDATE
	rf.state.setRole(role)
	term := rf.state.getCurrentTerm()
	term++
	rf.state.setCurrentTerm(term)
	DTPrintf("%d becomes candidate for term %d\n", rf.me, term)
	rf.state.setVotedFor(rf.me)

	t := getRandomDuration()
	//DTPrintf("%d got new timer %v", rf.me, t)
	startElecCh := time.After(t)

	args := new(RequestVoteArgs)
	args.Term = term
	args.CandidateId = rf.me
	args.LastLogIndex = rf.state.getLogLen() - 1
	args.LastLogTerm = rf.state.getLogEntry(args.LastLogIndex).Term

	electionDone := make(chan RaftState)
	go func(vDone chan RaftState) {
		term := rf.state.getCurrentTerm()
		// Should not use sync.WaitGroup here because that might block for too long time.
		//replies := make([]RequestVoteReply, len(rf.peers))
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

	return startElecCh, electionDone
}

func (rf *Raft) loop() {
	var heartbeatDone chan RaftState
	// Used for sinking outdated RPC replies
	var appendDone chan AppendEntriesReply
	var electionDone chan RaftState
	var nextHBCh <-chan time.Time
	var startElecCh <-chan time.Time
	var resCh chan resourceRes
	last_seen_nei := 0
	for {
		if resCh == nil {
			resCh = make(chan resourceRes)
		}
		select {
		case <-startElecCh:
			startElecCh, electionDone = rf.elect()
		case r := <-electionDone:
			electionDone = nil
			rf.state.setRole(r.role)
			rf.state.setCurrentTerm(r.CurrentTerm)
			if r.role == LEADER {
				DTPrintf("======= %d become a LEADER for term %d\n", rf.me, r.CurrentTerm)
				//DTPrintf("======= %d become a LEADER for term %d, its log len is %d, committed at %d =======\n",
				//rf.me, r.CurrentTerm, len(rf.state.Log), rf.state.commitIndex)
				go func() { rf.leaderProcess.start <- true }()
			} else {
				DTPrintf("%d is not a leader for term %d\n", rf.me, r.CurrentTerm)
			}
		case <-rf.electionProcess.start:
			startElecCh = time.After(getRandomDuration())
			electionDone = nil
			nextHBCh = nil
			heartbeatDone = nil
			appendDone = nil
		case rf.resourceCh <- resCh:
			// true is to reset
			switch r := <-resCh; {
			case r.kind == RES_VOTE && r.reset:
				go func() { rf.electionProcess.start <- true }()
			case r.kind == RES_APPEND && r.reset:
				go func() { rf.electionProcess.start <- true }()
			}
		case <-nextHBCh:
			nextHBCh = time.After(rf.heartbeatInterval)
			heartbeatDone = rf.sendHB()
		case r := <-heartbeatDone:
			heartbeatDone = nil
			if r.role == FOLLOWER {
				rf.state.setRole(r.role)
				rf.state.setCurrentTerm(r.CurrentTerm)
				DTPrintf("%d discovered new leader for term %d, switch to follower", rf.me, r.CurrentTerm)
				go func() {
					rf.electionProcess.start <- true
				}()
			}
		case <-rf.leaderProcess.start:
			startElecCh = nil
			electionDone = nil
			nextHBCh = time.After(0)
			heartbeatDone = nil
			appendDone = nil
			rf.state.rw.Lock()
			rf.state.nextIndexes = make([]int, len(rf.peers))
			for i := 0; i < len(rf.peers); i++ {
				rf.state.nextIndexes[i] = len(rf.state.Log)
			}
			rf.state.matchIndexes = make([]int, len(rf.peers))
			rf.state.rw.Unlock()
		case nei := <-rf.leaderProcess.newEntry:
			if nei > last_seen_nei {
				DTPrintf("%d: newEntry at %d\n", rf.me, nei)
				appendDone = make(chan AppendEntriesReply)
				go rf.appendNewEntries(appendDone)
				last_seen_nei = nei
			}
		case reply := <-appendDone:
			appendDone = nil
			if !reply.Success {
				rf.state.setCurrentTerm(reply.Term)
				rf.state.setRole(FOLLOWER)
				DTPrintf("%d discovered new leader for term %d, switch to follower",
					rf.me, reply.Term)
				nextHBCh = nil
				go func() { rf.electionProcess.start <- true }()
			} else {
				DTPrintf("%d updated its commitIndex\n", rf.me)
			}
		}
	}
}

// returns a duration in [300, 450) millisecond
func getRandomDuration() time.Duration {
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
	rf.resourceCh = make(chan chan resourceRes)
	rf.electionProcess = &electionProcess{start: make(chan bool)}
	rf.leaderProcess = &leaderProcess{start: make(chan bool), newEntry: make(chan int)}

	rf.commit = make(chan bool)
	rf.applyCh = applyCh

	go rf.loop()
	go rf.commitLoop()
	go func() {
		rf.electionProcess.start <- true
	}()
	DTPrintf("%d is created\n", rf.me)

	return rf
}
