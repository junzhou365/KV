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

type RaftEntry struct {
	Term    int
	Command interface{}
}

type RaftState struct {
	CurrentTerm int
	VotedFor    int
	role        int
	Log         []RaftEntry // initialized with term 0 entry (empty RaftEntry)

	commitIndex int
	lastApplied int

	nextIndexes  []int
	matchIndexes []int
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
	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	term = rf.state.CurrentTerm
	isleader = rf.state.role == LEADER
	rf.mu.Unlock()
	return term, isleader
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
	e.Encode(rf.state.CurrentTerm)
	e.Encode(rf.state.VotedFor)
	e.Encode(rf.state.Log)
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
		rf.state.CurrentTerm = 0
		rf.state.VotedFor = -1
		rf.state.Log = make([]RaftEntry, 1)
		return
	} else {
		r := bytes.NewBuffer(data)
		d := gob.NewDecoder(r)
		d.Decode(&rf.state.CurrentTerm)
		d.Decode(&rf.state.VotedFor)
		d.Decode(&rf.state.Log)
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
	// Your code here (2A, 2B).
	var resCh chan resourceRes
	resCh = <-rf.resourceCh
	rf.mu.Lock()
	rf.persist()

	// update term
	if args.Term > rf.state.CurrentTerm {
		rf.state.CurrentTerm = args.Term
		rf.state.role = FOLLOWER
		rf.state.VotedFor = -1
		go func() { rf.electionProcess.start <- true }()
	}

	isLeaderTermValid := args.Term == rf.state.CurrentTerm
	isVotedForValid := rf.state.VotedFor == -1 || rf.state.VotedFor == args.CandidateId
	isEntryUpToDate := false
	// Is it most up-to-date?
	lastLogIndex := len(rf.state.Log) - 1
	DTPrintf("%d received RequestVote RPC req | lastLogIndex: %d\n", rf.me, lastLogIndex)
	if len(rf.state.Log) == 1 ||
		args.LastLogTerm > rf.state.Log[lastLogIndex].Term ||
		args.LastLogTerm == rf.state.Log[lastLogIndex].Term &&
			args.LastLogIndex >= lastLogIndex {
		isEntryUpToDate = true
	}

	if isLeaderTermValid && isVotedForValid && isEntryUpToDate {
		reply.VoteGranted = true
		// XXX: VotedFor is implicitly updated in args.Term > term case
		rf.state.VotedFor = args.CandidateId
	} else {
		reply.VoteGranted = false
	}

	reply.Term = rf.state.CurrentTerm

	DTPrintf("%d voted back to %d with reply %v\n", rf.me, args.CandidateId, reply)
	rf.mu.Unlock()
	resCh <- resourceRes{reset: reply.VoteGranted, kind: RES_VOTE}
}

type AppendEntriesArgs struct {
	Term int

	PrevLogIndex int
	PrevLogTerm  int
	Entries      []RaftEntry
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
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.persist()

	DTPrintf("%d received Append RPC req for term %d. Its term is %d, commit is %d, log len is %d\n",
		rf.me, args.Term, rf.state.CurrentTerm, rf.state.commitIndex, len(rf.state.Log))

	role := rf.state.role
	switch {
	case args.Term > rf.state.CurrentTerm:
		rf.state.CurrentTerm = args.Term
		switch {
		case role == CANDIDATE:
			DTPrintf("%d switched to follower\n", rf.me)
		case role == LEADER:
			DTPrintf("%d switched to follower\n", rf.me)
		case role == FOLLOWER:
			DTPrintf("%d update its follower term\n", rf.me)
		}
		rf.state.role = FOLLOWER
	case args.Term < rf.state.CurrentTerm:
		reply.Term = rf.state.CurrentTerm
		reply.Success = false
		DTPrintf("%d received reset from stale leader\n", rf.me)
		// Did not receive RPC from *current* leader
		resCh <- resourceRes{reset: false, kind: RES_APPEND}
		return
	}

	if rf.state.role == LEADER {
		DTPrintf("Term %d Leader %d got Append RPC from another same term leader\n", args.Term, rf.me)
		log.Fatal("In AppendEntriesReply Dead!")
	}

	reply.Term = rf.state.CurrentTerm

	switch {
	case args.PrevLogIndex > len(rf.state.Log)-1:
		reply.Success = false
		reply.ConflictIndex = len(rf.state.Log)
		reply.ConflictTerm = -1
	case rf.state.Log[args.PrevLogIndex].Term != args.PrevLogTerm:
		reply.Success = false
		reply.ConflictTerm = rf.state.Log[args.PrevLogIndex].Term
		for i, l := range rf.state.Log {
			if l.Term == reply.ConflictTerm {
				reply.ConflictIndex = i
				break
			}
		}
	default:
		reply.Success = true
		if len(args.Entries) > 0 {
			deleteIndex := args.PrevLogIndex + 1
			for _, entry := range args.Entries {
				switch {
				case deleteIndex == len(rf.state.Log) ||
					rf.state.Log[deleteIndex].Term != entry.Term:
					rf.state.Log = append(rf.state.Log[:deleteIndex],
						args.Entries[deleteIndex-args.PrevLogIndex-1:]...)
					break
				default:
					deleteIndex++
				}
			}
			DTPrintf("%d Append RPC done Success: %t for term %d, the c_index is %d, c_term is %d\n",
				rf.me, reply.Success, args.Term, reply.ConflictIndex, reply.ConflictTerm)
		}
		if args.LeaderCommit > rf.state.commitIndex {
			rf.state.commitIndex = min(args.LeaderCommit, len(rf.state.Log)-1)
			go func() { rf.commit <- true }()
		}
	}

	resCh <- resourceRes{reset: true, kind: RES_APPEND}
}

func (rf *Raft) commitLoop() {
	for {
		select {
		case <-rf.commit:
			rf.mu.Lock()
			commitIndex := rf.state.commitIndex
			lastApplied := rf.state.lastApplied
			rf.mu.Unlock()
			DTPrintf("%d: try to apply with commitIndex: %d, lastApplied: %d\n", rf.me, commitIndex, lastApplied)
			for commitIndex > lastApplied {
				lastApplied++
				rf.mu.Lock()
				rf.state.lastApplied = lastApplied
				command := rf.state.Log[lastApplied].Command
				rf.mu.Unlock()
				rf.applyCh <- ApplyMsg{Index: lastApplied, Command: command}
				DTPrintf("%d: applied the command %v at log index %d\n",
					rf.me, command, lastApplied)
			}
		}
	}
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
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state.role != LEADER {
		isLeader = false
	} else {
		DTPrintf("%d: Start is called with command %v\n", rf.me, command)
		rf.state.Log = append(rf.state.Log, RaftEntry{Term: rf.state.CurrentTerm, Command: command})
		index = len(rf.state.Log) - 1
		term = rf.state.CurrentTerm
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
	rf.mu.Lock()
	if rf.state.role == LEADER {
		DTPrintf("%d: Wrong rf role %d\n", rf.me, rf.state.role)
		log.Fatal("In electionLoop, Wrong Raft Role. Dead!!!")
	}
	rf.state.role = CANDIDATE
	rf.state.CurrentTerm++
	term := rf.state.CurrentTerm
	DTPrintf("%d becomes candidate for term %d\n", rf.me, term)
	rf.state.VotedFor = rf.me
	rf.mu.Unlock()

	t := getRandomDuration()
	//DTPrintf("%d got new timer %v", rf.me, t)
	startElecCh := time.After(t)

	args := new(RequestVoteArgs)
	args.Term = term
	args.CandidateId = rf.me
	args.LastLogIndex = len(rf.state.Log) - 1
	args.LastLogTerm = rf.state.Log[args.LastLogIndex].Term

	electionDone := make(chan RaftState)
	go func(vDone chan RaftState) {
		rf.mu.Lock()
		term := rf.state.CurrentTerm
		rf.mu.Unlock()
		// Should not use sync.WaitGroup here because that might block for too long time.
		//replies := make([]RequestVoteReply, len(rf.peers))
		replyCh := make(chan RequestVoteReply)
		for i := 0; i < len(rf.peers); i++ {
			if i == rf.me {
				// Simulate that the server responds to its own RPC.
				rf.mu.Lock()
				rf.persist()
				rf.mu.Unlock()
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

// Should run in order
func (rf *Raft) appendNewEntries(appendDone chan AppendEntriesReply) {
	rf.mu.Lock()
	var llog []RaftEntry
	llog = append(llog, rf.state.Log...)
	DTPrintf("%d: starts sending Append RPCs.\nCurrent log len is %d\n", rf.me, len(llog))
	term := rf.state.CurrentTerm
	role := rf.state.role
	var nextIndexes []int
	nextIndexes = append(nextIndexes, rf.state.nextIndexes...)
	leaderCommit := rf.state.commitIndex
	rf.mu.Unlock()

	if role != LEADER {
		return
	}

	replyCh := make(chan AppendEntriesReply)
	for i, next := range nextIndexes {
		if next > len(llog)-1 {
			DTPrintf("%d: follower %d's next %d than last log index\n", rf.me, i, next)
			continue
		}

		if i == rf.me {
			// Simulate that the server responds to its own RPC.
			rf.mu.Lock()
			rf.persist()
			rf.mu.Unlock()
			continue
		}
		go func(i int, next int) {
			for {
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
					if !ok {
						DTPrintf("%d sends Append RPC to %d for term %d failed. Retry...\n", rf.me, i, term)
					}
				}
				switch {
				case !reply.Success && reply.Term > term:
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
					rf.mu.Lock()
					rf.state.nextIndexes[i] = next + len(args.Entries)
					rf.state.matchIndexes[i] = args.PrevLogIndex + len(args.Entries)
					rf.mu.Unlock()
					reply.index = i
					DTPrintf("%d: got Append RPC from %d for term %d\n", rf.me, i, term)
					replyCh <- *reply
					return
				default:
					DTPrintf("!!! reply: %v, term: %d\n", reply, term)
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

		rf.mu.Lock()
		count := 1
		for n := len(rf.state.Log) - 1; n > rf.state.commitIndex; n-- {
			//DTPrintf("%d: try to find n. matches: %v, n: %d, commit: %d\n", rf.me, rf.state.matchIndexes, n, rf.state.commitIndex)
			for j := 0; j < len(rf.peers); j++ {
				if j != rf.me && rf.state.matchIndexes[j] >= n {
					count++
				}
			}
			if count > len(rf.peers)/2 && n > 0 && rf.state.CurrentTerm == rf.state.Log[n].Term {
				DTPrintf("%d: commitIndex %d is updated to %d\n", rf.me, rf.state.commitIndex, n)
				rf.state.commitIndex = n
				go func() { rf.commit <- true }()
				break
			}
		}
		rf.mu.Unlock()
	}

	appendDone <- AppendEntriesReply{Term: term, Success: true}
}

func (rf *Raft) sendHB() chan RaftState {
	rf.mu.Lock()
	term := rf.state.CurrentTerm
	leaderCommit := rf.state.commitIndex
	prevLogIndex := len(rf.state.Log) - 1
	prevLogTerm := rf.state.Log[prevLogIndex].Term
	rf.mu.Unlock()

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
					if !ok {
						DTPrintf("%d send heartbeat to %d for term %d failed. Retry...\n", rf.me, i, term)
					}
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
			rf.mu.Lock()
			rf.state.role = r.role
			rf.state.CurrentTerm = r.CurrentTerm
			if r.role == LEADER {
				DTPrintf("======= %d become a LEADER for term %d, its log len is %d, committed at %d =======\n",
					rf.me, r.CurrentTerm, len(rf.state.Log), rf.state.commitIndex)
				go func() { rf.leaderProcess.start <- true }()
			} else {
				DTPrintf("%d is not a leader for term %d\n", rf.me, r.CurrentTerm)
			}
			rf.mu.Unlock()
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
				rf.mu.Lock()
				rf.state.role = r.role
				rf.state.CurrentTerm = r.CurrentTerm
				DTPrintf("%d discovered new leader for term %d, switch to follower", rf.me, r.CurrentTerm)
				rf.mu.Unlock()
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
			rf.mu.Lock()
			rf.state.nextIndexes = make([]int, len(rf.peers))
			for i := 0; i < len(rf.peers); i++ {
				rf.state.nextIndexes[i] = len(rf.state.Log)
			}
			rf.state.matchIndexes = make([]int, len(rf.peers))
			rf.mu.Unlock()
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
				rf.mu.Lock()
				rf.state.CurrentTerm = reply.Term
				rf.state.role = FOLLOWER
				DTPrintf("%d discovered new leader for term %d, switch to follower",
					rf.me, reply.Term)
				nextHBCh = nil
				rf.mu.Unlock()
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

	rf.state.role = FOLLOWER

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
