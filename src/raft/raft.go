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
	"labrpc"
	"log"
	"math/rand"
	"sync"
	"time"
)

// import "bytes"
// import "encoding/gob"

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
	electionProcess   *loopProcess
	leaderProces      *loopProcess
	heartbeatInterval time.Duration
	commit            chan bool
	applyCh           chan ApplyMsg
}

type RaftEntry struct {
	term    int
	command interface{}
}

type RaftState struct {
	term     int
	votedFor int
	role     int
	log      []RaftEntry

	commitIndex int
	lastApplied int

	nextIndexes  []int
	matchIndexes []int
}

type loopProcess struct {
	start    chan bool
	end      chan bool
	newEntry chan bool
}

const (
	FOLLOWER = iota
	CANDIDATE
	LEADER
)

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	term = rf.state.term
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
		return
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
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// update term
	if args.Term > rf.state.term {
		rf.state.term = args.Term
		rf.state.role = FOLLOWER
		rf.state.votedFor = -1
	}

	isLeaderTermValid := args.Term == rf.state.term
	isVotedForValid := rf.state.votedFor == -1 || rf.state.votedFor == args.CandidateId
	isEntryUpToDate := false
	if len(rf.state.log) == 0 ||
		args.LastLogTerm > rf.state.log[len(rf.state.log)-1].term ||
		args.LastLogTerm == rf.state.log[len(rf.state.log)-1].term &&
			args.LastLogIndex >= len(rf.state.log) {
		isEntryUpToDate = true
	}
	if isLeaderTermValid && isVotedForValid && isEntryUpToDate {
		reply.VoteGranted = true
		// XXX: votedFor is implicitly updated in args.Term > term case
		rf.state.votedFor = args.CandidateId
		rf.electionProcess.end <- true
		rf.electionProcess.start <- true
	} else {
		reply.VoteGranted = false
	}

	reply.Term = rf.state.term

	DTPrintf("%d voted back to %d with reply %v\n", rf.me, args.CandidateId, reply)
}

type AppendEntriesArgs struct {
	Term int

	PrevLogIndex int
	PrevLogTerm  int
	Entries      []RaftEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func min(a int, b int) int {
	if a < b {
		return a
	}
	return b
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	DTPrintf("%d got heartbeat for term %d, its term is %d\n", rf.me, args.Term, rf.state.term)
	defer rf.mu.Unlock()

	r := rf.state.role
	switch {
	case args.Term > rf.state.term:
		rf.state.term = args.Term
		switch {
		case r == CANDIDATE:
			DTPrintf("%d switched to follower\n", rf.me)
		case r == LEADER:
			rf.leaderProces.end <- true
			DTPrintf("%d switched to follower\n", rf.me)
		case r == FOLLOWER:
			DTPrintf("%d update its follower term\n", rf.me)
		}
		rf.state.role = FOLLOWER
	case args.Term < rf.state.term:
		reply.Term = rf.state.term
		reply.Success = false
		DTPrintf("%d received reset from stale leader\n", rf.me)
		return
	}

	if r == LEADER {
		log.Fatal("Same term Leader sent Append RPC to another leader")
	}

	reply.Term = rf.state.term
	reply.Success = true
	if args.PrevLogIndex-1 >= len(rf.state.log) ||
		rf.state.log[args.PrevLogIndex-1].term != args.PrevLogTerm {
		reply.Success = false
	}
	if reply.Success && len(args.Entries) > 0 {
		deleteIndex := args.PrevLogIndex + 1
		for _, entry := range args.Entries {
			if rf.state.log[deleteIndex-1].term != entry.term {
				break
			}
			deleteIndex++
		}
		rf.state.log = append(rf.state.log[:deleteIndex-1],
			args.Entries[deleteIndex-args.PrevLogIndex-1:]...)
	}
	// XXX: Do we need to add Success condition?
	if args.LeaderCommit > rf.state.commitIndex {
		rf.state.commitIndex = min(args.LeaderCommit, len(rf.state.log))
		rf.commit <- true
	}
	rf.electionProcess.end <- true
	rf.electionProcess.start <- true

	DTPrintf("%d Append RPC done for term %d\n", rf.me, args.Term)
}

func (rf *Raft) commitLoop() {
	for {
		select {
		case <-rf.commit:
			rf.mu.Lock()
			commitIndex := rf.state.commitIndex
			lastApplied := rf.state.lastApplied
			rf.mu.Unlock()
			for commitIndex > lastApplied {
				lastApplied++
				rf.mu.Lock()
				rf.state.lastApplied = lastApplied
				command := rf.state.log[lastApplied-1].command
				rf.mu.Unlock()
				rf.applyCh <- ApplyMsg{Index: lastApplied, Command: command}
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
		rf.state.log = append(rf.state.log, RaftEntry{term: rf.state.term, command: command})
		index = len(rf.state.log)
		term = rf.state.term
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

func (rf *Raft) electionLoop() {
	var voteResultDone chan RaftState
	var startElecCh <-chan time.Time
	for {
		select {
		case <-startElecCh:
			DTPrintf("<<<< Triggered, node: %d >>>>\n", rf.me)
			rf.mu.Lock()
			if rf.state.role == LEADER {
				log.Fatal("Wrong rf role ", rf.state.role)
			}
			rf.state.role = CANDIDATE
			rf.state.term++
			term := rf.state.term
			DTPrintf("%d becomes candidate for term %d\n", rf.me, term)
			rf.state.votedFor = rf.me
			rf.mu.Unlock()

			t := getRandomDuration()
			DTPrintf("%d got new timer %v", rf.me, t)
			startElecCh = time.After(t)

			args := new(RequestVoteArgs)
			args.Term = term
			args.CandidateId = rf.me

			voteResultDone = make(chan RaftState)
			go func(vDone chan RaftState) {
				rf.mu.Lock()
				term := rf.state.term
				rf.mu.Unlock()
				// Should not use sync.WaitGroup here because that might block for too long time.
				//replies := make([]RequestVoteReply, len(rf.peers))
				replyCh := make(chan RequestVoteReply)
				for i := 0; i < len(rf.peers); i++ {
					if i == rf.me {
						continue
					}
					go func(i int) {
						DTPrintf("%d send requestVote to %d for term %d\n", rf.me, i, term)
						reply := RequestVoteReply{index: i}
						for ok := false; !ok; {
							ok = rf.sendRequestVote(i, args, &reply)
							if !ok {
								DPrintf("%d sent RequestVote RPC; it failed at %d for term %d. Retry...", rf.me, i, term)
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
						vDone <- RaftState{role: FOLLOWER, term: reply.Term}
						return
					case reply.VoteGranted:
						yays++
					case reply.VoteGranted == false:
						neys++
					}

					switch {
					case yays+1 > len(rf.peers)/2:
						vDone <- RaftState{role: LEADER, term: term}
						return
					case neys > (len(rf.peers)+1)/2-1:
						vDone <- RaftState{role: FOLLOWER, term: term}
						return
					}
				}
				vDone <- RaftState{role: FOLLOWER, term: term}
			}(voteResultDone)

		case r := <-voteResultDone:
			voteResultDone = nil
			rf.mu.Lock()
			rf.state.role = r.role
			rf.state.term = r.term
			rf.mu.Unlock()
			if r.role == LEADER {
				DTPrintf("<<>>!!!!%d become a leader for term %d!!!!<<>>\n", rf.me, r.term)
				startElecCh = nil
				rf.leaderProces.start <- true
			} else {
				DTPrintf("%d is not a leader for term %d\n", rf.me, r.term)
			}
		case <-rf.electionProcess.start:
			DTPrintf("%d electionProcess start for term %d\n", rf.me, rf.state.term)
			t := getRandomDuration()
			DTPrintf("%d got new timer %v", rf.me, t)
			startElecCh = time.After(t)
		case <-rf.electionProcess.end:
			DTPrintf("%d electionProcess end for term %d\n", rf.me, rf.state.term)
			voteResultDone = nil
			startElecCh = nil
		}
	}
}

func (rf *Raft) appendNewEntries(appendDone chan AppendEntriesReply) {
	rf.mu.Lock()
	llog := rf.state.log // might be slow
	term := rf.state.term
	role := rf.state.role
	nextIndexes := rf.state.nextIndexes // might also be slow
	lastLogIndex := len(rf.state.log)
	leaderCommit := rf.state.commitIndex
	rf.mu.Unlock()

	if role != LEADER {
		return
	}

	replyCh := make(chan AppendEntriesReply)
	for i, next := range nextIndexes {
		if lastLogIndex < next {
			continue
		}

		if i == rf.me {
			rf.mu.Lock()
			rf.state.nextIndexes[i] = len(llog) + 1
			rf.state.matchIndexes[i] = len(llog)
			rf.mu.Unlock()
			continue
		}
		go func(i int, next int) {
			for {
				args := AppendEntriesArgs{}
				args.Term = term
				args.PrevLogIndex = next - 1
				args.PrevLogTerm = llog[args.PrevLogIndex-1].term
				args.Entries = llog[next-1:]
				args.LeaderCommit = leaderCommit

				reply := new(AppendEntriesReply)
				DTPrintf("%d sends Append RPC to %d for term %d\n", rf.me, i, term)
				for ok := false; !ok; {
					ok = rf.peers[i].Call("Raft.AppendEntries", &args, &reply)
				}
				switch {
				case reply.Success == false && reply.Term == term:
					next--
					DTPrintf("%d resend Append RPC with decremented index %d\n", next)
				case reply.Success == false && reply.Term > term:
					replyCh <- *reply
					return
				case reply.Success == true:
					rf.mu.Lock()
					rf.state.nextIndexes[i] = rf.state.nextIndexes[i] + len(args.Entries)
					rf.state.matchIndexes[i] = rf.state.matchIndexes[i] + len(args.Entries)
					rf.mu.Unlock()
					replyCh <- *reply
					return
				default:
					log.Fatal("Incorrect Append RPC reply")
				}
			}
		}(i, next)
	}

	for i := 0; i < len(rf.peers)-1; i++ {
		if r := <-replyCh; r.Success {
			appendDone <- r
			return
		}
	}

	appendDone <- AppendEntriesReply{Term: term, Success: true}
}

func (rf *Raft) leaderLoop() {
	var heartbeatDone chan RaftState
	var appendDone chan AppendEntriesReply
	var nextHBCh <-chan time.Time
	for {
		select {
		case <-nextHBCh:
			rf.mu.Lock()
			term := rf.state.term
			rf.mu.Unlock()

			nextHBCh = time.After(rf.heartbeatInterval)

			heartbeatDone = make(chan RaftState) // the channel is only used for this term
			go func(hDone chan RaftState) {
				//replies := make([]AppendEntriesReply, len(rf.peers))
				replyCh := make(chan AppendEntriesReply)
				for i, _ := range rf.peers {
					if i == rf.me {
						continue
					}
					go func(i int) {
						args := AppendEntriesArgs{}
						args.Term = rf.state.term
						reply := AppendEntriesReply{}
						DTPrintf("%d sends heartbeat to %d for term %d with args %v\n", rf.me, i, term, args)
						for ok := false; !ok; {
							ok = rf.peers[i].Call("Raft.AppendEntries", &args, &reply)
							if !ok {
								DTPrintf("%d send heartbeat to %d for term %d failed. Retry...\n", rf.me, i, term)
							}
						}
						DTPrintf("%d got heartbeat reply %v from %d for term %d\n", rf.me, reply, i, term)
						replyCh <- reply
					}(i)
				}

				for i := 0; i < len(rf.peers)-1; i++ {
					reply := <-replyCh
					if reply.Term > term {
						hDone <- RaftState{role: FOLLOWER, term: reply.Term}
						return
					}
				}
				hDone <- RaftState{role: LEADER, term: term}
			}(heartbeatDone)
		case r := <-heartbeatDone:
			heartbeatDone = nil
			if r.role == FOLLOWER {
				rf.mu.Lock()
				rf.state.role = r.role
				rf.state.term = r.term
				DTPrintf("%d discovered new leader for term %d, switch to follower", rf.me, r.term)
				rf.mu.Unlock()
				nextHBCh = nil
				rf.electionProcess.start <- true
			}
		case <-rf.leaderProces.start:
			nextHBCh = time.After(0)
			heartbeatDone = nil
			rf.state.nextIndexes = make([]int, len(rf.peers))
			for i := 0; i < len(rf.peers); i++ {
				rf.state.nextIndexes[i] = len(rf.state.log) + 1
			}
			rf.state.matchIndexes = make([]int, len(rf.peers))
		case <-rf.leaderProces.end:
			nextHBCh = nil
			heartbeatDone = nil
		case <-rf.leaderProces.newEntry:
			go rf.appendNewEntries(appendDone)
		case reply := <-appendDone:
			appendDone = nil
			if !reply.Success {
				rf.mu.Lock()
				rf.state.term = reply.Term
				DTPrintf("%d discovered new leader for term %d, switch to follower", rf.me, reply.Term)
				nextHBCh = nil
				rf.electionProcess.start <- true
				rf.mu.Unlock()
			} else {
				rf.mu.Lock()
				for n := len(rf.state.log); n > rf.state.commitIndex; n-- {
					//if n > 0 && rf.state.log[n-1].term < rf.state.term {
					//continue
					//}
					count := 0
					for _, match := range rf.state.matchIndexes {
						if match >= n {
							count++
						}
					}

					if count > len(rf.peers)/2 && n > 0 && rf.state.term == rf.state.log[n-1].term {
						rf.state.commitIndex = n
						rf.commit <- true
					}
				}
				rf.mu.Unlock()
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
	rf.state.role = FOLLOWER
	rf.state.votedFor = -1

	rf.heartbeatInterval = 100 * time.Millisecond // max number test permits
	rf.electionProcess = &loopProcess{
		start: make(chan bool), end: make(chan bool)}
	rf.leaderProces = &loopProcess{
		start: make(chan bool), end: make(chan bool), newEntry: make(chan bool)}

	rf.commit = make(chan bool)
	rf.applyCh = applyCh

	go rf.electionLoop()
	go rf.leaderLoop()
	go rf.commitLoop()
	go func() {
		rf.electionProcess.start <- true
	}()
	DTPrintf("%d is created\n", rf.me)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}
