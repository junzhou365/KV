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
	"fmt"
	"labrpc"
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
	Term        int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
	SavedCh     chan interface{}
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
	retryInterval     time.Duration
	commit            chan int
	applyCh           chan ApplyMsg
	newEntry          chan int
	//sendChs           []chan sendJob
	appendReplyCh chan bool
	shutDown      chan interface{}
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

func (rf *Raft) DiscardLogEnries(index int, term int) {
	jobDone := rf.Serialize("DiscardLogEnries")
	defer close(jobDone)

	// Has been trimmed?
	if rf.state.indexTrimmed(index) {
		return
	}
	rf.state.discardLogEnries(index, term)
}

func (rf *Raft) LoadBaseLogEntry(index int, term int) {
	jobDone := rf.Serialize("LoadBaseLogEntry")
	defer close(jobDone)

	rf.state.loadBaseLogEntry(index, term)
}

// return CurrentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// Your code here (2A).
	return rf.state.getCurrentTerm(), rf.state.getRole() == LEADER
}

func (rf *Raft) GetStateWithNoLock() (int, bool) {
	// Your code here (2A).
	return rf.state.CurrentTerm, rf.state.role == LEADER
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
	jobDone := rf.Serialize("RequestVote")
	defer close(jobDone)

	//defer DTPrintf("%d: disk usage: %d after request", rf.me, rf.persister.RaftStateSize())
	//defer DTPrintf("%d: done persist]]", rf.me)
	defer rf.state.persist(rf.persister)
	//defer DTPrintf("%d: [[start persist", rf.me)

	DTPrintf("%d: get Request RPC args: %+v\n", rf.me, args)

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
	lastTerm := rf.state.getLogEntryTerm(lastLogIndex)

	DTPrintf("%d received RequestVote RPC req %+v | votedFor: %d, lastLogIndex: %d, logLen: %d\n",
		rf.me, args, curVotedFor, lastLogIndex, logLen)
	if logLen == 1 || args.LastLogTerm > lastTerm ||
		args.LastLogTerm == lastTerm && args.LastLogIndex >= lastLogIndex {
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

func max(a int, b int) int {
	if a > b {
		return a
	}
	return b
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	DTPrintf("%d: get Append for term %d from leader. prevIndex: %d, len of entris: %d\n",
		rf.me, args.Term, args.PrevLogIndex, len(args.Entries))
	defer DTPrintf("%d reply Append for term %d to leader\n", rf.me, args.Term)

	jobDone := rf.Serialize("AppendEntries")
	defer close(jobDone)

	//defer DTPrintf("%d: disk usage: %d after append", rf.me, rf.persister.RaftStateSize())
	//defer DTPrintf("%d: done persist]]", rf.me)
	defer rf.state.persist(rf.persister)
	//defer DTPrintf("%d: [[start persist", rf.me)

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
		DTPrintf("%d received reset for term %d from stale leader\n", rf.me, args.Term)
		// Did not receive RPC from *current* leader
		resp.toFollower = false

		resCh <- resp
		return
	}

	// At this point the role could only be FOLLOWER and CANDIDATE.
	if rf.state.getRole() == LEADER {
		DTPrintf("Term %d Leader %d got Append RPC from another same term leader\n",
			args.Term, rf.me)
		panic("Wrong Leader!")
	}

	rf.state.setRole(FOLLOWER)
	resCh <- resp

	if rf.state.getRole() != FOLLOWER {
		return
	}
	reply.Term = curTerm
	logLen := rf.state.getLogLen()

	// args:      prev      last
	//             |         |
	// logs:   - - - - - - - - - -
	//               | |         |
	// local:     base 1st      last

	if args.PrevLogIndex > logLen-1 {
		reply.Success = false
		reply.ConflictIndex = logLen
		reply.ConflictTerm = -1
		DTPrintf("%d: doesn't have the prevIndex: %d. log base: %d, log len: %d\n",
			rf.me, args.PrevLogIndex, rf.state.logBase, logLen)
		return
	}

	localPrevIndex := args.PrevLogIndex
	if rf.state.baseIndexTrimmed(localPrevIndex) {
		//DTPrintf("%d: delayed Append RPC\n", rf.me)
		reply.Success = false
		reply.ConflictIndex = logLen
		reply.ConflictTerm = -1
		DTPrintf("%d: doesn't have the prevIndex: %d. log base: %d, log len: %d\n",
			rf.me, args.PrevLogIndex, rf.state.logBase, logLen)
		return
	}

	localPrevTerm := rf.state.getLogEntryTerm(localPrevIndex)

	if localPrevIndex > args.PrevLogIndex+len(args.Entries) {
		// already consistent till last index
		//DTPrintf("%d: already consistent till prevIndex: %d. log base: %d\n",
		//rf.me, args.PrevLogIndex, rf.state.logBase)
		reply.Success = true
		return
	}

	switch {
	case localPrevTerm != args.PrevLogTerm:
		reply.Success = false
		reply.ConflictTerm = localPrevTerm
		// find the first entry that has the conflicting term
		streamDone := make(chan interface{})
		for l := range rf.state.logEntryStream(streamDone) {
			//DTPrintf("%d: compare %v to term: %d\n", rf.me, l, localPrevTerm)
			index, entry := l.index, l.entry
			if entry.Term == reply.ConflictTerm {
				reply.ConflictIndex = index
				close(streamDone)
				break
			}
		}
		if reply.ConflictIndex == 0 {
			panic(fmt.Sprintf("%d: Did not find the conflictIndex for conflict term %d\n",
				rf.me, localPrevTerm))
		}

		DTPrintf("%d: conflict local term %d with arg term %d. Found first conflict index: %d\n",
			rf.me, localPrevTerm, args.PrevLogTerm, reply.ConflictIndex)

	default:
		reply.Success = true
		deleteIndex := localPrevIndex + 1
		// XXX: refactor
		entries := args.Entries[localPrevIndex-args.PrevLogIndex:]
		//DTPrintf("%d: entries: %v\n", rf.me, entries)
		for _, entry := range entries {
			// deleteIndex points to the end of the log
			if deleteIndex == logLen {
				rf.state.appendLogEntries(entries[deleteIndex-localPrevIndex-1:])
				break
			}

			if deleteTerm := rf.state.getLogEntryTerm(deleteIndex); deleteTerm != entry.Term {
				// delete conflicted entries
				rf.state.truncateLog(deleteIndex)
				rf.state.appendLogEntries(entries[deleteIndex-localPrevIndex-1:])
				break
			}

			deleteIndex++
		}
		//DTPrintf("%d: append entries succeeds. log len: %d, commitIndex: %d\n",
		//rf.me, rf.state.getLogLen(), rf.state.getCommitIndex())

		lastNewEntryIndex := localPrevIndex + len(args.Entries)
		commitIndex := min(args.LeaderCommit, lastNewEntryIndex)
		//if args.LeaderCommit > rf.state.getCommitIndex() { }
		if rf.state.getCommitIndex() < commitIndex {
			rf.state.setCommitIndex(commitIndex)
			rf.commit <- commitIndex
		}
	}
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

	DTPrintf("%d: get InstallSnapshot RPC args: %d\n", rf.me, args.LastIncludedEntryIndex)
	defer DTPrintf("%d: reply InstallSnapshot RPC args: %d\n", rf.me, args.LastIncludedEntryIndex)

	jobDone := rf.Serialize("InstallSnapshot")
	defer close(jobDone)

	//defer DTPrintf("%d: disk usage: %d after install", rf.me, rf.persister.RaftStateSize())
	//defer DTPrintf("%d: done persist]]", rf.me)
	defer rf.state.persist(rf.persister)
	//defer DTPrintf("%d: [[start persist", rf.me)

	DTPrintf("%d: received Snapshot RPC, lastIndex: %d, lastTerm: %d, args.Term: %d\n",
		rf.me, args.LastIncludedEntryIndex, args.LastIncludedEntryTerm, args.Term)

	var resCh chan rpcResp
	resCh = <-rf.rpcCh

	term := rf.state.getCurrentTerm()

	resp := rpcResp{toFollower: false}
	switch {
	case term < args.Term:
		rf.state.setCurrentTerm(args.Term)
		rf.state.setRole(FOLLOWER)
		resp.toFollower = true
		resCh <- resp

		reply.Term = args.Term
		return

	case term > args.Term:
		reply.Term = term
		resCh <- resp
		return
	}

	resCh <- resp

	// We have saved this snapshot
	if rf.state.indexTrimmed(args.LastIncludedEntryIndex) {
		//DTPrintf("%d: re-ordered snapshot request: %d\n", rf.me, args.LastIncludedEntryIndex)
		return
	}

	//DTPrintf("%d: own states before snapshot change. lastIncludedEntryIndex: %d, logLen: %d\n", rf.me,
	//rf.state.lastIncludedEntryIndex, rf.state.getLogLenWithNoLock())

	// XXX: save need to confirm
	savedCh := make(chan interface{})
	rf.applyCh <- ApplyMsg{
		UseSnapshot: false, Snapshot: args.Data, SavedCh: savedCh}
	<-savedCh

	lastBeyondLog := args.LastIncludedEntryIndex >= rf.state.getLogLen()-1
	if lastBeyondLog {
		rf.state.discardLogEnries(
			args.LastIncludedEntryIndex, args.LastIncludedEntryTerm)
		savedCh := make(chan interface{})
		rf.applyCh <- ApplyMsg{
			UseSnapshot: true, Snapshot: args.Data, SavedCh: savedCh}
		<-savedCh
		return
	}

	lastIndex := rf.state.getLogEntryTerm(args.LastIncludedEntryIndex)

	// If lastIncludedEntry exists in log and we have applied it, then we could
	// just delete up to that entry.
	if args.LastIncludedEntryTerm == lastIndex &&
		rf.state.getLastApplied() >= args.LastIncludedEntryIndex {

		rf.state.discardLogEnries(
			args.LastIncludedEntryIndex, args.LastIncludedEntryTerm)
	} else {
		rf.state.discardLogEnries(
			args.LastIncludedEntryIndex, args.LastIncludedEntryTerm)
		savedCh := make(chan interface{})
		rf.applyCh <- ApplyMsg{
			UseSnapshot: true, Snapshot: args.Data, SavedCh: savedCh}
		<-savedCh
	}

}

func (rf *Raft) commitLoop() {
	for {
		select {
		case <-rf.commit:
			entries := rf.state.appliedEntries()
			for _, entry := range entries {
				//DTPrintf("%d: try to apply at log index %d the command %+v\n",
				//rf.me, entry.Index, entry.Command)
				rf.applyCh <- entry
				//DTPrintf("%d: applied at log index %d the command %+v\n",
				//rf.me, entry.Index, entry.Command)
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
		index = rf.state.appendLogEntry(RaftLogEntry{Term: term, Command: command})
		go func() { rf.newEntry <- index }()
		DTPrintf("%d: Start call on leader with command %v finished at index: %d\n",
			rf.me, command, index)
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
	close(rf.shutDown)
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

		select {
		case <-rf.shutDown:
			return
		default:
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
	rf.state.me = me
	rf.state.numPeers = len(rf.peers)
	rf.state.queue = make(chan StateRequest)

	// Your initialization code here (2A, 2B, 2C).
	// initialize from state persisted before a crash
	rf.state.readPersist(persister)

	rf.state.setRole(FOLLOWER)

	rf.heartbeatInterval = 100 * time.Millisecond // max number test permits
	rf.retryInterval = 20 * time.Millisecond      // max number test permits
	rf.rpcCh = make(chan chan rpcResp)
	rf.newEntry = make(chan int)

	rf.commit = make(chan int)
	rf.applyCh = applyCh
	rf.shutDown = make(chan interface{})

	go rf.state.stateLoop()
	go rf.run()
	go rf.commitLoop()
	DTPrintf("%d is created, log len: %d\n", rf.me, rf.state.getLogLen())

	return rf
}
