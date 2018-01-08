package raft

import (
	"bytes"
	"encoding/gob"
	"sync"
)

type RaftState struct {
	rw          sync.RWMutex
	CurrentTerm int
	VotedFor    int
	role        int
	Log         []RaftLogEntry // initialized with term 0 entry (empty RaftEntry)

	commitIndex int
	lastApplied int

	nextIndexes  []int
	matchIndexes []int

	lastIncludedEntryIndex int
	lastIncludedEntryTerm  int
}

type RaftLogEntry struct {
	Term    int
	Command interface{}
}

func (rs *RaftState) getCurrentTerm() int {
	rs.rw.RLock()
	defer rs.rw.RUnlock()
	return rs.CurrentTerm
}

func (rs *RaftState) setCurrentTerm(term int) {
	rs.rw.Lock()
	defer rs.rw.Unlock()
	rs.CurrentTerm = term
}

func (rs *RaftState) getVotedFor() int {
	rs.rw.RLock()
	defer rs.rw.RUnlock()
	return rs.VotedFor
}

func (rs *RaftState) setVotedFor(v int) {
	rs.rw.Lock()
	defer rs.rw.Unlock()
	rs.VotedFor = v
}

func (rs *RaftState) getRole() int {
	rs.rw.RLock()
	defer rs.rw.RUnlock()
	return rs.role
}

func (rs *RaftState) setRole(r int) {
	rs.rw.Lock()
	defer rs.rw.Unlock()
	rs.role = r
}

func (rs *RaftState) getLogEntryTermWithNoLock(index int) int {
	offsettedLastIndex := index - rs.lastIncludedEntryIndex

	switch {
	//case offsettedLastIndex < 0:
	//return -1
	case offsettedLastIndex == 0:
		return rs.lastIncludedEntryTerm
	}

	return rs.Log[offsettedLastIndex].Term
}

func (rs *RaftState) getLogEntryTerm(index int) int {
	rs.rw.RLock()
	defer rs.rw.RUnlock()

	return rs.getLogEntryTermWithNoLock(index)
}

func (rs *RaftState) getLogEntry(index int) RaftLogEntry {
	rs.rw.RLock()
	defer rs.rw.RUnlock()
	return rs.Log[index-rs.lastIncludedEntryIndex]
}

func (rs *RaftState) getLogLenWithNoLock() int {
	return len(rs.Log) + rs.lastIncludedEntryIndex
}

func (rs *RaftState) getLogLen() int {
	rs.rw.RLock()
	defer rs.rw.RUnlock()
	return len(rs.Log) + rs.lastIncludedEntryIndex
}

func (rs *RaftState) getLogSize() int {
	rs.rw.RLock()
	defer rs.rw.RUnlock()
	return len(rs.Log)
}

func (rs *RaftState) appendLogEntry(entry RaftLogEntry) {
	rs.rw.Lock()
	defer rs.rw.Unlock()
	rs.Log = append(rs.Log, entry)
}

func (rs *RaftState) discardLogEnriesWithNoLock(newIndex int) {
	lastIndex := rs.lastIncludedEntryIndex
	rs.lastIncludedEntryIndex = newIndex
	rs.lastIncludedEntryTerm = rs.Log[newIndex-lastIndex].Term
	// Keep the nil head
	rs.Log = append(rs.Log[0:1], rs.Log[newIndex+1-lastIndex:]...)
}

// Discard log entries up to the newIndex and update last included index and term
func (rs *RaftState) discardLogEnries(newIndex int) {
	rs.rw.Lock()
	defer rs.rw.Unlock()

	rs.discardLogEnriesWithNoLock(newIndex)
}

func (rs *RaftState) loadSnapshotMetaData(index int, term int) {
	rs.rw.Lock()
	defer rs.rw.Unlock()

	rs.lastIncludedEntryIndex = index
	rs.lastIncludedEntryTerm = term

	rs.lastApplied = index
	rs.commitIndex = min(index, rs.commitIndex)
}

func (rs *RaftState) getCommitIndex() int {
	rs.rw.RLock()
	defer rs.rw.RUnlock()
	return rs.commitIndex
}

func (rs *RaftState) setCommitIndex(c int) {
	rs.rw.Lock()
	defer rs.rw.Unlock()
	rs.commitIndex = c
}

func (rs *RaftState) getLastApplied() int {
	rs.rw.RLock()
	defer rs.rw.RUnlock()
	return rs.lastApplied
}

func (rs *RaftState) setLastApplied(a int) {
	rs.rw.Lock()
	defer rs.rw.Unlock()
	rs.lastApplied = a
}

func (rs *RaftState) getNextIndex(i int) int {
	rs.rw.RLock()
	defer rs.rw.RUnlock()
	return rs.nextIndexes[i]
}

func (rs *RaftState) setNextIndex(i int, n int) {
	rs.rw.Lock()
	defer rs.rw.Unlock()
	rs.nextIndexes[i] = n
}

func (rs *RaftState) setMatchIndex(i int, m int) {
	rs.rw.Lock()
	defer rs.rw.Unlock()
	rs.matchIndexes[i] = m
}

func (rs *RaftState) getLastIndex() int {
	rs.rw.RLock()
	defer rs.rw.RUnlock()
	return rs.lastIncludedEntryIndex
}

func (rs *RaftState) setLastIndex(i int) {
	rs.rw.Lock()
	defer rs.rw.Unlock()
	rs.lastIncludedEntryIndex = i
}

func (rs *RaftState) getLastTerm() int {
	rs.rw.RLock()
	defer rs.rw.RUnlock()
	return rs.lastIncludedEntryTerm
}

func (rs *RaftState) setLastTerm(t int) {
	rs.rw.Lock()
	defer rs.rw.Unlock()
	rs.lastIncludedEntryTerm = t
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rs *RaftState) persist(persister *Persister) {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := gob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)

	rs.rw.RLock()
	defer rs.rw.RUnlock()

	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	e.Encode(rs.CurrentTerm)
	e.Encode(rs.VotedFor)

	e.Encode(rs.Log)

	data := w.Bytes()
	persister.SaveRaftState(data)
	DTPrintf("In persist(), Log size: %d, data size: %d\n", len(rs.Log), len(data))
}

//
// restore previously persisted state.
//
func (rs *RaftState) readPersist(persister *Persister) {
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := gob.NewDecoder(r)
	// d.Decode(&rf.xxx)
	// d.Decode(&rf.yyy)
	rs.rw.Lock()
	defer rs.rw.Unlock()

	data := persister.ReadRaftState()
	if data == nil || len(data) < 1 { // bootstrap without any state?
		rs.CurrentTerm = 0
		rs.VotedFor = -1
		rs.Log = make([]RaftLogEntry, 1)
	} else {
		r := bytes.NewBuffer(data)
		d := gob.NewDecoder(r)

		d.Decode(&rs.CurrentTerm)
		d.Decode(&rs.VotedFor)
		d.Decode(&rs.Log)
	}
}

// Reason we need this is RaftStateSize should be accessed with lock.
// Following operations should see the consistent size.
func (rs *RaftState) checkForTakingSnapshot(
	newIndex int, max int, delta int, persister *Persister,
	takeSnapshot func(int, int)) {

	rs.rw.Lock()
	defer rs.rw.Unlock()

	if max-persister.RaftStateSize() <= delta {
		// we must first save snapshot
		takeSnapshot(newIndex, rs.getLogEntryTermWithNoLock(newIndex))
		rs.discardLogEnriesWithNoLock(newIndex)
	}
}
