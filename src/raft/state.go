package raft

import (
	"bytes"
	"encoding/gob"
	"log"
	"sync"
)

type RaftState struct {
	rw sync.RWMutex
	me int //for debug

	CurrentTerm int
	VotedFor    int
	role        int

	logBase int            // log might be trimmed. We still want to have increasing indexes.
	Log     []RaftLogEntry // initialized with term 0 entry (empty RaftEntry)

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

type indexEntry struct {
	index int
	entry RaftLogEntry
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
	offsettedLastIndex := index - rs.logBase

	switch {
	case offsettedLastIndex < 0 || offsettedLastIndex >= len(rs.Log):
		DTPrintf("%d: logBase: %d, offset: %d\n", rs.me, rs.logBase, offsettedLastIndex)
	case offsettedLastIndex == 0:
		DTPrintf("%d: logBase: %d, lastTerm: %d\n", rs.me, rs.logBase, rs.lastIncludedEntryTerm)
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
	return rs.Log[index-rs.logBase]
}

func (rs *RaftState) getLogLenWithNoLock() int {
	return len(rs.Log) + rs.logBase
}

func (rs *RaftState) getLogLen() int {
	rs.rw.RLock()
	defer rs.rw.RUnlock()
	return rs.getLogLenWithNoLock()
}

func (rs *RaftState) getLogSize() int {
	rs.rw.RLock()
	defer rs.rw.RUnlock()
	return len(rs.Log)
}

func (rs *RaftState) logEntryStream(done <-chan interface{}) <-chan indexEntry {
	stream := make(chan indexEntry)
	go func() {
		rs.rw.RLock()
		defer rs.rw.RUnlock()

		defer close(stream)
		for i, l := range rs.Log {
			select {
			case stream <- indexEntry{index: i + rs.logBase, entry: l}:
			case <-done:
				return
			}
		}
	}()

	return stream
}

// Delete entries [deleteIndex:]
func (rs *RaftState) truncateLogWithNoLock(deleteIndex int) {
	if deleteIndex <= rs.logBase {
		log.Fatal("invalid deleteIndex")
	}
	rs.Log = rs.Log[:deleteIndex-rs.logBase]
}

// return last log entry index
func (rs *RaftState) appendLogEntriesWithNoLock(entries []RaftLogEntry) int {
	rs.Log = append(rs.Log, entries...)
	DTPrintf("%d: after append, log: %v\n", rs.me, rs.Log)
	return len(rs.Log) + rs.logBase - 1
}

func (rs *RaftState) appendLogEntry(entry RaftLogEntry) int {
	rs.rw.Lock()
	defer rs.rw.Unlock()
	return rs.appendLogEntriesWithNoLock([]RaftLogEntry{entry})
}

// start: inclusive, end: exclusive
func (rs *RaftState) getLogRange(start int, end int) []RaftLogEntry {
	rs.rw.RLock()
	defer rs.rw.RUnlock()

	offStart, offEnd := start-rs.logBase, end-rs.logBase
	return rs.Log[offStart:offEnd]
}

// Discard log entries up to the lastIndex
func (rs *RaftState) discardLogEnriesWithNoLock(lastIndex int) {
	// Keep the nil head
	rs.lastIncludedEntryIndex = lastIndex
	rs.lastIncludedEntryTerm = rs.getLogEntryTermWithNoLock(lastIndex)

	rs.Log = append(rs.Log[0:1], rs.Log[lastIndex+1-rs.logBase:]...)
	rs.logBase = lastIndex
}

// Discard log entries up to the lastIndex
func (rs *RaftState) discardLogEnries(lastIndex int) {
	rs.rw.Lock()
	defer rs.rw.Unlock()

	rs.discardLogEnriesWithNoLock(lastIndex)
}

func (rs *RaftState) loadSnapshotMetaData(index int, term int) {
	rs.rw.Lock()
	defer rs.rw.Unlock()

	rs.lastIncludedEntryIndex = index
	rs.lastIncludedEntryTerm = term

	rs.logBase = index

	rs.lastApplied = index
	rs.commitIndex = index
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

// Is index > logBase ?
func (rs *RaftState) indexExist(index int) bool {
	rs.rw.RLock()
	defer rs.rw.RUnlock()

	return index > rs.logBase
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
	DTPrintf("%d: In persist(), Log size: %d, data size: %d\n", rs.me, len(rs.Log), len(data))
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
