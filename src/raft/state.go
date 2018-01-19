package raft

import (
	"bytes"
	"encoding/gob"
	"sync"
)

type RaftState struct {
	rw       sync.RWMutex
	me       int //for debug
	numPeers int

	CurrentTerm int
	VotedFor    int
	role        int

	logBase int            // log might be trimmed. We still want to have increasing indexes.
	Log     []RaftLogEntry // initialized with term 0 entry (empty RaftEntry)

	commitIndex int
	lastApplied int

	nextIndexes  []int
	matchIndexes []int

	queue chan StateRequest
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

func (rs *RaftState) indexTrimmedWithNoLock(index int) bool {
	offsettedLastIndex := index - rs.logBase

	if offsettedLastIndex <= 0 {
		DTPrintf("%d: [WARNING] logBase: %d, offset: %d, log size: %d\n",
			rs.me, rs.logBase, offsettedLastIndex, len(rs.Log))

		return true
	}

	return false
}

func (rs *RaftState) indexTrimmed(index int) bool {
	rs.rw.RLock()
	defer rs.rw.RUnlock()
	return rs.indexTrimmedWithNoLock(index)
}

func (rs *RaftState) getLogEntryWithNoLock(index int) (RaftLogEntry, bool) {
	if rs.indexTrimmedWithNoLock(index) {
		return RaftLogEntry{}, false
	}

	return rs.Log[index-rs.logBase], true
}

func (rs *RaftState) getLogEntry(index int) (RaftLogEntry, bool) {
	rs.rw.RLock()
	defer rs.rw.RUnlock()

	return rs.getLogEntryWithNoLock(index)
}

func (rs *RaftState) getBaseLogEntryWithNoLock() (int, int) {
	return rs.logBase, rs.Log[0].Term
}

func (rs *RaftState) getBaseLogEntry() (int, int) {
	rs.rw.RLock()
	defer rs.rw.RUnlock()

	return rs.getBaseLogEntryWithNoLock()
}

func (rs *RaftState) getLogEntryTermWithNoLock(index int) (int, bool) {
	entry, ok := rs.getLogEntryWithNoLock(index)
	if !ok && index != rs.logBase {
		return 0, false
	}

	if index == rs.logBase {
		return rs.Log[0].Term, true
	}

	return entry.Term, true
}

func (rs *RaftState) getLogEntryTerm(index int) (int, bool) {
	rs.rw.RLock()
	defer rs.rw.RUnlock()

	return rs.getLogEntryTermWithNoLock(index)
}

func (rs *RaftState) getLogLenWithNoLock() int {
	return len(rs.Log) + rs.logBase
}

func (rs *RaftState) getLogLen() int {
	rs.rw.RLock()
	defer rs.rw.RUnlock()
	return rs.getLogLenWithNoLock()
}

func (rs *RaftState) logEntryStream(done <-chan interface{}) <-chan indexEntry {
	stream := make(chan indexEntry)
	go func() {
		defer close(stream)

		rs.rw.RLock()
		defer rs.rw.RUnlock()

		for i, l := range rs.Log[1:] {
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
func (rs *RaftState) truncateLog(deleteIndex int) {
	rs.rw.Lock()
	defer rs.rw.Unlock()

	if rs.indexTrimmedWithNoLock(deleteIndex) {
		DTPrintf("%d: truncate the log before %d", rs.me, deleteIndex)
		panic("deleteIndex didn't exist")
	}

	rs.Log = rs.Log[:deleteIndex-rs.logBase]
}

// return last log entry index
func (rs *RaftState) appendLogEntriesWithNoLock(entries []RaftLogEntry) int {
	rs.Log = append(rs.Log, entries...)
	return len(rs.Log) + rs.logBase - 1
}

func (rs *RaftState) appendLogEntries(entries []RaftLogEntry) int {
	rs.rw.Lock()
	defer rs.rw.Unlock()
	return rs.appendLogEntriesWithNoLock(entries)
}

func (rs *RaftState) appendLogEntry(entry RaftLogEntry) int {
	rs.rw.Lock()
	defer rs.rw.Unlock()
	return rs.appendLogEntriesWithNoLock([]RaftLogEntry{entry})
}

// start: inclusive, end: exclusive
func (rs *RaftState) getLogRangeWithNoLock(start int, end int) ([]RaftLogEntry, bool) {
	if rs.indexTrimmedWithNoLock(start) || rs.indexTrimmedWithNoLock(end-1) ||
		start >= end {
		return []RaftLogEntry{}, false
	}

	offStart, offEnd := start-rs.logBase, end-rs.logBase
	newEntries := []RaftLogEntry{}
	newEntries = append(newEntries, rs.Log[offStart:offEnd]...)

	return newEntries, true
}

func (rs *RaftState) getLogRange(start int, end int) ([]RaftLogEntry, bool) {
	rs.rw.RLock()
	defer rs.rw.RUnlock()

	return rs.getLogRangeWithNoLock(start, end)
}

// Discard log entries up to the lastIndex
func (rs *RaftState) discardLogEnriesWithNoLock(lastIndex int) bool {
	if lastIndex == -1 {
		lastIndex = rs.getLogLenWithNoLock() - 1
	}

	if rs.indexTrimmedWithNoLock(lastIndex) {
		return false
	}

	DTPrintf("%d: before discarding. lastIndex: %d, logBase: %d, log: %+v\n",
		rs.me, lastIndex, rs.logBase, rs.Log)

	rs.Log = rs.Log[lastIndex-rs.logBase:]
	rs.Log[0].Command = nil
	rs.logBase = lastIndex
	DTPrintf("%d: logBase changed to %d in discard\n", rs.me, lastIndex)
	return true
}

// Discard log entries up to the lastIndex
func (rs *RaftState) discardLogEnries(lastIndex int) bool {
	rs.rw.Lock()
	defer rs.rw.Unlock()

	return rs.discardLogEnriesWithNoLock(lastIndex)
}

//func (rs *RaftState) loadSnapshotMetaData(index int, term int) {
//rs.rw.Lock()
//defer rs.rw.Unlock()

//// XXX: index == rs.logBase
//switch {
//case index < rs.logBase:
//panic("index is less than logBase")
//case len(rs.Log) != 1:
//panic("log is not fully discarded")
//}

//rs.logBase = index
//rs.Log[0].Term = term

//rs.lastApplied = index
//rs.commitIndex = index
//}

func (rs *RaftState) getCommitIndexWithNoLock() int {
	return rs.commitIndex
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

func (rs *RaftState) getNextIndexWithNoLock(i int) int {
	return rs.nextIndexes[i]
}

func (rs *RaftState) setNextIndex(i int, n int) {
	rs.rw.Lock()
	defer rs.rw.Unlock()
	rs.setNextIndexWithNoLock(i, n)
}

func (rs *RaftState) setNextIndexWithNoLock(i int, n int) {
	if n > rs.matchIndexes[i] {
		rs.nextIndexes[i] = n
	} else {
		rs.nextIndexes[i] = rs.matchIndexes[i] + 1
	}
}

func (rs *RaftState) getMatchIndex(i int) int {
	rs.rw.RLock()
	defer rs.rw.RUnlock()
	return rs.matchIndexes[i]
}

func (rs *RaftState) getMatchIndexWithNoLock(i int) int {
	return rs.matchIndexes[i]
}

// Only succeeds if m > current match index
func (rs *RaftState) setMatchIndex(i int, m int) {
	rs.rw.Lock()
	defer rs.rw.Unlock()

	rs.setMatchIndexWithNoLock(i, m)
}

func (rs *RaftState) setMatchIndexWithNoLock(i int, m int) {
	if m > rs.matchIndexes[i] {
		rs.matchIndexes[i] = m
	}
}

func (rs *RaftState) initializeMatchAndNext() {
	rs.rw.Lock()
	logLen := rs.getLogLenWithNoLock()
	rs.nextIndexes = make([]int, rs.numPeers)
	for i := 0; i < rs.numPeers; i++ {
		rs.nextIndexes[i] = logLen
	}
	rs.matchIndexes = make([]int, rs.numPeers)
	rs.rw.Unlock()
}

// return -1 if not found
func (rs *RaftState) updateCommitIndex(term int) int {
	rs.rw.Lock()
	defer rs.rw.Unlock()

	logLen := rs.getLogLenWithNoLock()
	for n := logLen - 1; n > rs.commitIndex; n-- {
		count := 1
		for j := 0; j < rs.numPeers; j++ {
			if j != rs.me && rs.matchIndexes[j] >= n {
				count++
			}
		}
		//DTPrintf("%d: the count is %d, n is %d", rf.me, count, n)
		t, ok := rs.getLogEntryTermWithNoLock(n)
		if count > rs.numPeers/2 && n > rs.commitIndex && ok && term == t {
			rs.commitIndex = n
			return n
		}
	}

	return -1
}

func (rs *RaftState) appliedEntries() []ApplyMsg {
	rs.rw.Lock()
	defer rs.rw.Unlock()

	entries := []ApplyMsg{}

	for rs.commitIndex > rs.lastApplied {
		rs.lastApplied++
		DTPrintf("%d: updated lastApplied to %d, log base: %d, log len: %d\n",
			rs.me, rs.lastApplied, rs.logBase, rs.getLogLenWithNoLock())

		entry, ok := rs.getLogEntryWithNoLock(rs.lastApplied)
		if !ok {
			panic("entry doesn't exist")
		}

		entries = append(entries, ApplyMsg{
			Index:   rs.lastApplied,
			Term:    entry.Term,
			Command: entry.Command})
	}

	return entries
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

	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)

	rs.rw.RLock()
	e.Encode(rs.CurrentTerm)
	e.Encode(rs.VotedFor)
	e.Encode(rs.Log)
	rs.rw.RUnlock()

	data := w.Bytes()
	persister.SaveRaftState(data)
	//DTPrintf("%d: In persist(), Log size: %d, data size: %d\n", rs.me, len(rs.Log), len(data))
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

	data := persister.ReadRaftState()
	rs.rw.Lock()
	defer rs.rw.Unlock()
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

func (rs *RaftState) readSnapshot(persister *Persister) (
	lastIndex int, lastTerm int, snapshot []byte) {
	rs.rw.RLock()
	defer rs.rw.RUnlock()

	snapshot = persister.ReadSnapshot()
	r := bytes.NewBuffer(snapshot)
	d := gob.NewDecoder(r)

	d.Decode(&lastIndex)
	d.Decode(&lastTerm)
	return lastIndex, lastTerm, snapshot
}
