package raft

import (
	"log"
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

func (rs *RaftState) getLogEntryTerm(index int) int {
	rs.rw.RLock()
	defer rs.rw.RUnlock()

	offsettedLastIndex := index - rs.lastIncludedEntryIndex

	switch {
	case offsettedLastIndex < -1:
		log.Fatal("Could not get last term")
	case offsettedLastIndex == -1:
		return rs.lastIncludedEntryTerm
	}

	return rs.Log[offsettedLastIndex].Term
}

func (rs *RaftState) getLogEntry(index int) RaftLogEntry {
	rs.rw.RLock()
	defer rs.rw.RUnlock()
	return rs.Log[index-rs.lastIncludedEntryIndex]
}

func (rs *RaftState) getLogLen() int {
	rs.rw.RLock()
	defer rs.rw.RUnlock()
	return len(rs.Log) + rs.lastIncludedEntryIndex
}

func (rs *RaftState) appendLogEntry(entry RaftLogEntry) {
	rs.rw.Lock()
	defer rs.rw.Unlock()
	rs.Log = append(rs.Log, entry)
}

func (rs *RaftState) discardLogEnries(newIndex int) {
	rs.rw.Lock()
	defer rs.rw.Unlock()

	rs.lastIncludedEntryIndex = newIndex - 1
	rs.lastIncludedEntryTerm = rs.Log[newIndex-1].Term
	// Keep the nil head
	rs.Log = append(rs.Log[0:1], rs.Log[newIndex:]...)
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
