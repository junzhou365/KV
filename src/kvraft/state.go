package raftkv

import (
	"sync"
)

type KVState struct {
	rw sync.RWMutex

	Table map[string]string

	Duplicates map[int]Op

	lastIncludedIndex int
	lastIncludedTerm  int
}

func (k *KVState) getValue(key string) (string, bool) {
	k.rw.RLock()
	defer k.rw.RUnlock()
	value, ok := k.Table[key]
	return value, ok
}

func (k *KVState) setValue(key string, value string) {
	k.rw.Lock()
	defer k.rw.Unlock()
	k.Table[key] = value
}

func (k *KVState) appendValue(key string, value string) {
	k.rw.Lock()
	defer k.rw.Unlock()
	k.Table[key] = k.Table[key] + value
}

func (k *KVState) getDup(id int) (Op, bool) {
	k.rw.RLock()
	defer k.rw.RUnlock()
	op, ok := k.Duplicates[id]
	return op, ok
}

func (k *KVState) setDup(id int, op Op) {
	k.rw.Lock()
	defer k.rw.Unlock()
	k.Duplicates[id] = op
}

func (k *KVState) getLastIncludedIndex() int {
	k.rw.RLock()
	defer k.rw.RUnlock()

	return k.lastIncludedIndex
}
