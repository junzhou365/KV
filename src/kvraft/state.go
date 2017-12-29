package raftkv

import (
	"sync"
)

type KVState struct {
	rw sync.RWMutex

	table map[string]string

	duplicates map[int]uint
}

func (k *KVState) getValue(key string) (string, bool) {
	k.rw.RLock()
	defer k.rw.RUnlock()
	value, ok := k.table[key]
	return value, ok
}

func (k *KVState) setValue(key string, value string) {
	k.rw.Lock()
	defer k.rw.Unlock()
	k.table[key] = value
}

func (k *KVState) getDup(id int) (uint, bool) {
	k.rw.RLock()
	defer k.rw.RUnlock()
	seq, ok := k.duplicates[id]
	return seq, ok
}

func (k *KVState) setDup(id int, seq uint) {
	k.rw.Lock()
	defer k.rw.Unlock()
	k.duplicates[id] = seq
}
