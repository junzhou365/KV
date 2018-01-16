package raftkv

import (
	"sync"
)

type KVState struct {
	rw sync.RWMutex

	Table map[string]string

	Duplicates map[int]Op

	liveRequests map[int]*Request
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

func (k *KVState) getRequest(i int) (*Request, bool) {
	k.rw.RLock()
	defer k.rw.RUnlock()
	req, ok := k.liveRequests[i]
	return req, ok
}

func (k *KVState) putRequest(i int, req *Request) {
	k.rw.Lock()
	defer k.rw.Unlock()
	k.liveRequests[i] = req
}

func (k *KVState) delRequest(i int) {
	k.rw.Lock()
	defer k.rw.Unlock()
	delete(k.liveRequests, i)
}
