package shardkv

import (
	"shardmaster"
	"sync"
)

type KVState struct {
	rw sync.RWMutex

	Table map[string]string

	Duplicates map[int]Op

	// the shards state ShardKV has
	// If an op is executed but not in snapshot. Upon next restart,
	// The server replays the op, but finds out that the server doesn't own
	// this shard, then the op is omitted.
	// We need to put his into snapshot. Modification on this is coordinated by
	// raft sequential operations.
	Shards [shardmaster.NShards]int

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

func (k *KVState) setShard(shard int, gid int) {
	k.rw.Lock()
	defer k.rw.Unlock()

	k.Shards[shard] = gid
}

func (k *KVState) getShardGID(shard int) (gid int) {
	k.rw.RLock()
	defer k.rw.RUnlock()

	gid = k.Shards[shard]
	return gid
}
