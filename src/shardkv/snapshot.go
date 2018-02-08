package shardkv

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"raft"
	"shardmaster"
)

func (kv *ShardKV) takeSnapshot(lastIndex int, lastTerm int) {
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)

	kv.state.rw.Lock()
	kv.checkSnapshotHealth("take snapshot")
	kv.state.lastIncludedIndex = lastIndex
	kv.state.lastIncludedTerm = lastTerm

	e.Encode(kv.state.lastIncludedIndex)
	e.Encode(kv.state.lastIncludedTerm)
	e.Encode(kv.state.Table)
	e.Encode(kv.state.Duplicates)
	e.Encode(kv.state.Shards)
	e.Encode(kv.state.ConfigNums)
	snapshot := w.Bytes()
	//kv.KVPrintf("snapshot taken. lastIndex: %v, table: %v, duplicates: %v, shards: %v",
	//lastIndex, kv.state.Table, kv.state.Duplicates, kv.state.Shards)

	// Protected by Serialize
	kv.persister.SaveSnapshot(snapshot)
	kv.state.rw.Unlock()
}

func (kv *ShardKV) checkForTakingSnapshot(msg raft.ApplyMsg) {
	if kv.maxraftstate == -1 {
		return
	}
	// The raft size might be reduced by raft taking snapshots. But it's ok.
	if kv.maxraftstate-kv.persister.RaftStateSize() <= kv.stateDelta {
		if msg.Index <= kv.state.getLastIncludedIndex() {
			DTPrintf("%d-%d: new snapshot was given for %+v. Just return\n",
				kv.gid, kv.me, msg)
			return
		}

		kv.KVPrintf("take snapshot")
		// we must first save snapshot
		kv.takeSnapshot(msg.Index, msg.Term)
		go kv.rf.DiscardLogEnries(msg.Index, msg.Term)
		//kv.KVPrintf("after snapshot, table: %v, dup: %v, shards: %v",
		//kv.state.Table, kv.state.Duplicates, kv.state.Shards)
	}
}

func (kv *ShardKV) saveOrRestoreSnapshot(snapshot []byte, use bool) {
	if snapshot == nil { // bootstrap
		snapshot = kv.persister.ReadSnapshot()
	}

	if snapshot == nil || len(snapshot) < 1 { // bootstrap without any state?
		return
	}

	kv.persister.SaveSnapshot(snapshot)

	if use {
		kv.state.rw.Lock()
		r := bytes.NewBuffer(snapshot)
		d := gob.NewDecoder(r)

		var lastIndex int
		d.Decode(&lastIndex)
		kv.state.lastIncludedIndex = lastIndex

		var lastTerm int
		d.Decode(&lastTerm)
		kv.state.lastIncludedTerm = lastTerm

		var table map[string]string
		d.Decode(&table)
		kv.state.Table = table

		var duplicates map[int]Op
		d.Decode(&duplicates)
		kv.state.Duplicates = duplicates

		var shards [shardmaster.NShards]int
		d.Decode(&shards)
		kv.state.Shards = shards

		var nums [shardmaster.NShards]int
		d.Decode(&nums)
		kv.state.ConfigNums = nums

		//kv.KVPrintf("snapshot restored. lastIndex: %v, table: %v, duplicates: %v, shards: %v",
		//kv.state.lastIncludedIndex, kv.state.Table, kv.state.Duplicates, kv.state.Shards)

		kv.checkSnapshotHealth("receive snapshot")
		kv.state.rw.Unlock()
	}
}

func (kv *ShardKV) loadBaseLogEntry() {
	kv.state.rw.RLock()
	defer kv.state.rw.RUnlock()

	kv.rf.LoadBaseLogEntry(kv.state.lastIncludedIndex, kv.state.lastIncludedTerm)
}

// For debug
func (kv *ShardKV) checkSnapshotHealth(s string) {
	for k, _ := range kv.state.Table {
		if kv.state.Shards[key2shard(k)] != kv.gid {
			panic(fmt.Sprintf("%d-%d: health check failed for %s. Key: %v",
				kv.gid, kv.me, s, k))
		}
	}
}
