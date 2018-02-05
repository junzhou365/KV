package shardkv

import (
	"bytes"
	"encoding/gob"
	"raft"
)

func (kv *ShardKV) takeSnapshot(lastIndex int, lastTerm int) {
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)

	kv.state.rw.Lock()
	kv.state.lastIncludedIndex = lastIndex
	kv.state.lastIncludedTerm = lastTerm

	e.Encode(kv.state.lastIncludedIndex)
	e.Encode(kv.state.lastIncludedTerm)
	e.Encode(kv.state.Table)
	e.Encode(kv.state.Duplicates)
	kv.state.rw.Unlock()

	snapshot := w.Bytes()
	// Protected by Serialize
	kv.persister.SaveSnapshot(snapshot)
}

func (kv *ShardKV) checkForTakingSnapshot(msg raft.ApplyMsg) {
	if kv.maxraftstate == -1 {
		return
	}
	// The raft size might be reduced by raft taking snapshots. But it's ok.
	if kv.maxraftstate-kv.persister.RaftStateSize() <= kv.stateDelta {
		if msg.Index <= kv.state.getLastIncludedIndex() {
			DTPrintf("%d: new snapshot was given for %+v. Just return\n", kv.me, msg)
			return
		}

		DTPrintf("%d: take snapshot\n", kv.me)
		// we must first save snapshot
		kv.takeSnapshot(msg.Index, msg.Term)
		go kv.rf.DiscardLogEnries(msg.Index, msg.Term)
	}
}

func (kv *ShardKV) saveOrRestoreSnapshot(snapshot []byte, use bool) {
	defer DTPrintf("%d: restore snapshot use: %t, done\n", kv.me, use)

	if snapshot == nil { // bootstrap
		snapshot = kv.persister.ReadSnapshot()
	}

	if snapshot == nil || len(snapshot) < 1 { // bootstrap without any state?
		return
	}

	kv.persister.SaveSnapshot(snapshot)

	if use {
		r := bytes.NewBuffer(snapshot)
		d := gob.NewDecoder(r)

		kv.state.rw.Lock()
		d.Decode(&kv.state.lastIncludedIndex)
		d.Decode(&kv.state.lastIncludedTerm)
		d.Decode(&kv.state.Table)
		d.Decode(&kv.state.Duplicates)

		DTPrintf("%d: snapshot restored. lastIndex: %d, lastTerm: %d\n",
			kv.me, kv.state.lastIncludedIndex, kv.state.lastIncludedTerm)
		kv.state.rw.Unlock()
	}
}

func (kv *ShardKV) loadBaseLogEntry() {
	kv.state.rw.RLock()
	defer kv.state.rw.RUnlock()

	kv.rf.LoadBaseLogEntry(kv.state.lastIncludedIndex, kv.state.lastIncludedTerm)
}
