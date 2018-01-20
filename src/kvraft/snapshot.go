package raftkv

import (
	"bytes"
	"encoding/gob"
)

//type SnapshotOpRequest struct {
//done chan interface{}
//}

//func (kv *RaftKV) snapshotOpLoop() {
//for req := range kv.snapshotOpQueue {
//<-req.done
//}
//}

//func (kv *RaftKV) SerializeSnapshotOps() chan interface{} {
//req := SnapshotOpRequest{done: make(chan interface{})}
//kv.snapshotOpQueue <- req
//return req.done
//}

func (kv *RaftKV) saveOrRestoreSnapshot(snapshot []byte, use bool) {
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

func (kv *RaftKV) loadBaseLogEntry() {
	kv.state.rw.RLock()
	defer kv.state.rw.RUnlock()

	kv.rf.LoadBaseLogEntry(kv.state.lastIncludedIndex, kv.state.lastIncludedTerm)
}
