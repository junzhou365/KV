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

		var lastEntryIndex int
		d.Decode(&lastEntryIndex)

		var lastEntryTerm int
		d.Decode(&lastEntryTerm)

		kv.state.rw.Lock()
		d.Decode(&kv.state.Table)
		d.Decode(&kv.state.Duplicates)
		kv.state.rw.Unlock()

		DTPrintf("%d: snapshot restored. lastIndex: %d, lastTerm: %d\n",
			kv.me, lastEntryIndex, lastEntryTerm)
	}
}
