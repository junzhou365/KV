package raftkv

import (
	"bytes"
	"encoding/gob"
)

func (kv *RaftKV) takeSnapshotWithNoLock(lastIndex int, lastTerm int) {
	DTPrintf("%d: taking snapshot at %d\n", kv.me, lastIndex)
	defer DTPrintf("%d: taking snapshot done at %d\n", kv.me, lastIndex)

	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)

	e.Encode(lastIndex)
	e.Encode(lastTerm)

	e.Encode(kv.state.Table)
	e.Encode(kv.state.Duplicates)

	snapshot := w.Bytes()
	kv.persister.SaveSnapshot(snapshot)
}

func (kv *RaftKV) saveOrRestoreSnapshot(snapshot []byte, use bool) {
	kv.state.rw.Lock()
	defer kv.state.rw.Unlock()

	if snapshot == nil { // bootstrap
		snapshot = kv.persister.ReadSnapshot()
	}

	if snapshot == nil || len(snapshot) < 1 { // bootstrap without any state?
		return
	}

	kv.persister.SaveSnapshot(snapshot)

	restoreSnapshot := func() {
		req := StateRequest{done: make(chan interface{})}
		rf.state.queue <- req
		defer close(req.done)

		prw := kv.rf.GetPersisterLock()
		prw.Lock()
		defer prw.Unlock()

		r := bytes.NewBuffer(snapshot)
		d := gob.NewDecoder(r)

		var lastEntryIndex int
		d.Decode(&lastEntryIndex)

		var lastEntryTerm int
		d.Decode(&lastEntryTerm)

		if !use {
			// snapshot might have been taken before this
			if kv.rf.IndexExistWithNoLock(lastEntryIndex) {
				kv.rf.DiscardLogEnriesWithNoLock(lastEntryIndex)
				DTPrintf("%d: only discard entries up to %d\n", kv.me, lastEntryIndex)
			}
		} else {
			kv.rf.DiscardLogEnriesWithNoLock(-1)

			d.Decode(&kv.state.Table)
			d.Decode(&kv.state.Duplicates)

			kv.rf.LoadSnapshotMetaDataWithNoLock(lastEntryIndex, lastEntryTerm)
			DTPrintf("%d: snapshot restored. lastIndex: %d, lastTerm: %d\n",
				kv.me, lastEntryIndex, lastEntryTerm)
		}
	}

	restoreSnapshot()
}
