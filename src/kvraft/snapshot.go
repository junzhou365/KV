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

func (kv *RaftKV) restoreSnapshot() {
	snapshot := kv.readSnapshot()

	if snapshot == nil || len(snapshot) < 1 { // bootstrap without any state?
	} else {
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

		kv.rf.LoadSnapshotMetaData(lastEntryIndex, lastEntryTerm)
		DTPrintf("%d: snapshot restored. lastIndex: %d, lastTerm: %d\n",
			kv.me, lastEntryIndex, lastEntryTerm)
	}
}

func (kv *RaftKV) saveSnapshot(snapshot []byte) {
	rw := kv.rf.GetPersisterLock()

	rw.Lock()
	defer rw.Unlock()

	kv.persister.SaveSnapshot(snapshot)
}

func (kv *RaftKV) readSnapshot() []byte {
	rw := kv.rf.GetPersisterLock()

	rw.Lock()
	defer rw.Unlock()

	return kv.persister.ReadSnapshot()
}
