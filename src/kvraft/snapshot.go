package raftkv

import (
	"bytes"
	"encoding/gob"
)

func (kv *RaftKV) takeSnapshot(newIndex int, term int) {
	DTPrintf("%d: taking snapshot at %d\n", kv.me, newIndex)
	defer DTPrintf("%d: taking snapshot done at %d\n", kv.me, newIndex)

	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)

	e.Encode(newIndex)
	e.Encode(term)

	kv.state.rw.RLock()
	e.Encode(kv.state.Table)
	e.Encode(kv.state.Duplicates)
	kv.state.rw.RUnlock()

	snapshot := w.Bytes()
	kv.persister.SaveSnapshot(snapshot)
}

func (kv *RaftKV) restoreSnapshot() {
	snapshot := kv.persister.ReadSnapshot()

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
	}
}
