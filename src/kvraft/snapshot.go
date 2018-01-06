package raftkv

import (
	"bytes"
	"encoding/gob"
)

func (kv *RaftKV) takeSnapshot() {
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)

	e.Encode(kv.state.getLastIndex)
	e.Encode(kv.state.getLastTerm)

	kv.state.rw.RLock()
	e.Encode(kv.state.Table)
	e.Encode(kv.state.Duplicates)
	kv.state.rw.RUnlock()

	snapshot := w.Bytes()
	kv.persister.SaveSnapshot(snapshot)
}

func (kv *RaftKV) restoreSnapshot(snapshot []byte) {
	if snapshot == nil || len(snapshot) < 1 { // bootstrap without any state?
	} else {
		r := bytes.NewBuffer(snapshot)
		d := gob.NewDecoder(r)

		var lastEntryIndex int
		d.Decode(&lastEntryIndex)
		kv.state.setLastIndex(lastEntryIndex)

		var lastEntryTerm int
		d.Decode(&lastEntryTerm)
		kv.state.setLastTerm(lastEntryTerm)

		kv.state.rw.Lock()
		d.Decode(&kv.state.Table)
		d.Decode(&kv.state.Duplicates)
		kv.state.rw.Unlock()
	}
}
