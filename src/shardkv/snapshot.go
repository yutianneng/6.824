package shardkv

import (
	"6.824/labgob"
	"bytes"
)

func (kv *ShardKV) maybeSnapshot(index int) {
	if kv.maxraftstate == -1 {
		return
	}
	if kv.persister.RaftStateSize() > kv.maxraftstate {
		//DPrintf("maybeSnapshot starting, index: %v", index)
		kv.rf.Snapshot(index, kv.encodeSnapshot(index))
	}
}

//上层加锁
func (kv *ShardKV) encodeSnapshot(lastIncludedIndex int) []byte {

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.currentConfig)
	e.Encode(kv.lastConfig)
	e.Encode(lastIncludedIndex)
	e.Encode(kv.shardMap)
	return w.Bytes()
}

//上层加锁
func (kv *ShardKV) decodeSnapshot(snapshot []byte) bool {

	if len(snapshot) == 0 {
		return true
	}
	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)

	if err := d.Decode(&kv.currentConfig); err != nil {
		return false
	}
	if err := d.Decode(&kv.lastConfig); err != nil {
		return false
	}
	if err := d.Decode(&kv.lastIncludedIndex); err != nil {
		return false
	}
	if err := d.Decode(&kv.shardMap); err != nil {
		return false
	}
	return true
}
