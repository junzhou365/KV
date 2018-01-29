package shardmaster

import (
	"github.com/google/go-cmp/cmp"
	"testing"
)

func setup() *ShardMaster {
	sm := new(ShardMaster)
	sm.configs = make([]Config, 1)
	sm.configs[0].Groups = map[int][]string{}
	sm.duplicates = make(map[int]Op)
	return sm
}

func oneConfig(num int) Config {
	groups := make(map[int][]string)
	groups[12] = []string{"hello", "world"}
	groups[14] = []string{"see", "you"}

	config := Config{
		Num:    num,
		Shards: [NShards]int{1, 2, 3, 4, 5, 6, 7, 8, 1},
		Groups: groups}
	return config
}

func TestUnitGetLastConfigCopy(t *testing.T) {
	sm := setup()
	config := oneConfig(1)
	sm.configs = append(sm.configs, config)

	copy := sm.getLastConfigCopyWOLOCK()
	if !cmp.Equal(copy, config) {
		t.Error("Not the config that was given")
	}

	config.Groups[12] = append(config.Groups[12], "yooou")
	if cmp.Equal(copy, config) {
		t.Error("Not the config that was given")
	}
}

func TestUnitDistributeShards(t *testing.T) {
	groups := make(map[int][]string)
	groups[12] = []string{"hello", "world"}
	groups[14] = []string{"see", "you"}
	groups[22] = []string{"hello", "world"}

	distributeShards(groups)
}

func TestUnitChangeStateJoin(t *testing.T) {
	sm := setup()
	groups := make(map[int][]string)
	groups[12] = []string{"hello", "world"}
	groups[14] = []string{"see", "you"}

	op := Op{Type: "Join", Servers: groups}
	sm.changeState(op)
	if copy := sm.getLastConfigCopyWOLOCK(); !cmp.Equal(copy.Groups, groups) {
		t.Error("Not the config that was given")
	}
}

func TestUnitChangeStateLeave(t *testing.T) {
	sm := setup()
	config := oneConfig(1)
	sm.configs = append(sm.configs, config)
	op := Op{Type: "Leave", GIDs: []int{12}}
	sm.changeState(op)
	copy := sm.getLastConfigCopyWOLOCK()
	if v, ok := copy.Groups[12]; ok {
		DTPrintf("v: %v\n", v)
		t.Error("Not the config that was given")
	}
}

func TestUnitChangeStateMove(t *testing.T) {
	sm := setup()
	config := oneConfig(1)
	sm.configs = append(sm.configs, config)
	op := Op{Type: "Move", GIDs: []int{2}, Shard: 6}
	sm.changeState(op)
	copy := sm.getLastConfigCopyWOLOCK()
	if v := copy.Shards[6]; v != 2 {
		t.Error("Not the config that was given")
	}
}

func TestUnitChangeStateQuery(t *testing.T) {
	sm := setup()
	sm.configs = append(sm.configs, oneConfig(1))
	sm.configs = append(sm.configs, oneConfig(2))

	op := Op{Type: "Query"}
	ret := sm.changeState(op)
	if ret.Config.Num != 2 {
		t.Error("Not the config that was given")
	}

}
