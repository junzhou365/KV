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
	emptyConfig := Config{
		Groups: map[int][]string{}}
	if !cmp.Equal(sm.getLastConfigCopyWOLOCK(), emptyConfig) {
		t.Error("Not the config that was given")
	}

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
	var ret [NShards]int

	groups := make(map[int][]string)
	groups[12] = []string{"hello", "world"}
	groups[14] = []string{"see", "you"}
	groups[22] = []string{"hello", "world"}

	config := Config{
		Num:    0,
		Shards: [NShards]int{},
		Groups: groups}
	ret = distributeShards(config)

	// exact same groups
	groups = make(map[int][]string)
	groups[12] = []string{"hello", "world"}
	groups[14] = []string{"see", "you"}
	groups[22] = []string{"hello", "world"}
	config.Groups = groups
	ret = distributeShards(config)
	DTPrintf("new shards are %v\n", ret)

	config.Shards = ret
	if !cmp.Equal(ret, distributeShards(config)) {
		t.Error("Wrong shards")
	}

	groups = make(map[int][]string)
	for i := 1; i < 10+1; i++ {
		groups[i] = []string{}
	}
	config.Groups = groups
	ret = distributeShards(config)
	DTPrintf("new shards are %v\n", ret)

	config.Shards = ret
	groups = make(map[int][]string)
	for i := 1; i < 15+1; i++ {
		groups[i] = []string{}
	}
	config.Groups = groups
	ret = distributeShards(config)
	DTPrintf("new shards are %v\n", ret)

	// remove some gids
	config.Shards = ret
	groups = make(map[int][]string)
	for i := 1; i < 10+1; i++ {
		groups[i] = []string{}
	}
	config.Groups = groups
	ret = distributeShards(config)
	DTPrintf("new shards are %v\n", ret)

	// remove gid:14, 15
	groups = make(map[int][]string)
	groups[12] = []string{"hello", "world"}
	config.Groups = groups
	ret = distributeShards(config)
	DTPrintf("new shards are %v\n", ret)

	// add 22 back
	groups = make(map[int][]string)
	groups[12] = []string{"hello", "world"}
	groups[14] = []string{"see", "you"}
	groups[22] = []string{"hello", "world"}
	config.Groups = groups
	ret = distributeShards(config)
	DTPrintf("new shards are %v\n", ret)
}

func TestUnitChangeStateJoin(t *testing.T) {
	sm := setup()
	groups := make(map[int][]string)
	groups[12] = []string{"hello", "world"}
	groups[14] = []string{"see", "you"}

	op := Op{Seq: 0, Type: "Join", Servers: groups}
	sm.changeState(op)
	if copy := sm.getLastConfigCopyWOLOCK(); !cmp.Equal(copy.Groups, groups) {
		DTPrintf("groups: %v, shards: %v", copy.Groups, copy.Shards)
		t.Error("Not the config that was given")
	}

	groups = make(map[int][]string)
	groups[22] = []string{"a", "b"}
	groups[24] = []string{"c", "d"}
	op = Op{Seq: 1, Type: "Join", Servers: groups}
	sm.changeState(op)

	expectedGroups := make(map[int][]string)
	expectedGroups[12] = []string{"hello", "world"}
	expectedGroups[14] = []string{"see", "you"}
	expectedGroups[22] = []string{"a", "b"}
	expectedGroups[24] = []string{"c", "d"}
	if copy := sm.getLastConfigCopyWOLOCK(); !cmp.Equal(copy.Groups, expectedGroups) {
		DTPrintf("groups: %v, shards: %v", copy.Groups, copy.Shards)
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

	ret := sm.changeState(Op{Type: "Query", Seq: 1, Num: 1})
	if ret.Config.Num != 1 {
		DTPrintf("config: %v\n", ret.Config)
		t.Error("Not the config that was given")
	}

	ret = sm.changeState(Op{Type: "Query", Seq: 2, Num: 2})
	if ret.Config.Num != 2 {
		DTPrintf("ret: %v, lastest config: %v\n", ret, ret.Config)
		t.Error("Not the config that was given")
	}

}
