package shardkv

//
// Sharded key/value server.
// Lots of replica groups, each running op-at-a-time paxos.
// Shardmaster decides which group serves each shard.
// Shardmaster may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const (
	OK            = "OK"
	ErrNoKey      = "ErrNoKey"
	ErrNotLeader  = "ErrNotLeader"
	ErrWrongGroup = "ErrWrongGroup"
)

type Err string

type ReqState struct {
	Seq uint
	Id  int
}

// Put or Append
type PutAppendArgs struct {
	// You'll have to add definitions here.
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	State ReqState
}

type PutAppendReply struct {
	WrongLeader bool
	Err         Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	State ReqState
}

type GetReply struct {
	WrongLeader bool
	Err         Err
	Value       string
}

type MigrateArgs struct {
	Table      map[string]string
	Duplicates map[int]Op
	GID        int // sender GID
	Shard      int
	Index      int // for debug
	Num        int
}

type MigrateReply struct {
	WrongLeader bool
	Err         Err
}

type QueryOwnerArgs struct {
	Shard int
	GID   int
}

type QueryOwnerReply struct {
	WrongLeader bool
	Owner       int
	Err         Err
}
