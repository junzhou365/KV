package raftkv

import "labrpc"
import "crypto/rand"
import "math/big"
import "sync"

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	seq uint
	uid int
	rw  sync.RWMutex
}

func (ck *Clerk) increaseSeq() {
	ck.rw.Lock()
	defer ck.rw.Unlock()
	ck.seq++
}

func (ck *Clerk) getSeq() uint {
	ck.rw.RLock()
	defer ck.rw.RUnlock()
	return ck.seq
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.uid = int(nrand())
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("RaftKV.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {
	// You will have to modify this function.
	DTPrintf("Client Get, key: %s, value: %s, op: %s\n", key)
	args := GetArgs{
		Key:   key,
		State: ReqState{Seq: ck.getSeq(), Id: ck.uid}}

	for {
		for i := 0; i < len(ck.servers); i++ {
		RETRY:
			for ok := false; ; {
				reply := GetReply{}
				ok = ck.servers[i].Call("RaftKV.Get", &args, &reply)

				switch {
				case !ok:
					continue

				case reply.WrongLeader:
					break RETRY
				}

				ck.increaseSeq()
				return reply.Value
			}
		}
	}
	return ""
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("RaftKV.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	DTPrintf("Client PutAppend, key: %s, value: %s, op: %s\n", key, value, op)
	args := PutAppendArgs{
		Key:   key,
		Value: value,
		Op:    op,
		State: ReqState{Seq: ck.getSeq(), Id: ck.uid}}

	for {
		for i := 0; i < len(ck.servers); i++ {
		RETRY:
			for ok := false; ; {
				reply := PutAppendReply{}
				ok = ck.servers[i].Call("RaftKV.PutAppend", &args, &reply)

				switch {
				case !ok:
					continue
				case reply.WrongLeader:
					break RETRY
				}

				ck.increaseSeq()
				return
			}
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
