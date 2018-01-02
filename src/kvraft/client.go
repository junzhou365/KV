package raftkv

import "labrpc"
import "crypto/rand"
import "math/big"
import "sync"
import "time"

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	rw         sync.RWMutex
	seq        uint
	uid        int
	lastServer int
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

func (ck *Clerk) setLastServer(i int) {
	ck.rw.Lock()
	defer ck.rw.Unlock()
	ck.lastServer = i
}

func (ck *Clerk) getLastServer() int {
	ck.rw.RLock()
	defer ck.rw.RUnlock()
	return ck.lastServer
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
	ck.setLastServer(-1)
	return ck
}

type ReqRes struct {
	ok          bool
	wrongLeader bool
	value       string
	server      int
}

func (ck *Clerk) sendRequst(args interface{}) string {

	resCh := make(chan ReqRes)
	done := make(chan interface{})
	defer close(done)

	sendReqTo := func(server int) {

		res := ReqRes{server: server}

		switch args.(type) {
		case *GetArgs:
			reply := GetReply{}
			res.ok = ck.servers[server].Call("RaftKV.Get", args, &reply)
			res.value = reply.Value
			res.wrongLeader = reply.WrongLeader

		case *PutAppendArgs:
			reply := PutAppendReply{}
			res.ok = ck.servers[server].Call("RaftKV.PutAppend", args, &reply)
			res.wrongLeader = reply.WrongLeader
		}

		select {
		case resCh <- res:
		case <-done:
		}
	}

	serverToSendIndexes := func() <-chan int {
		serverStream := make(chan int)
		go func() {
			defer close(serverStream)
			if ls := ck.getLastServer(); ls != -1 {
				serverStream <- ls
			} else {
				for i := 0; i < len(ck.servers); i++ {
					serverStream <- i
				}
			}
		}()

		return serverStream
	}

	for {
		sent := 0
		for i := range serverToSendIndexes() {
			go sendReqTo(i)
			sent++
		}

		// process responses
		for res := range resCh {
			if res.ok && !res.wrongLeader {
				ck.setLastServer(res.server)
				ck.increaseSeq()
				return res.value
			} else {
				ck.setLastServer(-1)
			}

			if sent--; sent == 0 {
				break
			}
		}

		time.Sleep(20 * time.Millisecond)
	}

	return ""
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
	DTPrintf("Client [Get], key: %s\n", key)
	args := GetArgs{
		Key:   key,
		State: ReqState{Seq: ck.getSeq(), Id: ck.uid}}

	ret := ck.sendRequst(&args)
	return ret
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
	DTPrintf("Client [PutAppend], key: %s, value: %s, op: %s\n", key, value, op)
	args := PutAppendArgs{
		Key:   key,
		Value: value,
		Op:    op,
		State: ReqState{Seq: ck.getSeq(), Id: ck.uid}}

	ck.sendRequst(&args)
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
