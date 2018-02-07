package shardkv

import "log"
import "os"
import "fmt"

// Debugging
const Debug = 3

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

func DTPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 2 {
		log.SetOutput(os.Stdout)
		log.SetFlags(log.LstdFlags | log.Lmicroseconds)
		log.Printf(format, a...)
	}
	return
}

func (kv *ShardKV) printf(format string, a ...interface{}) {
	_, isLeader := kv.rf.GetState()
	l := "leader"
	if !isLeader {
		l = "follower"
	}
	of := fmt.Sprintf("[%d-%d-%s]: ", kv.gid, kv.me, l)
	cf := "\n"
	log.SetOutput(os.Stdout)
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)
	log.Printf(of+format+cf, a...)
}

func (kv *ShardKV) KVPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 2 {
		kv.printf(format, a...)
	}
	return
}

func (kv *ShardKV) KVLeaderPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 2 {
		if _, isLeader := kv.rf.GetState(); isLeader {
			kv.printf(format, a...)
		}
	}
	return
}

func DTESTPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 1 {
		of := "\033[34m<<<<==== "
		cf := " ====>>>>\033[0m\n"
		log.SetOutput(os.Stdout)
		log.SetFlags(log.LstdFlags | log.Lmicroseconds)
		log.Printf(of+format+cf, a...)
	}
	return
}
