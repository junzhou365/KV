package raft

import "log"
import "os"

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

func DTESTPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 1 {
		log.SetOutput(os.Stdout)
		log.SetFlags(log.LstdFlags | log.Lmicroseconds)
		log.Printf(format, a...)
	}
	return
}
