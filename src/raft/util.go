package raft

import "log"
import "os"

// Debugging
const Debug = 1

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
	of := "\033[34m<<<<==== "
	cf := " ====>>>>\033[0m\n"
	if Debug > 1 {
		log.SetOutput(os.Stdout)
		log.SetFlags(log.LstdFlags | log.Lmicroseconds)
		log.Printf(of+format+cf, a...)
	}
	return
}
