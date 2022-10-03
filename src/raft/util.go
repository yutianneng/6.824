package raft

import "log"

// Debugging
const Debug = false

func init() {
	log.SetFlags(log.Ltime)
}

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}
