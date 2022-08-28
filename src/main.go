package main

import (
	"6.824/raft"
	"fmt"
)

func main() {

	m := raft.Leader
	fmt.Println(getState(m))
}

func getState(m raft.MemberRole) bool {
	return m == raft.Leader
}
