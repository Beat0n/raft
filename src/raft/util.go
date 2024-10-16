package raft

import (
	"fmt"
	"log"
)

// Debugging
const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

func ServerName(server int, role raftRole) string {
	switch role {
	case Follower:
		return fmt.Sprintf("Follower[%d]", server)
	case Candidate:
		return fmt.Sprintf("Candidate[%d]", server)
	case Leader:
		return fmt.Sprintf("Leader[%d]", server)
	}
	return fmt.Sprintf("Server[%d]", server)
}
