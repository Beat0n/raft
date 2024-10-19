package raft

import (
	"fmt"
	"log"
)

func init() {
	log.SetFlags(log.Lmicroseconds)
}

// Debugging
const Debug = false

func DPrintf(format string, a ...interface{}) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

func DPrintf2(rf *Raft, format string, a ...interface{}) {
	if Debug {
		log.Printf("---Term %d--- %s %s", rf.currentTerm, ServerName(rf.me, rf.role), fmt.Sprintf(format, a...))
	}
	return
}

func ServerName(server int, role raftRole) string {
	switch role {
	case Follower:
		return fmt.Sprintf("{Follower %d}", server)
	case Candidate:
		return fmt.Sprintf("{Candidate %d}", server)
	case Leader:
		return fmt.Sprintf("{Leader %d}", server)
	}
	return fmt.Sprintf("{Server %d}", server)
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}
