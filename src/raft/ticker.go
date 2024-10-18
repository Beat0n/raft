package raft

import (
	"math/rand"
	"time"
)

const ElectionTimeout = 150
const HeartBeatTime = 100 * time.Millisecond

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	rf.resetElectionTime()
	for rf.killed() == false {
		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		time.Sleep(rf.heartBeatTime)
		rf.mu.Lock()
		if rf.role == Leader {
			go rf.sendEntries(true)
		}
		if time.Now().After(rf.electionTime) {
			go rf.startElection()
		}
		rf.mu.Unlock()
	}
}
func randomElectionTimeout() int {
	return rand.Intn(ElectionTimeout) + ElectionTimeout
}

func (rf *Raft) resetElectionTime() {
	rf.electionTime = time.Now().Add(time.Duration(randomElectionTimeout()) * time.Millisecond)
}
