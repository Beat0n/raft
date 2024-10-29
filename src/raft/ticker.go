package raft

import (
	"math/rand"
	"time"
)

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
			rf.sendEntries(false)
		}
		if time.Now().After(rf.electionTime) {
			rf.startElection()
		}
		rf.mu.Unlock()
	}
}

func randomElectionTimeout() int {
	return rand.Intn(ElectionTimeoutMax-ElectionTimeoutMin) + ElectionTimeoutMin
}

func (rf *Raft) resetElectionTime() {
	rf.electionTime = time.Now().Add(time.Duration(randomElectionTimeout()) * time.Millisecond)
}

// Apply log to state machine
func (rf *Raft) applier() {
	for {
		time.Sleep(ApplyFreq)
		var applyMsg []ApplyMsg
		rf.mu.Lock()
		for rf.lastApplied < rf.commitIndex {
			rf.lastApplied++
			log := &rf.logs[rf.lastApplied-rf.lastIncluded()]
			DPrintf2(rf, "apply log[%d], command is %v\n", rf.lastApplied, log.Command)
			applyMsg = append(applyMsg, ApplyMsg{
				CommandValid: true,
				Command:      log.Command,
				CommandIndex: log.Index,
				CommandTerm:  log.Term,
			})
		}
		rf.mu.Unlock()
		for _, msg := range applyMsg {
			rf.applyCh <- msg
		}
	}
}
