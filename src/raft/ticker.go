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
	t := randomElectionTimeout()
	DPrintf2(rf, "reset ElectionTime: %dms", t)
	rf.electionTime = time.Now().Add(time.Duration(t) * time.Millisecond)
}

// Apply log or snapshot to state machine
func (rf *Raft) applier() {
	for {
		time.Sleep(ApplyFreq)
		var applyMsg []ApplyMsg
		rf.mu.Lock()
		if rf.lastApplied < rf.lastIncluded() {
			DPrintf2(rf, "apply snapshot[%d]", rf.lastIncluded())
			applyMsg = append(applyMsg, ApplyMsg{
				CommandValid:  false,
				SnapshotValid: true,
				Snapshot:      rf.persister.ReadSnapshot(),
				SnapshotIndex: rf.logs[0].Index,
				SnapshotTerm:  rf.logs[0].Term,
			})
			rf.lastApplied = rf.lastIncluded()
			if rf.lastApplied > rf.commitIndex {
				panic("error when apply")
			}
		}
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
