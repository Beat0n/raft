package raft

import (
	"sync"
)

type votes struct {
	agree    int
	disagree int
	once     sync.Once // do becomeLeader once
}

func (rf *Raft) startElection() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.resetElectionTime()
	rf.role = Candidate
	rf.votedFor = rf.me // Vote for self
	rf.currentTerm++    // Increment currentTerm

	summer := votes{}
	summer.agree = 1
	summer.disagree = 0

	args := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: 0,
		LastLogTerm:  0,
	}
	// Send RequestVote RPCs to all other servers
	for server := range rf.peers {
		if server == rf.me {
			continue
		}
		reply := RequestVoteReply{}
		go rf.handleVote(server, &args, &reply, &summer)
	}
}

func (rf *Raft) handleVote(server int, args *RequestVoteArgs, reply *RequestVoteReply, summer *votes) {
	ok := rf.sendRequestVote(server, args, reply)
	// handle reply sequentially
	rf.mu.Lock()
	defer rf.mu.Unlock()
	n := len(rf.peers)
	if ok && reply.VoteGranted {
		summer.agree++
		// If votes received from majority of servers: become leader
		if summer.agree > (n / 2) {
			summer.once.Do(func() {
				rf.becomeLeader(true)
			})
		}
		return
	}
	summer.disagree++
	if reply.Term > rf.currentTerm || summer.disagree > (n/2) {
		summer.once.Do(func() {
			rf.becomeLeader(false)
		})
	}
}

func (rf *Raft) becomeLeader(success bool) {
	rf.votedFor = -1
	if success {
		DPrintf("---Term %d---%s become leader", rf.currentTerm, ServerName(rf.me, 3))
		rf.role = Leader
		go rf.sendEntries(true)
		for server := range rf.peers {
			rf.nextIndex[server] = rf.lastLog().Index + 1
			rf.matchIndex[server] = 0
		}
		rf.nMatch = make(map[int]int)
	} else {
		DPrintf("---Term %d---%s Election Failed", rf.currentTerm, ServerName(rf.me, 3))
		rf.role = Follower
	}
}
