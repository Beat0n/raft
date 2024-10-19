package raft

type votes struct {
	agree    int
	disagree int
	done     bool
}

func (rf *Raft) startElection() {
	rf.resetElectionTime()
	rf.role = Candidate
	rf.votedFor = rf.me // Vote for self
	rf.currentTerm++    // Increment currentTerm

	summer := new(votes)
	summer.agree = 1
	summer.disagree = 0
	summer.done = false

	lastLog := rf.lastLog()

	args := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: lastLog.Index,
		LastLogTerm:  lastLog.Term,
	}
	DPrintf2(rf, "start election")
	// Send RequestVote RPCs to all other servers
	for server := range rf.peers {
		if server == rf.me {
			continue
		}
		reply := RequestVoteReply{}
		go rf.handleVote(server, &args, &reply, summer)
	}
}

func (rf *Raft) handleVote(server int, args *RequestVoteArgs, reply *RequestVoteReply, summer *votes) {
	if !rf.sendRequestVote(server, args, reply) {
		return
	}
	// handle reply sequentially
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if summer.done || reply.Term < rf.currentTerm {
		return
	}
	if reply.Term > rf.currentTerm {
		rf.setNewTerm(reply.Term)
		summer.done = true
		return
	}
	n := len(rf.peers)
	if reply.VoteGranted {
		summer.agree++
		// If votes received from the majority of servers: become leader
		if summer.agree > (n/2) && rf.currentTerm == args.Term && rf.role == Candidate {
			rf.becomeLeader()
			summer.done = true
		}
		return
	}
	summer.disagree++
	if summer.disagree > (n / 2) {
		DPrintf2(rf, "election failed")
		rf.votedFor = -1
		rf.role = Follower
		summer.done = true
	}
}

func (rf *Raft) becomeLeader() {
	DPrintf2(rf, "become leader")
	rf.role = Leader
	rf.nMatch = make(map[int]int)
	rf.curLogStart = -1
	go rf.sendEntries(true)
	for server := range rf.peers {
		rf.nextIndex[server] = rf.lastLog().Index + 1
		rf.matchIndex[server] = 0
	}
}
