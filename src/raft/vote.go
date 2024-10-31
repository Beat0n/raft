package raft

type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int // index of candidate's last log entry
	LastLogTerm  int // term of candidate's last log entry
}

type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("---Term %d--- %s receive request vote from %s, args.Term is %d\n", rf.currentTerm, ServerName(rf.me, rf.role), ServerName(args.CandidateId, Candidate), args.Term)
	reply.VoteGranted = false
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		return
	}
	toBePersist := false
	if args.Term > rf.currentTerm {
		rf.setNewTerm(args.Term)
		toBePersist = true
	}
	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && rf.checkUp2Date(args) {
		rf.grantVote(args, reply)
		toBePersist = true
	}
	if toBePersist {
		rf.persist()
	}
	reply.Term = rf.currentTerm
	DPrintf("---Term %d--- %s send reply for request vote to %s, reply %v\n", rf.currentTerm, ServerName(rf.me, rf.role), ServerName(args.CandidateId, Candidate), reply.VoteGranted)
}

// Check candidate's log is at least as up-to-date as receiver's log
func (rf *Raft) checkUp2Date(args *RequestVoteArgs) bool {
	myLastLog := rf.lastLog()
	return args.LastLogTerm > myLastLog.Term || (args.LastLogTerm == myLastLog.Term && args.LastLogIndex >= myLastLog.Index)
}

func (rf *Raft) grantVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.resetElectionTime()
	reply.VoteGranted = true
	rf.votedFor = args.CandidateId
	rf.role = Follower
	rf.currentTerm = args.Term
}
