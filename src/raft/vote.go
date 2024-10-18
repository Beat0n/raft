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

//func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
//	// Your code here (2A, 2B).
//	rf.mu.Lock()
//	defer rf.mu.Unlock()
//	DPrintf("---Term %d--- %s receive request vote from %s, args.Term is %d\n", rf.currentTerm, ServerName(rf.me, rf.role), ServerName(args.CandidateId, Candidate), args.Term)
//
//	if args.Term < rf.currentTerm {
//		reply.Term = rf.currentTerm
//		reply.VoteGranted = false
//		return
//	}
//	if args.Term > rf.currentTerm {
//		rf.currentTerm = args.Term
//	}
//
//	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && rf.checkUp2Date(args) {
//		rf.grantVote(args, reply)
//	} else {
//		reply.VoteGranted = false
//	}
//	reply.Term = rf.currentTerm
//	DPrintf2(rf, "send reply for request vote to %s, reply %v\n", ServerName(args.CandidateId, Candidate), reply.VoteGranted)
//}

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
	if args.Term > rf.currentTerm {
		rf.setNewTerm(args.Term)
	}
	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && rf.checkUp2Date(args) {
		rf.grantVote(args, reply)
	}

	reply.Term = rf.currentTerm
	DPrintf("---Term %d--- %s send reply for request vote to %s, reply %v\n", rf.currentTerm, ServerName(rf.me, rf.role), ServerName(args.CandidateId, Candidate), reply.VoteGranted)
}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return. Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	DPrintf("---Term %d--- %s send request_vote to %s", args.Term, ServerName(rf.me, rf.role), ServerName(server, 3))
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

// Check candidate's log is at least as up-to-date as receiver's log
func (rf *Raft) checkUp2Date(args *RequestVoteArgs) bool {
	myLastLog := rf.lastLog()
	return args.LastLogTerm > myLastLog.Term || (args.LastLogTerm == myLastLog.Term && args.LastLogIndex >= myLastLog.Index)
}

func (rf *Raft) grantVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	reply.VoteGranted = true
	rf.votedFor = args.CandidateId
	rf.role = Follower
	rf.currentTerm = args.Term
	rf.resetElectionTime()
}
