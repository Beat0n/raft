package raft

type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
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
	DPrintf("---Term %d---%s receive request vote from %s, args.Term is %d\n", rf.currentTerm, ServerName(rf.me, rf.role), ServerName(args.CandidateId, Candidate), args.Term)
	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
	} else if args.Term == rf.currentTerm {
		if rf.votedFor != -1 && rf.votedFor != args.CandidateId {
			reply.VoteGranted = false
		} else {
			reply.VoteGranted = true
			rf.votedFor = args.CandidateId
		}
	} else { // if args.Term > rf.currentTerm
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
		rf.currentTerm = args.Term
	}

	if reply.VoteGranted {
		rf.role = Follower
		rf.resetElectionTime()
	}
	reply.Term = rf.currentTerm
	DPrintf("---Term %d---%s send reply for request vote to %s, reply %v\n", rf.currentTerm, ServerName(rf.me, rf.role), ServerName(args.CandidateId, Candidate), reply.VoteGranted)
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
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	DPrintf("---Term %d--- %s send request vote to %s", args.Term, ServerName(rf.me, rf.role), ServerName(server, 3))
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}