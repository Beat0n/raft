package raft

type entry struct {
	Command interface{}
	Term    int
	Index   int
}

type AppendEntriesArgs struct {
	Term     int
	LeaderId int
	Entries  []entry
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

// followers receive entries from leader
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term >= rf.currentTerm {
		rf.resetElectionTime()
		rf.setNewTerm(args.Term)
		if len(args.Entries) == 0 { // Receive heartbeat
			reply.Success = true
		} else {

		}
	} else { // if args.Term < rf.currentTerm
		// from old leader
	}
	reply.Term = rf.currentTerm
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	DPrintf("---Term %d---%s send entries to %s\n", args.Term, ServerName(rf.me, rf.role), ServerName(server, 3))
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) sendEntries(isHeartBeat bool) {
	rf.resetElectionTime()
	args := AppendEntriesArgs{
		Term:     rf.currentTerm,
		LeaderId: rf.me,
		Entries:  nil,
	}
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		reply := AppendEntriesReply{}
		go rf.sendAppendEntries(i, &args, &reply)
	}
}
