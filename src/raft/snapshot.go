package raft

type InstallSnapshotArgs struct {
	Term              int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
}

type InstallSnapshotReply struct {
	Term int
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer func() {
		reply.Term = rf.currentTerm
	}()
	DPrintf2(rf, "receive snapshot with LastIncludedIndex: %d", args.LastIncludedIndex)
	if args.Term < rf.currentTerm {
		return
	}
	rf.resetElectionTime()
	if args.Term > rf.currentTerm {
		rf.setNewTerm(args.Term)
	} else {
		rf.role = Follower
	}
	if rf.outdatedSnapshot(args.LastIncludedTerm, args.LastIncludedIndex) {
		DPrintf2(rf, "receive outdated snapshot with LastIncludedTerm {%d}, LastIncludedIndex {%d}", args.LastIncludedTerm, args.LastIncludedIndex)
		return
	}
	oldLogs := rf.logs
	if rf.lastLog().Index > args.LastIncludedIndex {
		rf.shrinkLogs(args.LastIncludedIndex - rf.lastIncluded())
	} else {
		rf.logs = []entry{{nil, args.LastIncludedTerm, args.LastIncludedIndex}}
	}
	if rf.commitIndex < args.LastIncludedIndex {
		DPrintf2(rf, "InstallSnapshot: Update commit index to %d", args.LastIncludedIndex)
		rf.commitIndex = args.LastIncludedIndex
	}
	if rf.lastApplied < args.LastIncludedIndex {
		rf.lastApplied = args.LastIncludedIndex
	}
	raftState := rf.encodeState()
	rf.persister.Save(raftState, args.Data)
	DPrintf2(rf, "Before: %v, After Install snapshot, logs: %v", oldLogs, rf.logs)
	go func() {
		rf.applyCh <- ApplyMsg{
			SnapshotValid: true,
			SnapshotIndex: args.LastIncludedIndex,
			SnapshotTerm:  args.LastIncludedTerm,
			Snapshot:      args.Data,
		}
	}()
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if index <= rf.lastIncluded() || index > rf.commitIndex {
		return
	}
	if index > rf.lastApplied {
		DPrintf2(rf, "Snapshot: Update lastApplied index to %d", index)
		rf.lastApplied = index
	}
	DPrintf2(rf, "snapshot..., bytes: %d", len(snapshot))
	begin := rf.lastIncluded() + 1
	rf.shrinkLogs(index - rf.lastIncluded() + 1)
	raftState := rf.encodeState()
	rf.persister.Save(raftState, snapshot)
	DPrintf2(rf, "Snapshot trim log[%d:%d]", begin, index)
}

// check if the snapshot is outdated
func (rf *Raft) outdatedSnapshot(lastIncludedTerm int, lastIncludedIndex int) bool {
	return rf.lastIncluded() > lastIncludedIndex || (rf.lastIncluded() == lastIncludedIndex && rf.logs[0].Term >= lastIncludedTerm)
}

// including index
func (rf *Raft) shrinkLogs(index int) {
	newLogs := make([]entry, 1+len(rf.logs[index:]))
	newLogs[0] = entry{
		Command: nil,
		Term:    rf.logs[index-1].Term,
		Index:   rf.logs[index-1].Index,
	}
	copy(newLogs[1:], rf.logs[index:])
	if rf.role == Leader {
		for i := range rf.logs[:index] {
			delete(rf.nMatch, rf.logs[i].Index)
		}
	}
	rf.logs = newLogs
	DPrintf2(rf, "shrink, length: %d, logs: %v", len(rf.logs), rf.logs)
}

func (rf *Raft) lastIncluded() int {
	return rf.logs[0].Index
}

func (rf *Raft) sendSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	DPrintf2(rf, "send snapshot to %s with LastIncludedIndex: %d, data bytes: %d", ServerName(server, Follower), args.LastIncludedIndex, len(args.Data))
	if !rf.peers[server].Call("Raft.InstallSnapshot", args, reply) {
		return
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// handle reply
	if reply.Term > rf.currentTerm {
		rf.setNewTerm(reply.Term)
		return
	}
	matchIndex := args.LastIncludedIndex
	rf.updateMatchIndex(server, matchIndex)
}
