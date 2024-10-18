package raft

import "time"

const ApplyFreq = 100 * time.Millisecond

type entry struct {
	Command interface{}
	Term    int
	Index   int
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int // index of log entry immediately preceding new ones
	PrevLogTerm  int // term of prevLogIndex entry
	Entries      []entry
	LeaderCommit int // leader's commitIndex
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

// AppendEntries followers receive entries from leader
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term >= rf.currentTerm {
		rf.resetElectionTime()
		rf.setNewTerm(args.Term)
		if len(rf.logs) <= args.PrevLogIndex || rf.logs[args.PrevLogIndex].Term != args.PrevLogTerm {
			reply.Success = false
		} else {
			reply.Success = true
			if args.Entries != nil {
				// append log
				logs := rf.logs[:args.PrevLogIndex+1]
				rf.logs = append(logs, args.Entries...)
			}
			if args.LeaderCommit > rf.commitIndex {
				rf.commitIndex = min(args.LeaderCommit, rf.lastLog().Index)
				DPrintf2(rf, "update commit index to %d\n", rf.commitIndex)
			}
		}
	} else { // if args.Term < rf.currentTerm
		// from old leader
		reply.Success = false
	}
	reply.Term = rf.currentTerm
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) sendEntries(isHeartBeat bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.resetElectionTime()
	for server := range rf.peers {
		if server == rf.me {
			continue
		}
		if isHeartBeat {
			rf.sendHeartBeat(server)
		} else {
			rf.sendLogs(server)
		}
	}
}

func (rf *Raft) sendHeartBeat(server int) {
	done := false
	DPrintf("---Term %d--- %s send heartbeat to %s\n", rf.currentTerm, ServerName(rf.me, rf.role), ServerName(server, 3))
	args := AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		Entries:      nil,
		LeaderCommit: rf.commitIndex,
	}
	reply := AppendEntriesReply{}
	go rf.handleAppendEntryReply(server, &args, &reply, &done)
}

func (rf *Raft) sendLogs(server int) {
	done := false
	nextIndex := rf.nextIndex[server]
	args := AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: rf.logs[nextIndex-1].Index,
		PrevLogTerm:  rf.logs[nextIndex-1].Term,
		Entries:      rf.logs[nextIndex:],
		LeaderCommit: rf.commitIndex,
	}
	if len(args.Entries) == 0 {
		DPrintf2(rf, "send empty logs to %s\n", ServerName(server, 3))
	} else {
		DPrintf2(rf, "send logs[%d:%d] to %s\n", nextIndex, len(rf.logs)-1, ServerName(server, 3))
	}
	reply := AppendEntriesReply{}
	go rf.handleAppendEntryReply(server, &args, &reply, &done)
}

func (rf *Raft) lastLog() *entry {
	return &rf.logs[len(rf.logs)-1]
}

func (rf *Raft) handleAppendEntryReply(server int, args *AppendEntriesArgs, reply *AppendEntriesReply, done *bool) {
	for !rf.sendAppendEntries(server, args, reply) {
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if *done {
		return
	}
	if reply.Term > rf.currentTerm {
		rf.setNewTerm(args.Term)
		*done = true
		return
	}
	if args.Term != rf.currentTerm {
		return
	}
	if args.Entries == nil {
		// send heartbeat
		if !reply.Success {
			DPrintf2(rf, "send heartbeat to %s receive failure", ServerName(server, 3))
		} else {
			DPrintf2(rf, "send heartbeat to %s receive success", ServerName(server, 3))
		}
	} else {
		// send logs
		if !reply.Success {
			DPrintf2(rf, "send logs to %s receive failure", ServerName(server, 3))
			rf.nextIndex[server]--
			// todo: if retry immediately?
			//go func(done *bool) {
			//	rf.handleAppendEntryReply(server, args, reply, done)
			//}(done)
		} else {
			DPrintf2(rf, "send logs to %s receive success", ServerName(server, 3))
			if len(args.Entries) != 0 {
				matchIndex := args.PrevLogIndex + len(args.Entries)
				nextIndex := matchIndex + 1
				rf.nextIndex[server] = max(rf.nextIndex[server], nextIndex)
				rf.matchIndex[server] = max(rf.matchIndex[server], matchIndex)
				rf.nMatch[matchIndex]++
				N := rf.nMatch[matchIndex]
				DPrintf2(rf, "Index: %d, N: %d", matchIndex, N)
				if N > len(rf.peers)/2 && matchIndex > rf.commitIndex && rf.logs[matchIndex].Term == rf.currentTerm {
					rf.commitIndex = matchIndex
				}
			}
		}
	}
}

// Apply log to state machine
func (rf *Raft) applier() {
	for {
		time.Sleep(ApplyFreq)
		rf.mu.Lock()
		for rf.lastApplied < rf.commitIndex {
			rf.lastApplied++
			DPrintf("---Term %d--- %s apply log[%d], command is %v\n", rf.currentTerm, ServerName(rf.me, rf.role), rf.lastApplied, rf.logs[rf.lastApplied].Command)
			rf.applyCh <- ApplyMsg{
				CommandValid:  true,
				Command:       rf.logs[rf.lastApplied].Command,
				CommandIndex:  rf.logs[rf.lastApplied].Index,
				SnapshotValid: false,
				Snapshot:      nil,
				SnapshotTerm:  0,
				SnapshotIndex: 0,
			}
		}
		rf.mu.Unlock()
	}
}
