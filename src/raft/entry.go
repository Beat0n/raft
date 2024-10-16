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
		if len(args.Entries) == 0 { // Receive heartbeat
			reply.Success = true
		} else {
			// Receive log entries
			if len(rf.logs) <= args.PrevLogIndex || rf.logs[args.PrevLogIndex].Term != args.PrevLogTerm {
				reply.Success = false
			} else {
				reply.Success = true
				logs := rf.logs[:args.PrevLogIndex+1]
				rf.logs = append(logs, args.Entries...)
			}
		}
		if args.LeaderCommit > rf.commitIndex {
			rf.commitIndex = min(args.LeaderCommit, rf.lastLog().Index)
			DPrintf("---Term %d---%s update commit index to %d\n", rf.currentTerm, ServerName(rf.me, rf.role), rf.commitIndex)
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
	if isHeartBeat {
		rf.sendHeartBeat()
	} else {
		rf.sendLogs()
	}
}

func (rf *Raft) sendHeartBeat() {
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		DPrintf("---Term %d---%s send heartbeat to %s\n", rf.currentTerm, ServerName(rf.me, rf.role), ServerName(i, 3))
		args := AppendEntriesArgs{
			Term:         rf.currentTerm,
			LeaderId:     rf.me,
			Entries:      nil,
			LeaderCommit: rf.commitIndex,
		}
		reply := AppendEntriesReply{}
		go rf.sendAppendEntries(i, &args, &reply)
	}
}

func (rf *Raft) sendLogs() {
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		nextIndex := rf.nextIndex[i]
		DPrintf("---Term %d---%s send logs[%d:%d] to %s\n", rf.currentTerm, ServerName(rf.me, rf.role), nextIndex, len(rf.logs)-1, ServerName(i, 3))
		args := AppendEntriesArgs{
			Term:         rf.currentTerm,
			LeaderId:     rf.me,
			PrevLogIndex: rf.logs[nextIndex-1].Index,
			PrevLogTerm:  rf.logs[nextIndex-1].Term,
			Entries:      rf.logs[nextIndex:],
			LeaderCommit: rf.commitIndex,
		}
		reply := AppendEntriesReply{}
		go rf.handleAppendEntryReply(i, &args, &reply)
	}
}

func (rf *Raft) lastLog() *entry {
	return &rf.logs[len(rf.logs)-1]
}

func (rf *Raft) handleAppendEntryReply(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) {
	for ok := false; !ok; {
		ok = rf.sendAppendEntries(server, args, reply)
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if reply.Term > rf.currentTerm {
		rf.setNewTerm(args.Term)
		return
	}
	if !reply.Success {
		rf.nextIndex[server]--
		// todo: check if retry immediately
	} else {
		DPrintf("---Term %d---%s send logs success\n", rf.currentTerm, ServerName(rf.me, rf.role))
		lastIndex := rf.lastLog().Index
		rf.nextIndex[server] = lastIndex + 1
		rf.matchIndex[server] = lastIndex
		rf.nMatch[lastIndex]++
		N := rf.nMatch[lastIndex]
		DPrintf("N: %d", N)
		if N > len(rf.peers)/2 && lastIndex > rf.commitIndex && rf.logs[lastIndex].Term == rf.currentTerm {
			rf.commitIndex = lastIndex
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
			DPrintf("---Term %d---%s apply log[%d], command is %v\n", rf.currentTerm, ServerName(rf.me, rf.role), rf.lastApplied, rf.logs[rf.lastApplied].Command)
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
