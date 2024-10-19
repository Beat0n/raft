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

// Append logs from leader to self
func (rf *Raft) appendLogs(args *AppendEntriesArgs) {
	i := args.PrevLogIndex + 1
	j := 0
	for i < len(rf.logs) && j < len(args.Entries) {
		if rf.logs[i] != args.Entries[j] {
			rf.logs[i] = args.Entries[j]
			rf.logs = append(rf.logs[:i], args.Entries[j:]...)
			DPrintf2(rf, "length: %d, logs: %v", len(rf.logs), rf.logs)
			return
		}
		i++
		j++
	}
	if j < len(args.Entries) {
		rf.logs = append(rf.logs, args.Entries[j:]...)
	}
	DPrintf2(rf, "length: %d, logs: %v", len(rf.logs), rf.logs)
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
			rf.appendLogs(args)
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
	DPrintf2(rf, "send heartbeat to %s\n", ServerName(server, 3))
	done := false
	nextIndex := rf.nextIndex[server]
	args := AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: rf.logs[nextIndex-1].Index,
		PrevLogTerm:  rf.logs[nextIndex-1].Term,
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
	if !rf.sendAppendEntries(server, args, reply) {
		return
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
			prevIndex := args.PrevLogIndex
			if rf.nextIndex[server] > prevIndex {
				rf.nextIndex[server]--
				// todo: if retry immediately?
				//go func(done *bool) {
				//	rf.handleAppendEntryReply(server, args, reply, done)
				//}(done)
			}
		} else {
			empty := len(args.Entries) == 0
			var str string = ""
			if empty {
				str = "empty "
			}
			DPrintf2(rf, "send %slogs to %s receive success", str, ServerName(server, 3))
			if !empty {
				matchIndex := args.PrevLogIndex + len(args.Entries)
				nextIndex := matchIndex + 1
				if matchIndex > rf.matchIndex[server] {
					rf.matchIndex[server] = matchIndex
					rf.nextIndex[server] = nextIndex
					rf.nMatch[matchIndex]++
					//todo: 不能提交之前任期内的日志
					N := rf.nMatch[matchIndex]
					DPrintf2(rf, "Index: %d, N: %d", matchIndex, N)
					if N > len(rf.peers)/2 && matchIndex > rf.commitIndex && rf.logs[matchIndex].Term == rf.currentTerm {
						rf.commitIndex = matchIndex
					}
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
			DPrintf2(rf, "apply log[%d], command is %v\n", rf.lastApplied, rf.logs[rf.lastApplied].Command)
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
