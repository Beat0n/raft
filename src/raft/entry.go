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
	XTerm   int
	XIndex  int
	XLen    int
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
		if args.Term > rf.currentTerm {
			rf.setNewTerm(args.Term)
		} else {
			rf.role = Follower
		}
		if len(rf.logs) <= args.PrevLogIndex {
			reply.Success = false
			reply.XTerm = -1
			reply.XLen = len(rf.logs)
		} else if rf.logs[args.PrevLogIndex].Term != args.PrevLogTerm {
			reply.Success = false
			reply.XTerm = rf.logs[args.PrevLogIndex].Term
			reply.XIndex = rf.findFirstLogByTerm(rf.logs[args.PrevLogIndex].Term)
			rf.logs = rf.logs[:args.PrevLogIndex]
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
	rf.resetElectionTime()
	for server := range rf.peers {
		if server == rf.me {
			continue
		}
		done := false
		nextIndex := rf.nextIndex[server]
		var entries []entry
		if !isHeartBeat {
			lastLogIndex := rf.lastLog().Index
			entries = make([]entry, lastLogIndex-nextIndex+1)
			copy(entries, rf.logs[nextIndex:])
		}
		args := AppendEntriesArgs{
			Term:         rf.currentTerm,
			LeaderId:     rf.me,
			PrevLogIndex: rf.logs[nextIndex-1].Index,
			PrevLogTerm:  rf.logs[nextIndex-1].Term,
			Entries:      entries,
			LeaderCommit: rf.commitIndex,
		}
		if isHeartBeat {
			DPrintf2(rf, "send heartbeat to %s\n", ServerName(server, 3))
		} else if len(args.Entries) == 0 {
			DPrintf2(rf, "send empty logs to %s\n", ServerName(server, 3))
		} else {
			DPrintf2(rf, "send logs[%d:%d] to %s\n", nextIndex, len(rf.logs)-1, ServerName(server, 3))
		}
		reply := AppendEntriesReply{}
		go rf.handleAppendEntryReply(server, &args, &reply, &done)
	}
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
	if len(args.Entries) == 0 {
		// send heartbeat
		if !reply.Success {
			DPrintf2(rf, "send heartbeat to %s receive failure", ServerName(server, 3))
			rf.backup(server, args, reply)
		} else {
			DPrintf2(rf, "send heartbeat to %s receive success", ServerName(server, 3))
		}
	} else {
		// send logs
		if !reply.Success {
			DPrintf2(rf, "send logs to %s receive failure", ServerName(server, 3))
			rf.backup(server, args, reply)
		} else {
			DPrintf2(rf, "send logs to %s receive success", ServerName(server, 3))
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

// Apply log to state machine
func (rf *Raft) applier() {
	for {
		time.Sleep(ApplyFreq)
		rf.mu.Lock()
		for rf.lastApplied < rf.commitIndex {
			rf.lastApplied++
			// ignore no-op log
			//if rf.logs[rf.lastApplied].Command == nil {
			//	continue
			//}
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

// Automatically append a noop log when become Leader
// without lock
func (rf *Raft) appendNoOpLog() {
	term := rf.currentTerm
	index := len(rf.logs)
	DPrintf("---Term %d--- %s append noop log{index: %d}\n", rf.currentTerm, ServerName(rf.me, rf.role), index)
	rf.logs = append(rf.logs, entry{nil, term, index})
	rf.nMatch[index] = 1
}

// Leader fast backup when ApependEntries to server failed
func (rf *Raft) backup(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) {
	if args.PrevLogIndex == rf.nextIndex[server]-1 {
		//rf.nextIndex[server]--
		if reply.XTerm == -1 {
			rf.nextIndex[server] = reply.XLen
		} else {
			rf.nextIndex[server] = rf.findFirstLogByTerm(reply.XTerm)
		}
	}
}

func (rf *Raft) findFirstLogByTerm(term int) int {
	left := 0
	right := len(rf.logs) - 1
	target := -1
	for left <= right {
		mid := (left + right) / 2
		if rf.logs[mid].Term >= term {
			right = mid - 1
			if rf.logs[mid].Term == term {
				target = mid
			}
		} else {
			left = mid + 1
		}
	}
	return target
}
