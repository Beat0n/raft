package shardctrler

import (
	"log"
	"time"
)

//
// Shard controler: assigns shards to replication groups.
//
// RPC interface:
// Join(servers) -- add a set of groups (gid -> server-list mapping).
// Leave(gids) -- delete a set of groups.
// Move(shard, gid) -- hand off one shard from current owner to gid.
// Query(num) -> fetch Config # num, or latest config if num==-1.
//
// A Config (configuration) describes a set of replica groups, and the
// replica group responsible for each shard. Configs are numbered. Config
// #0 is the initial configuration, with no groups and all shards
// assigned to group 0 (the invalid group).
//
// You will need to add fields to the RPC argument structs.
//

// The number of shards.
const NShards = 10

// A configuration -- an assignment of shards to groups.
// Please don't change this.
type Config struct {
	Num    int              // config number
	Shards [NShards]int     // shard -> gid
	Groups map[int][]string // gid -> servers[]
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	OpType    string
	ClientId  int64
	RequestId int32
	// Join
	Servers map[int][]string // new GID -> servers mappings
	// Leave
	GIDs []int
	// Move
	Shard int
	GID   int
	// Query
	Num int // desired config number
}

func compareOP(op1, op2 *Op) bool {
	if op1.OpType != op2.OpType || op1.ClientId != op2.ClientId || op1.RequestId != op2.RequestId {
		return false
	}
	switch op1.OpType {
	case JoinOP:
		return &op1.Servers == &op1.Servers
	case LeaveOP:
		return &op1.GIDs == &op1.GIDs
	case MoveOp:
		return op1.Shard == op2.Shard && op1.GID == op2.GID
	case QueryOp:
		return op1.Num == op2.Num
	}
	return false
}

type OpResult struct {
	op     *Op
	Err    Err
	Config Config
}

const (
	JoinOP  = "Join"
	LeaveOP = "Leave"
	MoveOp  = "Move"
	QueryOp = "Query"
)

const (
	OK               Err = "OK"
	ErrWrongLeader   Err = "WrongLeader"
	ErrInvalidArgs   Err = "InvalidArgs"
	ErrCommitTimeout Err = "ErrCommitTimeout"
	ErrCommitFailed  Err = "ErrCommitFailed"
	ErrDupReq        Err = "ErrDupReq"
	CommitTimerTime      = time.Millisecond * 200
)

type Err string

type JoinArgs struct {
	ClientId  int64
	RequestId int32
	Servers   map[int][]string // new GID -> servers mappings
}

type JoinReply struct {
	Err Err
}

type LeaveArgs struct {
	ClientId  int64
	RequestId int32
	GIDs      []int
}

type LeaveReply struct {
	Err Err
}

type MoveArgs struct {
	ClientId  int64
	RequestId int32
	Shard     int
	GID       int
}

type MoveReply struct {
	Err Err
}

type QueryArgs struct {
	ClientId  int64
	RequestId int32
	Num       int // desired config number
}

type QueryReply struct {
	Err    Err
	Config Config
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

// Debugging
const Debug = true

func DPrintf(format string, a ...interface{}) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}
