package kvraft

import (
	"log"
)

func DPrintf(format string, a ...interface{}) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type Op struct {

	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	OpType    string
	Key       string
	Value     string
	ClientId  int64
	RequestId int32
}

type OpResult struct {
	op    *Op
	Err   Err
	Value string
}

type Err string

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	RequestId int32
	ClientId  int64
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	RequestId int32
	ClientId  int64
}

type GetReply struct {
	Err   Err
	Value string
}

type CommandArgs struct {
	Key       string
	Value     string
	Op        string // "Put" or "Append" or "Get"
	RequestId int32
	ClientId  int64
}

type CommandReply struct {
	Err   Err
	Value string
}
