package kvraft

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
