package kvraft

import "time"

const Debug = true

const (
	PUT    = "Put"
	GET    = "Get"
	APPEND = "Append"
)

const (
	OK               Err = "OK"
	ErrNoKey         Err = "ErrNoKey"
	ErrDupReq        Err = "ErrDupReq"
	ErrWrongLeader   Err = "ErrWrongLeader"
	ErrCommitFailed      = "ErrCommitFailed"
	ErrNoLeader      Err = "ErrNoLeader"
	ErrCommitTimeout Err = "ErrCommitTimeout"
)

const (
	CommitTickerTime       = time.Millisecond * 40
	CommitTimerTime        = time.Millisecond * 200
	RPCTries               = 4
	SleepTimeWhenNoLeaders = time.Millisecond * 5
	ApplyChanSize          = 100
)
