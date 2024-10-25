package kvraft

import "time"

const (
	PUT    = "Put"
	GET    = "Get"
	APPEND = "Append"
)

const (
	OK               Err = "OK"
	ErrNoKey         Err = "ErrNoKey"
	ErrExpiredReq    Err = "ErrExpiredReq"
	ErrWrongLeader   Err = "ErrWrongLeader"
	ErrNoLeader      Err = "ErrNoLeader"
	ErrCommitTimeout Err = "ErrCommitTimeout"
)

const (
	CommitTickerTime       = time.Millisecond * 40
	CommitTimerTime        = time.Millisecond * 200
	RPCTries               = 4
	SleepTimeWhenNoLeaders = time.Millisecond * 5
)
