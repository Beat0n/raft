package kvraft

import "time"

const (
	PUT    = "Put"
	GET    = "Get"
	APPEND = "Append"
)

const (
	OK              Err = "OK"
	ErrNoKey        Err = "ErrNoKey"
	ErrExpiredReq   Err = "ErrExpiredReq"
	ErrWrongLeader  Err = "ErrWrongLeader"
	ErrNoLeader     Err = "ErrNoLeader"
	ErrStartTimeout Err = "ErrTimeout"
)

const (
	StartTickerTime        = time.Millisecond * 1
	StartTimerTime         = time.Millisecond * 4
	RPCTries               = 4
	SleepTimeWhenNoLeaders = time.Millisecond * 5
)
