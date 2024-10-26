package raft

import "time"

const (
	ElectionTimeoutMin = 200
	ElectionTimeoutMax = 400
	HeartBeatTime      = 100 * time.Millisecond
	ApplyFreq          = 10 * time.Millisecond
	RPCRetry           = 4
)
