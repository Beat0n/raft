package raft

import "time"

const (
	ElectionTimeoutMin = 250
	ElectionTimeoutMax = 400
	HeartBeatTime      = 100 * time.Millisecond
	ApplyFreq          = 10 * time.Millisecond
	RPCRetry           = 2
	RPCSleepTime       = 10 * time.Millisecond
)
