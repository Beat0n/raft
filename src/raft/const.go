package raft

import "time"

const (
	ElectionTimeoutMin = 150
	ElectionTimeoutMax = 300
	HeartBeatTime      = 80 * time.Millisecond
	ApplyFreq          = 100 * time.Millisecond
	RPCRetry           = 4
)
