package kvraft

const (
	PUT    = "Put"
	GET    = "Get"
	APPEND = "Append"
)

const (
	OK             Err = "OK"
	ErrNoKey       Err = "ErrNoKey"
	ErrWrongLeader Err = "ErrWrongLeader"
	ErrNoLeader    Err = "ErrNoLeader"
)
