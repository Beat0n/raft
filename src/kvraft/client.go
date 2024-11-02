package kvraft

import (
	"6.5840/labrpc"
)
import "crypto/rand"
import "math/big"

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	leaderId  int
	requestId int32
	selfId    int64
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.leaderId = 0
	ck.requestId = 0
	ck.selfId = nrand()
	return ck
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) Get(key string) string {
	return ck.Command(key, "", GET)
}

func (ck *Clerk) Put(key string, value string) {
	ck.Command(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.Command(key, value, "Append")
}

func (ck *Clerk) Command(key, value, op string) string {
	args := CommandArgs{
		Key:       key,
		Value:     value,
		Op:        op,
		ClientId:  ck.selfId,
		RequestId: ck.requestId,
	}
	ok := false
	ret := ""
	for !ok {
		reply := &CommandReply{}
		DPrintf("{Client %d} send RPC[%d] to {Server %d} | %s key : %s, value: %s |", ck.selfId, args.RequestId, ck.leaderId, op, key, value)
		ok = ck.servers[ck.leaderId].Call("KVServer.Command", &args, &reply)

		if ok {
			DPrintf("{Client %d} send RPC[%d] to {Server %d} success! | reply: %v, value: %v", ck.selfId, args.RequestId, ck.leaderId, reply.Err, reply.Value)
			switch reply.Err {
			case OK:
				ret = reply.Value
				break
			case ErrNoKey:
				ret = ""
				break
			case ErrCommitFailed:
				ok = false
			case ErrWrongLeader:
				ok = false
				ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
			case ErrCommitTimeout:
				ok = false
				ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
			case ErrDupReq:
				break
			}
		} else {
			ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
		}
	}
	ck.requestId++
	return ret
}
