package kvraft

import (
	"6.5840/labrpc"
	"sync"
	"sync/atomic"
	"time"
)
import "crypto/rand"
import "math/big"

type commonReply struct {
	value string
	err   Err
}

type summer struct {
	mu sync.Mutex
	n  int
	ch chan *commonReply
}

func makeSummer() *summer {
	return &summer{n: 0, ch: make(chan *commonReply)}
}

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	leaderId  int32
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

	// You will have to modify this function.
	n := len(ck.servers)
	requestId := atomic.AddInt32(&ck.requestId, 1)
	for {
		leaderId := int(atomic.LoadInt32(&ck.leaderId))
		for i := 0; i < n; i++ {
			args := &GetArgs{key, requestId, ck.selfId}
			reply := &GetReply{}
			server := (i + leaderId) % n
			DPrintf("{Client %d} send request[%d] to {Server %d}: {Get Key: %s}", ck.selfId, requestId, server, key)
			if !ck.servers[server].Call("KVServer.Get", args, reply) {
				DPrintf("{Client %d} send request[%d] RPC to {Server %d} {Get Key: %s} failed", ck.selfId, requestId, server, args.Key)
				continue
			}
			DPrintf("{Client %d} receive reply for request[%d] from {Server %d} {reply: %+v}", ck.selfId, requestId, server, *reply)
			if reply.Err == OK || reply.Err == ErrNoKey {
				atomic.StoreInt32(&ck.leaderId, int32(server))
				return reply.Value
			} else if reply.Err == ErrStartTimeout {
				atomic.StoreInt32(&ck.leaderId, int32((server+1)%n))
				break
			}
		}
		time.Sleep(SleepTimeWhenNoLeaders)
	}
}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.

	n := len(ck.servers)
	requestId := atomic.AddInt32(&ck.requestId, 1)
	DPrintf("{Client %d} send request[%d]: {%s Key: %s, Value: %s}", ck.selfId, requestId, op, key, value)
	for {
		leaderId := int(atomic.LoadInt32(&ck.leaderId))
		for i := 0; i < n; i++ {
			args := &PutAppendArgs{key, value, op, requestId, ck.selfId}
			reply := &PutAppendReply{}
			server := (i + leaderId) % n
			if !ck.servers[server].Call("KVServer.PutAppend", args, reply) {
				DPrintf("{Client %d} send request[%d] to {Server %d} {%s Key: %s, Value: %s} failed", ck.selfId, requestId, server, args.Op, args.Key, args.Value)
				continue
			}
			if reply.Err == OK {
				DPrintf("{Client %d} received reply for request[%d] from server[%d], {%s Key: %s, Value: %s}", ck.selfId, requestId, server, op, key, value)
				atomic.StoreInt32(&ck.leaderId, int32(server))
				return
			} else if reply.Err == ErrStartTimeout {
				atomic.StoreInt32(&ck.leaderId, int32((server+1)%n))
				break
			}
		}
		time.Sleep(SleepTimeWhenNoLeaders)
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}

//func (ck *Clerk) sendPutAppend(server int, args *PutAppendArgs, reply *PutAppendReply) {
//	if !ck.servers[server].Call("KVServer.PutAppend", args, reply) {
//		DPrintf("{Client} send RPC {%s Key: %s, Value: %s} failed", args.Op, args.Key, args.Value)
//		return
//	}
//	if reply.Err == OK {
//		DPrintf("{Client} {%s Key: %s, Value: %s} success!", args.Op, args.Key, args.Value)
//		sum.ch <- &commonReply{
//			value: "",
//			err:   OK,
//		}
//	} else {
//		sum.mu.Lock()
//		sum.n++
//		if sum.n == len(ck.servers) {
//			DPrintf("{Client} {%s Key: %s, Value: %s} failed: No leaders now", args.Op, args.Key, args.Value)
//			sum.ch <- &commonReply{
//				value: "",
//				err:   ErrNoLeader,
//			}
//		}
//		sum.mu.Unlock()
//	}
//}
//
//func (ck *Clerk) sendGet(server int, args *GetArgs, reply *GetReply) {
//	if !ck.servers[server].Call("KVServer.Get", args, reply) {
//		return
//	}
//	if reply.Err == OK || reply.Err == ErrNoKey {
//		sum.ch <- &commonReply{
//			value: reply.Value,
//			err:   reply.Err,
//		}
//	} else {
//		sum.mu.Lock()
//		sum.n++
//		if sum.n == len(ck.servers) {
//			sum.ch <- &commonReply{
//				value: "",
//				err:   ErrNoLeader,
//			}
//		}
//		sum.mu.Unlock()
//	}
//}
