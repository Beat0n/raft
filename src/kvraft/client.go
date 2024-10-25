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

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//func (ck *Clerk) PutAppend(key string, value string, op string) {
//	// You will have to modify this function.
//
//	n := len(ck.servers)
//	requestId := atomic.AddInt32(&ck.requestId, 1)
//	DPrintf("{Client %d} send request[%d]: {%s Key: %s, Value: %s}", ck.selfId, requestId, op, key, value)
//	for {
//		leaderId := int(atomic.LoadInt32(&ck.leaderId))
//		for i := 0; i < n; i++ {
//			args := &PutAppendArgs{key, value, op, requestId, ck.selfId}
//			reply := &PutAppendReply{}
//			server := (i + leaderId) % n
//			if !ck.servers[server].Call("KVServer.PutAppend", args, reply) {
//				DPrintf("{Client %d} send request[%d] to {Server %d} {%s Key: %s, Value: %s} failed", ck.selfId, requestId, server, args.Op, args.Key, args.Value)
//				continue
//			}
//			if reply.Err == OK {
//				DPrintf("{Client %d} received reply for request[%d] from server[%d], {%s Key: %s, Value: %s}", ck.selfId, requestId, server, op, key, value)
//				atomic.StoreInt32(&ck.leaderId, int32(server))
//				return
//			} else if reply.Err == ErrStartTimeout {
//				atomic.StoreInt32(&ck.leaderId, int32((server+1)%n))
//				break
//			}
//		}
//		time.Sleep(SleepTimeWhenNoLeaders)
//	}
//}

func (ck *Clerk) Put(key string, value string) {
	ck.Command(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.Command(key, value, "Append")
}

//	func (ck *Clerk) sendPutAppend(server int, args *PutAppendArgs, reply *PutAppendReply) {
//		if !ck.servers[server].Call("KVServer.PutAppend", args, reply) {
//			DPrintf("{Client} send RPC {%s Key: %s, Value: %s} failed", args.Op, args.Key, args.Value)
//			return
//		}
//		if reply.Err == OK {
//			DPrintf("{Client} {%s Key: %s, Value: %s} success!", args.Op, args.Key, args.Value)
//			sum.ch <- &commonReply{
//				value: "",
//				err:   OK,
//			}
//		} else {
//			sum.mu.Lock()
//			sum.n++
//			if sum.n == len(ck.servers) {
//				DPrintf("{Client} {%s Key: %s, Value: %s} failed: No leaders now", args.Op, args.Key, args.Value)
//				sum.ch <- &commonReply{
//					value: "",
//					err:   ErrNoLeader,
//				}
//			}
//			sum.mu.Unlock()
//		}
//	}
//
//	func (ck *Clerk) sendGet(server int, args *GetArgs, reply *GetReply) {
//		if !ck.servers[server].Call("KVServer.Get", args, reply) {
//			return
//		}
//		if reply.Err == OK || reply.Err == ErrNoKey {
//			sum.ch <- &commonReply{
//				value: reply.Value,
//				err:   reply.Err,
//			}
//		} else {
//			sum.mu.Lock()
//			sum.n++
//			if sum.n == len(ck.servers) {
//				sum.ch <- &commonReply{
//					value: "",
//					err:   ErrNoLeader,
//				}
//			}
//			sum.mu.Unlock()
//		}
//	}
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
		DPrintf("{Client %d} send %s RPC[%d] to {Server %d} | key : %s, value: %s |", ck.selfId, op, args.RequestId, ck.leaderId, key, value)
		ok = ck.servers[ck.leaderId].Call("KVServer.Command", &args, &reply)

		if ok {
			DPrintf("{Client %d} send %s RPC[%d] to {Server %d} success! | reply: %v", ck.selfId, op, args.RequestId, ck.leaderId, reply.Err)
			switch reply.Err {
			case OK:
				ret = reply.Value
				break
			case ErrNoKey:
				ret = ""
				break
			case ErrWrongLeader:
				ok = false
				ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
			case ErrCommitTimeout:
				ok = false
				ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
			case ErrExpiredReq:
				break
			}
		} else {
			ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
		}
	}
	ck.requestId++
	return ret
}
