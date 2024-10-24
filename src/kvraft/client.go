package kvraft

import (
	"6.5840/labrpc"
	"sync"
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
	DPrintf("{Client} Get Key: %s", key)
	sum := makeSummer()
	for server := range ck.servers {
		args := GetArgs{key}
		reply := GetReply{}
		go ck.sendGet(server, &args, &reply, sum)
	}
	reply := <-sum.ch
	if reply.err == OK || reply.err == ErrNoKey {
		DPrintf("{Client} Get success! {Key: %s, Value: %s}", key, reply.value)
		return reply.value
	}
	// retry
	//time.Sleep(100 * time.Millisecond)
	return ck.Get(key)
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
	DPrintf("{Client} {%s Key: %s, Value: %s}", op, key, value)
	sum := makeSummer()

	n := len(ck.servers)
	for server := 0; server < n; server++ {
		args := PutAppendArgs{key, value, op}
		reply := PutAppendReply{}
		go ck.sendPutAppend(server, &args, &reply, sum)
	}
	reply := <-sum.ch
	if reply.err == ErrNoLeader {
		//time.Sleep(100 * time.Millisecond)
		ck.PutAppend(key, value, op)
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}

func (ck *Clerk) sendPutAppend(server int, args *PutAppendArgs, reply *PutAppendReply, sum *summer) {
	if !ck.servers[server].Call("KVServer.PutAppend", args, reply) {
		DPrintf("{Client} send RPC {%s Key: %s, Value: %s} failed", args.Op, args.Key, args.Value)
		return
	}
	if reply.Err == OK {
		DPrintf("{Client} {%s Key: %s, Value: %s} success!", args.Op, args.Key, args.Value)
		sum.ch <- &commonReply{
			value: "",
			err:   OK,
		}
	} else {
		sum.mu.Lock()
		sum.n++
		if sum.n == len(ck.servers) {
			DPrintf("{Client} {%s Key: %s, Value: %s} failed: No leaders now", args.Op, args.Key, args.Value)
			sum.ch <- &commonReply{
				value: "",
				err:   ErrNoLeader,
			}
		}
		sum.mu.Unlock()
	}
}

func (ck *Clerk) sendGet(server int, args *GetArgs, reply *GetReply, sum *summer) {
	if !ck.servers[server].Call("KVServer.Get", args, reply) {
		return
	}
	if reply.Err == OK || reply.Err == ErrNoKey {
		sum.ch <- &commonReply{
			value: reply.Value,
			err:   reply.Err,
		}
	} else {
		sum.mu.Lock()
		sum.n++
		if sum.n == len(ck.servers) {
			sum.ch <- &commonReply{
				value: "",
				err:   ErrNoLeader,
			}
		}
		sum.mu.Unlock()
	}
}
