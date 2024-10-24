package kvraft

import (
	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

const Debug = true

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type Op struct {

	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	OpType    string
	Key       string
	Value     string
	ClientId  int64
	RequestId int32
}

type client struct {
	clientId     int64
	requestId    int32
	prevGetReply *GetReply
	getCh        chan *GetReply
	putAppendCh  chan struct{}
}

func makeClient(clientId int64) *client {
	c := new(client)
	c.clientId = clientId
	c.requestId = 1
	c.getCh = nil
	c.putAppendCh = nil
	return c
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	database map[string]string
	clients  map[int64]*client
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	DPrintf("{server %d} receive request[%d] from {Client %d} {Get, Key: %s}", kv.me, args.RequestId, args.ClientId, args.Key)
	defer func() {
		DPrintf("{server %d} send {reply: %v} to {Client %d, request[%d]}", kv.me, reply.Err, args.ClientId, args.RequestId)
	}()
	kv.mu.Lock()
	if kv.clients[args.ClientId] == nil {
		kv.clients[args.ClientId] = makeClient(args.ClientId)
	}
	if kv.clients[args.ClientId].requestId == args.RequestId+1 {
		if kv.clients[args.ClientId].prevGetReply != nil {
			reply.Err = kv.clients[args.ClientId].prevGetReply.Err
			reply.Value = kv.clients[args.ClientId].prevGetReply.Value
		} else {
			reply.Err = ErrWrongLeader
		}
		kv.mu.Unlock()
		return
	}
	if kv.clients[args.ClientId].requestId > args.RequestId+1 {
		kv.mu.Unlock()
		DPrintf("{server %d} received expired request[%d] from {Client %d}", kv.me, args.RequestId, args.ClientId)
		reply.Err = ErrExpiredReq
		return
	}
	kv.mu.Unlock()
	op := Op{
		OpType:    GET,
		Key:       args.Key,
		Value:     "",
		ClientId:  args.ClientId,
		RequestId: args.RequestId,
	}
	_, term1, ok := kv.rf.Start(op)
	if ok {
		ticker := time.NewTicker(StartTickerTime)
		timer := time.NewTimer(StartTimerTime)
		// blocked until all write request before applied
		kv.mu.Lock()
		kv.clients[args.ClientId].getCh = make(chan *GetReply, 1)
		ch := kv.clients[args.ClientId].getCh
		kv.mu.Unlock()
		for {
			select {
			case <-timer.C:
				DPrintf("{server %d} start request[%d] from {Client %d} {Get, Key: %s} timeout,", kv.me, args.RequestId, args.ClientId, args.Key)
				reply.Err = ErrStartTimeout
				return
			case <-ticker.C:
				DPrintf("{Server %d} is doing start, tick...", kv.me)
				if term2, isLeader := kv.rf.GetState(); !isLeader || term1 != term2 {
					DPrintf("{server %d} start request[%d] from {Client %d} {Get, Key: %s} failed: leader changed", kv.me, args.RequestId, args.ClientId, args.Key)
					reply.Err = ErrStartTimeout
					return
				}
			case tmp := <-ch:
				reply.Err = tmp.Err
				reply.Value = tmp.Value
			}
		}
	} else {
		reply.Err = ErrWrongLeader
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	DPrintf("{server %d} receive request[%d] from {Client %d} {%s, Key: %s, Value: %s}", kv.me, args.RequestId, args.ClientId, args.Op, args.Key, args.Value)
	defer func() {
		DPrintf("{server %d} send {reply: %v} to {Client %d, request[%d]}", kv.me, reply.Err, args.ClientId, args.RequestId)
	}()
	kv.mu.Lock()
	if _, connected := kv.clients[args.ClientId]; !connected {
		kv.clients[args.ClientId] = makeClient(args.ClientId)
	}
	if kv.clients[args.ClientId].requestId == args.RequestId+1 {
		kv.mu.Unlock()
		reply.Err = OK
		return
	}
	if kv.clients[args.ClientId].requestId > args.RequestId+1 {
		kv.mu.Unlock()
		DPrintf("{server %d} received expired request[%d] from {Client %d}", kv.me, args.RequestId, args.ClientId)
		reply.Err = ErrExpiredReq
		return
	}
	kv.mu.Unlock()
	op := Op{
		OpType:    args.Op,
		Key:       args.Key,
		Value:     args.Value,
		ClientId:  args.ClientId,
		RequestId: args.RequestId,
	}
	_, term1, ok := kv.rf.Start(op)
	ticker := time.NewTicker(StartTickerTime)
	timer := time.NewTimer(StartTimerTime)
	if ok {
		// blocked until raft reach agreement
		kv.mu.Lock()
		kv.clients[args.ClientId].putAppendCh = make(chan struct{}, 1)
		ch := kv.clients[args.ClientId].putAppendCh
		kv.mu.Unlock()
		for {
			select {
			case <-timer.C:
				DPrintf("{server %d} start request[%d] from {Client %d} {%s, Key: %s, Value: %s} timeout,", kv.me, args.RequestId, args.ClientId, args.Op, args.Key, args.Value)
				reply.Err = ErrStartTimeout
				return
			case <-ticker.C:
				DPrintf("{Server %d} is doing start, tick...", kv.me)
				if term2, isLeader := kv.rf.GetState(); !isLeader || term1 != term2 {
					DPrintf("{server %d} start request[%d] from {Client %d} {%s, Key: %s, Value: %s} failed: leader changed", kv.me, args.RequestId, args.ClientId, args.Op, args.Key, args.Value)
					reply.Err = ErrWrongLeader
					return
				}
			case <-ch:
				DPrintf("{server %d} {%s, Key: %s, Value: %s} success!", kv.me, args.Op, args.Key, args.Value)
				reply.Err = OK
				return
			}
		}
	} else {
		reply.Err = ErrWrongLeader
		DPrintf("{server %d} %s Key: %s, reply.Err: %v", kv.me, args.Op, args.Key, reply.Err)
		return
	}
}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.database = make(map[string]string)
	kv.clients = make(map[int64]*client)

	// You may need initialization code here.
	go kv.applier()
	return kv
}

func (kv *KVServer) applier() {
	for !kv.killed() {
		msg := <-kv.applyCh
		if msg.CommandValid {
			op := msg.Command.(Op)
			index := msg.CommandIndex
			clientId := op.ClientId
			requestId := op.RequestId
			DPrintf("{server %d} applied log[%d]: %+v", kv.me, index, op)
			kv.mu.Lock()
			if kv.clients[clientId] == nil {
				kv.clients[clientId] = makeClient(clientId)
			}
			if kv.clients[clientId].requestId > requestId {
				kv.mu.Unlock()
				continue
			}
			kv.clients[clientId].requestId = requestId + 1
			switch op.OpType {
			case GET:
				if kv.clients[clientId].getCh != nil {
					reply := GetReply{}
					if value, exist := kv.database[op.Key]; exist {
						reply.Err = OK
						reply.Value = value
					} else {
						reply.Err = ErrNoKey
						reply.Value = ""
					}
					kv.clients[clientId].prevGetReply = &reply
					kv.clients[clientId].getCh <- &reply
					kv.clients[clientId].getCh = nil
				}
			case PUT:
				kv.database[op.Key] = op.Value
			case APPEND:
				kv.database[op.Key] += op.Value
			}
			DPrintf("{Key :%s, Value: %s}", op.Key, kv.database[op.Key])
			if (op.OpType == PUT || op.OpType == APPEND) && kv.clients[clientId].putAppendCh != nil {
				kv.clients[clientId].putAppendCh <- struct{}{}
				kv.clients[clientId].putAppendCh = nil
			}
			kv.mu.Unlock()
		}
	}
}
