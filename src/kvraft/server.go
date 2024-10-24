package kvraft

import (
	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
	"log"
	"sync"
	"sync/atomic"
)

const Debug = false

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
	OpType string
	Key    string
	Value  string
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
	channels map[int]chan struct{}
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	op := Op{
		OpType: GET,
		Key:    args.Key,
		Value:  "",
	}
	_, _, ok := kv.rf.Start(op)
	if ok {
		kv.mu.Lock()
		value, exist := kv.database[args.Key]
		if exist {
			reply.Err = OK
			reply.Value = value
		} else {
			reply.Err = ErrNoKey
			reply.Value = ""
		}
		kv.mu.Unlock()
	} else {
		reply.Err = ErrWrongLeader
	}
	DPrintf("{server %d} Get Key: %s, reply: %v", kv.me, args.Key, *reply)
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	op := Op{
		OpType: args.Op,
		Key:    args.Key,
		Value:  args.Value,
	}
	var index int
	var ok = false
	index, _, ok = kv.rf.Start(op)
	if ok {
		kv.mu.Lock()
		kv.channels[index] = make(chan struct{})
		kv.mu.Unlock()
		reply.Err = OK
		DPrintf("{server %d} %s Key: %s success!", kv.me, args.Op, args.Key)
		// blocked until raft reach agreement
		<-kv.channels[index]
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
	kv.channels = make(map[int]chan struct{})

	// You may need initialization code here.
	go kv.apply()
	return kv
}

func (kv *KVServer) apply() {
	for {
		msg := <-kv.applyCh
		if msg.CommandValid {
			op := msg.Command.(Op)
			index := msg.CommandIndex
			DPrintf("{server %d} applied log[%d]: {%s Key: %s, Value: %s}", kv.me, index, op.OpType, op.Key, op.Value)
			kv.mu.Lock()
			if op.OpType == PUT {
				kv.database[op.Key] = op.Value
			} else if op.OpType == APPEND {
				kv.database[op.Key] += op.Value
			} else if op.OpType == GET {

			}
			DPrintf("{Key :%s, Value: %s}", op.Key, kv.database[op.Key])
			if _, exist := kv.channels[index]; exist {
				kv.channels[index] <- struct{}{}
				delete(kv.channels, index)
			}
			kv.mu.Unlock()
		}
	}
}
