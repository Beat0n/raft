package kvraft

import (
	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
	"bytes"
	"sync"
	"sync/atomic"
	"time"
)

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	database         map[string]string
	clients          map[int64]int32
	blockedOps       map[int]chan *OpResult
	lastAppliedIndex int
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
	kv.clients = make(map[int64]int32)
	kv.blockedOps = make(map[int]chan *OpResult)
	kv.lastAppliedIndex = 0
	kv.readSnapshot(kv.rf.GetPersister().ReadSnapshot())

	// You may need initialization code here.
	go kv.applier()
	return kv
}

func (kv *KVServer) applier() {
	for !kv.killed() {
		msg := <-kv.applyCh
		kv.lastAppliedIndex++
		if msg.CommandValid {
			op := msg.Command.(Op)
			index := msg.CommandIndex
			kv.processOp(&op, index)
			if kv.maxraftstate != -1 && kv.rf.GetPersister().RaftStateSize() > kv.maxraftstate {
				kv.mu.Lock()
				snapshot := kv.makeSnapshot()
				kv.mu.Unlock()
				DPrintf("{Server %d} Create a snapshot at Index: %d", kv.me, index)
				kv.rf.Snapshot(index, snapshot)
			}
		} else if msg.SnapshotValid {
			DPrintf("{Server %d} Read a snapshot at Index: %d", kv.me, msg.SnapshotIndex)
			kv.mu.Lock()
			kv.readSnapshot(msg.Snapshot)
			kv.mu.Unlock()
		}
	}
}

func (kv *KVServer) Command(args *CommandArgs, reply *CommandReply) {
	op := &Op{
		ClientId:  args.ClientId,
		RequestId: args.RequestId,
		OpType:    args.Op,
		Key:       args.Key,
		Value:     args.Value,
	}

	prepareErr, index := kv.prepare(op)
	if prepareErr != OK {
		reply.Err = prepareErr
		return
	}

	reply.Err, reply.Value = kv.waitForCommit(op, index)
}

func (kv *KVServer) processOp(op *Op, index int) {
	opResult := OpResult{
		op:    op,
		Value: op.Value,
		Err:   OK,
	}
	kv.mu.Lock()
	defer func() {
		kv.mu.Unlock()
		kv.notify(&opResult, index)
	}()
	if op.OpType == GET {
		exist := false
		opResult.Value, exist = kv.database[op.Key]
		if !exist {
			opResult.Value = ""
			opResult.Err = ErrNoKey
		}
		kv.clients[op.ClientId] = op.RequestId
		DPrintf("{Server %d} commit log[%d] (request[%d] from {Client %d}) | Get Key: %s | Now Value is %s", kv.me, index, op.RequestId, op.ClientId, op.Key, kv.database[op.Key])
	} else if op.OpType == PUT || op.OpType == APPEND {
		if !kv.isOpExecuted(op.ClientId, op.RequestId) {
			if op.OpType == PUT {
				kv.database[op.Key] = op.Value
			} else if op.OpType == APPEND {
				kv.database[op.Key] += op.Value
			}
			kv.clients[op.ClientId] = op.RequestId
			DPrintf("{Server %d} commit log[%d] (request[%d] from {Client %d}) | %s Key: %s Value: %s | Now Value is %s", kv.me, index, op.RequestId, op.ClientId, op.OpType, op.Key, op.Value, kv.database[op.Key])
		} else {
			DPrintf("{Server %d} Duplicate request[%d] from {Client %d} | %s Key: %s Value: %s", kv.me, op.RequestId, op.ClientId, op.OpType, op.Key, op.Value)
			opResult.Err = ErrDupReq
		}
	}
}

func (kv *KVServer) notify(result *OpResult, index int) {
	kv.mu.Lock()
	ch := kv.GetBlockedOpCh(index, false)
	kv.mu.Unlock()
	if ch != nil {
		ch <- result
	}
}

func (kv *KVServer) GetBlockedOpCh(index int, create bool) chan *OpResult {
	ch, ok := kv.blockedOps[index]
	if !ok && create {
		ch = make(chan *OpResult, 1)
		kv.blockedOps[index] = ch
	}
	return ch
}

func (kv *KVServer) isOpExecuted(clientId int64, opId int32) bool {
	executedOpId, ok := kv.clients[clientId]
	if !ok {
		return false
	}
	return opId <= executedOpId
}

func (kv *KVServer) prepare(op *Op) (Err, int) {
	index, _, isLeader := kv.rf.Start(*op)
	if !isLeader {
		return ErrWrongLeader, -1
	}
	DPrintf("{Server %d} prepare log[%d] | request[%d] from {Client %d} |", kv.me, index, op.RequestId, op.ClientId)
	return OK, index
}

func (kv *KVServer) waitForCommit(op *Op, index int) (Err, string) {
	kv.mu.Lock()
	ch := kv.GetBlockedOpCh(index, true)
	kv.mu.Unlock()

	var err Err
	var value string

	select {
	case <-time.After(CommitTimerTime):
		err = ErrCommitTimeout
		break
	case result := <-ch:
		if *(result.op) != *op { // if the commited_op from channel same as arg_op ?
			err = ErrCommitFailed
			break
		}
		value = result.Value
		err = result.Err
	}
	kv.mu.Lock()
	delete(kv.blockedOps, index)
	kv.mu.Unlock()
	return err, value
}

func (kv *KVServer) makeSnapshot() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.database)
	e.Encode(kv.clients)
	return w.Bytes()
}

func (kv *KVServer) readSnapshot(data []byte) {
	if len(data) == 0 {
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	if d.Decode(&kv.database) != nil || d.Decode(&kv.clients) != nil {
		panic("KVServer read snapshot fail")
	}
	DPrintf("{MAP}: %+v", kv.database)
}
