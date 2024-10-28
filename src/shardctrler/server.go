package shardctrler

import (
	"6.5840/raft"
	"sync/atomic"
	"time"
)
import "6.5840/labrpc"
import "sync"
import "6.5840/labgob"

type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	dead    int32
	applyCh chan raft.ApplyMsg

	// Your data here.
	clients          map[int64]int32
	blockedOps       map[int]chan *OpResult
	lastAppliedIndex int
	configs          []Config // indexed by config num
	groups           map[int]*group
}

func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	DPrintf("{ShardCtrler %d} | Join %v", sc.me, args.Servers)
	op := &Op{
		ClientId:  args.ClientId,
		RequestId: args.RequestId,
		OpType:    JoinOP,
		Servers:   args.Servers,
	}

	var index int
	var ch chan *OpResult
	reply.Err, index, ch = sc.prepare(op)
	if reply.Err == ErrWrongLeader {
		return
	}
	reply.Err, _ = sc.waitForCommit(op, index, ch)
}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	DPrintf("{ShardCtrler %d} | Leave %v", sc.me, args.GIDs)
	op := &Op{
		ClientId:  args.ClientId,
		RequestId: args.RequestId,
		OpType:    LeaveOP,
		GIDs:      args.GIDs,
	}

	var index int
	var ch chan *OpResult
	reply.Err, index, ch = sc.prepare(op)
	if reply.Err == ErrWrongLeader {
		return
	}
	reply.Err, _ = sc.waitForCommit(op, index, ch)
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	DPrintf("{ShardCtrler %d} | Move shard[%d] -> {GID %d}", sc.me, args.Shard, args.GID)
	op := &Op{
		ClientId:  args.ClientId,
		RequestId: args.RequestId,
		OpType:    MoveOp,
		Shard:     args.Shard,
		GID:       args.GID,
	}

	var index int
	var ch chan *OpResult
	reply.Err, index, ch = sc.prepare(op)
	if reply.Err == ErrWrongLeader {
		return
	}
	reply.Err, _ = sc.waitForCommit(op, index, ch)
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	op := &Op{
		ClientId:  args.ClientId,
		RequestId: args.RequestId,
		OpType:    QueryOp,
		Num:       sc.GetValidConfigNum(args.Num),
	}
	DPrintf("{ShardCtrler %d} | Query Configs[%d]", sc.me, op.Num)
	var index int
	var ch chan *OpResult
	reply.Err, index, ch = sc.prepare(op)
	if reply.Err == ErrWrongLeader {
		return
	}
	reply.Err, _ = sc.waitForCommit(op, index, ch)
}

// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (sc *ShardCtrler) Kill() {
	atomic.StoreInt32(&sc.dead, 1)
	sc.rf.Kill()
	// Your code here, if desired.
}

func (sc *ShardCtrler) killed() bool {
	z := atomic.LoadInt32(&sc.dead)
	return z == 1
}

// needed by shardsc tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me

	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)

	// Your code here.
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)
	sc.clients = make(map[int64]int32)
	sc.blockedOps = make(map[int]chan *OpResult)
	sc.groups = make(map[int]*group)
	sc.lastAppliedIndex = 0

	go sc.applier()
	return sc
}

func (sc *ShardCtrler) applier() {
	for !sc.killed() {
		msg := <-sc.applyCh
		if msg.CommandValid && msg.CommandIndex > sc.lastAppliedIndex {
			op := msg.Command.(Op)
			index := msg.CommandIndex
			sc.lastAppliedIndex = index
			sc.processOp(&op, index)
		}
	}
}

func (sc *ShardCtrler) processOp(op *Op, index int) {
	opResult := OpResult{
		op:  op,
		Err: OK,
	}
	sc.mu.Lock()
	defer func() {
		sc.mu.Unlock()
		sc.notify(&opResult, index)
	}()
	if op.OpType == QueryOp {
		cfgNum := op.Num
		opResult.Config = sc.configs[cfgNum]
	} else {
		if sc.isOpExecuted(op.ClientId, op.RequestId) {
			opResult.Err = ErrDupReq
		} else {
			switch op.OpType {
			case JoinOP:
				sc.configs = append(sc.configs, *sc.copyLastConfig())
				newGroup := make([]int, 0)
				average := NShards / (len(sc.groups) + len(op.Servers))
				var shards []int
				if len(sc.groups) > 0 {
					shards = make([]int, 0)
					for _, g := range sc.groups {
						for len(g.shards) > average {
							shard := g.pop()
							shards = append(shards, shard)
						}
					}
				} else {
					shards = make([]int, NShards)
					for i := range shards {
						shards[i] = i
					}
				}
				for gid, svr := range op.Servers {
					sc.lastConfig().Groups[gid] = svr
					newGroup = append(newGroup, gid)
					sc.getGroup(gid)
				}
				sc.allocate(shards, newGroup)
			case LeaveOP:
				sc.configs = append(sc.configs, *sc.copyLastConfig())
				lastConfig := sc.lastConfig()
				lastConfig.Num++
				shards := make([]int, 0)
				for _, gid := range op.GIDs {
					shards = append(shards, sc.groups[gid].shards...)
					delete(lastConfig.Groups, gid)
					delete(sc.groups, gid)
				}
				newGroup := make([]int, 0)
				for gid, _ := range lastConfig.Groups {
					newGroup = append(newGroup, gid)
				}
				sc.allocate(shards, newGroup)
			case MoveOp:
				if op.Shard < 0 || op.Shard >= NShards {
					opResult.Err = ErrInvalidArgs
					break
				}
				shard := op.Shard
				gid := op.GID
				oriGID := sc.lastConfig().Shards[shard]
				sc.configs = append(sc.configs, *sc.copyLastConfig())
				sc.lastConfig().Num++
				g1, g2 := sc.groups[oriGID], sc.groups[gid]
				g1.remove(shard)
				g2.add(sc.lastConfig(), shard)
			}
		}
	}
	sc.clients[op.ClientId] = op.RequestId
	DPrintf("{Server %d} | Commit OP[%d]: %v | configs: %v | result: %v", sc.me, sc.lastAppliedIndex, *op, sc.configs, opResult)
}

func (sc *ShardCtrler) notify(result *OpResult, index int) {
	sc.mu.Lock()
	ch := sc.GetBlockedOpCh(index, false)
	sc.mu.Unlock()
	if ch != nil {
		ch <- result
	}
}

func (sc *ShardCtrler) GetBlockedOpCh(index int, create bool) chan *OpResult {
	ch, ok := sc.blockedOps[index]
	if !ok && create {
		ch = make(chan *OpResult, 1)
		sc.blockedOps[index] = ch
	}
	return ch
}

func (sc *ShardCtrler) isOpExecuted(clientId int64, opId int32) bool {
	executedOpId, ok := sc.clients[clientId]
	if !ok {
		return false
	}
	return opId <= executedOpId
}

func (sc *ShardCtrler) prepare(op *Op) (Err, int, chan *OpResult) {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	index, _, isLeader := sc.rf.Start(*op)
	if !isLeader {
		return ErrWrongLeader, -1, nil
	}
	ch := sc.GetBlockedOpCh(index, true)
	return OK, index, ch
}

func (sc *ShardCtrler) waitForCommit(op *Op, index int, ch chan *OpResult) (Err, Config) {
	var err Err
	var cfg Config

	select {
	case <-time.After(CommitTimerTime):
		err = ErrCommitTimeout
		break
	case result := <-ch:
		if !compareOP(result.op, op) { // if the commited_op from channel same as arg_op ?
			err = ErrCommitFailed
			break
		}
		if op.OpType == QueryOp {
			cfg = result.Config
		}
		err = result.Err
	}
	sc.mu.Lock()
	delete(sc.blockedOps, index)
	sc.mu.Unlock()
	return err, cfg
}

func (sc *ShardCtrler) copyLastConfig() *Config {
	lastConfig := sc.lastConfig()
	newConfig := Config{}
	newConfig.Num = lastConfig.Num + 1
	newConfig.Shards = lastConfig.Shards
	newConfig.Groups = make(map[int][]string)
	for gid, servers := range lastConfig.Groups {
		newConfig.Groups[gid] = servers
	}
	return &newConfig
}

func (sc *ShardCtrler) lastConfig() *Config {
	return &sc.configs[len(sc.configs)-1]
}

func (sc *ShardCtrler) allocate(shards, groups []int) {
	nShards := len(shards)
	nGroup := len(groups)
	allocated := make([]int, nGroup)
	lastConfig := sc.lastConfig()
	for i := 0; i < nGroup && nShards > 0; i++ {
		allocated[i] = max(1, nShards/nGroup)
		nShards -= allocated[i]
		nGroup--
	}
	i := 0
	j := 0
	for _, g := range sc.groups {
		alloc := allocated[i]
		for k := 0; k < alloc; k++ {
			g.add(lastConfig, shards[j])
			j++
		}
	}
}

func (sc *ShardCtrler) getGroup(gid int) *group {
	if g, exist := sc.groups[gid]; exist {
		return g
	}
	g := new(group)
	g.shards = make([]int, 0)
	g.gid = gid
	sc.groups[gid] = g
	return g
}

func (sc *ShardCtrler) GetValidConfigNum(configNum int) int {
	maxConfigNum := len(sc.configs) - 1
	if configNum > maxConfigNum {
		configNum = maxConfigNum
	}
	if configNum < 0 {
		configNum = maxConfigNum + 1 + configNum
		if configNum < 0 {
			configNum = 0
		}
	}
	return configNum
}
