package shardctrler

//
// Shardctrler clerk.
//

import "6.5840/labrpc"
import "time"
import "crypto/rand"
import "math/big"

type Clerk struct {
	servers []*labrpc.ClientEnd
	// Your data here.
	leaderId  int
	selfId    int64
	requestId int32
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
	// Your code here.
	ck.leaderId = 0
	ck.selfId = nrand()
	return ck
}

func (ck *Clerk) Query(num int) Config {
	args := &QueryArgs{}
	// Your code here.
	args.Num = num
	args.ClientId = ck.selfId
	args.RequestId = ck.requestId
	ck.requestId++
	n := len(ck.servers)
	for {
		// try each known server.
		for i := 0; i < n; i++ {
			var reply QueryReply
			ok := ck.servers[ck.leaderId].Call("ShardCtrler.Query", args, &reply)
			if !ok {
				ck.leaderId = (ck.leaderId + 1) % n
				continue
			}
			DPrintf("{Client %d} Send Query to {Server %d} num:{%d} Success! | reply.Err : %v, Config: %v", ck.selfId, ck.leaderId, args.Num, reply.Err, reply.Config)
			if reply.Err == OK {
				return reply.Config
			} else {
				ck.leaderId = (ck.leaderId + 1) % n
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Join(servers map[int][]string) {
	args := &JoinArgs{}
	// Your code here.
	args.Servers = servers
	args.ClientId = ck.selfId
	args.RequestId = ck.requestId
	ck.requestId++
	n := len(ck.servers)

	for {
		// try each known server.
		for i := 0; i < n; i++ {
			var reply JoinReply
			ok := ck.servers[ck.leaderId].Call("ShardCtrler.Join", args, &reply)
			if !ok {
				ck.leaderId = (ck.leaderId + 1) % n
				continue
			}
			if reply.Err == OK {
				return
			} else {
				ck.leaderId = (ck.leaderId + 1) % n
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Leave(gids []int) {
	args := &LeaveArgs{}
	// Your code here.
	args.GIDs = gids
	args.ClientId = ck.selfId
	args.RequestId = ck.requestId
	ck.requestId++
	n := len(ck.servers)

	for {
		// try each known server.
		for i := 0; i < n; i++ {
			var reply LeaveReply
			ok := ck.servers[ck.leaderId].Call("ShardCtrler.Leave", args, &reply)
			if !ok {
				ck.leaderId = (ck.leaderId + 1) % n
				continue
			}
			switch reply.Err {
			case OK:
				return
			case ErrWrongLeader:
				ck.leaderId = (ck.leaderId + 1) % n
				continue
			case ErrDupReq:
				return
			}
			ck.leaderId = (ck.leaderId + 1) % n
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Move(shard int, gid int) {
	args := &MoveArgs{}
	// Your code here.
	args.Shard = shard
	args.GID = gid
	args.ClientId = ck.selfId
	args.RequestId = ck.requestId
	ck.requestId++
	n := len(ck.servers)

	for {
		// try each known server.
		for i := 0; i < n; i++ {
			var reply MoveReply
			ok := ck.servers[ck.leaderId].Call("ShardCtrler.Move", args, &reply)
			if !ok {
				ck.leaderId = (ck.leaderId + 1) % n
				continue
			}
			switch reply.Err {
			case OK:
				return
			case ErrWrongLeader:
				ck.leaderId = (ck.leaderId + 1) % n
				continue
			case ErrDupReq:
				return
			}
			ck.leaderId = (ck.leaderId + 1) % n
		}
		time.Sleep(100 * time.Millisecond)
	}
}
