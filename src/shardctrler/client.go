package shardctrler

//
// Shardctrler clerk.
//

import (
	"6.824/labrpc"
	"sync"
	"sync/atomic"
)
import "time"
import "crypto/rand"
import "math/big"

var clientGerarator int32

type Clerk struct {
	servers []*labrpc.ClientEnd

	mu              sync.Mutex
	lastRpcServerId int
	clientId        int
	nextRequestId   uint64 //clientId<<12+nextRequestId
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
	ck.mu = sync.Mutex{}
	ck.lastRpcServerId = 0
	ck.clientId = int(atomic.AddInt32(&clientGerarator, 1))
	ck.nextRequestId = 0
	return ck
}

func (ck *Clerk) Query(num int) Config {
	ck.mu.Lock()
	defer ck.mu.Unlock()
	args := &QueryArgs{
		Num:       num,
		ClientId:  ck.clientId,
		RequestId: atomic.AddUint64(&ck.nextRequestId, 1),
	}
	for {
		for _, srv := range ck.servers {
			var reply QueryReply
			ok := srv.Call("ShardCtrler.Query", args, &reply)
			if ok && reply.WrongLeader == false {
				return reply.Config
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Join(servers map[int][]string) {
	ck.mu.Lock()
	defer ck.mu.Unlock()
	args := &JoinArgs{
		Servers:   servers,
		ClientId:  ck.clientId,
		RequestId: atomic.AddUint64(&ck.nextRequestId, 1),
	}

	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply JoinReply
			ok := srv.Call("ShardCtrler.Join", args, &reply)
			if ok && reply.WrongLeader == false {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Leave(gids []int) {
	ck.mu.Lock()
	defer ck.mu.Unlock()
	args := &LeaveArgs{
		ClientId:  ck.clientId,
		RequestId: atomic.AddUint64(&ck.nextRequestId, 1),
		GIDs:      gids,
	}

	for {
		for _, srv := range ck.servers {
			var reply LeaveReply
			ok := srv.Call("ShardCtrler.Leave", args, &reply)
			if ok && reply.WrongLeader == false {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Move(shard int, gid int) {
	ck.mu.Lock()
	defer ck.mu.Unlock()
	args := &MoveArgs{
		ClientId:  ck.clientId,
		RequestId: atomic.AddUint64(&ck.nextRequestId, 1),
		Shard:     shard,
		GID:       gid,
	}
	for {
		for _, srv := range ck.servers {
			var reply MoveReply
			ok := srv.Call("ShardCtrler.Move", args, &reply)
			if ok && reply.WrongLeader == false {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}
