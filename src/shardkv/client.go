package shardkv

//
// client code to talk to a sharded key/value service.
//
// the client first talks to the shardctrler to find out
// the assignment of shards (keys) to groups, and then
// talks to the group that holds the key's shard.
//

import (
	"6.824/labrpc"
	"6.824/mr"
	"sync"
	"sync/atomic"
)
import "crypto/rand"
import "math/big"
import "6.824/shardctrler"
import "time"

var clientGerarator int32

//
// which shard is a key in?
// please use this function,
// and please do not change it.
//
func key2shard(key string) int {
	shard := 0
	if len(key) > 0 {
		shard = int(key[0])
	}
	shard %= shardctrler.NShards
	return shard
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

type Clerk struct {
	sm              *shardctrler.Clerk
	config          shardctrler.Config
	make_end        func(string) *labrpc.ClientEnd
	mu              sync.Mutex
	groupClientsMap map[int][]*labrpc.ClientEnd //其他group的客户端代理

	lastRpcServerId int
	clientId        int
	nextRequestId   uint64 //clientId<<12+nextRequestId
}

func MakeClerk(ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.sm = shardctrler.MakeClerk(ctrlers)
	//给new group创建ClientEnd来通信
	ck.make_end = make_end
	ck.groupClientsMap = make(map[int][]*labrpc.ClientEnd)
	// You'll have to add code here.
	ck.lastRpcServerId = 0
	ck.clientId = int(atomic.AddInt32(&clientGerarator, 1))
	ck.nextRequestId = 0
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
// You will have to modify this function.
//
func (ck *Clerk) Get(key string) string {
	ck.mu.Lock()
	defer ck.mu.Unlock()
	for ck.config.Num == 0 {
		ck.config = ck.sm.Query(-1)
	}
	shard := key2shard(key)
	args := GetArgs{
		ClientId:  ck.clientId,
		RequestId: atomic.AddUint64(&ck.nextRequestId, 1),
		Key:       key,
	}
	for {
		gid := ck.config.Shards[shard]
		if servers, ok := ck.config.Groups[gid]; ok {
			// try each server for the shard.
			for si := 0; si < len(servers); si++ {

				srv := ck.make_end(servers[si])
				var reply GetReply
				ok := srv.Call("ShardKV.Get", &args, &reply)
				DPrintf("ShardKV Clerk Get,gid: %v, shardId: %v, args: %v, reply: %v, config: %v", gid, shard, mr.Any2String(args), mr.Any2String(reply), mr.Any2String(ck.config))
				if ok && (reply.Err == OK) {
					DPrintf("ShardKV Clerk Get,success args: %v, reply: %v", mr.Any2String(args), mr.Any2String(reply))
					return reply.Value
				}
				if ok && (reply.Err == ErrWrongGroup) {
					break
				}
			}
		}
		time.Sleep(100 * time.Millisecond)
		ck.config = ck.sm.Query(-1)
		DPrintf("Clerk update config: %v", mr.Any2String(ck.config))
	}

}

//
// shared by Put and Append.
// You will have to modify this function.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	ck.mu.Lock()
	defer ck.mu.Unlock()
	for ck.config.Num == 0 {
		ck.config = ck.sm.Query(-1)
	}
	shard := key2shard(key)

	args := PutAppendArgs{
		ClientId:  ck.clientId,
		RequestId: atomic.AddUint64(&ck.nextRequestId, 1),
		Key:       key,
		Value:     value,
		Op:        op,
	}
	for {
		gid := ck.config.Shards[shard]
		if servers, ok := ck.config.Groups[gid]; ok {
			for si := 0; si < len(servers); si++ {
				srv := ck.make_end(servers[si])
				var reply PutAppendReply
				ok := srv.Call("ShardKV.PutAppend", &args, &reply)
				DPrintf("shardKV Clerk.PutAppend args: %v reply: %v", mr.Any2String(args), mr.Any2String(reply))
				if ok && (reply.Err == OK || reply.Err == ErrDuplicated) {
					return
				}
				if ok && reply.Err == ErrWrongGroup {
					break
				}
			}
		}
		time.Sleep(100 * time.Millisecond)
		ck.config = ck.sm.Query(-1)
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
