package kvraft

import (
	"6.824/labrpc"
	"sync"
	"sync/atomic"
	"time"
)
import "crypto/rand"
import "math/big"

type Clerk struct {
	mu              sync.Mutex
	servers         []*labrpc.ClientEnd
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
	ck.lastRpcServerId = int(nrand()) % len(ck.servers)
	ck.clientId = int(nrand() + time.Now().UnixNano())
	ck.nextRequestId = uint64(nrand() % 1000)
	return ck
}

func (ck *Clerk) currentRpcServerId() int {
	ck.mu.Lock()
	defer ck.mu.Unlock()
	return ck.lastRpcServerId
}
func (ck *Clerk) setRpcServerId(rpcServerId int) {
	ck.mu.Lock()
	defer ck.mu.Unlock()
	ck.lastRpcServerId = rpcServerId
	ck.lastRpcServerId %= len(ck.servers)
}

//
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
//
func (ck *Clerk) Get(key string) string {

	start := time.Now()
	defer func() {
		DPrintf("client Get cost: %v", time.Now().Sub(start).Milliseconds())
	}()
	args := &GetArgs{
		UniqueRequestId: UniqueRequestId(ck.clientId, atomic.AddUint64(&ck.nextRequestId, 1)),
		Key:             key,
	}
	reply := &GetReply{}
	rpcServerId := ck.currentRpcServerId()

	for {
		for i := 0; i < len(ck.servers); i++ {
			ok := ck.servers[rpcServerId].Call("KVServer.Get", args, reply)
			//DPrintf("client Get, args: %v, reply: %v", mr.Any2String(args), mr.Any2String(reply))
			if !ok {
				rpcServerId++
				rpcServerId %= len(ck.servers)
			} else if reply.Err == OK {
				return reply.Value
			} else if reply.LeaderId != -1 {
				rpcServerId = reply.LeaderId
			} else {
				rpcServerId++
				rpcServerId %= len(ck.servers)
			}
			time.Sleep(time.Millisecond * 1)
		}
		//轮询一遍都失败了，可能正常选主中
		//time.Sleep(time.Millisecond * 50)
	}
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {

	start := time.Now()
	defer func() {
		DPrintf("client Get cost: %v", time.Now().Sub(start).Milliseconds())
	}()
	args := &PutAppendArgs{
		UniqueRequestId: UniqueRequestId(ck.clientId, atomic.AddUint64(&ck.nextRequestId, 1)),
		Key:             key,
		Value:           value,
		Op:              op,
	}
	reply := &PutAppendReply{}
	rpcServerId := ck.currentRpcServerId()

	for {
		for i := 0; i < len(ck.servers); i++ {
			ok := ck.servers[rpcServerId].Call("KVServer.PutAppend", args, reply)
			//DPrintf("client Put, args: %v, reply: %v", mr.Any2String(args), mr.Any2String(reply))
			if !ok {
				rpcServerId++
				rpcServerId %= len(ck.servers)

			} else if reply.Err == OK {
				ck.setRpcServerId(rpcServerId)
				//DPrintf("client Put, args: %v, reply: %v", mr.Any2String(args), mr.Any2String(reply))
				return
			} else if reply.LeaderId != -1 {
				rpcServerId = reply.LeaderId
			} else {
				rpcServerId++
				rpcServerId %= len(ck.servers)
			}
			time.Sleep(time.Millisecond * 1)
		}
		//轮询一遍都失败了，可能正常选主中
		//time.Sleep(time.Millisecond * 50)
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
