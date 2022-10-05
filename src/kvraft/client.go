package kvraft

import (
	"6.824/labrpc"
	"6.824/mr"
	"sync"
	"sync/atomic"
	"time"
)
import "crypto/rand"
import "math/big"

var clientGerarator int32

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
	ck.lastRpcServerId = 0
	ck.clientId = int(atomic.AddInt32(&clientGerarator, 1))
	ck.nextRequestId = 0
	return ck
}

func (ck *Clerk) currentRpcServerId() int {
	//ck.mu.Lock()
	//defer ck.mu.Unlock()
	return ck.lastRpcServerId
}
func (ck *Clerk) setRpcServerId(rpcServerId int) {
	//ck.mu.Lock()
	//defer ck.mu.Unlock()
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
	ck.mu.Lock()
	defer ck.mu.Unlock()
	args := &GetArgs{
		ClientId:  ck.clientId,
		RequestId: atomic.AddUint64(&ck.nextRequestId, 1),
		Key:       key,
	}
	rpcServerId := ck.currentRpcServerId()

	for {
		reply := &GetReply{}
		ok := ck.servers[rpcServerId].Call("KVServer.Get", args, reply)
		//DPrintf("client Get, args: %v, reply: %v", mr.Any2String(args), mr.Any2String(reply))
		if !ok {
			rpcServerId++
			rpcServerId %= len(ck.servers)
		} else if reply.Err == OK {
			ck.setRpcServerId(rpcServerId)
			return reply.Value
		} else {
			rpcServerId++
			rpcServerId %= len(ck.servers)
		}
		time.Sleep(time.Millisecond * 1)
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

	ck.mu.Lock()
	defer ck.mu.Unlock()
	args := &PutAppendArgs{
		ClientId:  ck.clientId,
		RequestId: atomic.AddUint64(&ck.nextRequestId, 1),
		Key:       key,
		Value:     value,
		Op:        op,
	}
	defer func() {
		DPrintf("client Put cost: %v,op: %v, key: %v, value: %v, args: %v", time.Now().Sub(start).Milliseconds(), op, key, value, mr.Any2String(args))
	}()
	rpcServerId := ck.currentRpcServerId()

	for {
		reply := &PutAppendReply{}
		ok := ck.servers[rpcServerId].Call("KVServer.PutAppend", args, reply)
		//DPrintf("client Put, args: %v, reply: %v", mr.Any2String(args), mr.Any2String(reply))
		if !ok {
			DPrintf("client Put, ok: %v, rpcServerId: %d, args: %v, reply: %v", ok, rpcServerId, mr.Any2String(args), mr.Any2String(reply))
			rpcServerId++
			rpcServerId %= len(ck.servers)
		} else if reply.Err == OK {
			ck.setRpcServerId(rpcServerId)
			DPrintf("client Put, args: %v, reply: %v", mr.Any2String(args), mr.Any2String(reply))
			return
		} else {
			DPrintf("client Put, rpcServerId: %d, args: %v, reply: %v", rpcServerId, mr.Any2String(args), mr.Any2String(reply))
			rpcServerId++
			rpcServerId %= len(ck.servers)
		}
		time.Sleep(time.Millisecond * 1)
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
