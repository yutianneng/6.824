package kvraft

import (
	"6.824/labrpc"
	"6.824/mr"
	"sync"
	"time"
)
import "crypto/rand"
import "math/big"

type Clerk struct {
	mu              sync.Mutex
	servers         []*labrpc.ClientEnd
	lastRpcServerId int
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
	ck.lastRpcServerId = 0
	ck.mu = sync.Mutex{}
	return ck
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

	args := &GetArgs{Key: key}
	reply := &GetReply{}
	defer func() {
		DPrintf("client Get, args: %v, reply: %v", mr.Any2String(args), mr.Any2String(reply))
	}()
	ok := ck.servers[ck.lastRpcServerId].Call("KVServer.Get", args, reply)
	if !ok {
		return ""
	}
	return reply.Value
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
	// You will have to modify this function.

	args := &PutAppendArgs{
		Key:   key,
		Value: value,
		Op:    op,
	}
	reply := &PutAppendReply{}
	ck.mu.Lock()
	defer ck.mu.Unlock()

	for {
		for i := 0; i < len(ck.servers); i++ {
			ok := ck.servers[ck.lastRpcServerId].Call("KVServer.PutAppend", args, reply)
			if !ok {
				ck.lastRpcServerId++
				ck.lastRpcServerId %= len(ck.servers)
			} else if reply.Err == OK {
				DPrintf("client Put, args: %v, reply: %v", mr.Any2String(args), mr.Any2String(reply))
				return
			} else if reply.Err == ErrWrongLeader {
				ck.lastRpcServerId++
				ck.lastRpcServerId %= len(ck.servers)
			}
		}
		//当前可能还在选举，等待100ms
		time.Sleep(time.Millisecond * 100)
	}

}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
