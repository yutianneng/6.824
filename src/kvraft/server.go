package kvraft

import (
	"6.824/labgob"
	"6.824/labrpc"
	"6.824/mr"
	"6.824/raft"
	"encoding/json"
	"log"
	"strconv"
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
	Key   string
	Value string
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	proposeC chan Op           //用于线性处理client请求，发送给raft
	kvStore  map[string]string //k-v对

	applyWaitMap map[string]chan int //用于监听日志是否提交
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	defer func() {
		DPrintf("receive Get, args: %v, reply: %v", mr.Any2String(args), mr.Any2String(reply))
	}()
	if val, ok := kv.kvStore[args.Key]; ok {
		reply.Err = OK
		reply.Value = val
		return
	}
	reply.Err = ErrNoKey
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	defer func() {
		DPrintf("server PutAppend, args: %v, reply: %v", mr.Any2String(args), mr.Any2String(reply))
	}()
	op := &Op{
		Key:   args.Key,
		Value: args.Value,
	}
	if args.Op == "Append" {
		if v, ok := kv.kvStore[args.Key]; ok {
			val := v + args.Value
			op.Value = val
		}
	}
	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	index, _, ok := kv.rf.Start(op)
	if !ok {
		reply.Err = ErrWrongLeader
		return
	}
	DPrintf("leaderId: %v", kv.rf.LeaderId())
	//阻塞等待
	kv.mu.Lock()
	wait := make(chan int, 1)
	kv.applyWaitMap[strconv.Itoa(index)+"-"+op.Key] = wait
	kv.mu.Unlock()

	select {
	case <-wait:
		reply.Err = OK
	case <-time.After(time.Second):
		reply.Err = ErrNoKey
	}
	kv.mu.Lock()
	delete(kv.applyWaitMap, strconv.Itoa(index)+"-"+op.Key)
	kv.mu.Unlock()
}
func convert(command interface{}) *Op {

	bytes, _ := json.Marshal(command)
	op := &Op{}
	json.Unmarshal(bytes, &op)
	return op
}
func (kv *KVServer) applyStateMachineLoop() {

	for !kv.killed() {

		select {
		case applyMsg := <-kv.applyCh:
			if applyMsg.CommandValid {

				command := convert(applyMsg.Command)
				DPrintf("command: %v", mr.Any2String(command))
				kv.kvStore[command.Key] = command.Value
				//使得写入的client能够响应
				if c, ok := kv.applyWaitMap[strconv.Itoa(applyMsg.CommandIndex)+"-"+command.Key]; ok {
					c <- 1
				}
			}
		}
	}
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

//
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
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	kv.kvStore = make(map[string]string)
	kv.applyWaitMap = make(map[string]chan int)

	go kv.applyStateMachineLoop()

	return kv
}
