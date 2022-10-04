package kvraft

import (
	"6.824/labgob"
	"6.824/labrpc"
	"6.824/mr"
	"6.824/raft"
	"log"
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

type OpType string

const (
	OpTypeGet    = "Get"
	OpTypePut    = "Put"
	OpTypeAppend = "Append"
)

type Op struct {
	UniqueRequestId uint64
	OpType          OpType
	Key             string
	Value           string
	StartTimestamp  int64
}

type OpContext struct {
	UniqueRequestId uint64
	Op              *Op
	Term            int
	WaitCh          chan string
}

func NewOpContext(uniqueRequestId uint64, op *Op, term int) *OpContext {
	return &OpContext{
		UniqueRequestId: uniqueRequestId,
		Op:              op,
		Term:            term,
		WaitCh:          make(chan string, 1),
	}
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	proposeC chan *Op          //用于线性处理client请求，发送给raft
	kvStore  map[string]string //k-v对

	opContextMap   map[uint64]*OpContext //用于每个请求的上下文
	idempotencyMap map[uint64]int64      //幂等性
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	op := &Op{
		UniqueRequestId: args.UniqueRequestId,
		OpType:          OpTypeGet,
		Key:             args.Key,
		StartTimestamp:  time.Now().UnixMilli(),
	}
	reply.LeaderId = kv.rf.LeaderId()
	//Append不能先append然后将日志传给raft
	term := 0
	isLeader := false
	if term, isLeader = kv.rf.GetState(); !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	start := time.Now()
	defer func() {
		DPrintf("server Get cost: %v, node: %v, leaderId: %d", time.Now().Sub(start).Milliseconds(), kv.me, kv.rf.LeaderId())
	}()
	opContext := NewOpContext(op.UniqueRequestId, op, term)
	kv.opContextMap[op.UniqueRequestId] = opContext
	_, _, ok := kv.rf.Start(*op)

	defer func() {
		//DPrintf("server Get, args: %v, reply: %v", mr.Any2String(args), mr.Any2String(reply))
		delete(kv.opContextMap, op.UniqueRequestId)
	}()
	if !ok {
		reply.Err = ErrWrongLeader
		return
	}
	//阻塞等待
	select {
	case c := <-opContext.WaitCh:
		reply.Err = OK
		reply.Value = c
	case <-time.After(time.Millisecond * 500):
		reply.Err = ErrTimeout
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {

	op := &Op{
		UniqueRequestId: args.UniqueRequestId,
		OpType:          OpType(args.Op),
		Key:             args.Key,
		Value:           args.Value,
		StartTimestamp:  time.Now().UnixMilli(),
	}
	reply.LeaderId = kv.rf.LeaderId()
	term := 0
	isLeader := false
	if term, isLeader = kv.rf.GetState(); !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	start := time.Now()
	defer func() {
		DPrintf("server PutAppend cost: %v, node: %d, leaderId: %d", time.Now().Sub(start).Milliseconds(), kv.me, kv.rf.LeaderId())
	}()
	opContext := NewOpContext(op.UniqueRequestId, op, term)
	kv.opContextMap[op.UniqueRequestId] = opContext
	_, _, ok := kv.rf.Start(*op)
	defer func() {
		//DPrintf("server PutAppend, args: %v, reply: %v", mr.Any2String(args), mr.Any2String(reply))
		delete(kv.opContextMap, op.UniqueRequestId)
	}()
	if !ok {
		reply.Err = ErrWrongLeader
		return
	}
	//阻塞等待
	select {
	case <-opContext.WaitCh:
		reply.Err = OK
	case <-time.After(time.Millisecond * 500):
		reply.Err = ErrTimeout
	}
}

func (kv *KVServer) write2RaftLoop() {

}

//串行写状态机
func (kv *KVServer) applyStateMachineLoop() {

	for !kv.killed() {

		select {
		case applyMsg := <-kv.applyCh:
			if applyMsg.CommandValid {

				op := applyMsg.Command.(Op)
				//保证幂等性
				if _, ok := kv.idempotencyMap[op.UniqueRequestId]; ok {
					break
				}
				DPrintf("op: %v, cost: %v", mr.Any2String(op), time.Now().UnixMilli()-op.StartTimestamp)
				kv.idempotencyMap[op.UniqueRequestId] = time.Now().UnixMilli()
				kv.mu.Lock()
				switch op.OpType {
				case OpTypePut:
					kv.kvStore[op.Key] = op.Value
				case OpTypeAppend:
					kv.kvStore[op.Key] += op.Value
				case OpTypeGet:
				}
				val := kv.kvStore[op.Key]
				kv.mu.Unlock()
				//使得写入的client能够响应
				if c, ok := kv.opContextMap[op.UniqueRequestId]; ok {
					c.WaitCh <- val
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
	kv.opContextMap = make(map[uint64]*OpContext)
	kv.idempotencyMap = make(map[uint64]int64)
	kv.proposeC = make(chan *Op, 10)

	go kv.applyStateMachineLoop()

	return kv
}
