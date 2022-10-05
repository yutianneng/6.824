package kvraft

import (
	"6.824/labgob"
	"6.824/labrpc"
	"6.824/mr"
	"6.824/raft"
	"bytes"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

const Debug = false

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
	ClientId       int
	RequestId      uint64
	OpType         OpType
	Key            string
	Value          string
	StartTimestamp int64
}

type OpContext struct {
	ClientId        int
	RequestId       uint64
	UniqueRequestId uint64
	Op              *Op
	Term            int
	WaitCh          chan string
}

func NewOpContext(op *Op, term int) *OpContext {
	return &OpContext{
		ClientId:        op.ClientId,
		RequestId:       op.RequestId,
		UniqueRequestId: UniqueRequestId(op.ClientId, op.RequestId),
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

	maxraftstate      int // snapshot if log grows this big
	persister         *raft.Persister
	lastIncludedIndex int

	// Your definitions here.
	kvStore          map[string]string     //k-v对
	opContextMap     map[uint64]*OpContext //用于每个请求的上下文
	lastRequestIdMap map[int]uint64        //clientId-->lastRequestId，维持幂等性，需要客户端能够保证串行
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	op := &Op{
		ClientId:       args.ClientId,
		RequestId:      args.RequestId,
		OpType:         OpTypeGet,
		Key:            args.Key,
		StartTimestamp: time.Now().UnixMilli(),
	}
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
	kv.mu.Lock()
	opContext := NewOpContext(op, term)
	kv.opContextMap[opContext.UniqueRequestId] = opContext
	kv.mu.Unlock()
	_, _, ok := kv.rf.Start(*op)

	defer func() {
		//DPrintf("server Get, args: %v, reply: %v", mr.Any2String(args), mr.Any2String(reply))
		kv.mu.Lock()
		delete(kv.opContextMap, opContext.UniqueRequestId)
		kv.mu.Unlock()
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
	case <-time.After(time.Millisecond * 1000):
		reply.Err = ErrTimeout
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {

	op := &Op{
		ClientId:       args.ClientId,
		RequestId:      args.RequestId,
		OpType:         OpType(args.Op),
		Key:            args.Key,
		Value:          args.Value,
		StartTimestamp: time.Now().UnixMilli(),
	}
	term := 0
	isLeader := false
	reply.Err = ErrWrongLeader
	if term, isLeader = kv.rf.GetState(); !isLeader {
		return
	}

	start := time.Now()
	defer func() {
		DPrintf("server PutAppend cost: %v, requestId: %d, node: %d, leaderId: %d", time.Now().Sub(start).Milliseconds(), op.RequestId, kv.me, kv.rf.LeaderId())
	}()
	kv.mu.Lock()
	//可能存在前一次请求超时，但是这个请求实际上执行成功了，那么就直接return掉
	if lastRequestId, ok := kv.lastRequestIdMap[op.ClientId]; ok && lastRequestId >= op.RequestId {
		reply.Err = OK
		kv.mu.Unlock()
		return
	}
	opContext := NewOpContext(op, term)
	kv.opContextMap[UniqueRequestId(op.ClientId, op.RequestId)] = opContext
	kv.mu.Unlock()
	_, _, ok := kv.rf.Start(*op)
	defer func() {
		//DPrintf("server PutAppend, args: %v, reply: %v", mr.Any2String(args), mr.Any2String(reply))
		kv.mu.Lock()
		delete(kv.opContextMap, UniqueRequestId(op.ClientId, op.RequestId))
		kv.mu.Unlock()
	}()
	if !ok {
		return
	}
	//阻塞等待
	select {
	case <-opContext.WaitCh:
		reply.Err = OK
	case <-time.After(time.Millisecond * 1000):
		reply.Err = ErrTimeout
	}
}

//串行写状态机
func (kv *KVServer) applyStateMachineLoop() {

	for !kv.killed() {

		select {
		case applyMsg := <-kv.applyCh:
			if applyMsg.CommandValid {
				func() {
					kv.mu.Lock()
					defer kv.mu.Unlock()
					op := applyMsg.Command.(Op)
					//保证幂等性
					if op.RequestId <= kv.lastRequestIdMap[op.ClientId] {
						return
					}
					//过滤掉snapshot前的日志
					if applyMsg.CommandIndex <= kv.lastIncludedIndex && op.OpType != OpTypeGet {
						if c, ok := kv.opContextMap[UniqueRequestId(op.ClientId, op.RequestId)]; ok {
							c.WaitCh <- "0"
						}
						return
					}
					switch op.OpType {
					case OpTypePut:
						kv.kvStore[op.Key] = op.Value
						kv.lastRequestIdMap[op.ClientId] = op.RequestId
						kv.maybeSnapshot(applyMsg.CommandIndex)
					case OpTypeAppend:
						kv.kvStore[op.Key] += op.Value
						kv.lastRequestIdMap[op.ClientId] = op.RequestId
						kv.maybeSnapshot(applyMsg.CommandIndex)
					case OpTypeGet:
						//Get请求不需要更新lastRequestId
					}
					DPrintf("op: %v, value: %v, node: %v cost: %v,requestId: %v, stateMachine: %v", mr.Any2String(op), kv.kvStore[op.Key], kv.me, time.Now().UnixMilli()-op.StartTimestamp, op.RequestId, mr.Any2String(kv.kvStore))
					val := kv.kvStore[op.Key]
					//使得写入的client能够响应
					if c, ok := kv.opContextMap[UniqueRequestId(op.ClientId, op.RequestId)]; ok {
						c.WaitCh <- val
					}
				}()
			} else if applyMsg.SnapshotValid {
				func() {
					kv.mu.Lock()
					defer kv.mu.Unlock()
					if kv.decodeSnapshot(applyMsg.Snapshot) {
						kv.rf.CondInstallSnapshot(applyMsg.SnapshotTerm, applyMsg.SnapshotIndex, applyMsg.Snapshot)
					}
				}()
			}
		}
		DPrintf("snapshot size: %v, stateMachine: %v", kv.persister.SnapshotSize(), mr.Any2String(kv.kvStore))
	}
}

func (kv *KVServer) maybeSnapshot(index int) {
	if kv.maxraftstate == -1 {
		return
	}
	if kv.persister.RaftStateSize() > kv.maxraftstate {
		DPrintf("maybeSnapshot starting, index: %v", index)
		kv.rf.Snapshot(index, kv.encodeSnapshot(index))
	}
}

//上层加锁
func (kv *KVServer) encodeSnapshot(lastIncludedIndex int) []byte {

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.kvStore)
	e.Encode(lastIncludedIndex)
	e.Encode(kv.lastRequestIdMap) //持久化每个client的最大已执行过的写请求
	return w.Bytes()
}

//上层加锁
func (kv *KVServer) decodeSnapshot(snapshot []byte) bool {

	if len(snapshot) == 0 {
		return true
	}
	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)

	if err := d.Decode(&kv.kvStore); err != nil {
		return false
	}
	if err := d.Decode(&kv.lastIncludedIndex); err != nil {
		return false
	}
	//持久化每个client的最大已执行过的写请求
	if err := d.Decode(&kv.lastRequestIdMap); err != nil {
		return false
	}
	return true
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
	kv.persister = persister

	kv.kvStore = make(map[string]string)
	kv.opContextMap = make(map[uint64]*OpContext)
	kv.lastRequestIdMap = make(map[int]uint64)
	kv.applyCh = make(chan raft.ApplyMsg, 10)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	kv.decodeSnapshot(persister.ReadSnapshot())

	go kv.applyStateMachineLoop()

	return kv
}
