package shardkv

import (
	"6.824/labrpc"
	"6.824/mr"
	"6.824/shardctrler"
	"log"
	"sync/atomic"
	"time"
)
import "6.824/raft"
import "sync"
import "6.824/labgob"

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

func NewOpContext(op *Op, term int) *OpContext {
	return &OpContext{
		ClientId:        op.ClientId,
		RequestId:       op.RequestId,
		UniqueRequestId: UniqueRequestId(op.ClientId, op.RequestId),
		Op:              op,
		Term:            term,
		WaitCh:          make(chan *ExecuteResponse, 1),
	}
}

type ShardKV struct {
	mu           sync.RWMutex
	me           int
	rf           *raft.Raft
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	ctrlers      []*labrpc.ClientEnd
	mck          *shardctrler.Clerk
	maxraftstate int   // snapshot if log grows this big
	dead         int32 // set by Kill()

	nextRequestId uint64
	applyCh       chan raft.ApplyMsg
	lastApplied   int

	persister         *raft.Persister
	lastIncludedIndex int

	lastConfig    shardctrler.Config
	currentConfig shardctrler.Config
	shardMap      map[int]*Shard
	opContextMap  map[uint64]*OpContext //用于每个请求的上下文
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {

	kv.mu.Lock()
	defer func() {
		DPrintf("ShardKV Get, gid[%d] node[%d] leaderId[%d] config: %v, shardMap: %v args: %v, reply: %v", kv.gid, kv.me, kv.rf.LeaderId(), mr.Any2String(kv.currentConfig), mr.Any2String(kv.shardMap), mr.Any2String(args), mr.Any2String(reply))
	}()
	term, err := kv.check(OpTypeGet, args.Key, args.ClientId, args.RequestId)

	reply.Err = err
	if err != OK {
		kv.mu.Unlock()
		return
	}
	op := &Op{
		ClientId:       args.ClientId,
		RequestId:      args.RequestId,
		OpType:         OpTypeGet,
		Key:            args.Key,
		StartTimestamp: time.Now().UnixMilli(),
	}
	opContext := NewOpContext(op, term)
	kv.opContextMap[opContext.UniqueRequestId] = opContext
	kv.mu.Unlock()
	defer func() {
		//DPrintf("server Get, args: %v, reply: %v", mr.Any2String(args), mr.Any2String(reply))
		kv.mu.Lock()
		delete(kv.opContextMap, opContext.UniqueRequestId)
		kv.mu.Unlock()
	}()
	_, _, ok := kv.rf.Start(*op)
	if !ok {
		reply.Err = ErrWrongLeader
		return
	}
	//阻塞等待
	select {
	case res := <-opContext.WaitCh:
		reply.Err = res.Err
		if reply.Err == OK && res.Value != nil {
			reply.Value = res.Value.(string)
		}
	case <-time.After(time.Millisecond * 1000):
		reply.Err = ErrTimeout
	}
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {

	kv.mu.Lock()
	term, err := kv.check(OpType(args.Op), args.Key, args.ClientId, args.RequestId)
	if err != OK {
		reply.Err = err
		kv.mu.Unlock()
		return
	}
	op := &Op{
		ClientId:       args.ClientId,
		RequestId:      args.RequestId,
		OpType:         OpType(args.Op),
		Key:            args.Key,
		Value:          args.Value,
		StartTimestamp: time.Now().UnixMilli(),
	}
	opContext := NewOpContext(op, term)
	kv.opContextMap[UniqueRequestId(op.ClientId, op.RequestId)] = opContext
	defer func() {
		//DPrintf("server PutAppend, args: %v, reply: %v", mr.Any2String(args), mr.Any2String(reply))
		kv.mu.Lock()
		delete(kv.opContextMap, UniqueRequestId(op.ClientId, op.RequestId))
		kv.mu.Unlock()
	}()
	kv.mu.Unlock()
	_, _, ok := kv.rf.Start(*op)
	if !ok {
		reply.Err = ErrWrongLeader
		return
	}
	//阻塞等待
	select {
	case res := <-opContext.WaitCh:
		reply.Err = res.Err
	case <-time.After(time.Millisecond * 1000):
		reply.Err = ErrTimeout
	}
}
func (kv *ShardKV) check(opType OpType, key string, clientId int, requestId uint64) (int, Err) {

	term, isLeader := kv.rf.GetState()
	if !isLeader {
		return term, ErrWrongLeader
	}
	shardId := key2shard(key)
	//DPrintf("ShardKV server, gid: %v, node: %v, leaderId: %v, shardId: %v, shardMap: %v", kv.gid, kv.me, kv.rf.LeaderId(), shardId, mr.Any2String(kv.shardMap))
	shard := kv.shardMap[shardId]
	if shard.ShardStatus == Pulling {
		return 0, ErrNotReady
	}
	if shard.ShardStatus == NoServing || shard.ShardStatus == BePulling {
		return 0, ErrWrongGroup
	}

	if opType == OpTypeGet {
		return term, OK
	}
	if lastRequestId, ok := kv.shardMap[shardId].LastRequestIdMap[clientId]; ok && lastRequestId >= requestId {
		return term, ErrDuplicated
	}
	return term, OK
}

//串行写状态机
func (kv *ShardKV) applyStateMachineLoop() {

	for !kv.killed() {

		select {
		case applyMsg := <-kv.applyCh:
			if applyMsg.CommandValid {
				func() {
					kv.mu.Lock()
					defer kv.mu.Unlock()
					if applyMsg.CommandIndex <= kv.lastApplied {
						return
					}
					kv.lastApplied = applyMsg.CommandIndex
					op := applyMsg.Command.(Op)

					var resp *ExecuteResponse
					switch op.OpType {
					case OpTypePut:
						resp = kv.applyPutOperation(&op)
					case OpTypeAppend:
						resp = kv.applyAppendOperation(&op)
					case OpTypeGet:
						resp = kv.applyGetOperation(&op)
					case OpTypeUpdateConfig:
						resp = kv.applyConfiguration(&op)
					case OpTypeAddShard:
						resp = kv.applyAddShard(&op)
					case OpTypeDeleteShard:
						resp = kv.applyDeleteShard(&op)
					}
					DPrintf("shardKV applyStateMachineLoop, gid[%d] node[%d] op: %v, resp: %v", kv.gid, kv.me, mr.Any2String(op), mr.Any2String(resp))

					//使得写入的client能够响应
					if c, ok := kv.opContextMap[UniqueRequestId(op.ClientId, op.RequestId)]; ok {
						c.WaitCh <- resp
					}
					kv.maybeSnapshot(applyMsg.CommandIndex)
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
	}
}

//apply由上层加锁
func (kv *ShardKV) applyGetOperation(op *Op) *ExecuteResponse {

	shardId := key2shard(op.Key)
	resp := &ExecuteResponse{Err: OK}
	defer func() {
		//DPrintf("applyGetOperation, gid[%d] node[%d] leaderId[%d] op: %v, resp: %v, shardMap: %v", kv.gid, kv.me, kv.rf.LeaderId(), mr.Any2String(op), mr.Any2String(resp), mr.Any2String(kv.shardMap))
	}()
	if err := kv.isAvailable(shardId); err != OK {
		resp.Err = err
		return resp
	}
	if val, ok := kv.shardMap[shardId].KvStore[op.Key]; !ok {
		resp.Err = ErrNoKey
	} else {
		resp.Value = val
	}
	return resp
}
func (kv *ShardKV) applyPutOperation(op *Op) *ExecuteResponse {
	shardId := key2shard(op.Key)
	if err := kv.isAvailable(shardId); err != OK {
		return &ExecuteResponse{Err: err}
	}
	if kv.isDuplicatedRequest(shardId, op.ClientId, op.RequestId) {
		return &ExecuteResponse{Err: ErrDuplicated}
	}
	kv.shardMap[shardId].KvStore[op.Key] = op.Value.(string)
	kv.shardMap[shardId].LastRequestIdMap[op.ClientId] = op.RequestId
	return &ExecuteResponse{Err: OK}
}
func (kv *ShardKV) isAvailable(shardId int) Err {
	if kv.currentConfig.Shards[shardId] != kv.gid {
		return ErrWrongGroup
	}
	if kv.shardMap[shardId].ShardStatus == Serving || kv.shardMap[shardId].ShardStatus == GCing {
		return OK
	}
	return ErrNotReady
}
func (kv *ShardKV) isDuplicatedRequest(shardId, clientId int, requestId uint64) bool {
	return kv.shardMap[shardId].LastRequestIdMap[clientId] >= requestId
}
func (kv *ShardKV) applyAppendOperation(op *Op) *ExecuteResponse {
	shardId := key2shard(op.Key)
	if err := kv.isAvailable(shardId); err != OK {
		return &ExecuteResponse{Err: err}
	}
	if kv.isDuplicatedRequest(shardId, op.ClientId, op.RequestId) {
		return &ExecuteResponse{Err: ErrDuplicated}
	}
	kv.shardMap[shardId].KvStore[op.Key] += op.Value.(string)
	kv.shardMap[shardId].LastRequestIdMap[op.ClientId] = op.RequestId
	return &ExecuteResponse{Err: OK}
}

//执行Op，写入到raft同步到其他peer
func (kv *ShardKV) execute(op Op) Err {
	if _, _, isLeader := kv.rf.Start(op); !isLeader {
		return ErrWrongLeader
	}
	return OK
}

//启动定时任务，包括配置更新、分片迁移等
func (kv *ShardKV) StartEventLoop(eventLoop func(), timeOut time.Duration) {
	for !kv.killed() {
		if _, isLeader := kv.rf.GetState(); isLeader {
			eventLoop()
		}
		time.Sleep(timeOut)
	}
}

//获取需要迁移的分片及它所在gid
func (kv *ShardKV) getShardIdsByStatus(status Status) map[int][]int {
	gid2ShardIds := map[int][]int{}
	for shardId, shard := range kv.shardMap {
		if shard.ShardStatus == status {
			gid := kv.lastConfig.Shards[shardId]
			if _, ok := gid2ShardIds[gid]; !ok {
				gid2ShardIds[gid] = make([]int, 0)
			}
			gid2ShardIds[gid] = append(gid2ShardIds[gid], shardId)
		}
	}
	return gid2ShardIds
}

func (kv *ShardKV) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *ShardKV) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})
	labgob.Register(shardctrler.Config{})
	labgob.Register(PullShardArgs{})
	labgob.Register(PullShardReply{})
	labgob.Register(DeleteShardArgs{})
	labgob.Register(DeleteShardReply{})
	labgob.Register(Shard{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	//总控节点，更新配置
	kv.ctrlers = ctrlers
	kv.mck = shardctrler.MakeClerk(kv.ctrlers)

	kv.persister = persister
	kv.opContextMap = make(map[uint64]*OpContext)
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.lastConfig = shardctrler.DefaultConfig()
	kv.currentConfig = shardctrler.DefaultConfig()
	kv.shardMap = map[int]*Shard{}
	for shardId, _ := range kv.currentConfig.Shards {
		kv.shardMap[shardId] = &Shard{
			ShardStatus:      NoServing,
			KvStore:          map[string]string{},
			LastRequestIdMap: map[int]uint64{},
		}
	}

	kv.decodeSnapshot(persister.ReadSnapshot())
	DPrintf("init kvserver, group: %v, node[%d]", gid, me)

	go kv.applyStateMachineLoop()

	go kv.StartEventLoop(kv.updateConfigurationEventLoop, time.Millisecond*100)
	go kv.StartEventLoop(kv.pullShardEventLoop, time.Millisecond*100)
	go kv.StartEventLoop(kv.gcShardEventLoop, time.Millisecond*100)
	//go kv.StartEventLoop(kv.gcEventLoop, time.Millisecond*100)

	return kv
}
