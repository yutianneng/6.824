package shardkv

import (
	"6.824/labrpc"
	"6.824/mr"
	"6.824/shardctrler"
	"bytes"
	"log"
	"sync/atomic"
	"time"
)
import "6.824/raft"
import "sync"
import "6.824/labgob"

const Debug = true

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type OpType string

const (
	OpTypeGet          = "Get"
	OpTypePut          = "Put"
	OpTypeAppend       = "Append"
	OpTypeUpdateConfig = "UpdateConfig"
	OpTypeMigration    = "Migration"
)

type Op struct {
	ClientId       int
	RequestId      uint64
	OpType         OpType
	ShardId        int
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

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	ctrlers      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big

	dead int32 // set by Kill()

	nextRequestId uint64

	persister         *raft.Persister
	lastIncludedIndex int

	lastConfig             shardctrler.Config
	currentConfig          shardctrler.Config
	pendingMigrationShards []int

	shardConfigMap map[int]*ShardConfig
	kvStatus       status

	opContextMap     map[uint64]*OpContext //用于每个请求的上下文
	lastRequestIdMap map[int]uint64        //clientId-->lastRequestId，维持幂等性，需要客户端能够保证串行
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {

	kv.mu.Lock()
	term, err := kv.check(OpTypeGet, args.ShardId, args.ClientId, args.RequestId)
	DPrintf("ShardKV server.Get, err: %v gid: %v node: %v shardConfigMap: %v", err, kv.gid, kv.me, mr.Any2String(kv.shardConfigMap))

	if err != OK {
		reply.Err = err
		kv.mu.Unlock()
		return
	}
	op := &Op{
		ClientId:       args.ClientId,
		RequestId:      args.RequestId,
		OpType:         OpTypeGet,
		Key:            args.Key,
		ShardId:        args.ShardId,
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
	case c := <-opContext.WaitCh:
		if c == ErrWrongGroup {
			reply.Err = ErrWrongGroup
		} else {
			reply.Err = OK
			reply.Value = c
		}
	case <-time.After(time.Millisecond * 1000):
		reply.Err = ErrTimeout
	}
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {

	kv.mu.Lock()
	term, err := kv.check(OpType(args.Op), args.ShardId, args.ClientId, args.RequestId)
	defer func() {
		//DPrintf("ShardKV Server.PutAppend, config: %v, args: %v, reply: %v, shardConfigMap: %v", mr.Any2String(kv.currentConfig), mr.Any2String(args), mr.Any2String(reply), mr.Any2String(kv.shardConfigMap))

	}()
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
		ShardId:        args.ShardId,
		StartTimestamp: time.Now().UnixMilli(),
	}

	//start := time.Now()
	defer func() {
		//DPrintf("shardKV server PutAppend cost: %v, requestId: %d, node: %d, leaderId: %d", time.Now().Sub(start).Milliseconds(), op.RequestId, kv.me, kv.rf.LeaderId())
	}()
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
		reply.Err = Err(res)
	case <-time.After(time.Millisecond * 1000):
		reply.Err = ErrTimeout
	}
}
func (kv *ShardKV) check(opType OpType, shardId, clientId int, requestId uint64) (int, Err) {

	if kv.currentConfig.Num == 0 {
		return 0, ErrShardNotArrived
	}
	if shard, ok := kv.shardConfigMap[shardId]; !ok || shard.ShardStatus != Normal {
		return 0, ErrWrongGroup
	}
	term, isLeader := kv.rf.GetState()
	if !isLeader {
		return term, ErrWrongLeader
	}
	if opType == OpTypeGet {
		return term, OK
	}
	if lastRequestId, ok := kv.lastRequestIdMap[clientId]; ok && lastRequestId >= requestId {
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
					op := applyMsg.Command.(Op)
					//DPrintf("shardKV applyStateMachineLoop, gid[%d] node[%d] op: %v", kv.gid, kv.me, mr.Any2String(op))
					//保证幂等性, 过滤掉snapshot前的日志
					if op.RequestId <= kv.lastRequestIdMap[op.ClientId] || applyMsg.CommandIndex <= kv.lastIncludedIndex {
						if c, ok := kv.opContextMap[UniqueRequestId(op.ClientId, op.RequestId)]; ok {
							c.WaitCh <- ErrDuplicated
						}
						return
					}
					//不能正常接收客户端读写请求
					if op.OpType == OpTypeGet || op.OpType == OpTypeAppend || op.OpType == OpTypePut {
						if shard, ok := kv.shardConfigMap[op.ShardId]; !ok || shard.ShardStatus != Normal {
							//DPrintf("shardKV server.applyStateMachineLoop, not apply op: %v to machine, shard: %v", mr.Any2String(op), mr.Any2String(shard))
							if c, ok1 := kv.opContextMap[UniqueRequestId(op.ClientId, op.RequestId)]; ok1 {
								c.WaitCh <- ErrWrongGroup
							}
							return
						}
					}
					val := ""
					switch op.OpType {
					case OpTypePut:
						kv.shardConfigMap[op.ShardId].KvStore[op.Key] = op.Value
						kv.lastRequestIdMap[op.ClientId] = op.RequestId
						val = OK
						//kv.maybeSnapshot(applyMsg.CommandIndex)
					case OpTypeAppend:
						kv.shardConfigMap[op.ShardId].KvStore[op.Key] += op.Value
						kv.lastRequestIdMap[op.ClientId] = op.RequestId
						val = OK
						//kv.maybeSnapshot(applyMsg.CommandIndex)
					case OpTypeGet:
						//Get请求不需要更新lastRequestId
						val = kv.shardConfigMap[op.ShardId].KvStore[op.Key]
					case OpTypeUpdateConfig:
						//已经有配置在更新
						conf := Decode2Config([]byte(op.Value))
						kv.updateShardConfig(conf)

					case OpTypeMigration:
						r := bytes.NewBuffer([]byte(op.Value))
						d := labgob.NewDecoder(r)
						num, shardId := 0, -1
						d.Decode(&num)
						d.Decode(&shardId)
						if _, ok := kv.shardConfigMap[op.ShardId]; !ok {
							kv.shardConfigMap[op.ShardId] = &ShardConfig{}
						}
						if num < kv.shardConfigMap[op.ShardId].Num {
							return
						}
						kvStore := map[string]string{}
						d.Decode(&kvStore)

						kv.shardConfigMap[op.ShardId].KvStore = kvStore
						kv.shardConfigMap[op.ShardId].ShardStatus = Normal //迁移成功
						DPrintf("shardKV migrate shard, gid: %v, node: %v, num: %v shardConfigMap: %v", kv.gid, kv.me, num, mr.Any2String(kv.shardConfigMap))

					}
					//DPrintf("applyLoop, op: %v write to shardId[%d] gid[%d] node[%d]: %v", mr.Any2String(op), op.ShardId, kv.gid, kv.me, mr.Any2String(kv.shardConfigMap[op.ShardId]))
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
	}
}
func (kv *ShardKV) updateShardConfig(conf *shardctrler.Config) {

	if conf != nil && conf.Num <= kv.currentConfig.Num {
		return
	}
	kv.lastConfig = kv.currentConfig
	kv.currentConfig = *conf
	//更新分片
	//首次更新，不需要迁移
	if kv.lastConfig.Num == 0 {
		for shardId, gid := range conf.Shards {
			kv.shardConfigMap[shardId] = &ShardConfig{}
			kv.shardConfigMap[shardId].Num = conf.Num
			kv.shardConfigMap[shardId].KvStore = map[string]string{}
			if gid == kv.gid {
				kv.shardConfigMap[shardId].ShardStatus = Normal
			} else {
				kv.shardConfigMap[shardId].ShardStatus = NoResponsible
			}
		}
	} else {
		pengdingShards := make([]int, 0)
		for shardId, gid := range conf.Shards {
			kv.shardConfigMap[shardId].Num = conf.Num
			if gid == kv.gid {
				//等待迁移，需要考虑两种情况，前一个迁移的请求还没处理完，后一个请求就已来到，此时要不要继续迁移
				if kv.shardConfigMap[shardId].ShardStatus != Normal {
					kv.kvStatus = Migrating
					kv.shardConfigMap[shardId].ShardStatus = Waiting
					pengdingShards = append(pengdingShards, shardId)
				}
			} else if kv.shardConfigMap[shardId].ShardStatus == Normal {
				//需要被迁移走的分片
				kv.shardConfigMap[shardId].ShardStatus = WaitingToBeMigrated
			}
		}
		if len(pengdingShards) > 0 {
			go kv.startMigration(pengdingShards)
		}

	}
	DPrintf("shardKV updateShardConfig, gid: %v, node: %v, lastConfig: %v, currentConfig: %v, shardConfigMap: %v", kv.gid, kv.me, mr.Any2String(kv.lastConfig), mr.Any2String(kv.currentConfig), mr.Any2String(kv.shardConfigMap))

}
func (kv *ShardKV) maybeSnapshot(index int) {
	if kv.maxraftstate == -1 {
		return
	}
	if kv.persister.RaftStateSize() > kv.maxraftstate {
		DPrintf("maybeSnapshot starting, index: %v", index)
		kv.rf.Snapshot(index, kv.encodeSnapshot(index))
	}
}

//上层加锁
func (kv *ShardKV) encodeSnapshot(lastIncludedIndex int) []byte {

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.shardConfigMap)
	e.Encode(lastIncludedIndex)
	e.Encode(kv.lastRequestIdMap) //持久化每个client的最大已执行过的写请求
	return w.Bytes()
}

//上层加锁
func (kv *ShardKV) decodeSnapshot(snapshot []byte) bool {

	if len(snapshot) == 0 {
		return true
	}
	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)

	if err := d.Decode(&kv.shardConfigMap); err != nil {
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
func (kv *ShardKV) checkConfigurationLoop() {
	for !kv.killed() {
		time.Sleep(time.Second * 1)
		//DPrintf("pull configuration from controller, config: %v", mr.Any2String(conf))

		func() {
			//由leader拉取配置并同步给group
			if _, isLeader := kv.rf.GetState(); !isLeader {
				return
			}
			conf := kv.pullConfiguration()

			kv.mu.Lock()
			defer kv.mu.Unlock()
			//正在迁移中
			if kv.kvStatus != Normal {
				flag := true
				for _, sid := range kv.pendingMigrationShards {
					if kv.shardConfigMap[sid].ShardStatus == Waiting {
						flag = false
					}
				}
				if !flag {
					return
				}
				kv.kvStatus = Normal
			}
			if conf.Num <= kv.currentConfig.Num {
				return
			}
			op := Op{
				ClientId:       kv.gid*1000 + kv.me,
				RequestId:      uint64(conf.Num),
				OpType:         OpTypeUpdateConfig,
				Value:          string(Encode2Byte(conf)),
				StartTimestamp: time.Now().UnixMilli(),
			}
			kv.rf.Start(op)
		}()
	}
}

//从其他group迁移分片数据并同步到本group的所有peer
func (kv *ShardKV) startMigration(pendingMigrationShards []int) {
	if _, isLeader := kv.rf.GetState(); !isLeader {
		return
	}

	for _, shardId := range pendingMigrationShards {
		go func(shardId int) {
			reply := kv.sendPullShard(kv.lastConfig.Shards[shardId], shardId)
			op := &Op{
				ClientId:       kv.gid*1000 + kv.me,
				RequestId:      atomic.AddUint64(&kv.nextRequestId, 1),
				OpType:         OpTypeMigration,
				ShardId:        shardId,
				Value:          string(reply.Data),
				StartTimestamp: time.Now().UnixMilli(),
			}
			kv.rf.Start(*op)
		}(shardId)
	}
}
func (kv *ShardKV) sendPullShard(destGid, shardId int) *PullShardReply {
	args := &PullShardArgs{
		ClientId:  kv.gid*1000 + kv.me,
		RequestId: atomic.AddUint64(&kv.nextRequestId, 1),
		ShardId:   shardId,
	}
	for {
		for _, servername := range kv.currentConfig.Groups[destGid] {
			reply := &PullShardReply{}
			clientEnd := kv.make_end(servername)
			ok := clientEnd.Call("ShardKV.PullShard", args, reply)
			if ok && (reply.Err == OK || reply.Err == ErrDuplicated) {
				return reply
			}
		}
		time.Sleep(time.Millisecond * 100)
	}
}

//如果存在该分片，则传输分片
func (kv *ShardKV) PullShard(args *PullShardArgs, reply *PullShardReply) {

	defer func() {
		DPrintf("shardKV PullShard args: %v reply: %v", mr.Any2String(args), mr.Any2String(reply))
	}()
	kv.mu.Lock()
	defer kv.mu.Unlock()
	reply.Err = ErrWrongLeader
	if _, isLeader := kv.rf.GetState(); !isLeader {
		return
	}
	reply.Err = ErrWrongGroup
	if reqId, ok := kv.lastRequestIdMap[args.ClientId]; ok && args.RequestId <= reqId {
		reply.Err = ErrDuplicated
		return
	}
	kv.lastRequestIdMap[args.ClientId] = args.RequestId
	if _, ok := kv.shardConfigMap[args.ShardId]; ok {

		w := bytes.Buffer{}
		e := labgob.NewEncoder(&w)
		e.Encode(kv.shardConfigMap[args.ShardId].Num)
		e.Encode(args.ShardId)
		e.Encode(kv.shardConfigMap[args.ShardId].KvStore)
		reply.Data = Encode2Byte(w.Bytes())
		kv.shardConfigMap[args.ShardId].ShardStatus = Migrating
		reply.Err = OK
	}
	return
}

func (kv *ShardKV) pullConfiguration() *shardctrler.Config {
	for {
		args := &shardctrler.QueryArgs{
			ClientId:  kv.gid*1000 + kv.me,
			RequestId: atomic.AddUint64(&kv.nextRequestId, 1),
			Num:       -1,
		}
		reply := &shardctrler.QueryReply{}
		for i := 0; i < len(kv.ctrlers); i++ {
			kv.ctrlers[i].Call("ShardCtrler.Query", args, reply)
			if reply.Err == OK {
				return &reply.Config
			}
		}
		time.Sleep(time.Millisecond * 100)
	}
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

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	//总控节点，更新配置
	kv.ctrlers = ctrlers

	kv.persister = persister
	kv.opContextMap = make(map[uint64]*OpContext)
	kv.lastRequestIdMap = make(map[int]uint64)
	kv.applyCh = make(chan raft.ApplyMsg, 10)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.shardConfigMap = map[int]*ShardConfig{}
	// kv.mck = shardctrler.MakeClerk(kv.ctrlers)

	//kv.decodeSnapshot(persister.ReadSnapshot())
	//DPrintf("init kvserver, group: %v, node[%d]", gid, me)

	go kv.applyStateMachineLoop()
	go kv.checkConfigurationLoop()
	return kv
}
