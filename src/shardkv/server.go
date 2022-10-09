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

const Debug = false

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
	maxraftstate int                         // snapshot if log grows this big
	shardKVs     map[int][]*labrpc.ClientEnd //与其他分片通信的client

	dead int32 // set by Kill()

	nextRequestId uint64

	persister         *raft.Persister
	lastIncludedIndex int

	lastConfig    shardctrler.Config
	currentConfig shardctrler.Config

	shardConfigMap map[int]*ShardConfig

	opContextMap     map[uint64]*OpContext //用于每个请求的上下文
	lastRequestIdMap map[int]uint64        //clientId-->lastRequestId，维持幂等性，需要客户端能够保证串行
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {

	kv.mu.Lock()
	term, err := kv.check(OpTypeGet, args.ShardId, args.ClientId, args.RequestId)
	DPrintf("ShardKV.Get, config: %v, args: %v, reply: %v", mr.Any2String(kv.currentConfig), mr.Any2String(args), mr.Any2String(reply))

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
		reply.Err = OK
		reply.Value = c
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

	start := time.Now()
	defer func() {
		DPrintf("shardKV server PutAppend cost: %v, requestId: %d, node: %d, leaderId: %d", time.Now().Sub(start).Milliseconds(), op.RequestId, kv.me, kv.rf.LeaderId())
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
	case <-opContext.WaitCh:
		reply.Err = OK
	case <-time.After(time.Millisecond * 1000):
		reply.Err = ErrTimeout
	}
}
func (kv *ShardKV) check(opType OpType, shardId, clientId int, requestId uint64) (int, Err) {

	for kv.currentConfig.Num == 0 {
		conf := kv.pullConfiguration()
		if conf.Num > 0 {
			kv.lastConfig = kv.currentConfig
			kv.currentConfig = *conf

			for sid, gid := range conf.Shards {
				if gid == kv.gid {
					kv.shardConfigMap[sid] = &ShardConfig{
						ShardStatus: Normal,
						KvStore:     map[string]string{},
					}
				} else {
					kv.shardConfigMap[sid] = &ShardConfig{
						ShardStatus: NoResponsible,
						KvStore:     map[string]string{},
					}
				}
			}
		}
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
					DPrintf("shardKV applyStateMachineLoop, node[%d] op: %v", kv.me, mr.Any2String(op))
					//保证幂等性, 过滤掉snapshot前的日志
					if op.RequestId <= kv.lastRequestIdMap[op.ClientId] || (applyMsg.CommandIndex <= kv.lastIncludedIndex && op.OpType != OpTypeGet) {
						if c, ok := kv.opContextMap[UniqueRequestId(op.ClientId, op.RequestId)]; ok {
							c.WaitCh <- ""
						}
						return
					}
					if shard, ok := kv.shardConfigMap[op.ShardId]; !ok || shard.ShardStatus != Normal {
						DPrintf("shardKV server.applyStateMachineLoop, not apply op: %v to machine, shard: %v", mr.Any2String(op), mr.Any2String(shard))
						if c, ok1 := kv.opContextMap[UniqueRequestId(op.ClientId, op.RequestId)]; ok1 {
							c.WaitCh <- ""
						}
						return
					}
					switch op.OpType {
					case OpTypePut:
						kv.shardConfigMap[op.ShardId].KvStore[op.Key] = op.Value
						kv.lastRequestIdMap[op.ClientId] = op.RequestId
						//kv.maybeSnapshot(applyMsg.CommandIndex)
					case OpTypeAppend:
						kv.shardConfigMap[op.ShardId].KvStore[op.Key] += op.Value
						kv.lastRequestIdMap[op.ClientId] = op.RequestId
						//kv.maybeSnapshot(applyMsg.CommandIndex)
					case OpTypeGet:
					//Get请求不需要更新lastRequestId
					case OpTypeUpdateConfig:
						conf := Decode2Config([]byte(op.Value))
						if conf != nil && conf.Num > kv.currentConfig.Num {
							kv.lastConfig = kv.currentConfig
							kv.currentConfig = *conf
						}
						go kv.startMigration()

					case OpTypeMigration:
						//不重复
						r := bytes.NewBuffer([]byte(op.Value))
						d := labgob.NewDecoder(r)
						num, shardId := 0, -1
						d.Decode(&num)
						d.Decode(&shardId)
						if num < kv.currentConfig.Num {
							return
						}
						kvStore := map[string]string{}
						d.Decode(&kvStore)
						kv.shardConfigMap[op.ShardId].KvStore = kvStore
						kv.shardConfigMap[op.ShardId].ShardStatus = Normal //迁移成功
						go func() {

						}()

					}
					val := kv.shardConfigMap[op.ShardId].KvStore[op.Key]
					DPrintf("applyLoop, op: %v write to shard: %v", mr.Any2String(op), mr.Any2String(kv.shardConfigMap[op.ShardId]))
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
		//DPrintf("snapshot size: %v, stateMachine: %v", kv.persister.SnapshotSize(), mr.Any2String(kv.kvStore))
	}
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

//用于通知指定group，目前用来通知shard迁移成功
func (kv *ShardKV) Notify() {

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
			if conf.Num <= kv.currentConfig.Num {
				return
			}

			op := Op{
				ClientId:       kv.gid*1000 + kv.me,
				RequestId:      atomic.AddUint64(&kv.nextRequestId, 1),
				OpType:         OpTypeUpdateConfig,
				Value:          string(Encode2Byte(conf)),
				StartTimestamp: time.Now().UnixMilli(),
			}
			for {
				if _, _, ok := kv.rf.Start(op); ok {
					return
				}
			}
			//启动迁移
			//1 更新到其他group的客户端代理
			//kv.lastConfig = kv.currentConfig
			//kv.currentConfig = *conf
			//for gid, servers := range conf.Groups {
			//	if gid == 0 {
			//		continue
			//	}
			//	clientEnds := make([]*labrpc.ClientEnd, 0)
			//	for _, server := range servers {
			//		end := kv.make_end(server)
			//		clientEnds = append(clientEnds, end)
			//	}
			//	kv.shardKVs[gid] = clientEnds
			//}
			////2 迁移分片
			//kv.startMigration()
		}()
	}
}
func (kv *ShardKV) startMigration() {
	if kv.lastConfig.Num == 0 {
		return
	}
	flag := true
	for flag {
		flag = false
		for shardId, gid := range kv.currentConfig.Shards {
			if gid != kv.gid {
				continue
			}
			//该分片首次被分配，不需要迁移
			if kv.lastConfig.Shards[shardId] == 0 {
				flag = true
				kv.shardConfigMap[shardId].ShardStatus = Normal
				kv.shardConfigMap[shardId].KvStore = map[string]string{}
				continue
			}
			//缺少应当负责的shard，从其他group拉取
			if shard, ok := kv.shardConfigMap[shardId]; !ok || shard.ShardStatus != Normal {
				flag = true
				reply := kv.sendPullShard(kv.lastConfig.Shards[shardId], shardId)
				kv.shardConfigMap[shardId].KvStore = Decode2Map(reply.Data)
				kv.shardConfigMap[shardId].ShardStatus = Normal
				break
			}
		}
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
			if ok && reply.Err == OK {
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
	reply.Err = ErrWrongGroup
	if reqId, ok := kv.lastRequestIdMap[args.ClientId]; ok && args.RequestId <= reqId {
		return
	}
	kv.lastRequestIdMap[args.ClientId] = args.RequestId
	if shard, ok := kv.shardConfigMap[args.ShardId]; ok && shard.ShardStatus != NoResponsible {
		reply.Data = Encode2Byte(kv.shardConfigMap[args.ShardId].KvStore)
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

//
// the tester calls Kill() when a ShardKV instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *ShardKV) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *ShardKV) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

//
// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardctrler.
//
// pass ctrlers[] to shardctrler.MakeClerk() so you can send
// RPCs to the shardctrler.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use ctrlers[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
//
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
	kv.shardKVs = map[int][]*labrpc.ClientEnd{}
	// kv.mck = shardctrler.MakeClerk(kv.ctrlers)

	kv.decodeSnapshot(persister.ReadSnapshot())

	go kv.applyStateMachineLoop()
	go kv.checkConfigurationLoop()
	return kv
}
