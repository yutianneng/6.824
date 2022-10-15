package shardctrler

import (
	"6.824/mr"
	"6.824/raft"
	"log"
	"sort"
	"time"
)
import "6.824/labrpc"
import "sync"
import "6.824/labgob"

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	ClientId       int
	RequestId      uint64
	OpType         OpType
	Number         int
	Value          ConfigEdit
	StartTimestamp int64
}

type OpContext struct {
	ClientId        int
	RequestId       uint64
	UniqueRequestId uint64
	Op              *Op
	Term            int
	WaitCh          chan Config
}

func NewOpContext(op *Op, term int) *OpContext {
	return &OpContext{
		ClientId:        op.ClientId,
		RequestId:       op.RequestId,
		UniqueRequestId: UniqueRequestId(op.ClientId, op.RequestId),
		Op:              op,
		Term:            term,
		WaitCh:          make(chan Config, 1),
	}
}

type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	nextNumber int      //版本编号
	configs    []Config // indexed by config num

	opContextMap     map[uint64]*OpContext //用于每个请求的上下文
	lastRequestIdMap map[int]uint64        //clientId-->lastRequestId，维持幂等性，需要客户端能够保证串行
}

func (sc *ShardCtrler) Config(number int) Config {
	if number == -1 || number >= len(sc.configs)-1 {
		return sc.configs[len(sc.configs)-1]
	}
	return sc.configs[number]
}

//gids是leader当前config的gids，因为go map遍历是随机的，需要确保主从一致
func (sc *ShardCtrler) rebalanceJoin(number int, newGroups map[int][]string) *Config {

	conf := sc.configs[len(sc.configs)-1]
	//合并当前版本的raft group和新加入的group
	groups := map[int][]string{}
	gids := make([]int, 0)
	for gid, servers := range conf.Groups {
		dst := make([]string, len(servers))
		copy(dst, servers)
		groups[gid] = dst
		gids = append(gids, gid)
	}
	for gid, servers := range newGroups {
		dst := make([]string, len(servers))
		copy(dst, servers)
		groups[gid] = dst
		gids = append(gids, gid)
	}
	sort.Slice(gids, func(i, j int) bool {
		return gids[i] < gids[j]
	})
	var shards [NShards]int
	copy(shards[:], conf.Shards[:])

	for i := 0; i < len(shards); i++ {
		shards[i] = gids[i%len(gids)]
	}
	return &Config{
		Num:    number,
		Shards: shards,
		Groups: groups,
	}
}

func (sc *ShardCtrler) rebalanceLeave(number int, leaveGids []int) *Config {
	//获取这些分片，以及剩余每个group的分片数量
	conf := sc.configs[len(sc.configs)-1]
	var shards [NShards]int
	copy(shards[:], conf.Shards[:])

	groups := map[int][]string{}
	for gid, servers := range conf.Groups {
		dst := make([]string, len(servers))
		copy(dst, servers)
		groups[gid] = dst
	}
	//移除掉删除的group
	for _, gid := range leaveGids {
		delete(groups, gid)
	}
	gids := make([]int, 0)
	for gid, _ := range groups {
		gids = append(gids, gid)
	}
	sort.Slice(gids, func(i, j int) bool {
		return gids[i] < gids[j]
	})
	if len(gids) == 0 {
		shards = [NShards]int{}
	} else {
		for i := 0; i < len(shards); i++ {
			shards[i] = gids[i%len(gids)]
		}
	}
	return &Config{
		Num:    number,
		Shards: shards,
		Groups: groups,
	}

}

func (sc *ShardCtrler) rebalanceMove(number, shardId, togid int) *Config {
	//获取这些分片，以及剩余每个group的分片数量
	conf := sc.configs[len(sc.configs)-1]
	//copy分片
	var newShards [NShards]int
	copy(newShards[:], conf.Shards[:])
	newShards[shardId] = togid

	//copy group
	newGroups := map[int][]string{}
	for gid, servers := range conf.Groups {
		dst := make([]string, len(servers))
		copy(dst, servers)
		newGroups[gid] = dst
	}

	return &Config{
		Num:    number,
		Shards: newShards,
		Groups: newGroups,
	}
}
func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {

	if len(args.Servers) == 0 {
		reply.Err = ErrParamInvalid
		return
	}

	term := 0
	isLeader := false
	if term, isLeader = sc.rf.GetState(); !isLeader {
		reply.WrongLeader = true
		return
	}
	sc.mu.Lock()
	//添加group并rebalance，尽可能减少迁移
	//conf := sc.rebalanceJoin(number, args.Servers)
	//DPrintf("server Join rebalanceJoin, config: %v", mr.Any2String(conf))
	op := &Op{
		ClientId:  args.ClientId,
		RequestId: args.RequestId,
		OpType:    OpTypeJoin,
		Value: ConfigEdit{
			NewGroups: args.Servers,
		},
		StartTimestamp: time.Now().UnixMilli(),
	}
	opContext := NewOpContext(op, term)
	sc.opContextMap[opContext.UniqueRequestId] = opContext
	sc.mu.Unlock()
	defer func() {
		//DPrintf("server Get, args: %v, reply: %v", mr.Any2String(args), mr.Any2String(reply))
		sc.mu.Lock()
		delete(sc.opContextMap, opContext.UniqueRequestId)
		sc.mu.Unlock()
	}()
	if _, _, ok := sc.rf.Start(*op); !ok {
		reply.WrongLeader = true
		return
	}
	//阻塞等待
	select {
	case <-opContext.WaitCh:
		reply.Err = OK
		reply.WrongLeader = false
	case <-time.After(time.Millisecond * 1000):
		reply.Err = ErrTimeout
		reply.WrongLeader = false
	}
	//DPrintf("controller server join, args: %v, reply: %v", mr.Any2String(args), mr.Any2String(reply))

}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	if len(args.GIDs) == 0 {
		reply.Err = ErrParamInvalid
		return
	}
	term := 0
	isLeader := false
	if term, isLeader = sc.rf.GetState(); !isLeader {
		reply.WrongLeader = true
		return
	}
	op := &Op{
		ClientId:  args.ClientId,
		RequestId: args.RequestId,
		OpType:    OpTypeLeave,
		Value: ConfigEdit{
			LeaveGids: args.GIDs,
		},
		StartTimestamp: time.Now().UnixMilli(),
	}

	opContext := NewOpContext(op, term)
	sc.mu.Lock()
	sc.opContextMap[opContext.UniqueRequestId] = opContext
	sc.mu.Unlock()
	defer func() {
		//DPrintf("controller server Leave, args: %v, reply: %v", mr.Any2String(args), mr.Any2String(reply))
		sc.mu.Lock()
		delete(sc.opContextMap, opContext.UniqueRequestId)
		sc.mu.Unlock()
	}()
	if _, _, ok := sc.rf.Start(*op); !ok {
		reply.WrongLeader = true
		return
	}
	//阻塞等待
	select {
	case <-opContext.WaitCh:
		reply.Err = OK
		reply.WrongLeader = false
	case <-time.After(time.Millisecond * 1000):
		reply.Err = ErrTimeout
		reply.WrongLeader = false
	}
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {

	term := 0
	isLeader := false
	if term, isLeader = sc.rf.GetState(); !isLeader {
		reply.WrongLeader = true
		return
	}
	op := &Op{
		ClientId:  args.ClientId,
		RequestId: args.RequestId,
		OpType:    OpTypeMove,
		Value: ConfigEdit{
			ShardId: args.Shard,
			DestGid: args.GID,
		},
		StartTimestamp: time.Now().UnixMilli(),
	}
	opContext := NewOpContext(op, term)
	sc.mu.Lock()
	sc.opContextMap[opContext.UniqueRequestId] = opContext
	sc.mu.Unlock()
	defer func() {
		//DPrintf("controller server Move, args: %v, reply: %v", mr.Any2String(args), mr.Any2String(reply))
		sc.mu.Lock()
		delete(sc.opContextMap, opContext.UniqueRequestId)
		sc.mu.Unlock()
	}()
	if _, _, ok := sc.rf.Start(*op); !ok {
		reply.WrongLeader = true
		return
	}
	//阻塞等待
	select {
	case <-opContext.WaitCh:
		reply.Err = OK
		reply.WrongLeader = false
	case <-time.After(time.Millisecond * 1000):
		reply.Err = ErrTimeout
		reply.WrongLeader = false
	}
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {

	term := 0
	isLeader := false
	if term, isLeader = sc.rf.GetState(); !isLeader {
		reply.WrongLeader = true
		return
	}
	sc.mu.Lock()
	op := &Op{
		ClientId:       args.ClientId,
		RequestId:      args.RequestId,
		OpType:         OpTypeQuery,
		Number:         args.Num,
		StartTimestamp: time.Now().UnixMilli(),
	}
	opContext := NewOpContext(op, term)
	sc.opContextMap[opContext.UniqueRequestId] = opContext
	sc.mu.Unlock()

	defer func() {
		DPrintf("controller server Query, args: %v, reply: %v, configs: %v", mr.Any2String(args), mr.Any2String(reply), mr.Any2String(sc.configs))
		sc.mu.Lock()
		delete(sc.opContextMap, opContext.UniqueRequestId)
		sc.mu.Unlock()
	}()
	if _, _, ok := sc.rf.Start(*op); !ok {
		reply.WrongLeader = true
		return
	}
	//DPrintf("entering server Query, args: %v, reply: %v", mr.Any2String(args), mr.Any2String(reply))

	//阻塞等待
	select {
	case v := <-opContext.WaitCh:
		reply.Err = OK
		reply.WrongLeader = false
		reply.Config = v
	case <-time.After(time.Millisecond * 1000):
		reply.Err = ErrTimeout
		reply.WrongLeader = false
	}
}

//串行写状态机
func (sc *ShardCtrler) applyStateMachineLoop() {

	for {

		select {
		case applyMsg := <-sc.applyCh:
			if applyMsg.CommandValid {
				func() {
					sc.mu.Lock()
					defer sc.mu.Unlock()
					op := applyMsg.Command.(Op)
					//保证幂等性
					if op.RequestId <= sc.lastRequestIdMap[op.ClientId] {
						return
					}
					val := Config{}

					switch op.OpType {
					case OpTypeJoin:
						number := sc.nextNumber
						sc.nextNumber++
						//old := sc.Config(-1)
						conf := sc.rebalanceJoin(number, op.Value.NewGroups)
						sc.configs = append(sc.configs, *conf)
						sc.lastRequestIdMap[op.ClientId] = op.RequestId
						//DPrintf("controller applyLoop join, old config: %v new config: %v", mr.Any2String(old), mr.Any2String(conf))
					case OpTypeLeave:
						number := sc.nextNumber
						sc.nextNumber++
						//old := sc.Config(-1)
						conf := sc.rebalanceLeave(number, op.Value.LeaveGids)
						sc.configs = append(sc.configs, *conf)
						sc.lastRequestIdMap[op.ClientId] = op.RequestId
						//DPrintf("controller applyLoop leave, old config: %v new config: %v", mr.Any2String(old), mr.Any2String(conf))

					case OpTypeMove:
						number := sc.nextNumber
						sc.nextNumber++
						//old := sc.Config(-1)
						conf := sc.rebalanceMove(number, op.Value.ShardId, op.Value.DestGid)
						sc.configs = append(sc.configs, *conf)
						sc.lastRequestIdMap[op.ClientId] = op.RequestId
						//DPrintf("controller applyLoop move, old config: %v new config: %v", mr.Any2String(old), mr.Any2String(conf))
					case OpTypeQuery:
						//Get请求不需要更新lastRequestId
						val = sc.Config(op.Number)
					}
					//DPrintf("op: %v, config: %v, node: %v cost: %v,requestId: %v, stateMachine: %v", mr.Any2String(op), mr.Any2String(val), sc.me, time.Now().UnixMilli()-op.StartTimestamp, op.RequestId, mr.Any2String(sc.configs))
					//使得写入的client能够响应
					if c, ok := sc.opContextMap[UniqueRequestId(op.ClientId, op.RequestId)]; ok {
						c.WaitCh <- val
					}
				}()
			}
		}
	}
}

//
// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (sc *ShardCtrler) Kill() {
	sc.rf.Kill()
	// Your code here, if desired.
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me

	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	sc.applyCh = make(chan raft.ApplyMsg, 10)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)

	sc.nextNumber = 1
	sc.lastRequestIdMap = map[int]uint64{}
	sc.opContextMap = map[uint64]*OpContext{}

	go sc.applyStateMachineLoop()

	return sc
}
