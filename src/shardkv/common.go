package shardkv

import (
	"encoding/json"
)

//
// Sharded key/value server.
// Lots of replica groups, each running Raft.
// Shardctrler decides which group serves each shard.
// Shardctrler may change shard assignment from time to time.
//
// You will have to modify these definitions.
//
type Err string

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongGroup  = "ErrWrongGroup"
	ErrWrongLeader = "ErrWrongLeader"
	ErrPutAppend   = "ErrPutAppend"
	ErrTimeout     = "ErrTimeout"
	ErrDuplicated  = "ErrDuplicated"
	ErrNotReady    = "ErrNotReady"
	ErrOutDated    = "ErrOutDated"
)

type Status int

//每个group拥有的分片可能是以下情况

const (
	Serving   Status = 1 //拥有属于自己的分片，可正常读写
	Pulling   Status = 2 //正在迁移隶属自己的分片
	BePulling Status = 3 //拥有但不属于自己的分片，等待对方迁移
	GCing     Status = 4
	NoServing Status = 5
)

type OpType string

const (
	OpTypeGet          = "Get"
	OpTypePut          = "Put"
	OpTypeAppend       = "Append"
	OpTypeUpdateConfig = "UpdateConfig"
	OpTypeAddShard     = "AddShard"
	OpTypeDeleteShard  = "DeleteShard"
)

type Op struct {
	ClientId       int
	RequestId      uint64
	OpType         OpType
	Key            string
	Value          interface{}
	StartTimestamp int64
}

type OpContext struct {
	ClientId        int
	RequestId       uint64
	UniqueRequestId uint64
	Op              *Op
	Term            int
	WaitCh          chan *ExecuteResponse
}
type ExecuteResponse struct {
	Err   Err
	Value interface{}
}
type Shard struct {
	ShardStatus      Status
	KvStore          map[string]string
	LastRequestIdMap map[int]uint64 //去重
}

func (s *Shard) String() string {
	bytes, _ := json.Marshal(s)
	return string(bytes)
}
func (s *Shard) deepCopy() *Shard {
	shard := &Shard{
		ShardStatus: s.ShardStatus,
	}
	lastRequestIdMap := map[int]uint64{}
	for k, v := range s.LastRequestIdMap {
		lastRequestIdMap[k] = v
	}
	shard.LastRequestIdMap = lastRequestIdMap
	kvStore := map[string]string{}
	for k, v := range s.KvStore {
		kvStore[k] = v
	}
	shard.KvStore = kvStore
	return shard
}

// Put or Append
type PutAppendArgs struct {
	ClientId  int
	RequestId uint64
	Key       string
	Value     string
	Op        string // "Put" or "Append"
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	ClientId  int
	RequestId uint64
	Key       string
}

type GetReply struct {
	Err   Err
	Value string
}

type PullShardArgs struct {
	Num      int
	ShardIds []int
}

type PullShardReply struct {
	Err      Err
	Num      int
	ShardMap map[int]*Shard
}

type DeleteShardArgs struct {
	Num      int
	ShardIds []int
}
type DeleteShardReply struct {
	Err Err
}

func UniqueRequestId(clientId int, requestId uint64) uint64 {
	return uint64(clientId<<32) + requestId&0xffffffff
}
