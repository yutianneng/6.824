package shardkv

import (
	"6.824/shardctrler"
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
	OK                 = "OK"
	ErrNoKey           = "ErrNoKey"
	ErrWrongGroup      = "ErrWrongGroup"
	ErrWrongLeader     = "ErrWrongLeader"
	ErrPutAppend       = "ErrPutAppend"
	ErrTimeout         = "ErrTimeout"
	ErrDuplicated      = "ErrDuplicated"
	ErrShardNotArrived = "ErrShardNotArrived"
)

type status int

const (
	Normal              status = 1 //正常可读写
	Migrating           status = 2 //迁移中，暂时不可读写
	Migrated            status = 3 //已迁移走了
	Waiting             status = 4 //等待迁移
	NoResponsible       status = 5 //不是自己负责的shard
	WaitingToBeMigrated status = 6
)

type ShardConfig struct {
	Num         int
	ShardStatus status
	KvStore     map[string]string
}

func (s *ShardConfig) String() string {
	bytes, _ := json.Marshal(s)
	return string(bytes)
}

// Put or Append
type PutAppendArgs struct {
	ClientId  int
	RequestId uint64
	Key       string
	Value     string
	ShardId   int
	Op        string // "Put" or "Append"
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	ClientId  int
	RequestId uint64
	Key       string
	ShardId   int
}

type GetReply struct {
	Err   Err
	Value string
}

type PullShardArgs struct {
	ClientId  int
	RequestId uint64
	Num       int
	ShardId   int
}

type PullShardReply struct {
	Err  Err
	Data []byte
}

func UniqueRequestId(clientId int, requestId uint64) uint64 {
	return uint64(clientId<<32) + requestId&0xffffffff
}

func Decode2Map(data []byte) map[string]string {
	m := map[string]string{}
	json.Unmarshal(data, &m)
	return m
}
func Decode2Config(data []byte) *shardctrler.Config {
	conf := &shardctrler.Config{}
	json.Unmarshal(data, conf)
	return conf
}
func Encode2Byte(m interface{}) []byte {
	bytes, _ := json.Marshal(m)
	return bytes
}
