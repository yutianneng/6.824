package shardctrler

//
// Shard controler: assigns shards to replication groups.
//
// RPC interface:
// Join(servers) -- add a set of groups (gid -> server-list mapping).
// Leave(gids) -- delete a set of groups.
// Move(shard, gid) -- hand off one shard from current owner to gid.
// Query(num) -> fetch Config # num, or latest config if num==-1.
//
// A Config (configuration) describes a set of replica groups, and the
// replica group responsible for each shard. Configs are numbered. Config
// #0 is the initial configuration, with no groups and all shards
// assigned to group 0 (the invalid group).
//
// You will need to add fields to the RPC argument structs.
//

// The number of shards.
const NShards = 10

// A configuration -- an assignment of shards to groups.
// Please don't change this.
type Config struct {
	Num    int              // config number
	Shards [NShards]int     // shard -> gid
	Groups map[int][]string // gid -> servers[]
}

func DefaultConfig() Config {
	return Config{
		Num:    0,
		Shards: [10]int{},
		Groups: map[int][]string{},
	}
}

type OpType string

const (
	OpTypeJoin  = "Join"
	OpTypeLeave = "Leave"
	OpTypeMove  = "Move"
	OpTypeQuery = "Query"
)

type ConfigEdit struct {
	NewGroups map[int][]string //Join
	LeaveGids []int            //Leave

	//Move
	ShardId int
	DestGid int
}

const (
	OK              = "OK"
	ErrParamInvalid = "ErrParamInvalid"
	ErrTimeout      = "ErrTimeout"
)

type Err string

type JoinArgs struct {
	ClientId  int
	RequestId uint64
	Servers   map[int][]string // new GID -> servers mappings
}

type JoinReply struct {
	WrongLeader bool
	Err         Err
}

type LeaveArgs struct {
	ClientId  int
	RequestId uint64
	GIDs      []int
}

type LeaveReply struct {
	WrongLeader bool
	Err         Err
}

type MoveArgs struct {
	ClientId  int
	RequestId uint64
	Shard     int
	GID       int
}

type MoveReply struct {
	WrongLeader bool
	Err         Err
}

type QueryArgs struct {
	ClientId  int
	RequestId uint64
	Num       int // desired config number
}

type QueryReply struct {
	WrongLeader bool
	Err         Err
	Config      Config
}

func UniqueRequestId(clientId int, requestId uint64) uint64 {
	return uint64(clientId<<32) + requestId&0xffffffff
}
