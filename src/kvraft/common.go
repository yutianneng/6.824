package kvraft

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
	ErrPutAppend   = "ErrPutAppend"
	ErrTimeout     = "ErrTimeout"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	UniqueRequestId uint64
	Key             string
	Value           string
	Op              string // "Put" or "Append"
}

type PutAppendReply struct {
	Err      Err
	LeaderId int
}

type GetArgs struct {
	UniqueRequestId uint64
	Key             string
}

type GetReply struct {
	Err      Err
	Value    string
	LeaderId int
}

func UniqueRequestId(clientId int, requestId uint64) uint64 {
	return uint64(clientId<<32) + requestId&0xffffffff
}
