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

func UniqueRequestId(clientId int, requestId uint64) uint64 {
	return uint64(clientId<<32) + requestId&0xffffffff
}
