package shardkv

import (
	"6.824/mr"
	"sync"
	"sync/atomic"
	"time"
)

func (kv *ShardKV) gcShardEventLoop() {

	kv.mu.RLock()
	gid2ShardIds := kv.getShardIdsByStatus(GCing)
	defer func() {
		DPrintf("getShardIdsByStatus, gid[%d] node[%d] num[%d] status: %v, gid2ShardIds: %v, shardMap: %v", kv.gid, kv.me, kv.currentConfig.Num, GCing, mr.Any2String(gid2ShardIds), mr.Any2String(kv.shardMap))
	}()
	wg := &sync.WaitGroup{}
	for gid, shardIds := range gid2ShardIds {
		wg.Add(1)
		go func(servers []string, num int, shardIds []int) {
			defer wg.Done()
			args := DeleteShardArgs{
				Num:      num,
				ShardIds: shardIds,
			}
			for _, server := range servers {
				reply := &DeleteShardReply{}
				srv := kv.make_end(server)
				if srv.Call("ShardKV.DeleteShard", &args, reply) && reply.Err == OK {
					op := Op{
						OpType:         OpTypeDeleteShard,
						Value:          args,
						StartTimestamp: time.Now().UnixMilli(),
					}
					kv.execute(op)
					break
				}
				//DPrintf("gcShardEventLoop, gid[%d] node[%d] num[%d] status: %v, gid2ShardIds: %v, reply: %v", kv.gid, kv.me, kv.currentConfig.Num, GCing, mr.Any2String(gid2ShardIds), mr.Any2String(reply))

			}
		}(kv.lastConfig.Groups[gid], kv.currentConfig.Num, shardIds)
	}
	kv.mu.RUnlock()
	wg.Wait()
}

//groupA从groupB拉取了一个shard，然后groupA会不断的通知groupB这个shard可以删除
//groupB收到后就会写入到raft，在apply时删除这个分片的数据，再通知groupA成功
//groupA收到反馈后就会更新GCing-->Serving

func (kv *ShardKV) DeleteShard(args *DeleteShardArgs, reply *DeleteShardReply) {

	term, isLeader := kv.rf.GetState()
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	defer func() {
		//DPrintf("DeleteShard, gid[%d] node[%d], num: %v, args: %v, reply: %v, config: %v", kv.gid, kv.me, kv.currentConfig.Num, mr.Any2String(args), mr.Any2String(reply), mr.Any2String(kv.currentConfig))
	}()
	kv.mu.Lock()
	//过期请求也算删除成功
	if kv.currentConfig.Num > args.Num {
		reply.Err = OK
		defer kv.mu.Unlock()
		return
	}

	op := Op{
		ClientId:       kv.gid*1000 + kv.me,
		RequestId:      atomic.AddUint64(&kv.nextRequestId, 1),
		OpType:         OpTypeDeleteShard,
		Value:          *args,
		StartTimestamp: time.Now().UnixMilli(),
	}
	opContext := NewOpContext(&op, term)
	kv.opContextMap[UniqueRequestId(op.ClientId, op.RequestId)] = opContext
	kv.mu.Unlock()

	defer func() {
		kv.mu.Lock()
		delete(kv.opContextMap, UniqueRequestId(op.ClientId, op.RequestId))
		kv.mu.Unlock()
	}()
	err := kv.execute(op)
	if err != OK {
		reply.Err = err
		return
	}
	select {
	case resp := <-opContext.WaitCh:
		reply.Err = resp.Err
	case <-time.After(time.Second):
		reply.Err = ErrTimeout
	}
}

func (kv *ShardKV) applyDeleteShard(op *Op) *ExecuteResponse {
	defer func() {
		//DPrintf("applyDeleteShard, gid[%d] node[%d], op: %v, config: %v", kv.gid, kv.me, mr.Any2String(op), mr.Any2String(kv.currentConfig))
	}()
	args := op.Value.(DeleteShardArgs)
	num, shardIds := args.Num, args.ShardIds
	if num == kv.currentConfig.Num {
		for _, shardId := range shardIds {
			shard := kv.shardMap[shardId]
			if shard.ShardStatus == GCing {
				shard.ShardStatus = Serving
			} else if shard.ShardStatus == BePulling {
				kv.shardMap[shardId] = &Shard{
					ShardStatus:      NoServing,
					KvStore:          map[string]string{},
					LastRequestIdMap: map[int]uint64{},
				}
			} else {
				break
			}
		}
	}
	return &ExecuteResponse{Err: OK}
}
