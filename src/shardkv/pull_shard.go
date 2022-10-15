package shardkv

import (
	"sync"
	"time"
)

func (kv *ShardKV) pullShardEventLoop() {
	kv.mu.RLock()
	gid2ShardId := kv.getShardIdsByStatus(Pulling)
	//DPrintf("pullShardEventLoop, gid: %v, node: %v, leaderId: %v,  gid2ShardId: %v, shardMap: %v", kv.gid, kv.me, kv.rf.LeaderId(), mr.Any2String(gid2ShardId), mr.Any2String(kv.shardMap))
	wg := &sync.WaitGroup{}
	for gid, shardIds := range gid2ShardId {
		wg.Add(1)
		go func(servers []string, configNum int, shardIds []int) {
			defer wg.Done()
			args := &PullShardArgs{
				Num:      configNum,
				ShardIds: shardIds,
			}
			for _, server := range servers {
				reply := &PullShardReply{}
				srv := kv.make_end(server)
				//拉取分片成功后就写入raft
				if srv.Call("ShardKV.PullShard", args, reply) && reply.Err == OK {
					op := Op{
						OpType:         OpTypeAddShard,
						Value:          *reply,
						StartTimestamp: time.Now().UnixMilli(),
					}
					kv.execute(op)
					break
				}
			}
		}(kv.lastConfig.Groups[gid], kv.currentConfig.Num, shardIds)
	}
	kv.mu.RUnlock()
	wg.Wait()
}

//如果存在该分片，则传输分片
func (kv *ShardKV) PullShard(args *PullShardArgs, reply *PullShardReply) {

	defer func() {
		//DPrintf("shardKV PullShard, gid: %v, node: %v,  args: %v, reply: %v, currentConfig.Num: %v, args.Num: %v, shardMap: %v", kv.gid, kv.me, mr.Any2String(args), mr.Any2String(reply), kv.currentConfig.Num, args.Num, mr.Any2String(kv.shardMap))
	}()
	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	kv.mu.RLock()
	defer kv.mu.RUnlock()
	//configNum逐次递增，每个配置更新完成表示该group只有Serving和NoServing状态的shard
	//如果本group的configNum>args.Num，表示本group一定没有对方的shard
	//如果本group的configNum<args.Num，那么需要等待本group的config跟上来才行，避免迁移到旧shard
	if kv.currentConfig.Num != args.Num {
		reply.Err = ErrNotReady
		return
	}
	shardMap := map[int]*Shard{}
	for _, shardId := range args.ShardIds {
		shardMap[shardId] = kv.shardMap[shardId].deepCopy()
	}
	reply.ShardMap = shardMap
	reply.Num = kv.currentConfig.Num
	reply.Err = OK
}
func (kv *ShardKV) applyAddShard(op *Op) *ExecuteResponse {
	reply := op.Value.(PullShardReply)
	//DPrintf("applyAddShard, gid: %v, node: %v, shard: %v", kv.gid, kv.me, mr.Any2String(reply))
	if reply.Num == kv.currentConfig.Num {
		for shardId, shard := range reply.ShardMap {
			if kv.shardMap[shardId].ShardStatus == Pulling {
				kv.shardMap[shardId] = shard.deepCopy()
				kv.shardMap[shardId].ShardStatus = GCing //对方可以删除这个分片了
			} else {
				//DPrintf("gid[%d] node[%d] duplicated insert shard: %v", kv.gid, kv.me, mr.Any2String(shard))
				break
			}
		}
		return &ExecuteResponse{Err: OK}
	}
	return &ExecuteResponse{Err: ErrOutDated}
}
