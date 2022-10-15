package shardkv

import (
	"6.824/mr"
	"6.824/shardctrler"
	"sync/atomic"
	"time"
)

func (kv *ShardKV) updateConfigurationEventLoop() {
	canUpdateConfig := true
	defer func() {
		DPrintf("updateConfigurationEventLoop, gid[%v] node[%v] leaderId[%v], canUpdateConfig[%v] config: %v,  shardMap: %v", kv.gid, kv.me, kv.rf.LeaderId(), canUpdateConfig, mr.Any2String(kv.currentConfig), mr.Any2String(kv.shardMap))
	}()
	kv.mu.RLock()
	for _, shard := range kv.shardMap {
		if shard.ShardStatus != Serving && shard.ShardStatus != NoServing {
			canUpdateConfig = false
		}
	}
	num := kv.currentConfig.Num
	kv.mu.RUnlock()
	//前一个配置没有迁移完，不能再次更新配置
	if !canUpdateConfig {
		return
	}
	//依序号顺序更新
	conf := kv.mck.Query(num + 1)
	if conf.Num == num+1 {
		op := Op{
			ClientId:       kv.gid*1000 + kv.me,
			RequestId:      atomic.AddUint64(&kv.nextRequestId, 1),
			OpType:         OpTypeUpdateConfig,
			Value:          conf,
			StartTimestamp: time.Now().UnixMilli(),
		}
		kv.execute(op)
	}
}

func (kv *ShardKV) applyConfiguration(op *Op) *ExecuteResponse {

	conf := op.Value.(shardctrler.Config)
	defer func() {
		DPrintf("applyConfiguration, gid[%v] node[%v] leaderId[%v], currentConfig: %v, lastConfig: %v", kv.gid, kv.me, kv.rf.LeaderId(), mr.Any2String(kv.currentConfig), mr.Any2String(kv.lastConfig))
	}()
	//配置必须逐次递增的更新，不能跳跃，避免导致旧数据等奇怪问题
	if conf.Num != kv.currentConfig.Num+1 {
		return &ExecuteResponse{Err: ErrOutDated}
	}
	//此时所有分片的状态只有Serving和NoServing两种
	for shardId, gid := range conf.Shards {
		if gid == kv.gid {
			if kv.currentConfig.Shards[shardId] == 0 {
				//该分片上一次没有被分配，则本次不需要迁移
				kv.shardMap[shardId].ShardStatus = Serving
			} else if kv.shardMap[shardId].ShardStatus == NoServing {
				//该分片上次和本次都由本gid负责，不需要迁移，否则迁移
				kv.shardMap[shardId].ShardStatus = Pulling
			}
		} else {
			if kv.shardMap[shardId].ShardStatus == Serving {
				kv.shardMap[shardId].ShardStatus = BePulling
			}
		}
	}

	kv.lastConfig = kv.currentConfig
	kv.currentConfig = conf
	return &ExecuteResponse{Err: OK}
}
