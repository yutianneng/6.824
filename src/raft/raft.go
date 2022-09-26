package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"6.824/labgob"
	"6.824/mr"
	"bytes"
	"math/rand"
	"sort"
	"time"

	//	"bytes"
	"sync"
	"sync/atomic"

	//	"6.824/labgob"
	"6.824/labrpc"
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
	CommandTerm  int

	//For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()
	nPeers    int

	timeoutInterval time.Duration //follower leader candidate
	lastActiveTime  time.Time     //超时开始计算时间，收到心跳时会更新

	//选举
	term     int
	role     MemberRole
	leaderId int
	votedFor int

	//提交情况
	//log         *RaftLog
	logs        []*LogEntry
	nextIndex   []int
	matchIndex  []int
	commitIndex int
	lastApplied int

	//状态机
	applyCond *sync.Cond
	applyChan chan ApplyMsg
}

type MemberRole int

const (
	Leader    MemberRole = 1
	Follower  MemberRole = 2
	Candidate MemberRole = 3

	RoleNone = -1
	None     = 0
)

func (m MemberRole) String() string {
	switch m {
	case Leader:
		return "Leader"
	case Follower:
		return "Follower"
	case Candidate:
		return "Candidate"
	}
	return "Unknown"
}

type LogType int

const (
	HeartBeatLogType   LogType = 1
	AppendEntryLogType LogType = 2
)

func (l LogType) String() string {
	switch l {
	case HeartBeatLogType:
		return "HeartBeatLogType"
	case AppendEntryLogType:
		return "AppendEntryLogType"
	}
	return "Unknown"
}

type LogEntry struct {
	LogTerm int
	Command interface{}
}

const (
	ElectionTimeout = 200 * time.Millisecond
	HeatBeatTimeout = 150 * time.Millisecond
)

//选举时需要传递自己拥有的最后一条log的term和index
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

//
// 心跳或者日志追加
//
type AppendEntriesArgs struct {
	LogType  LogType
	LeaderId int
	Term     int //leader currentTerm
	//用于日志复制，确保前面日志能够匹配
	PrevLogTerm         int
	PrevLogIndex        int
	LeaderCommitedIndex int
	LogEntries          []*LogEntry
}

//
// 心跳或者日志追加
//
type AppendEntriesReply struct {
	Success bool
	Term    int
	//用于探测日志匹配点
	NextIndex int
	Msg       string
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	return rf.term, rf.role == Leader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
//外层加锁，内层不能够再加锁了
func (rf *Raft) persist() {
	// Your code here (2C).
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	//持久化当前term以及是否给其他结点投过票，避免同一个term多次投票的情况
	e.Encode(rf.term)
	e.Encode(rf.votedFor)
	e.Encode(rf.leaderId)
	e.Encode(rf.logs)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
//一般刚刚启动时执行
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	rf.mu.Lock()
	d.Decode(&rf.term)
	d.Decode(&rf.votedFor)
	d.Decode(&rf.leaderId)
	d.Decode(&rf.logs)
	rf.mu.Unlock()
}
func (rf *Raft) lastLogTermAndLastLogIndex() (int, int) {
	logIndex := len(rf.logs) - 1
	logTerm := rf.logs[logIndex].LogTerm
	return logTerm, logIndex
}

func (rf *Raft) lastLogIndex() int {
	return len(rf.logs) - 1
}
func (rf *Raft) logTerm(logIndex int) int {
	return rf.logs[logIndex].LogTerm
}

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer func() {
		DPrintf("node[%d] role[%v] received vote from node[%d], now[%d], args: %v, reply: %v", rf.me, rf.role, args.CandidateId, time.Now().UnixMilli(), mr.Any2String(args), mr.Any2String(reply))
	}()
	defer rf.mu.Unlock()
	reply.Term = rf.term
	reply.VoteGranted = false
	//不接收小于自己term的请求
	if rf.term > args.Term {
		return
	}

	if args.Term > rf.term {
		rf.role = Follower //leader转换为follower
		rf.term = args.Term
		//需要比较最新一条日志的情况再决定要不要投票
		rf.votedFor = RoleNone
		rf.leaderId = RoleNone
		rf.persist()
	}
	//避免重复投票
	if rf.votedFor == RoleNone || rf.votedFor == args.CandidateId {
		lastLogTerm, lastLogIndex := rf.lastLogTermAndLastLogIndex()
		//最后一条日志任期更大或者任期一样但是更长
		if args.LastLogTerm > lastLogTerm || (lastLogTerm == args.LastLogTerm && lastLogIndex <= args.LastLogIndex) {
			rf.role = Follower
			rf.votedFor = args.CandidateId
			rf.leaderId = args.CandidateId
			rf.lastActiveTime = time.Now()
			rf.timeoutInterval = randElectionTimeout()
			reply.VoteGranted = true
			rf.persist()
		}
	}
}

//如果收到term比自己大的AppendEntries请求，则表示发生过新一轮的选举，此时拒绝掉，等待超时选举
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {

	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer func() {
		DPrintf("id[%d] role[%v] args: %v, reply: %v", rf.me, rf.role, mr.Any2String(args), mr.Any2String(reply))
	}()

	reply.Term = rf.term
	reply.Success = false
	//拒绝旧leader请求
	if args.Term < rf.term {
		return
	}
	//发现一个更大的任期，转变成这个term的follower，leader、follower--> follower
	if args.Term > rf.term {
		rf.term = args.Term
		rf.role = Follower
		//发现term大于等于自己的日志复制请求，则认其为主
		rf.votedFor = RoleNone
		rf.leaderId = RoleNone
		rf.persist()
	}
	rf.leaderId = args.LeaderId
	rf.votedFor = args.LeaderId
	rf.lastActiveTime = time.Now()
	//还缺少前面的日志或者前一条日志匹配不上
	if args.PrevLogIndex > rf.lastLogIndex() {
		reply.NextIndex = rf.lastLogIndex()
		return
	}
	//前一条日志的任期不匹配，找到冲突term首次出现的地方
	if args.PrevLogTerm != rf.logTerm(args.PrevLogIndex) {
		index := args.PrevLogIndex
		term := rf.logTerm(index)
		for ; index > 0 && rf.logTerm(index) == term; index-- {
		}
		reply.NextIndex = index
		return
	}
	//args.PrevLogIndex<=lastLogIndex，有可能发生截断的情况
	if rf.lastLogIndex() > args.PrevLogIndex {
		rf.logs = rf.logs[:args.PrevLogIndex+1]
	}
	rf.logs = append(rf.logs, args.LogEntries...)
	if args.LeaderCommitedIndex > rf.commitIndex {
		rf.commitIndex = args.LeaderCommitedIndex
		if rf.lastLogIndex() < rf.commitIndex {
			rf.commitIndex = rf.lastLogIndex()
		}
	}
	rf.matchIndex[rf.me] = rf.lastLogIndex()
	rf.nextIndex[rf.me] = rf.matchIndex[rf.me] + 1
	rf.persist()
	reply.Success = true
	reply.NextIndex = rf.nextIndex[rf.me]
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) heartBeatLoop() {
	for rf.killed() == false {
		//改成10ms一次就通过了？
		time.Sleep(time.Millisecond * 20)
		func() {
			rf.mu.Lock()
			defer rf.mu.Unlock()
			if rf.role != Leader {
				return
			}
			//如果没有超时或者没有需要发送的数据，则直接返回
			if time.Now().Sub(rf.lastActiveTime) < HeatBeatTimeout-50 {
				return
			}
			rf.lastActiveTime = time.Now()
			for i := 0; i < rf.nPeers; i++ {
				if rf.me == i {
					rf.matchIndex[i] = rf.lastLogIndex()
					rf.nextIndex[i] = rf.matchIndex[i] + 1
					continue
				}
				//DPrintf("lastLogIndex: %v, logs: %v", lastLogIndex, mr.Any2String(rf.logs))
				//记录每个node本次发送日志的前一条日志
				prevLogIndex := rf.matchIndex[i]
				if prevLogIndex > rf.lastLogIndex() {
					prevLogIndex = rf.lastLogIndex()
				}
				//有可能follower的matchIndex比leader还大，此时要担心是否越界
				//fmt.Printf("node[%d] role[%v] term[%d] lastLogIndex[%d] matchIndex[%d], log: %v\n", rf.me, rf.role, rf.term, rf.lastLogIndex(), rf.matchIndex[i], mr.Any2String(rf.logs))
				//fmt.Printf("node[%d] role[%v] term[%d] matchIndex: %v\n", rf.me, rf.role, rf.term, mr.Any2String(rf.matchIndex))
				argsI := &AppendEntriesArgs{
					LogType:             HeartBeatLogType,
					Term:                rf.term,
					LeaderId:            rf.me,
					PrevLogIndex:        prevLogIndex,
					PrevLogTerm:         rf.logTerm(prevLogIndex),
					LeaderCommitedIndex: rf.commitIndex, //对上一次日志复制请求的二阶段
				}

				//本次复制的最后一条日志
				lastLogIndex := rf.lastLogIndex()
				if rf.matchIndex[i] < lastLogIndex {
					argsI.LogType = AppendEntryLogType
					argsI.LogEntries = make([]*LogEntry, 0)
					//因为此时没有加锁，担心有新日志写入，必须保证每个节点复制的最后一条日志一样才能起到过半提交的效果
					argsI.LogEntries = append(argsI.LogEntries, rf.logs[rf.nextIndex[i]:]...)
				}

				go func(server int, args *AppendEntriesArgs) {
					reply := &AppendEntriesReply{}
					ok := rf.sendAppendEntries(server, args, reply)
					if !ok {
						return
					}
					rf.mu.Lock()
					defer rf.mu.Unlock()
					//如果term变了，表示该结点不再是leader，什么也不做
					if rf.term != args.Term {
						return
					}
					//发现更大的term，本结点是旧leader
					if reply.Term > rf.term {
						rf.term = reply.Term
						rf.votedFor = RoleNone
						rf.leaderId = RoleNone
						rf.role = Follower
						rf.persist()
						return
					}
					if reply.Success {
						rf.nextIndex[server] = reply.NextIndex
						rf.matchIndex[server] = rf.nextIndex[server] - 1
						//提交到哪个位置需要根据中位数来判断，中位数表示过半提交的日志位置，
						//每次提交日志向各结点发送的日志并不完全一样，不能光靠是否发送成功来判断
						matchIndexSlice := make([]int, rf.nPeers)
						for index, matchIndex := range rf.matchIndex {
							matchIndexSlice[index] = matchIndex
						}
						sort.Slice(matchIndexSlice, func(i, j int) bool {
							return matchIndexSlice[i] < matchIndexSlice[j]
						})
						//fmt.Printf("matchIndexSlice: %v, newcommitIndex: %v, lastLogIndex: %v\n", mr.Any2String(matchIndexSlice), matchIndexSlice[rf.nPeers/2], rf.lastLogIndex())
						newCommitIndex := matchIndexSlice[rf.nPeers/2]
						//不能提交不属于当前term的日志
						if newCommitIndex > rf.commitIndex && rf.logs[newCommitIndex].LogTerm == rf.term {
							DPrintf("id[%d] role[%v] commitIndex %v update to newcommitIndex %v, command: %v", rf.me, rf.role, rf.commitIndex, newCommitIndex, rf.logs[newCommitIndex])
							//如果commitIndex比自己实际的日志长度还大，这时需要减小
							if newCommitIndex > rf.lastLogIndex() {
								rf.commitIndex = rf.lastLogIndex()
							} else {
								rf.commitIndex = newCommitIndex
							}
						}
					} else {
						//follower缺少的之前的日志，探测缺少的位置
						//后退策略，可以按term探测，也可以二分，此处采用线性探测，简单一些
						rf.nextIndex[server] = reply.NextIndex
						rf.matchIndex[server] = reply.NextIndex - 1
					}
				}(i, argsI)
			}
		}()
	}
}

func (rf *Raft) applyLogLoop(applyCh chan ApplyMsg) {

	for !rf.killed() {
		time.Sleep(time.Millisecond * 10)
		applyMsgs := make([]ApplyMsg, 0)
		func() {
			rf.mu.Lock()
			defer rf.mu.Unlock()

			//没有数据需要上传给应用层
			if rf.lastApplied >= rf.commitIndex {
				return
			}
			for rf.lastApplied < rf.commitIndex {
				rf.lastApplied++
				applyMsgs = append(applyMsgs, ApplyMsg{
					CommandValid: true,
					Command:      rf.logs[rf.lastApplied].Command,
					CommandIndex: rf.lastApplied,
				})
			}
		}()
		go func() {
			//锁外提交给应用
			for i := 0; i < len(applyMsgs); i++ {
				DPrintf("id[%v] role[%v] upload log to application, lastApplied[%d], commitIndex[%d]", rf.me, rf.role, applyMsgs[i].CommandIndex, rf.commitIndex)

				applyCh <- applyMsgs[i]
			}
		}()
	}
}
func (rf *Raft) electionLoop() {
	for rf.killed() == false {
		time.Sleep(time.Millisecond * 1)
		func() {
			rf.mu.Lock()
			defer rf.mu.Unlock()

			if rf.role == Leader {
				return
			}
			if time.Now().Sub(rf.lastActiveTime) < rf.timeoutInterval {
				//不超时不需要进入下一步，只需要接收RequestVote和AppendEntries请求即可
				return
			}
			//超时处理逻辑
			if rf.role == Follower {
				rf.role = Candidate
			}
			DPrintf("become candidate... node[%v] term[%v] role[%v] lastActiveTime[%v], timeoutInterval[%d], now[%v]", rf.me, rf.term, rf.role, rf.lastActiveTime.UnixMilli(), rf.timeoutInterval.Milliseconds(), time.Now().Sub(rf.lastActiveTime).Milliseconds())
			rf.lastActiveTime = time.Now()
			rf.timeoutInterval = randElectionTimeout()
			rf.votedFor = rf.me
			rf.term++
			rf.persist()
			lastLogTerm, lastLogIndex := rf.lastLogTermAndLastLogIndex()
			rf.mu.Unlock()

			maxTerm, voteGranted := rf.becomeCandidate(lastLogIndex, lastLogTerm)
			rf.mu.Lock()
			//DPrintf("node[%d] get vote num[%d]", rf.me, totalVote)

			//在这过程中接收到更大term的请求，导致退化为follower
			if rf.role != Candidate {
				DPrintf("node[%d] role[%v] failed to leader, voteGranted[%d]", rf.me, rf.role, voteGranted)
				return
			}
			if maxTerm > rf.term {
				rf.role = Follower
				rf.term = maxTerm
				rf.votedFor = RoleNone
				rf.leaderId = RoleNone
				rf.persist()
			} else if voteGranted > rf.nPeers/2 {
				rf.leaderId = rf.me
				rf.role = Leader
				rf.lastActiveTime = time.Unix(0, 0)
				rf.persist()
			}
			DPrintf("node[%d] role[%v] maxTerm[%d] voteGranted[%d] nPeers[%d]", rf.me, rf.role, maxTerm, voteGranted, rf.nPeers)
		}()
	}
}

func (rf *Raft) becomeCandidate(lastLogIndex, lastLogTerm int) (int, int) {

	type RequestVoteResult struct {
		peerId int
		resp   *RequestVoteReply
	}
	voteChan := make(chan *RequestVoteResult, rf.nPeers-1)
	args := &RequestVoteArgs{
		Term:         rf.term,
		CandidateId:  rf.me,
		LastLogIndex: lastLogIndex,
		LastLogTerm:  lastLogTerm,
	}
	for i := 0; i < rf.nPeers; i++ {
		if rf.me == i {
			continue
		}
		go func(server int, args *RequestVoteArgs) {
			reply := &RequestVoteReply{}
			ok := rf.sendRequestVote(server, args, reply)
			if ok {
				voteChan <- &RequestVoteResult{
					peerId: server,
					resp:   reply,
				}
			} else {
				voteChan <- &RequestVoteResult{
					peerId: server,
					resp:   nil,
				}
			}
		}(i, args)
	}

	maxTerm := rf.term
	voteGranted := 1
	totalVote := 1
	for i := 0; i < rf.nPeers-1; i++ {
		select {
		case vote := <-voteChan:
			totalVote++
			if vote.resp != nil {
				if vote.resp.VoteGranted {
					voteGranted++
				}
				//出现更大term就退回follower
				if vote.resp.Term > maxTerm {
					maxTerm = vote.resp.Term
				}
			}
		}
		if voteGranted > rf.nPeers/2 || totalVote == rf.nPeers {
			return maxTerm, voteGranted
		}
	}
	return maxTerm, voteGranted
}

//写入数据
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.role != Leader {
		return -1, -1, false
	}
	entry := &LogEntry{
		LogTerm: rf.term,
		Command: command,
	}
	rf.logs = append(rf.logs, entry)
	index := rf.lastLogIndex()
	term := rf.term
	//写入后立刻持久化
	rf.persist()
	DPrintf("node[%d] term[%d] role[%v] add entry: %v, logIndex[%d]", rf.me, rf.term, rf.role, mr.Any2String(entry), index)
	return index, term, true
}

//初始化raft, 所有raft的任务都要另起协程，测试文件采用的是协程模拟rpc
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {

	rf := &Raft{
		mu:        sync.Mutex{},
		peers:     peers,
		persister: persister,
		me:        me,
		dead:      -1,
		nPeers:    len(peers),

		leaderId:       RoleNone,
		term:           None,
		votedFor:       RoleNone,
		role:           Follower,
		lastActiveTime: time.Now(),
		//lastHeartBeatTime: time.Now(),
		timeoutInterval: randElectionTimeout(),
		commitIndex:     None,
		lastApplied:     None,
		applyChan:       applyCh,
	}
	rf.applyCond = sync.NewCond(&rf.mu)
	DPrintf("starting new raft node, id[%d], lastActiveTime[%v], timeoutInterval[%d]", me, rf.lastActiveTime.UnixMilli(), rf.timeoutInterval.Milliseconds())

	rf.logs = make([]*LogEntry, 0)
	rf.nextIndex = make([]int, rf.nPeers)
	rf.matchIndex = make([]int, rf.nPeers)
	for i := 0; i < rf.nPeers; i++ {
		rf.nextIndex[i] = 1
		rf.matchIndex[i] = 0
	}
	rf.logs = append(rf.logs, &LogEntry{
		LogTerm: 0,
	})

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	// start ticker goroutine to start elections
	go rf.electionLoop()
	go rf.heartBeatLoop()
	go rf.applyLogLoop(applyCh)

	DPrintf("starting raft node[%d]", rf.me)

	return rf
}

func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}
func randElectionTimeout() time.Duration {
	return ElectionTimeout + time.Duration(rand.Uint32())%ElectionTimeout
}

func heartBeatTimeout() time.Duration {
	return HeatBeatTimeout
}
