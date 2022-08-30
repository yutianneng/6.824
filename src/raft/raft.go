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
	"6.824/mr"
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

	// For 2D:
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

	lastHeartBeatTime time.Time
	timeoutInterval   time.Duration //follower leader candidate
	lastActiveTime    time.Time     //超时开始计算时间，收到心跳时会更新

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

	appendEntriesChan chan AppendEntriesStruct
	//状态机
	applyCond *sync.Cond
	applyChan chan ApplyMsg
}

type AppendEntriesStruct struct {
	Type LogType
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

	DetectMatchIndexLogType LogType = 5
)

type LogEntry struct {
	LogTerm  int
	LogIndex int
	Command  interface{}
}

//
//type RaftLog struct {
//	//term和index从1开始，0不用，第一个log我们做为dummy，避免判空
//	Entries []*LogEntry
//
//	NextIndexs  []int
//	MatchIndexs []int
//}

const (
	ElectionTimeout = 250 * time.Millisecond
	HeatBeatTimeout = 100 * time.Millisecond
)

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
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
	Type     LogType
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
	Msg     string
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	return int(rf.term), rf.role == Leader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

func (rf *Raft) lastLogTermAndLastLogIndex() (int, int) {
	if len(rf.logs) == 0 {
		return 0, 0
	}
	logTerm := rf.logs[len(rf.logs)-1].LogTerm
	logIndex := rf.logs[len(rf.logs)-1].LogIndex
	return logTerm, logIndex
}

func (rf *Raft) lastLogIndex() int {
	if len(rf.logs) == 0 {
		return 0
	}
	return rf.logs[len(rf.logs)-1].LogIndex
}
func (rf *Raft) logTerm(logIndex int) int {
	return rf.logs[logIndex].LogTerm
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
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

//副本刚投票，结果收到更大的term的投票请求怎么办
//
// 处理投票请求，决定是否投票
//
//1. leader只有收到更高任期的请求时才会退化为follower，如果被隔离，那么它会一直认为自己是leader
//2. follower在收到大于自己term的请求时需要更新自己的term，避免旧leader的干扰
//3. 收到有效的请求时才将自己的超时计时器重置
//4. 每个term只会有一个leader
//5. 某个突然网络连通的结点进行选举，而且term和当前leader一样，此时需要判断voteFor，不要重复投票
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).

	defer func() {
		//DPrintf("args: %v, reply: %v", mr.Any2String(args), mr.Any2String(reply))
	}()
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.term
	reply.VoteGranted = false
	//DPrintf("id: %d received RequestVote, from id: %d term: %d to id: %d term: %d, role: %v voteFor: %v",
	//	rf.me, args.CandidateId, args.Term, rf.me, rf.term, rf.role, rf.votedFor)
	//不接收小于自己term的请求
	if rf.term > args.Term {
		return
	}
	if args.Term > rf.term {
		rf.role = Follower //leader转换为follower
		rf.term = args.Term
		rf.votedFor = -1
		rf.leaderId = -1
	}
	//避免重复投票
	if rf.votedFor == RoleNone || rf.votedFor == args.CandidateId {
		lastLogTerm, lastLogIndex := rf.lastLogTermAndLastLogIndex()
		//最后一条日志任期更大或者任期一样但是更长
		if args.LastLogTerm > rf.term || (lastLogIndex <= args.LastLogIndex && lastLogTerm <= args.LastLogTerm) {
			rf.votedFor = args.CandidateId
			rf.leaderId = args.CandidateId
			reply.VoteGranted = true
			//为其他人投票，则重置自己的超时时间
			rf.lastActiveTime = time.Now()
			rf.timeoutInterval = randElectionTimeout()
		}
	}
	rf.persist()
}

//只有follower、candidate能够接收到这个请求
//如果收到term比自己大的AppendEntries请求，则表示发生过新一轮的选举，此时拒绝掉，等待超时选举
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {

	rf.mu.Lock()
	defer rf.mu.Unlock()

	//DPrintf("node[%d] handle AppendEntries, from node[%d] term[%d] to node[%d] term[%d], role=[%v]",
	//	rf.me, args.LeaderId, args.Term, rf.me, rf.term, rf.role)
	defer func() {
		//DPrintf("node[%d] return AppendEntries, from node[%d] term[%d] to node[%d] term[%d], role=[%v]m args: %v, reply: %v",
		//	rf.me, args.LeaderId, args.Term, rf.me, rf.term, rf.role, mr.Any2String(args), mr.Any2String(reply))
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
		rf.votedFor = RoleNone
		rf.leaderId = RoleNone
		rf.persist()
	}
	//发现term大于等于自己的日志复制请求，则认其为主
	rf.leaderId = args.LeaderId
	rf.votedFor = args.LeaderId
	//DPrintf("node[%d] role[%v] received from node[%d], reset lastActiveTime[%v]", rf.me, rf.role, args.LeaderId, rf.lastActiveTime.UnixMilli())
	rf.lastActiveTime = time.Now()

	//默认没有日志时这两个值为0
	lastLogTerm, lastLogIndex := rf.lastLogTermAndLastLogIndex()
	//还缺少前面的日志或者前一条日志匹配不上，不复制
	if lastLogIndex < args.PrevLogIndex || args.PrevLogTerm < lastLogTerm {
		return
	}
	//如果这个结点是旧leader，日志更长也可以走到这一步，此时要覆盖
	index := args.PrevLogIndex
	for i := 0; i < len(args.LogEntries); i++ {
		index++
		if index > lastLogIndex {
			rf.logs = append(rf.logs, args.LogEntries[i])
		} else {
			//此节点和leader结点日志不一致，截断
			if rf.logs[index].LogTerm != args.LogEntries[i].LogTerm {
				rf.logs = rf.logs[:index]
				rf.logs = append(rf.logs, args.LogEntries[i])
			}
		}
	}
	//follower更新commitIndex
	rf.commitIndex = index
	rf.nextIndex[rf.me] = index + 1
	rf.matchIndex[rf.me] = index
	DPrintf("node: %v role: %v log: %v, index: %v commitIndex[%v]", rf.me, rf.role, mr.Any2String(rf.logs), index, rf.commitIndex)
	rf.persist()
	reply.Success = true
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
//写入数据
func (rf *Raft) Start(command interface{}) (int, int, bool) {

	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	DPrintf("application enter to raft, command: %v, node[%v] term[%d] role[%v]", mr.Any2String(command), rf.me, rf.term, rf.role)
	if rf.role != Leader {
		return -1, -1, false
	}
	entry := &LogEntry{
		LogTerm:  rf.term,
		LogIndex: rf.lastLogIndex() + 1,
		Command:  command,
	}
	rf.logs = append(rf.logs, entry)
	index = entry.LogIndex
	term = entry.LogTerm
	rf.persist()
	DPrintf("node[%d] term[%d] role[%v] add entry: %v, logIndex[%d]", rf.me, rf.term, rf.role, mr.Any2String(entry), index)
	return index, term, isLeader
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) heartBeatLoop() {
	for rf.killed() == false {

		time.Sleep(time.Millisecond * 1)
		func() {
			if rf.role != Leader {
				return
			}
			//如果没有超时或者没有需要发送的数据，则直接返回
			if time.Now().Sub(rf.lastHeartBeatTime) < 100 && rf.commitIndex >= rf.lastLogIndex() {
				return
			}
			rf.lastHeartBeatTime = time.Now()
			appendEntriesStruct := AppendEntriesStruct{Type: HeartBeatLogType}
			if rf.commitIndex < rf.lastLogIndex() {
				DPrintf("append log to follower")
				appendEntriesStruct.Type = AppendEntryLogType
			}
			//DPrintf("trigger type[%v] appendEntriesLoop: %v", appendEntriesStruct.Type, mr.Any2String(appendEntriesStruct))
			rf.appendEntriesChan <- appendEntriesStruct
			//maxTerm := rf.sendHeartBeat()
		}()
	}
}

//日志提交循环，不采用定期唤醒，而是条件变量
func (rf *Raft) applyLogLoop(applyCh chan ApplyMsg) {

	if !rf.killed() {
		for {
			applyMsgs := make([]ApplyMsg, 0)
			func() {
				rf.mu.Lock()
				defer rf.mu.Unlock()
				rf.applyCond.Wait()
				for rf.lastApplied < rf.commitIndex {
					rf.lastApplied++
					applyMsgs = append(applyMsgs, ApplyMsg{
						CommandValid: true,
						Command:      rf.logs[rf.lastApplied].Command,
						CommandIndex: int(rf.logs[rf.lastApplied].LogIndex),
						CommandTerm:  int(rf.logs[rf.lastApplied].LogTerm),
					})
				}
			}()
			//锁外提交给应用
			for i := 0; i < len(applyMsgs); i++ {
				applyCh <- applyMsgs[i]
			}
			DPrintf("upload log to application, applyMsgs: %v", mr.Any2String(applyMsgs))
		}
	}
}
func (rf *Raft) electionLoop() {
	for rf.killed() == false {
		//leader结点只是空循环，不做实际操作
		time.Sleep(time.Millisecond * 1)
		func() {
			if rf.role == Leader {
				return
			}
			elapses := time.Now().Sub(rf.lastActiveTime)
			timeoutInterval := randElectionTimeout()
			if elapses < timeoutInterval {
				//不超时不需要进入下一步，只需要接收RequestVote和AppendEntries请求即可
				return
			}
			rf.mu.Lock()
			if rf.role == Follower {
				DPrintf("node[%d] term: %d Follower -> Candidate", rf.me, rf.term)
				rf.role = Candidate
			}
			//DPrintf("become candidate... node[%v] term[%v] role[%v] elapses>=timeoutInterval[%v]", rf.me, rf.term, rf.role, elapses >= timeoutInterval)

			maxTerm, voteGranted := rf.becomeCandidate()

			rf.mu.Lock()
			//rf.timeoutInterval = randElectionTimeout()
			rf.lastActiveTime = time.Now()
			defer rf.mu.Unlock()
			if rf.role != Candidate {
				return
			}
			if maxTerm > rf.term {
				rf.role = Follower
				rf.term = maxTerm
				rf.votedFor = RoleNone
				rf.leaderId = RoleNone
			} else if voteGranted > rf.nPeers/2 {
				rf.lastHeartBeatTime = time.Unix(0, 0)
				rf.role = Leader
				rf.leaderId = rf.me
			}
			DPrintf("node[%d] role[%v] maxTerm[%d] voteGranted[%d] nPeers[%d]", rf.me, rf.role, maxTerm, voteGranted, rf.nPeers)
			rf.persist()
		}()
	}
}

func (rf *Raft) appendEntriesLoop() {

	type AppendEntriesResult struct {
		peerId int
		resp   *AppendEntriesReply
	}
	for !rf.killed() {

		c := <-rf.appendEntriesChan
		resultChan := make(chan *AppendEntriesResult, rf.nPeers-1)
		//本次复制的最后一条日志
		lastLogIndex := rf.lastLogIndex()

		//DPrintf("process appendEntries for log to follower, type[%v]", c.Type)
		for i := 0; i < rf.nPeers; i++ {
			if rf.me == i {
				continue
			}
			//记录每个node本次发送日志的前一条日志
			prevLogIndex := rf.nextIndex[i] - 1
			argsI := &AppendEntriesArgs{
				Term:                rf.term,
				LeaderId:            rf.me,
				LeaderCommitedIndex: rf.commitIndex,
				PrevLogIndex:        prevLogIndex,
				PrevLogTerm:         rf.logs[prevLogIndex].LogTerm,
			}
			if c.Type == AppendEntryLogType {
				argsI.LogEntries = make([]*LogEntry, 0)
				//因为此时没有加锁，担心有新日志写入，必须保证每个节点复制的最后一条日志一样才能起到过半提交的效果
				argsI.LogEntries = append(argsI.LogEntries, rf.logs[rf.nextIndex[i]:lastLogIndex+1]...)
			}

			go func(server int, args *AppendEntriesArgs) {
				reply := &AppendEntriesReply{}
				//defer func() {
				//	DPrintf("args: %v reply: %v", mr.Any2String(args), mr.Any2String(reply))
				//}()
				ok := rf.sendAppendEntries(server, args, reply)
				if !ok {
					resultChan <- &AppendEntriesResult{
						peerId: server,
						resp:   nil,
					}
					return
				}
				resultChan <- &AppendEntriesResult{
					peerId: server,
					resp:   reply,
				}
				rf.mu.Lock()
				defer rf.mu.Unlock()
				//如果term变了，表示该结点不再是leader，什么也不做
				if rf.term != args.Term {
					return
				}
				if reply.Term > rf.term {
					rf.term = reply.Term
					rf.lastActiveTime = time.Now()
					rf.votedFor = RoleNone
					rf.leaderId = RoleNone
					rf.role = Follower
					rf.persist()
					return
				}
				if reply.Success {
					rf.nextIndex[server] += len(args.LogEntries)
					rf.matchIndex[server] = rf.nextIndex[server] - 1
				} else {
					rf.nextIndex[server] -= 1
					if rf.nextIndex[server] < 1 {
						rf.nextIndex[server] = 1
					}
				}
				//DPrintf("node[%d] term[%v] role[%v] send heartbeat to node[%d], reply.Term>rf.term", rf.me, rf.term, rf.role.String(), server)
			}(i, argsI)
		}

		var maxTerm = rf.term
		accepted := 1
		for {
			select {
			case result := <-resultChan:
				if result.resp != nil && result.resp.Term > maxTerm {
					maxTerm = result.resp.Term
				}
				if result.resp != nil && result.resp.Success {
					accepted++
				}
			}
			if accepted > rf.nPeers/2 {
				break
			}
		}
		rf.mu.Lock()
		if maxTerm > rf.term {
			//先更新时间，避免leader一进入follower就超时
			rf.term = maxTerm
			rf.lastActiveTime = time.Now()
			rf.role = Follower
			rf.votedFor = RoleNone
			rf.leaderId = RoleNone
		} else {
			rf.lastHeartBeatTime = time.Now()
			//rf.timeoutInterval = heartBeatTimeout()
		}
		matchIndexs := make([]int, rf.nPeers)
		copy(matchIndexs, rf.matchIndex)
		sort.Slice(matchIndexs, func(i, j int) bool {
			return matchIndexs[i] < matchIndexs[j]
		})
		//本次复制的数据
		if lastLogIndex > rf.commitIndex {
			rf.matchIndex[rf.me] = lastLogIndex
		}
		newCommitIndex := matchIndexs[rf.nPeers/2]
		if newCommitIndex > rf.commitIndex {
			rf.commitIndex = newCommitIndex
		}

		rf.nextIndex[rf.me] = lastLogIndex + 1

		if c.Type == AppendEntryLogType {
			rf.applyCond.Signal()
		}
		DPrintf("matchIndexs: %v, nextIndexs: %v", mr.Any2String(rf.matchIndex), mr.Any2String(rf.nextIndex))
		rf.mu.Unlock()
	}
}

func (rf *Raft) becomeCandidate() (int, int) {

	rf.role = Candidate
	rf.votedFor = rf.me
	rf.term++
	rf.persist()
	lastLogTerm, lastLogIndex := rf.lastLogTermAndLastLogIndex()
	rf.mu.Unlock()

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
			if voteGranted > rf.nPeers/2 {
				return maxTerm, voteGranted
			}
		}
	}
	return maxTerm, voteGranted
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
//初始化raft, 所有raft的任务都要另起协程，测试文件采用的是协程模拟rpc
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {

	rf := &Raft{
		mu:                sync.Mutex{},
		peers:             peers,
		persister:         persister,
		me:                me,
		nPeers:            len(peers),
		leaderId:          RoleNone,
		term:              None,
		votedFor:          RoleNone,
		role:              Follower,
		lastActiveTime:    time.Now(),
		commitIndex:       None,
		lastApplied:       None,
		applyChan:         applyCh,
		appendEntriesChan: make(chan AppendEntriesStruct),
	}
	rf.applyCond = sync.NewCond(&rf.mu)
	DPrintf("starting new raft node, id: %d", me)
	//超时设置

	rf.logs = make([]*LogEntry, 0)
	rf.nextIndex = make([]int, rf.nPeers)
	rf.matchIndex = make([]int, rf.nPeers)
	for i := 0; i < rf.nPeers; i++ {
		rf.nextIndex[i] = 1
	}
	rf.logs = append(rf.logs, &LogEntry{
		LogTerm:  0,
		LogIndex: 0,
		Command:  100,
	})

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	// start ticker goroutine to start elections
	go rf.electionLoop()
	go rf.heartBeatLoop()
	go rf.applyLogLoop(applyCh)
	go rf.appendEntriesLoop()

	return rf
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
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
