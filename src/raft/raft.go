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
	mu             sync.Mutex          // Lock to protect shared access to this peer's state
	peers          []*labrpc.ClientEnd // RPC end points of all peers
	persister      *Persister          // Object to hold this peer's persisted state
	me             int                 // this peer's index into peers[]
	dead           int32               // set by Kill()
	nPeers         int
	VoteForApprove int

	role                MemberRole
	leaderId            int
	timeoutChan         chan struct{}
	timeoutInterval     time.Duration //follower leader candidate
	startTimeout        time.Time     //超时开始计算时间，收到心跳时会更新
	electionTimeoutChan chan struct{} //leader需要有两个
	tick                *time.Ticker  //reset(2s)表示2s会返回时间戳

	appendEntriesChan chan *LogEntry
	// Your data here (2A, 2B, 2C).
	term              int64
	votedFor          int
	votedTerm         int64
	receivedVotes     chan RequestVoteArgs
	receivedHeartBeat chan int
	receives          chan struct{}

	//提交情况
	log         *RaftLog
	commitIndex int64
	lastApplied int64

	//状态机
	applyCond *sync.Cond
	applyChan chan ApplyMsg

	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

}

type MemberRole int

const (
	Leader    = 1
	Follower  = 2
	Candidate = 3

	None     = 0
	RoleNone = -1
)

type LogType int

const (
	HeartBeatLogType      LogType = 1
	AppendEntryLogType    LogType = 2
	AppendSnapshotLogType LogType = 3
	RequestVoteLogType    LogType = 4

	DetectMatchIndexLogType LogType = 5
)

type LogEntry struct {
	Type     LogType
	LogTerm  int64
	LogIndex int64
	data     []byte
}

type RaftLog struct {
	Entries []*LogEntry

	NextIndexs  []int64
	MatchIndexs []int64
}

const (
	ElectionTimeout = 400 * time.Millisecond
	HeatBeatTimeout = 250 * time.Millisecond
)

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

func (rf *Raft) lastLogTermAndLastLogIndex() (int64, int64) {
	if len(rf.log.Entries) == 0 {
		return 0, 0
	}
	logTerm := rf.log.Entries[len(rf.log.Entries)-1].LogTerm
	logIndex := rf.log.Entries[len(rf.log.Entries)-1].LogIndex
	return logTerm, logIndex
}

func (rf *Raft) logTerm(logIndex int64) int64 {
	return rf.log.Entries[logIndex].LogTerm
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

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
//选举时需要传递自己拥有的最后一条log的term和index
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int64
	CandidateId  int
	LastLogIndex int64
	LastLogTerm  int64
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	Term        int64
	VoteGranted bool
	LeaderId    int
}

//
// 心跳或者日志追加
//
type AppendEntriesArgs struct {
	Id         int
	Term       int64 //leader currentTerm
	LogEntries []*LogEntry
}

//
// 心跳或者日志追加
//
type AppendEntriesReply struct {
	IsAccept     bool
	Term         int64
	NextLogTerm  int64
	NextLogIndex int64
	Msg          string
}

//副本刚投票，结果收到更大的term的投票请求怎么办
//
// 处理投票请求，决定是否投票
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	defer func() {
		DPrintf("id: %d received RequestVote, args: %v, reply: %v, rf.term: %d, voteFor: %d, now: %v",
			rf.me, mr.Any2String(args), mr.Any2String(reply), rf.term, rf.votedFor, time.Now().UnixMilli())
	}()
	if args.Term < rf.term {
		reply.Term = rf.term
		reply.VoteGranted = false
		reply.LeaderId = rf.leaderId
		DPrintf("declined this RequestVote, leaderId: %d", rf.leaderId)
		return
	}
	//任期大于自己则投票, 是否合理
	//任期好比自己大很多，则不投票并将自身的term提升
	//if args.Term > rf.term+1 {
	//	DPrintf("term>rf.term+1 accept this RequestVote, from id: %d term: %d to id: %d term: %d", args.CandidateId, args.Term, rf.me, rf.term)
	//	rf.mu.Lock()
	//	reply.VoteGranted = true
	//	reply.Term = rf.term
	//	rf.term = args.Term
	//	rf.leaderId = args.CandidateId
	//	rf.votedFor = args.CandidateId
	//	rf.votedTerm=args.Term
	//	rf.startTimeout = time.Now()
	//	if rf.role == Leader {
	//		rf.electionTimeoutChan <- struct{}{} //退化为follower
	//		reply.VoteGranted = false
	//		rf.term = args.Term
	//		rf.role = Follower
	//	}
	//	rf.tick.Reset(randHeartBeatTimeout(Follower))
	//	rf.mu.Unlock()
	//	return
	//}
	if args.Term == rf.votedTerm && args.CandidateId == rf.votedFor {
		reply.VoteGranted = true
		return
	}
	//最后一条日志的任期和自己一样，而且log_index大于等于自己
	lastLogTerm, lastLogIndex := rf.lastLogTermAndLastLogIndex()
	if rf.term < args.Term {
		rf.mu.Lock()
		//follower，不能是candidate
		if rf.votedFor != rf.me && lastLogTerm == args.LastLogTerm && lastLogIndex <= args.LastLogIndex {
			DPrintf("lastTerm> accept this RequestVote, from id: %d term: %d to id: %d term: %d", args.CandidateId, args.Term, rf.me, rf.term)

			reply.VoteGranted = true
			reply.Term = rf.term
			rf.term = args.Term
			rf.leaderId = args.CandidateId
			rf.votedFor = args.CandidateId
			rf.startTimeout = time.Now()
			if rf.role == Leader {
				rf.electionTimeoutChan <- struct{}{} //退化为follower
				rf.term = args.Term
				rf.role = Follower
				rf.tick.Reset(randHeartBeatTimeout(Follower))
			}
			rf.tick.Reset(randHeartBeatTimeout(Follower))
			rf.mu.Unlock()
			return
		} else if rf.votedFor != rf.me {
			reply.VoteGranted = false
			rf.term = args.Term
			rf.votedFor = RoleNone
			if rf.role == Leader {
				rf.electionTimeoutChan <- struct{}{} //退化为follower
				rf.role = Follower
			}
			rf.tick.Reset(randHeartBeatTimeout(Follower))
			rf.mu.Unlock()
		}
		return
	}

}
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	logEntries := args.LogEntries
	if len(logEntries) == 0 {
		reply.Msg = "mistake request"
		return
	}
	DPrintf("AppendEntriesArgs from id: %d Term: %d to id: %d term: %d", args.Id, args.Term, rf.me, rf.term)
	//不处理term小于自己的请求
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.term > args.Term {
		reply.Term = rf.term
		return
	}
	if rf.term < args.Term {
		rf.term = args.Term
	}
	if rf.term == args.Term && rf.votedFor != args.Id {
		reply.IsAccept = false
		return
	}
	reply.IsAccept = true
	for _, logEntry := range logEntries {

		switch logEntry.Type {
		case HeartBeatLogType:
			DPrintf("received heartbeat...")
			rf.startTimeout = time.Now()
			rf.timeoutInterval = randHeartBeatTimeout(Follower)
			rf.tick.Reset(rf.timeoutInterval)
			rf.votedFor = None
			//rf.leaderId=logEntry.
		case AppendEntryLogType:

		}
	}
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
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
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

	return index, term, isLeader
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

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	DPrintf("starting ticker... id: %d", rf.me)
	for rf.killed() == false {
		rf.mu.Lock()
		c := <-rf.tick.C
		rf.mu.Unlock()

		//发送心跳
		if rf.role == Leader {
			DPrintf("leader heart beat timeout, role: %v, id: %d, startTimeout: %v, timeoutInterval: %v, ticker.c: %v",
				rf.role, rf.me, rf.startTimeout.UnixMilli(), rf.timeoutInterval.Milliseconds(), c.UnixMilli())
			rf.sendHeartBeat()
		} else if c.After(rf.startTimeout.Add(rf.timeoutInterval)) {
			DPrintf("follower timeout, id: %v", rf.me)
			//超时，在timeoutInterval没有收到心跳，或者说选举超时时间到
			//让转向candidate
			rf.timeoutChan <- struct{}{}
		}
	}
}

func (rf *Raft) becomeFollower() {

	rf.role = Follower
	rf.persist()
	DPrintf("become follower, leaderId: %d id: %d\n", rf.leaderId, rf.me)
	beCandidate := false
	for {
		select {
		case <-rf.timeoutChan: //心跳计时器超时
			beCandidate = true
		case logEntry := <-rf.appendEntriesChan:
			if logEntry.Type == AppendEntryLogType {
				//todo
			}
		}
		if beCandidate {
			break
		}

	}
	//状态转换
	if beCandidate {
		rf.becomeCandidate()
	}

}

func (rf *Raft) becomeLeader() {
	DPrintf("entering becomeLeader, id: %d, term: %d", rf.me, rf.term)
	rf.mu.Lock()
	rf.role = Leader
	rf.leaderId = rf.me
	rf.votedFor = RoleNone
	rf.mu.Unlock()
	rf.sendHeartBeat()

	rf.persist()

	beFollower := false
	for {
		select {
		case <-rf.timeoutChan:
			rf.sendHeartBeat()
		case <-rf.electionTimeoutChan:
			beFollower = true
		}
		if beFollower {
			break
		}
	}
	if beFollower {
		rf.becomeFollower()
	}
}
func (rf *Raft) detectMatch(server int) {
	_, matchIndex := rf.lastLogTermAndLastLogIndex()

	args := &AppendEntriesArgs{}
	logEntries := make([]*LogEntry, 0)
	logEntries = append(logEntries, &LogEntry{})
	args.LogEntries = logEntries
	for {
		args.LogEntries[0].LogTerm = rf.logTerm(matchIndex)
		args.LogEntries[0].LogIndex = matchIndex
		args.LogEntries[0].Type = DetectMatchIndexLogType
		reply := &AppendEntriesReply{}
		ok := rf.sendAppendEntries(server, args, reply)
		if ok {
			continue
		}
		rf.mu.Lock()
		//探测到follower的匹配logIndex
		//if reply.IsAccept[0] {
		//	break
		//}
		if matchIndex > reply.NextLogIndex-1 {
			matchIndex = reply.NextLogIndex - 1
		} else {
			matchIndex--
		}
		rf.mu.Unlock()
	}
	rf.log.MatchIndexs[server] = matchIndex
	rf.log.NextIndexs[server] = matchIndex + 1
}
func (rf *Raft) sendHeartBeat() {

	args := &AppendEntriesArgs{}
	logEntries := make([]*LogEntry, 0)
	logEntries = append(logEntries, &LogEntry{
		Type: HeartBeatLogType,
	})
	args.Term = rf.term
	args.Id = rf.me
	args.LogEntries = logEntries
	accept := true
	wg := &sync.WaitGroup{}
	for i := 0; i < rf.nPeers; i++ {
		if rf.me == i {
			continue
		}
		wg.Add(1)
		go func(server int, args *AppendEntriesArgs) {
			defer wg.Done()
			reply := &AppendEntriesReply{}
			rf.sendAppendEntries(server, args, reply)
			if !reply.IsAccept {
				rf.mu.Lock()
				accept = false
				rf.mu.Unlock()
			}
		}(i, args)
	}
	wg.Wait()
	if !accept {
		//旧leader
		rf.mu.Lock()
		rf.votedFor = RoleNone
		rf.electionTimeoutChan <- struct{}{}
		rf.leaderId = RoleNone
		rf.mu.Unlock()
	}
}

//
func (rf *Raft) becomeCandidate() {
	rf.role = Candidate

	wg := &sync.WaitGroup{}
	rf.mu.Lock()
	rf.tick.Reset(time.Second * 2)
	rf.votedFor = rf.me
	rf.term++
	rf.mu.Unlock()
	DPrintf("entering becomeCandidate id: %d, term: %d", rf.me, rf.term)
	voteRes := 1
	grantRes := 1

	lastLogTerm, lastLogIndex := rf.lastLogTermAndLastLogIndex()
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
		wg.Add(1)
		go func(server int, args *RequestVoteArgs) {
			defer wg.Done()
			reply := &RequestVoteReply{}
			rf.sendRequestVote(server, args, reply)
			DPrintf("sendRequestVote, args: %v, reply: %v, from id: %v, to id: %d", mr.Any2String(args), mr.Any2String(reply), rf.me, server)
			rf.mu.Lock()
			defer rf.mu.Unlock()
			voteRes++

			if reply.Term > rf.term {
				return
			}
			if reply.VoteGranted {
				grantRes++
			}
		}(i, args)
	}
	wg.Wait()
	rf.mu.Lock()
	if grantRes >= rf.VoteForApprove {
		//过半投票
		DPrintf("RequestVote success, receivedVote: %v, totalVote: %v, id: %d", grantRes, voteRes, rf.me)
		rf.timeoutInterval = randHeartBeatTimeout(Leader)
		rf.tick.Reset(rf.timeoutInterval)
		rf.startTimeout = time.Now() //记录超时起始时间
		close(rf.timeoutChan)
		rf.timeoutChan = make(chan struct{})
		rf.mu.Unlock()
		rf.becomeLeader()
	} else {
		DPrintf("RequestVote failed, receivedVote: %v, totalVote: %v", grantRes, voteRes)
		//随机一个选举超时时间
		rf.timeoutInterval = randElectionTimeout()
		rf.tick.Reset(rf.timeoutInterval)
		close(rf.timeoutChan)
		rf.timeoutChan = make(chan struct{})
		rf.startTimeout = time.Now()
		rf.mu.Unlock()
		rf.becomeFollower()
	}

}
func (rf *Raft) becomePreCandidate(term uint64, lead uint64) {

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
		mu:                  sync.Mutex{},
		peers:               peers,
		persister:           persister,
		me:                  me,
		nPeers:              len(peers),
		VoteForApprove:      (len(peers) + 1) / 2,
		leaderId:            RoleNone,
		timeoutChan:         make(chan struct{}, 0),
		appendEntriesChan:   make(chan *LogEntry, 10),
		electionTimeoutChan: make(chan struct{}, 0),
		term:                None,
		votedFor:            RoleNone,
		commitIndex:         0,
		lastApplied:         0,
		applyCond:           nil,
		applyChan:           applyCh,
	}
	DPrintf("starting new raft node, id: %d", me)
	//超时设置
	rf.timeoutInterval = randHeartBeatTimeout(Follower)
	rf.startTimeout = time.Now()
	rf.tick = time.NewTicker(rf.timeoutInterval)

	//rf.heartBeatTimeout = HeatBeatTimeout

	rf.log = &RaftLog{
		Entries:     make([]*LogEntry, 0),
		NextIndexs:  make([]int64, len(rf.peers)),
		MatchIndexs: make([]int64, len(rf.peers)),
	}

	// Your initialization code here (2A, 2B, 2C).
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	// start ticker goroutine to start elections
	go rf.ticker()

	go rf.becomeFollower()

	return rf
}

func (rf *Raft) applier() {

}

func randElectionTimeout() time.Duration {
	return ElectionTimeout + time.Duration(rand.Uint32())%ElectionTimeout
}

func randHeartBeatTimeout(role MemberRole) time.Duration {
	timeOut := HeatBeatTimeout + time.Duration(rand.Uint32())%HeatBeatTimeout/2
	if role == Leader {
		return timeOut - 125
	}
	return timeOut
}
