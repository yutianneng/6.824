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
	term     int64
	role     MemberRole
	leaderId int
	votedFor int

	//提交情况
	log         *RaftLog
	commitIndex int64
	lastApplied int64

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
	ElectionTimeout = 300 * time.Millisecond
	HeatBeatTimeout = 100 * time.Millisecond
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
}

//
// 心跳或者日志追加
//
type AppendEntriesArgs struct {
	LeaderId   int
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
		DPrintf("args: %v, reply: %v", mr.Any2String(args), mr.Any2String(reply))
	}()
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.term
	reply.VoteGranted = false
	DPrintf("id: %d received RequestVote, from id: %d term: %d to id: %d term: %d, role: %v voteFor: %v",
		rf.me, args.CandidateId, args.Term, rf.me, rf.term, rf.role, rf.votedFor)
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
	if rf.votedFor == RoleNone || rf.votedFor == args.CandidateId {
		lastLogTerm, lastLogIndex := rf.lastLogTermAndLastLogIndex()
		if lastLogIndex <= args.LastLogIndex && lastLogTerm <= args.LastLogTerm {
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
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {

	rf.mu.Lock()
	defer rf.mu.Unlock()

	DPrintf("node[%d] handle AppendEntries, from node[%d] term[%d] to node[%d] term[%d], role=[%v]",
		rf.me, args.LeaderId, args.Term, rf.me, rf.term, rf.role)
	defer func() {
		DPrintf("node[%d] return AppendEntries, from node[%d] term[%d] to node[%d] term[%d], role=[%v]",
			rf.me, args.LeaderId, args.Term, rf.me, rf.term, rf.role)
	}()
	reply.Term = rf.term
	reply.IsAccept = false
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
	}
	//接收对方的请求就是认定它为leader
	rf.leaderId = args.LeaderId
	rf.lastActiveTime = time.Now()
	rf.timeoutInterval = randElectionTimeout()
	rf.persist()
}

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
func (rf *Raft) heartBeatLoop() {
	for rf.killed() == false {

		time.Sleep(time.Millisecond * 1)
		func() {
			if rf.role != Leader {
				return
			}
			if time.Now().Sub(rf.lastHeartBeatTime) < rf.timeoutInterval {
				return
			}
			rf.sendHeartBeat()
			rf.lastHeartBeatTime = time.Now()
			rf.timeoutInterval = heartBeatTimeout()
			rf.mu.Lock()
			if rf.role != Leader {
				rf.lastActiveTime = time.Now()
				rf.timeoutInterval = randElectionTimeout()
			}
			rf.mu.Unlock()
		}()
	}
}
func (rf *Raft) electionLoop() {
	for rf.killed() == false {
		//leader结点只是空循环，不做实际操作
		time.Sleep(time.Millisecond * 1)
		func() {
			elapses := time.Now().Sub(rf.lastActiveTime)
			rf.mu.Lock()
			if rf.role == Follower {
				//超时进入candidate
				if elapses >= rf.timeoutInterval {
					DPrintf("node[%d] term: %d Follower -> Candidate", rf.me, rf.term)
					rf.role = Candidate
				} else {
					rf.mu.Unlock()
					//不超时不需要进入下一步，只需要接收RequestVote和AppendEntries请求即可
					return
				}
			}
			if rf.role != Candidate || elapses < rf.timeoutInterval {
				rf.mu.Unlock()
				return
			}

			//会释放锁
			maxTerm, voteGranted := rf.becomeCandidate()
			DPrintf("node[%d] term[%d] role[%v] voteGranted: %d maxTerm: %d", rf.me, rf.term, rf.role, voteGranted, maxTerm)
			rf.mu.Lock()
			if maxTerm > rf.term {
				rf.role = Follower
				rf.votedFor = RoleNone
				rf.leaderId = RoleNone
				rf.term = maxTerm
			} else if rf.role == Candidate && voteGranted > rf.nPeers/2 {
				rf.role = Leader
				rf.leaderId = rf.me
				rf.lastHeartBeatTime = time.Unix(0, 0)
			}
			rf.lastActiveTime = time.Now()
			rf.timeoutInterval = randElectionTimeout()
			rf.persist()
			rf.mu.Unlock()
		}()
	}
}

func (rf *Raft) sendHeartBeat() {

	logEntries := make([]*LogEntry, 0)
	logEntries = append(logEntries, &LogEntry{
		Type: HeartBeatLogType,
	})
	args := &AppendEntriesArgs{
		Term:       rf.term,
		LeaderId:   rf.me,
		LogEntries: logEntries,
	}
	type AppendEntriesResult struct {
		peerId int
		resp   *AppendEntriesReply
	}

	for i := 0; i < rf.nPeers; i++ {
		if rf.me == i {
			continue
		}
		DPrintf("node[%d] term[%v] role[%v] send heartbeat to node[%d]", rf.me, rf.term, rf.role.String(), i)
		go func(server int, args *AppendEntriesArgs) {
			reply := &AppendEntriesReply{}
			ok := rf.sendAppendEntries(server, args, reply)
			if ok {
				//发现更大的term则退回Follower
				if reply.Term > rf.term {
					rf.mu.Lock()
					rf.role = Follower
					rf.votedFor = RoleNone
					rf.leaderId = RoleNone
					rf.term = reply.Term
					rf.mu.Unlock()
				}
			}
		}(i, args)
	}

}

//
func (rf *Raft) becomeCandidate() (int64, int) {

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
					peerId: i,
					resp:   reply,
				}
			} else {
				voteChan <- &RequestVoteResult{
					peerId: i,
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
			if vote.resp != nil {
				if vote.resp.VoteGranted {
					voteGranted++
				}
				//出现更大term就退回follower
				if vote.resp.Term > maxTerm {
					maxTerm = vote.resp.Term
				}
			}
			totalVote++
		}
	}
	return maxTerm, voteGranted
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
		mu:              sync.Mutex{},
		peers:           peers,
		persister:       persister,
		me:              me,
		nPeers:          len(peers),
		leaderId:        RoleNone,
		term:            None,
		votedFor:        RoleNone,
		role:            Follower,
		lastActiveTime:  time.Now(),
		timeoutInterval: randElectionTimeout(),
		commitIndex:     0,
		lastApplied:     0,
		applyCond:       nil,
		applyChan:       applyCh,
	}
	DPrintf("starting new raft node, id: %d", me)
	//超时设置

	rf.log = &RaftLog{
		Entries:     make([]*LogEntry, 0),
		NextIndexs:  make([]int64, len(rf.peers)),
		MatchIndexs: make([]int64, len(rf.peers)),
	}

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	// start ticker goroutine to start elections
	go rf.electionLoop()
	go rf.heartBeatLoop()

	return rf
}

func (rf *Raft) applier() {

}

func randElectionTimeout() time.Duration {
	return ElectionTimeout + time.Duration(rand.Uint32())%ElectionTimeout
}

func heartBeatTimeout() time.Duration {
	return HeatBeatTimeout
}
