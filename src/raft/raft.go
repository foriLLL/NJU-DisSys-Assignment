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
	"bytes"
	"encoding/gob"
	"fmt"
	"math/rand"
	"sync"
	"time"
)
import "labrpc"

// import "bytes"
// import "encoding/gob"

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}

const (
	Follower  = 1
	Candidate = 2
	Leader    = 3
)

type LogEntry struct {
	Term      int
	Operation string // 应该是什么类型？
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex
	peers     []*labrpc.ClientEnd
	persister *Persister
	me        int // index into peers[]

	// Your data here.
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	/** 所有 server 的持久状态 **/
	currentTerm int // last term ever seen
	votedFor    int // null if none
	log         []LogEntry

	/** 所有 server 的易失状态 **/
	commitIndex int
	lastApplied int

	/** leader 的易失状态 **/
	nextIndex  []int
	matchIndex []int

	timeout time.Duration
	timer   *time.Timer
	state   int // Follower, Candidate or Leader
	ballot  int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here.
	term = rf.currentTerm
	isleader = rf.state == Leader
	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
func (rf *Raft) persist() {
	// Your code here.
	// Example:
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	// Your code here.
	// Example:
	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)
	d.Decode(&rf.currentTerm)
	d.Decode(&rf.votedFor)
	d.Decode(&rf.log)
}

// example RequestVote RPC arguments structure.
type RequestVoteArgs struct {
	// Your data here.
	Term         int // candidate's term
	CandidateId  int // candidate requesting vote
	LastLogIndex int // index of candidate's last log entry
	LastLogTerm  int // term of candidate's last log entry (decides whether up-to-date, rather than term does)
}

// example RequestVote RPC reply structure.
type RequestVoteReply struct {
	// Your data here.
	Term        int // currentTerm, for candidate to update
	VoteGranted bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here.
	// note that rf here is the receiver
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// 处理 args 并 填充 reply
	reply.Term = rf.currentTerm // if fresher, candidate will update
	reply.VoteGranted = false
	if args.Term < rf.currentTerm { // 见过这届领导人了，申请下一届
		fmt.Println("<<<<<<<<<<<,")
		return
	}

	if args.Term > rf.currentTerm { // 如果你竞选的比我新一届，那我认同你
		rf.state = Follower
		rf.votedFor = args.CandidateId
	}

	rf.currentTerm = args.Term // update own term
	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && rf.incomingFresher(args) {
		fmt.Printf("server%d voted for server%d\n", rf.me, args.CandidateId)
		rf.state = Follower // bug:有必要吗
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
		rf.tick() // bug：这里有个问题是在currentTerm 和 args.Term 相同时是否需要判断这个人就是我之前投票的人
	} else {
		fmt.Printf("server%d NOT voted for server%d for term%d\n", rf.me, args.CandidateId, args.Term)
		fmt.Printf("voted for %d in term%d\n", rf.votedFor, rf.currentTerm)
	}
}

func (rf *Raft) incomingFresher(args RequestVoteArgs) bool {
	if len(rf.log) == 0 {
		return true
	}
	if args.LastLogTerm == rf.log[len(rf.log)-1].Term {
		return len(rf.log) <= args.LastLogIndex
	}
	return args.LastLogTerm > rf.log[len(rf.log)-1].Term
}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// returns true if labrpc says the RPC was delivered.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	return index, term, isLeader
}

// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (rf *Raft) Kill() {
	// Your code here, if desired.
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here.
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.log = make([]LogEntry, 0)

	rf.commitIndex = 0
	rf.lastApplied = 0

	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	timeout := rand.Intn(150) + 150
	rf.timeout = time.Duration(timeout) * time.Millisecond
	rf.state = Follower
	rf.ballot = 0

	rf.tick()
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}

func (rf *Raft) handleVoteResult(reply RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	fmt.Printf("*****************server %d in state %d in Term %d received vote %t\n", rf.me, rf.state, rf.currentTerm, reply.VoteGranted)
	if reply.Term > rf.currentTerm {
		rf.currentTerm = reply.Term
		rf.state = Follower
		rf.ballot = 0 // 好像多余了，超时会自动置为1
		rf.votedFor = -1
		rf.tick()
		return
	}

	if reply.VoteGranted && rf.state == Candidate {
		rf.ballot++
		if rf.ballot > len(rf.peers)/2 {
			fmt.Printf("**** server %d becomes leader\n", rf.me)
			rf.state = Leader
			for i := 0; i < len(rf.peers); i++ {
				if i == rf.me {
					continue
				}
				rf.nextIndex[i] = len(rf.log)
				rf.matchIndex[i] = 0 // bug: -1？
			}
			rf.tick()
			go func() {
				rf.stepUpCallback() // send heartbeat
			}()
		}
	}
}

func (rf *Raft) tick() {
	if rf.timer != nil {
		// 不是初始化，停止之前的 timer
		rf.timer.Stop()
	}
	rf.timer = time.AfterFunc(rf.timeout, func() { rf.timeoutCallback() })
}

func (rf *Raft) timeoutCallback() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state == Leader {
		fmt.Printf("*** leader%d timeout\n", rf.me)
		// leader simply refresh timer
		rf.tick()
	} else {
		rf.state = Candidate
		rf.votedFor = rf.me // vote for himself
		rf.ballot = 1
		rf.currentTerm += 1
		fmt.Printf("*** server%d timeout, becomes candidate for term%d\n", rf.me, rf.currentTerm)

		// construct arguments
		var args RequestVoteArgs
		args.Term = rf.currentTerm
		args.CandidateId = rf.me
		args.LastLogIndex = len(rf.log) - 1
		if len(rf.log) == 0 {
			args.LastLogTerm = -1
		} else {
			args.LastLogTerm = rf.log[len(rf.log)-1].Term
		}

		// send RequestForVote and handle response
		f := func(server int, args RequestVoteArgs) {
			var resp RequestVoteReply
			var ifSuccess = rf.sendRequestVote(server, args, &resp)
			if ifSuccess {
				rf.handleVoteResult(resp)
			}
		}
		for i := 0; i < len(rf.peers); i++ {
			if i != rf.me {
				// parallel
				go f(i, args)
			}
		}
		rf.tick()
	}
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) handleAppendEntriesResult(reply AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	fmt.Printf("server %d in term %d received reply with term %d\n", rf.me, rf.currentTerm, reply.Term)
	// 存在收到心跳但是Leader的Term比自己的currentTerm还小的情况吗。。？
	if reply.Term > rf.currentTerm {
		// step down
		fmt.Println("*********************存在收到心跳但是Leader的Term比自己的currentTerm还小的情况*** step down")
		rf.currentTerm = reply.Term
		rf.state = Follower // step down 自己封闭太久了
		rf.votedFor = -1
		//rf.ballot = 0
	}
	rf.tick()
}

func (rf *Raft) stepUpCallback() {
	for rf.state == Leader {
		var args AppendEntriesArgs
		args.Term = rf.currentTerm
		args.LeaderId = rf.me
		args.PrevLogIndex = len(rf.log) - 1
		if len(rf.log) == 0 {
			args.PrevLogTerm = -1
		} else {
			args.PrevLogTerm = rf.log[len(rf.log)-1].Term
		}
		// no payload yet

		for i := 0; i < len(rf.peers); i++ {
			if i != rf.me {
				go func(server int, args AppendEntriesArgs) { // 并行效率更高
					var reply AppendEntriesReply
					ok := rf.sendAppendEntries(server, &args, &reply) //进行RPC
					if ok {
						rf.handleAppendEntriesResult(reply) //对于获取到的结果进行处理
					}
				}(i, args)
			}
		}
		time.Sleep(time.Duration(10) * time.Millisecond) //等待10ms
	}
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	if rf.currentTerm > args.Term {
		reply.Success = false
		return
	} else {
		rf.currentTerm = args.Term
		rf.state = Follower // in case disconnected leader wake up
		rf.votedFor = -1
		reply.Term = rf.currentTerm
		//fmt.Println(rf.currentTerm)
		rf.tick()
	}
}
