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
	"math/rand"
	"sync"
	"time"
)
import "labrpc"

// ApplyMsg
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}

type LogEntry struct {
	Term    int
	Command interface{}
}

// enum type for server state
const (
	Follower  = 1
	Candidate = 2
	Leader    = 3
)

// Raft
// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex
	peers     []*labrpc.ClientEnd
	persister *Persister
	me        int // index into peers[]

	// Your data here.
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// all servers' persistent staate
	currentTerm int // last term ever seen
	votedFor    int // null if none
	log         []LogEntry

	// all servers' volatile state
	commitIndex int
	lastApplied int

	// leader's volatile state
	nextIndex  []int
	matchIndex []int

	timeout time.Duration
	timer   *time.Timer
	state   int // enum: Follower, Candidate or Leader
	ballot  int

	applyCh chan ApplyMsg
}

// RequestVoteArgs
// example RequestVote RPC arguments structure.
type RequestVoteArgs struct {
	// Your data here.
	Term         int // candidate's term
	CandidateId  int // candidate requesting vote
	LastLogIndex int // index of candidate's last log entry
	LastLogTerm  int // term of candidate's last log entry (decides whether up-to-date, rather than term does)
}

// RequestVoteReply
// example RequestVote RPC reply structure.
type RequestVoteReply struct {
	// Your data here.
	Term        int // currentTerm, for candidate to update
	VoteGranted bool
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

// GetState
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

// refresh timer
func (rf *Raft) tick() {
	if rf.timer != nil {
		// if timer already initialized, stop previous timer
		rf.timer.Stop()
	}
	rf.timer = time.AfterFunc(rf.timeout, func() { rf.timeoutCallback() }) // refresh timer
}

// RequestVote
// RequestVote RPC handler.
func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here.
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// handle args and complete reply
	reply.Term = rf.currentTerm // if fresher, candidate will update
	reply.VoteGranted = false
	if args.Term < rf.currentTerm { // all ready seen leader for this term
		return
	}

	if args.Term > rf.currentTerm { // agree if your term is more recent
		rf.state = Follower
		rf.votedFor = args.CandidateId
	}

	rf.currentTerm = args.Term // update own term
	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && rf.incomingFresher(args) {
		rf.state = Follower
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
		rf.tick()
	}
}

// if incoming RequestVote is fresher than oneself
func (rf *Raft) incomingFresher(args RequestVoteArgs) bool {
	if len(rf.log) == 0 {
		return true
	}
	if args.LastLogTerm == rf.log[len(rf.log)-1].Term {
		return len(rf.log)-1 <= args.LastLogIndex // 这里出过错，len应该减一
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

// handle when received RequestVoteReply
func (rf *Raft) handleVoteResult(reply RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if reply.Term > rf.currentTerm {
		rf.currentTerm = reply.Term
		rf.state = Follower
		//rf.ballot = 0 // no need to set since it will automatically reset if timeout
		rf.votedFor = -1
		rf.tick()
		return
	}

	if reply.VoteGranted && rf.state == Candidate {
		rf.ballot++
		if rf.ballot > len(rf.peers)/2 { // majority
			rf.state = Leader // step up
			for i := 0; i < len(rf.peers); i++ {
				if i == rf.me {
					continue
				}
				rf.nextIndex[i] = len(rf.log)
				rf.matchIndex[i] = -1 // index from 0, so init -1
			}
			rf.tick()
			go func() {
				rf.stepUpCallback() // send heartbeat
			}()
		}
	}
}

// Start
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

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state != Leader {
		return index, term, false
	}

	newLog := LogEntry{rf.currentTerm, command}
	rf.log = append(rf.log, newLog)

	index = len(rf.log) // note that starts from 1
	term = rf.currentTerm

	return index, term, true
}

// Kill
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (rf *Raft) Kill() {
	// Your code here, if desired.
}

// Make
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

	rf.commitIndex = -1
	rf.lastApplied = -1

	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	timeout := rand.Intn(150) + 150
	rf.timeout = time.Duration(timeout) * time.Millisecond
	rf.state = Follower
	rf.ballot = 0

	rf.applyCh = applyCh

	rf.tick()
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// note that we haven't initialized timer in Make, so that tick could handle all about timeout
	return rf
}

// what to do for a server when timeout
func (rf *Raft) timeoutCallback() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state == Leader {
		// leader simply refresh timer
		rf.tick()
	} else {
		rf.state = Candidate
		rf.votedFor = rf.me // vote for himself
		rf.ballot = 1
		rf.currentTerm += 1

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

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// send AppendEntry to certain server
func (rf *Raft) sendAppendEntry2Certain(nodeId int) {
	if nodeId == rf.me {
		return
	}
	var args AppendEntriesArgs // construct args
	args.Term = rf.currentTerm
	args.LeaderId = rf.me
	args.PrevLogIndex = rf.nextIndex[nodeId] - 1
	if args.PrevLogIndex >= 0 {
		args.PrevLogTerm = rf.log[args.PrevLogIndex].Term
	} else { // -1
		args.PrevLogTerm = -1
	}
	args.Entries = rf.log[rf.nextIndex[nodeId]:]
	args.LeaderCommit = rf.commitIndex

	go func(server int, args AppendEntriesArgs) { // parallel sending
		var reply AppendEntriesReply
		ok := rf.sendAppendEntries(server, &args, &reply)
		if ok {
			rf.handleAppendEntriesResult(reply, server)
		}
	}(nodeId, args)
}

// server(probably leader) receiving replies from handleAppendEntries
func (rf *Raft) handleAppendEntriesResult(reply AppendEntriesReply, nodeId int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state != Leader {
		return
	}

	if reply.Term > rf.currentTerm {
		// step down
		rf.currentTerm = reply.Term
		rf.state = Follower // step down, for having been shut down for too long
		rf.votedFor = -1
	}

	if reply.Success == true {
		rf.matchIndex[nodeId] = rf.nextIndex[nodeId] - 1
		rf.nextIndex[nodeId] = len(rf.log) // update, check if up-to-date in the next time

		// may optimize
		for i := len(rf.log) - 1; i >= 0 && i > rf.commitIndex; i-- {
			// currentTerm starts backward from the last one uncommitted by leader
			if rf.log[i].Term != rf.currentTerm {
				break
			}

			count := 1
			for server := 0; server < len(rf.peers); server++ {
				if server == rf.me {
					continue
				}
				if rf.matchIndex[server] >= i {
					count++
				}
			}
			if count > len(rf.peers)/2 { // majority
				rf.commitIndex = i // update commitIndex
				rf.refreshCommits()
			}

		}
	} else {
		rf.nextIndex[nodeId]--
	}

}

// first become Leader and continue sending heart beat
func (rf *Raft) stepUpCallback() {
	// init
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		rf.nextIndex[i] = len(rf.log)
		rf.matchIndex[i] = -1
	}

	for rf.state == Leader { // while
		for i := 0; i < len(rf.peers); i++ {
			if i != rf.me {
				rf.sendAppendEntry2Certain(i)
			}
		}
		time.Sleep(time.Duration(100) * time.Millisecond) // send heartbeat every 100ms
	}
}

// AppendEntries handler
// AppendEntries
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
		rf.tick()

		if args.PrevLogIndex > -1 {
			if len(rf.log) <= args.PrevLogIndex || // not as long
				rf.log[args.PrevLogIndex].Term != args.PrevLogTerm { // or unmatched term
				// failed to match
				reply.Success = false
				return
			}
		}

		// prevLogIndex is -1 or successfully matched
		rf.log = rf.log[:args.PrevLogIndex+1] // discard all following
		rf.log = append(rf.log, args.Entries...)
		if rf.commitIndex < args.LeaderCommit {
			rf.commitIndex = args.LeaderCommit
			go rf.refreshCommits()
		}

		reply.Success = true
	}
}

// apply uncommitted commands by comparing lastApplied and commitIndex of oneself
func (rf *Raft) refreshCommits() {
	// NOTE NO LOCK HERE, OR WILL CAUSE DEADLOCK

	for logIndex := rf.lastApplied + 1; logIndex <= rf.commitIndex; logIndex++ {
		rf.applyCh <- ApplyMsg{Index: logIndex + 1, Command: rf.log[logIndex].Command} // note that index need to plus 1
	}

	rf.lastApplied = rf.commitIndex
}
