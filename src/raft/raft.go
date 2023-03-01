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
	//	"bytes"

	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	l "log"

	"6.824/labgob"
	"6.824/labrpc"
)

type serverState string

const (
	leader         serverState = "Leader"
	follower       serverState = "Follower"
	candidate      serverState = "Candidate"
	MinElecTimout  int64       = 360
	AppendInterval int64       = 120
	nilInt         int         = -1
)

type logEntry struct {
	Term    int
	Index   int         `default:nil`
	Command interface{} `default:nil`
}

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

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	//Persistent state on all server
	currentTerm int `default:0`
	votedFor    int `default:nil`
	log         []logEntry

	// Volatile state on all servers
	commitIndex         int
	lastApplied         int
	state               serverState
	lastAppendEntryTime time.Time

	//
	applyCh chan ApplyMsg

	//Volatile state on leader
	nextIndex  []int
	matchIndex []int

	// term for snapshot
	SnapshotData  []byte
	SnapshotTerm  int
	SnapshotIndex int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.state == leader
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
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
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
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var log []logEntry
	if d.Decode(&currentTerm) != nil || d.Decode(&votedFor) != nil || d.Decode(&log) != nil {
		l.Fatal("Error occurs while decoding")
	} else {
		rf.mu.Lock()
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.log = log
		rf.mu.Unlock()
	}
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
	// Your data here (2A).
	Term        int
	VoteGranted bool `default:false`
}

type AppendEntryArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []logEntry
	LeaderCommit int
}

type AppendEntryReply struct {
	Term    int
	Success bool `default: false`
}

type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	offset            int
	data              []byte
	done              bool `default:True`
}

type InstallSnapshotReply struct {
	Term int
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm
	reply.VoteGranted = false
	if (rf.currentTerm > args.Term) || (rf.currentTerm == args.Term && rf.votedFor != nilInt) {
		return
	}
	if rf.IsUpdate(args.LastLogIndex, args.LastLogTerm) {
		rf.lastAppendEntryTime = time.Now()
		rf.convertToFollower(args.Term, args.CandidateId)
		reply.VoteGranted = true
		return
	}
}

func (rf *Raft) AppendEntry(args *AppendEntryArgs, reply *AppendEntryReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm
	reply.Success = false
	rf.lastAppendEntryTime = time.Now()
	if args.Term < rf.currentTerm {
		return
	}
	if (args.Term > rf.currentTerm) || (args.Term == rf.currentTerm && rf.votedFor != args.LeaderId) {
		rf.convertToFollower(args.Term, args.LeaderId)
	}
	if rf.AppendCheck(args.PrevLogIndex, args.PrevLogTerm) && (rf.votedFor == args.LeaderId) {
		reply.Success = true
		rf.log = rf.getEntryUpTo(args.PrevLogIndex)
		rf.UpdatePersistentState(rf.currentTerm, rf.votedFor, args.Entries)
		if rf.commitIndex < args.LeaderCommit {
			rf.startCommit(args.LeaderCommit)
		}
		return
	}
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm
	if rf.currentTerm > args.Term {
		return
	}

}

func (rf *Raft) AppendCheck(PrevLogIndex int, PrevLogTerm int) bool {
	if PrevLogIndex == nilInt {
		return true
	} else if rf.SnapshotIndex == PrevLogIndex {
		return PrevLogTerm == rf.SnapshotTerm
	}
	firstLogIndex := -1
	LastLogIndex := -1
	if len(rf.log) > 0 {
		firstLogIndex = rf.log[0].Index
		LastLogIndex = rf.log[len(rf.log)-1].Index
	}
	return LastLogIndex >= PrevLogIndex && rf.log[PrevLogIndex-firstLogIndex].Term == PrevLogTerm && rf.log[PrevLogIndex-firstLogIndex].Index == PrevLogIndex
}

func (rf *Raft) IsUpdate(LastLogIndex int, LastLogTerm int) bool {
	term := nilInt
	index := rf.getLogIndex(len(rf.log) - 1)
	if index >= 0 {
		term = rf.getIndexTerm(index)
	}
	return (LastLogTerm > term) || (LastLogTerm == term && LastLogIndex >= index)
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

func (rf *Raft) sendAppendEntry(server int, args *AppendEntryArgs, reply *AppendEntryReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntry", args, reply)
	return ok
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

func (rf *Raft) KickStartElection() {

	rf.mu.Lock()
	rf.convertToCandidate()

	term := rf.currentTerm
	candidateId := rf.me
	lastLogIndex := rf.getLogIndex(len(rf.log) - 1)
	lastLogTerm := nilInt
	if lastLogIndex >= 0 {
		lastLogTerm = rf.getIndexTerm(lastLogIndex)
	}
	rf.mu.Unlock()

	cond := sync.NewCond(&rf.mu)

	peerDone := 1
	peerLength := len(rf.peers)
	majority := peerLength/2 + 1
	vote := 1
	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}
		go func(peer int) {
			args := RequestVoteArgs{term, candidateId, lastLogIndex, lastLogTerm}
			reply := RequestVoteReply{}
			ok := rf.sendRequestVote(peer, &args, &reply)
			rf.mu.Lock()
			peerDone++
			if ok {
				if reply.VoteGranted {
					vote++
				} else if reply.Term > rf.currentTerm {
					rf.convertToFollower(reply.Term, nilInt)
				}
			}

			cond.Broadcast()
			rf.mu.Unlock()
		}(peer)
	}

	rf.mu.Lock()
	for {
		rf.lastAppendEntryTime = time.Now()
		if rf.state != candidate || (peerLength-peerDone) < (majority-vote) {
			rf.convertToFollower(rf.currentTerm, nilInt)
			break
		} else if vote >= majority {
			rf.convertToLeader()
			break
		}
		cond.Wait()
	}
	rf.mu.Unlock()

}

func (rf *Raft) convertToCandidate() {
	rf.state = candidate
	rf.DestroyLeaderSession()
	rf.UpdatePersistentState(rf.currentTerm+1, rf.me, nil)
}

func (rf *Raft) convertToLeader() {
	rf.state = leader
	rf.lastAppendEntryTime = time.Now().Add(-time.Duration(AppendInterval) * time.Microsecond)
	rf.AssignNextIndex(len(rf.log))
}

func (rf *Raft) convertToFollower(term int, CandidateId int) {
	rf.state = follower
	rf.UpdatePersistentState(term, CandidateId, nil)
	rf.DestroyLeaderSession()
}

func (rf *Raft) AssignNextIndex(nextIndex int) {
	for range rf.peers {
		rf.nextIndex = append(rf.nextIndex, nextIndex)
	}
}

func (rf *Raft) DestroyLeaderSession() {
	rf.nextIndex = nil
	rf.matchIndex = nil
}

func (rf *Raft) getLogIndex(Index int) int {
	if Index == -1 {
		return rf.SnapshotIndex
	}
	return rf.log[Index].Index
}

func (rf *Raft) getIndexTerm(Index int) int {
	if Index == -1 {
		return -1
	} else if rf.SnapshotIndex == Index {
		return rf.SnapshotIndex
	}
	firstLogIndex := rf.log[0].Index
	return rf.log[Index-firstLogIndex].Term
}

func (rf *Raft) sendEntry() bool {
	rf.mu.Lock()
	term := rf.currentTerm
	LeaderId := rf.me
	leaderCommit := rf.commitIndex
	lastEntryIndex := len(rf.log)
	rf.mu.Unlock()

	cond := sync.NewCond(&rf.mu)

	peerDone := 1
	peerLength := len(rf.peers)
	majority := peerLength/2 + 1
	success := 1

	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}
		go func(peer int) {
			for {
				rf.mu.Lock()
				if rf.state != leader {
					peerDone++
					rf.mu.Unlock()
					break
				}
				entries := rf.getEntry(peer, lastEntryIndex)
				PrevLogIndex := rf.nextIndex[peer] - 1
				PrevLogTerm := nilInt
				if PrevLogIndex >= 0 {
					PrevLogTerm = rf.log[PrevLogIndex].Term
				}
				rf.mu.Unlock()
				args := AppendEntryArgs{term, LeaderId, PrevLogIndex, PrevLogTerm, entries, leaderCommit}
				reply := AppendEntryReply{}
				ok := rf.sendAppendEntry(peer, &args, &reply)
				rf.mu.Lock()
				if ok {
					if rf.state != leader {
						rf.mu.Unlock()
						continue
					}
					if !reply.Success {
						if reply.Term > rf.currentTerm {
							rf.convertToFollower(reply.Term, nilInt)
						} else {
							rf.nextIndex[peer]--
							rf.mu.Unlock()
							continue
						}
					} else {
						success++
						rf.nextIndex[peer] = lastEntryIndex
					}
				}
				peerDone++
				cond.Broadcast()
				rf.mu.Unlock()
				break
			}
		}(peer)
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	for {
		if rf.state != leader || (peerLength-peerDone) < (majority-success) {
			rf.lastAppendEntryTime = time.Now()
			return false
		} else if success >= majority {
			if rf.commitIndex < lastEntryIndex-1 {
				rf.startCommit(lastEntryIndex - 1)
			}
			rf.lastAppendEntryTime = time.Now()
			return true
		}
		cond.Wait()
	}
}

func (rf *Raft) getEntryUpTo(Index int) []logEntry {
	if len(rf.log) == 0 {
		return nil
	}
	firstIndex := rf.log[0].Index
	if rf.log[0].Index > Index {
		return nil
	}
	return rf.log[:Index-firstIndex+1]
}

func (rf *Raft) getEntry(peer int, lastEntryIndex int) []logEntry {
	var entries []logEntry
	for i := rf.nextIndex[peer]; i < lastEntryIndex; i++ {
		rf.log[i].Term = rf.currentTerm
		entries = append(entries, rf.log[i])
	}
	return entries
	// return rf.log[rf.nextIndex[peer]:lastEntryIndex]
}

func (rf *Raft) startCommit(CommitIndex int) {
	rf.commitIndex = CommitIndex
	rf.applyEntry(rf.applyCh)
}

func (rf *Raft) applyEntry(applyCh chan ApplyMsg) {
	// rf.mu.Lock()
	// defer rf.mu.Unlock()
	// DPrintf("%v,%v,%v,%v", rf.me, rf.lastApplied, rf.commitIndex, rf.log)
	firstIndex := rf.log[0].Index
	for rf.lastApplied < rf.commitIndex {
		rf.lastApplied++
		// DPrintf("%v,%v applied", rf.me, rf.lastApplied)
		CommandIndex := rf.lastApplied - firstIndex
		applyLog := ApplyMsg{CommandValid: true, Command: rf.log[CommandIndex].Command, CommandIndex: CommandIndex + 1}
		applyCh <- applyLog
	}
}

// func (rf *Raft) savePersistent() {
// 	for {
// 		// Your Implementation for storing persistent storage if their is any changes
// 		rf.persistentMu.Lock()
// 		rf.persistentCond.Wait()
// 		rf.persist()
// 		rf.persistentMu.Unlock()
// 	}
// }

func (rf *Raft) UpdatePersistentState(term int, votedFor int, log []logEntry) {
	rf.currentTerm = term
	rf.votedFor = votedFor
	rf.log = append(rf.log, log...)
	rf.persist()
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
	index := nilInt
	term := nilInt
	isLeader := false
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state == leader {
		index = rf.getLogIndex(len(rf.log)-1) + 1
		term = rf.currentTerm
		isLeader = true
		var log []logEntry
		log = append(log, logEntry{term, index, command})
		rf.UpdatePersistentState(term, rf.votedFor, log)
	}
	return index + 1, term, isLeader
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
	for rf.killed() == false {

		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		electionTimeOut := MinElecTimout + int64(rand.Intn(240))
		time.Sleep(50 * time.Millisecond)
		rf.mu.Lock()
		if time.Since(rf.lastAppendEntryTime).Milliseconds() >= electionTimeOut {
			rf.mu.Unlock()
			rf.KickStartElection()
		} else {
			rf.mu.Unlock()
		}
	}
}

func (rf *Raft) heartbeat() {
	for rf.killed() == false {
		time.Sleep(20 * time.Millisecond)
		rf.mu.Lock()
		if rf.state == leader && time.Since(rf.lastAppendEntryTime).Milliseconds() >= AppendInterval {
			rf.mu.Unlock()
			rf.sendEntry()
		} else {
			rf.mu.Unlock()
		}
	}
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
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{
		peers:     peers,
		persister: persister,
		me:        me,

		state:               follower,
		lastAppendEntryTime: time.Now(),
		applyCh:             applyCh,
		commitIndex:         nilInt,
		lastApplied:         nilInt,
		SnapshotTerm:        nilInt,
		SnapshotIndex:       nilInt,
	}

	// Your initialization code here (2A, 2B, 2C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	// start ticker goroutine to start elections
	go rf.ticker()

	go rf.heartbeat()

	// go rf.savePersistent()

	return rf
}
