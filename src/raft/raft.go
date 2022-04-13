package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, Term, isleader)
//   start agreement on a new log entry
// rf.GetState() (Term, isLeader)
//   ask a Raft for its current Term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"6.824/tools"
	"math/rand"
	//	"bytes"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labrpc"
)

const (
	Leader    int32 = 1
	Follower  int32 = 2
	Candidate int32 = 3

	ElectionTimeout             = 300 * time.Millisecond
	ElectionTimeoutSwellCeiling = 150
	HeartBeatPeriod             = 140 * time.Millisecond

	ColvTZziDebug = true
)

const (
	dClient  tools.LogTopic = "CLNT"
	dCommit  tools.LogTopic = "CMIT"
	dDrop    tools.LogTopic = "DROP"
	dError   tools.LogTopic = "ERRO"
	dInfo    tools.LogTopic = "INFO"
	dLeader  tools.LogTopic = "LEAD"
	dLog     tools.LogTopic = "LOG1"
	dLog2    tools.LogTopic = "LOG2"
	dPersist tools.LogTopic = "PERS"
	dSnap    tools.LogTopic = "SNAP"
	dTerm    tools.LogTopic = "TERM"
	dTest    tools.LogTopic = "TEST"
	dTImer   tools.LogTopic = "TIMR"
	dTrace   tools.LogTopic = "TRCE"
	dVote    tools.LogTopic = "VOTE"
	dWarn    tools.LogTopic = "WARN"
)

// ApplyMsg
// as each Raft peer becomes aware that successive log Entries are
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

// Raft
// A Go object implementing a single Raft peer.
//
type Raft struct {
	muPeers   sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int32               // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// Persistent state on all servers (Updated on stable storage before responding to RPCs)
	currentTerm *tools.ConcurrentVarInt32 // latest Term server has seen (initialized to 0 on first boot, increases monotonically)
	votedFor    int32                     // CandidateId that received vote in current Term (or null if none)
	log         []LogEntry                // log Entries; each entry contains command for state machine, and Term when entry was received by leader(first index is 1) ??

	// Volatile state on all servers
	commitIndex int // index of the highest log entry known to be committed (initialized to 0, increases monotonically)
	lastApplied int // index of the highest log entry applied to state machine (initialized to 0, increases monotonically)

	// Volatile state on leaders (Reinitialized after election)
	nextIndex  []int // for each server, index of the next log entry to send to that server(initialized to leader last log index + 1)
	matchIndex []int // for each server, index of the highest log entry known to be replicated on server(initialized to 0, increases monotonically)

	// Leader election
	role  *tools.ConcurrentVarInt32
	alive bool // should read the latest data

	// Log Replication
	applyCh chan ApplyMsg

	// appendEntriesWorker
	goAhead    bool
	workerCond *sync.Cond
}

// LogEntry
// todo
type LogEntry struct {
	Term    int32
	Command interface{}
}

// GetState
// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	term = int(rf.currentTerm.Read())

	isleader = rf.role.Read() == Leader

	return term, isleader
}

// persist
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
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

// readPersist
// restore previously persisted state.
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

// RequestVoteArgs
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int32 // candidate's Term
	CandidateId  int32 // candidate requesting vote
	LastLogIndex int   // index of candidate's last log entry
	LastLogTerm  int32 // Term of candidate's last log entry
}

// RequestVoteReply
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int32 // currentTerm, for candidate to update itself
	VoteGranted bool  // True means candidate received vote
}

// RequestVote
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).

	// Be careful:
	// 		if one server's current term is smaller than the other's, then it updates its current term to the large
	// value. If a candidate or leader discovers that its term is out of date, it immediately reverts to follower
	// state. If a server receives a request with a stale term number, it rejects the request.

	// 1. Reply false if Term < currentTerm
	if args.Term < rf.currentTerm.Read() {
		reply.Term = rf.currentTerm.Read()
		reply.VoteGranted = false
		return
	}

	if rf.currentTerm.SmallerAndSet(args.Term) {
		rf.role.Write(Follower)
		atomic.StoreInt32(&rf.votedFor, -1)
		rf.alive = true
	}

	// 2. If votedFor is null or CandidateId, and candidate's log is at least as up-to-date as receiver's log, grant vote
	// Election restriction specification:
	// 		Raft determines which of two logs is more up-to-date by comparing the index and term of the last entries in
	// the logs. If the logs have last entries with different terms, then the log with the later term is more up-to-date,
	// If the logs end with the same term, then whichever log is longer is more up-to-date.

	if (args.LastLogTerm > rf.log[len(rf.log)-1].Term || (args.LastLogTerm == rf.log[len(rf.log)-1].Term && args.LastLogIndex >= len(rf.log)-1)) && (atomic.CompareAndSwapInt32(&rf.votedFor, -1, args.CandidateId) || rf.votedFor == args.CandidateId) {
		tools.Debug(dVote, "S%v Granting Vote to S%v at T%v\n", rf.me, args.CandidateId, rf.currentTerm.Read())
		rf.role.Write(Follower)
		rf.alive = true
		reply.Term = rf.currentTerm.Read()
		reply.VoteGranted = true
	} else {
		reply.VoteGranted = false
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

// AppendEntriesArgs
// Invoked by leader to replicated log Entries; also used as heartbeat
type AppendEntriesArgs struct {
	Term         int32      // leader's Term
	LeaderId     int32      // so follower can redirect clients
	PrevLogIndex int        // index of log entry immediately preceding new ones
	PrevLogTerm  int32      // Term of PrevLogIndex entry
	Entries      []LogEntry // log Command to store (empty for heartbeat; may send more than one for efficiency)
	LeaderCommit int        // leader's commitIndex
}

// AppendEntriesReply
// correspond to AppendEntriesArgs
type AppendEntriesReply struct {
	Term    int32 // currentTerm, for leader to update itself
	Success bool  // true if follower contained entry matching PrevLogIndex and PrevLogTerm
}

// AppendEntries
// AppendEntries RPC handler.
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// Receiver implementation
	// 1. Reply false if Term < currentTerm
	if args.Term < rf.currentTerm.Read() {
		reply.Term = rf.currentTerm.Read()
		reply.Success = false
		return
	}

	rf.alive = true
	if rf.currentTerm.SmallerAndSet(args.Term) {
		rf.role.Write(Follower)
		atomic.StoreInt32(&rf.votedFor, -1)
	}

	reply.Term = rf.currentTerm.Read()

	// 2. Reply false if log doesn't contain an entry at PrevLogIndex whose Term matches preLogTerm
	if args.PrevLogIndex >= len(rf.log) || args.PrevLogTerm != rf.log[args.PrevLogIndex].Term {
		reply.Success = false
		return
	}

	// 3. If an existing entry conflicts with a new one (same index but different terms), delete the existing entry and all that follow it
	// 4. Append any new Entries not already in the log
	reply.Success = true
	rf.log = rf.log[:args.PrevLogIndex+1]
	rf.log = append(rf.log, args.Entries[:]...)

	// 5. If LeaderCommit > commitIndex, set commitIndex = min(LeaderCommit, index of last new entry)
	if args.LeaderCommit > rf.commitIndex {
		if args.LeaderCommit > len(rf.log)-1 {
			rf.commitIndex = len(rf.log) - 1
		} else {
			rf.commitIndex = args.LeaderCommit
		}

		for rf.lastApplied < rf.commitIndex {
			rf.lastApplied++
			rf.applyCh <- ApplyMsg{
				CommandValid:  true,
				Command:       rf.log[rf.lastApplied].Command,
				CommandIndex:  rf.lastApplied,
				SnapshotValid: false,
				Snapshot:      nil,
				SnapshotTerm:  0,
				SnapshotIndex: 0,
			}
		}
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// Start
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. Otherwise, start the
// agreement and return immediately. Even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// Term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).
	if !rf.role.IsEqual(Leader) {
		isLeader = false
	} else {
		index = len(rf.log)

		term = int(rf.currentTerm.Read())

		// there is no guarantee that this command will ever be
		// committed to the Raft log, since the leader may fail
		// or lose an election.
		rf.log = append(rf.log, LogEntry{rf.currentTerm.Read(), command})

		rf.workerCond.L.Lock()
		rf.goAhead = true
		rf.workerCond.L.Unlock()
		rf.workerCond.Signal()
	}

	return index, term, isLeader
}

// Kill
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

// ticker
// The ticker go routine starts a new election if this peer hasn't received
// heartbeats recently.
func (rf *Raft) ticker() {
	for rf.killed() == false {
		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		if rf.role.IsEqual(Leader) || rf.alive {
			rf.alive = false
		} else {
			go rf.election()
		}

		time.Sleep(ElectionTimeout + time.Duration(rand.Int()%(ElectionTimeoutSwellCeiling+1))*time.Millisecond)
	}
}

// election
// choose a new leader
func (rf *Raft) election() {
	rf.currentTerm.AddOne()
	rf.role.Write(Candidate)
	rf.votedFor = rf.me

	mu := sync.Mutex{}
	poll := 1

	requestVote := RequestVoteArgs{
		Term:         rf.currentTerm.Read(),
		CandidateId:  rf.me,
		LastLogIndex: len(rf.log) - 1,
		LastLogTerm:  rf.log[len(rf.log)-1].Term,
	}

	replies := make([]RequestVoteReply, len(rf.peers))

	// wait for receiving all replies
	waitGroup := sync.WaitGroup{}
	waitGroup.Add(len(rf.peers) - 1)

	for server, _ := range rf.peers {
		if !rf.role.IsEqual(Candidate) {
			return
		}

		if server == int(rf.me) {
			continue
		}

		go func(s int) {
			ok := rf.sendRequestVote(s, &requestVote, &replies[s])

			if ok && rf.role.IsEqual(Candidate) {
				if !replies[s].VoteGranted {
					if rf.currentTerm.SmallerAndSet(replies[s].Term) {
						rf.role.Write(Follower)
						atomic.StoreInt32(&rf.votedFor, -1)
						rf.alive = true
					}
				} else if replies[s].Term == rf.currentTerm.Read() {
					tools.Debug(dVote, "S%v <- S%v Got vote\n", rf.me, s)
					mu.Lock()
					poll++
					mu.Unlock()
				}
			}

			waitGroup.Done()
		}(server)
	}

	waitGroup.Wait()
	if poll > len(rf.peers)/2 {
		tools.Debug(dLeader, "S%v Achieved Majority for T%v (%v), converting to Leader\n", rf.me, rf.currentTerm.Read(), poll)

		rf.role.Write(Leader)
		rf.nextIndex = make([]int, len(rf.peers))
		for server, _ := range rf.nextIndex {
			rf.nextIndex[server] = len(rf.log)
		}

		rf.matchIndex = make([]int, len(rf.peers))

		for rf.role.IsEqual(Leader) {
			tools.Debug(dTImer, "S%v Leader, checking heartbeats\n", rf.me)
			rf.heartBeat()
			time.Sleep(HeartBeatPeriod)
		}
	}
}

func (rf *Raft) heartBeat() {
	// sends heartBeat message to all the other servers to establish its authority and prevent new elections
	for server, _ := range rf.peers {
		if server == int(rf.me) {
			continue
		}

		go func(s int) {
			appendEntriesArgs := AppendEntriesArgs{rf.currentTerm.Read(),
				rf.me,
				rf.nextIndex[s] - 1,
				rf.log[rf.nextIndex[s]-1].Term,
				rf.log[rf.nextIndex[s]:],
				rf.commitIndex}

			appendEntriesReply := AppendEntriesReply{}
			ok := rf.sendAppendEntries(s, &appendEntriesArgs, &appendEntriesReply)

			if ok {
				if appendEntriesReply.Success {
					rf.nextIndex[s] = len(rf.log)
					rf.matchIndex[s] = len(rf.log) - 1
				} else {
					if rf.currentTerm.SmallerAndSet(appendEntriesReply.Term) {
						atomic.StoreInt32(&rf.votedFor, -1)
						rf.role.Write(Follower)
						rf.alive = true
					} else if rf.role.IsEqual(Leader) {
						// backing and forwarding log
						if rf.nextIndex[s] > 1 {
							rf.nextIndex[s]--
						}
					}
				}
			}
		}(server)
	}
}

func (rf *Raft) appendEntriesWorker() {
	for true {
		rf.workerCond.L.Lock()

		for !rf.goAhead {
			rf.workerCond.Wait()
		}

		rf.workerCond.L.Unlock()
		rf.goAhead = false
		rf.appendEntries()
	}
}

func (rf *Raft) appendEntries() {
	mu := sync.Mutex{}
	shortFall := len(rf.peers) / 2

	for server, _ := range rf.peers {
		if server == int(rf.me) {
			continue
		}

		go func(s int) {
			for rf.role.IsEqual(Leader) {
				appendEntriesArgs := AppendEntriesArgs{rf.currentTerm.Read(),
					rf.me,
					rf.nextIndex[s] - 1,
					rf.log[rf.nextIndex[s]-1].Term,
					rf.log[rf.nextIndex[s]:],
					rf.commitIndex}

				appendEntriesReply := AppendEntriesReply{}
				ok := rf.sendAppendEntries(s, &appendEntriesArgs, &appendEntriesReply)

				if ok {
					if appendEntriesReply.Success {
						mu.Lock()
						shortFall--
						mu.Unlock()
						rf.nextIndex[s] = len(rf.log)
						rf.matchIndex[s] = len(rf.log) - 1
						break
					} else {
						if rf.currentTerm.SmallerAndSet(appendEntriesReply.Term) {
							atomic.StoreInt32(&rf.votedFor, -1)
							rf.role.Write(Follower)
							rf.alive = true
						} else if rf.role.IsEqual(Leader) {
							// backing and forwarding log
							rf.nextIndex[s]--
						}
					}
				}
			}
		}(server)
	}

	// wait majority Follower return true or become Follower
	for rf.role.IsEqual(Leader) {
		// Don't have these loops execute continuously without pausing, since that will
		// slow your implementation enough that it fails tests
		time.Sleep(10 * time.Millisecond)

		if shortFall <= 0 {
			//Should we double-check
			rf.commitIndex = len(rf.log) - 1
			tools.Debug(dCommit, "S%v commit entries from previous terms, lastCommit %v\n", rf.me, rf.commitIndex)
			for rf.lastApplied < rf.commitIndex {
				rf.lastApplied++
				rf.applyCh <- ApplyMsg{
					CommandValid:  true,
					Command:       rf.log[rf.lastApplied].Command,
					CommandIndex:  rf.lastApplied,
					SnapshotValid: false,
					Snapshot:      nil,
					SnapshotTerm:  0,
					SnapshotIndex: 0,
				}
			}
			break
		}
	}
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
//
func Make(peers []*labrpc.ClientEnd, me int, persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = int32(me)

	// Your initialization code here (2A, 2B, 2C).
	rf.currentTerm = new(tools.ConcurrentVarInt32)
	rf.votedFor = -1
	rf.log = make([]LogEntry, 1)

	rf.commitIndex = 0
	rf.lastApplied = 0

	rf.role = new(tools.ConcurrentVarInt32)
	rf.role.Write(Follower)
	rf.alive = false

	rf.applyCh = applyCh

	rf.workerCond = sync.NewCond(new(sync.Mutex))
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	// worker queue for AppendEntries
	go rf.appendEntriesWorker()

	return rf
}
