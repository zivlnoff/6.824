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
	"6.824/labgob"
	"6.824/tools"
	"bytes"
	"log"
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

	HEARTBEAT     = 1
	APPENDENTRIES = 2

	CheckPollPeriod             = 2 * time.Microsecond
	ElectionTimeout             = 300 * time.Millisecond
	ElectionTimeoutSwellCeiling = 150

	HeartBeatPeriod = 130 * time.Millisecond
	ApplyPeriod     = 2 * time.Millisecond

	AppendPeriod     = 30 * time.Millisecond
	AppendRPCTimeout = 150 * time.Millisecond

	CASSleepTime        = 1 * time.Millisecond
	NetworkCrashTimeout = 100 * time.Millisecond
	SnapshotRPCTimeout  = 150 * time.Millisecond
)

const (
	dClient  tools.LogTopic = "CLNT"
	dCommit  tools.LogTopic = "CMIT"
	dDrop    tools.LogTopic = "DROP"
	dError   tools.LogTopic = "ERRO"
	dInfo    tools.LogTopic = "INFO"
	dLeader  tools.LogTopic = "LEAD"
	dLog1    tools.LogTopic = "LOG1"
	dApply   tools.LogTopic = "APPY"
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
type ApplyMsg struct {
	// Apply
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// Snapshot
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

// Raft
// A Go object implementing a single Raft peer.
type Raft struct {
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int32               // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Persistent state on all servers (Updated on stable storage before responding to RPCs)
	currentTerm *tools.ConcurrentVarInt32 // latest Term server has seen (initialized to 0 on first boot, increases monotonically)
	votedFor    int32                     // CandidateId that received vote in current Term (or null if none)

	// log
	muLog             sync.Mutex // prevent compete resource
	log               []LogEntry // log Entries; each entry contains command for state machine, and Term when entry was received by leader(first index is 1) ??
	lastIncludedIndex int
	lastIncludedTerm  int32

	// Volatile state on all servers
	commitIndex int   // index of the highest log entry known to be committed (initialized to 0, increases monotonically)
	lastApplied int   // index of the highest log entry applied to state machine (initialized to 0, increases monotonically)
	muAppend    int32 // for AppendEntries to prevent stale reply
	muCommit    int32 // for CommitCompute

	// Volatile state on leaders (Reinitialized after election)
	nextIndex         []int // for each server, index of the next log entry to send to that server(initialized to leader last log index + 1)
	matchIndex        []int // for each server, index of the highest log entry known to be replicated on server(initialized to 0, increases monotonically)
	computeCommit     bool
	computeCommitCond sync.Cond
	goAhead           []bool
	muGoAhead         sync.Mutex

	// Leader election
	role  *tools.ConcurrentVarInt32
	alive bool // should read the latest data

	// Log Replication
	muApply sync.Mutex
	applyCh chan ApplyMsg
}

// LogEntry
// store the information about each received Command
type LogEntry struct {
	Index   int
	Term    int32
	Command interface{}
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
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.currentTerm.Lock()
	rf.role.Lock()
	rf.muLog.Lock()
	rf.muGoAhead.Lock()
	defer rf.role.UnLock()
	defer rf.currentTerm.UnLock()
	defer rf.muLog.Unlock()
	defer rf.muGoAhead.Unlock()

	if rf.role.ReadNoLock() != Leader {
		return -1, -1, false
	} else {
		term := int(rf.currentTerm.ReadNoLock())
		index := rf.log[len(rf.log)-1].Index + 1
		rf.log = append(rf.log, LogEntry{index, rf.currentTerm.ReadNoLock(), command})
		rf.persist()

		rf.matchIndex[rf.me] = index

		for server, _ := range rf.peers {
			if server == int(rf.me) {
				continue
			}
			rf.goAhead[server] = true
		}

		return index, term, true
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
func Make(peers []*labrpc.ClientEnd, me int, persister *Persister, applyCh chan ApplyMsg) *Raft {
	var rf *Raft
	if persister == nil || persister.RaftStateSize() < 1 {
		// start a initial state raft
		rf = &Raft{
			peers:       peers,
			persister:   persister,
			me:          int32(me),
			dead:        0,
			currentTerm: tools.NewConcurrentVarInt32(0),
			votedFor:    -1,
			muLog:       sync.Mutex{},
			log:         make([]LogEntry, 1),
			muAppend:    1,
			muCommit:    1,
			nextIndex:   make([]int, len(peers)),
			matchIndex:  make([]int, len(peers)),
			goAhead:     make([]bool, len(peers)),
			muGoAhead:   sync.Mutex{},
			role:        tools.NewConcurrentVarInt32(Follower),
			alive:       true,
			muApply:     sync.Mutex{},
			applyCh:     applyCh,
		}
		rf.persist()
	} else {
		// initialize from state persisted before a crash
		rf = &Raft{
			peers:      peers,
			persister:  persister,
			me:         int32(me),
			dead:       0,
			muLog:      sync.Mutex{},
			muAppend:   1,
			muCommit:   1,
			nextIndex:  make([]int, len(peers)),
			matchIndex: make([]int, len(peers)),
			goAhead:    make([]bool, len(peers)),
			muGoAhead:  sync.Mutex{},
			role:       tools.NewConcurrentVarInt32(Follower),
			alive:      true,
			muApply:    sync.Mutex{},
			applyCh:    applyCh,
		}
		rf.readPersist(persister.ReadRaftState())
	}

	// start electionTicker goroutine to start elections
	go rf.electionTicker()

	// start applyTicker goroutine to apply log to the state machine
	go rf.applyTicker()

	return rf
}

// electionTicker
// The electionTicker go routine starts a new election if this peer hasn't received
// heartbeats recently.
func (rf *Raft) electionTicker() {
	for !rf.killed() {
		kill := make(chan bool, 1)
		if rf.role.ReadNoLock() == Leader || rf.alive {
			rf.alive = false
		} else {
			go rf.election(kill)
		}

		time.Sleep(ElectionTimeout + time.Duration(rand.Int()%(ElectionTimeoutSwellCeiling+1))*time.Millisecond)
		kill <- true
	}
}

// election
// choose a new leader
func (rf *Raft) election(kill chan bool) {
	rf.currentTerm.Lock()
	rf.role.Lock()
	rf.muLog.Lock()

	rf.currentTerm.AddOneNoAtomic()
	rf.role.WriteNoLock(Candidate)
	rf.votedFor = rf.me
	rf.persist()
	tools.Debug(dTerm, "S%v Converting to Candidate, calling election T:%v\n", rf.me, rf.currentTerm.ReadNoLock())

	requestVote := RequestVoteArgs{
		Term:         rf.currentTerm.ReadNoLock(),
		CandidateId:  rf.me,
		LastLogIndex: rf.log[len(rf.log)-1].Index,
		LastLogTerm:  rf.log[len(rf.log)-1].Term,
	}

	rf.muLog.Unlock()
	rf.role.UnLock()
	rf.currentTerm.UnLock()

	// Request for Poll
	poll := new(tools.ConcurrentVarInt32)
	poll.AddOne()

	replies := make([]RequestVoteReply, len(rf.peers))

	for server, _ := range rf.peers {
		if server == int(rf.me) {
			continue
		}

		go func(s int) {
			ok := rf.sendRequestVote(s, &requestVote, &replies[s])

			if ok {
				if !replies[s].VoteGranted {
					if oldTerm, ok := rf.currentTerm.SmallerAndSet(replies[s].Term); ok {
						rf.toFollowerByTermUpgrade(oldTerm)
					}
				} else {
					tools.Debug(dVote, "S%v <- S%v Got vote(T%v)\n", rf.me, s, replies[s].Term)
					poll.AddOne()
				}
			}

		}(server)
	}

	// election timeout checker
	for {
		select {
		case <-kill:
			return
		case <-time.After(CheckPollPeriod):
			if rf.role.ReadNoLock() == Follower {
				return
			}

			if poll.ReadNoLock() > int32(len(rf.peers)/2) {
				rf.leader(requestVote.Term)
				return
			}
		}
	}
}

// leader
// change state to Leader and initial data struct
func (rf *Raft) leader(term int32) {
	rf.currentTerm.Lock()
	rf.role.Lock()
	rf.muLog.Lock()
	rf.muGoAhead.Lock()
	defer rf.currentTerm.UnLock()
	defer rf.role.UnLock()
	defer rf.muLog.Unlock()
	defer rf.muGoAhead.Unlock()

	tools.Debug(dLeader, "S%v Achieved Majority for T%v, converting to Leader\n", rf.me, term)
	rf.role.WriteNoLock(Leader)
	rf.computeCommit = false
	rf.computeCommitCond = *sync.NewCond(&sync.Mutex{})

	for server, _ := range rf.peers {
		rf.nextIndex[server] = rf.log[len(rf.log)-1].Index + 1
		// dont't forget this
		rf.matchIndex[server] = 0
		rf.goAhead[server] = false

		if server != int(rf.me) {
			rf.heartBeat(term, server)
			rf.startAppendEntriesWorker(term, server)
		}
	}
}

// heartBeat
// sends heartBeat message to all the other servers to establish its authority and prevent new elections
func (rf *Raft) heartBeat(term int32, server int) {
	rf.sendAppendEntries(server, HEARTBEAT, term)
}

// startAppendEntriesWorker
// append Entries routine
func (rf *Raft) startAppendEntriesWorker(term int32, server int) {
	go func() {
		for !rf.killed() && rf.role.IsEqual(Leader) && term == rf.currentTerm.ReadNoLock() {
			for !rf.goAhead[server] {
				time.Sleep(AppendPeriod)
			}

			// solve the two leader case
			rf.muGoAhead.Lock()
			if term < rf.currentTerm.ReadNoLock() {
				rf.muGoAhead.Unlock()
				return
			}
			rf.goAhead[server] = false
			rf.muGoAhead.Unlock()

			rf.sendAppendEntries(server, APPENDENTRIES, term)
		}
	}()

	rf.commitTicker(term)
}

// computeCommit
// set up a goroutine to find the CommitIndex
func (rf *Raft) commitTicker(term int32) {
	go func() {
		for !rf.killed() && rf.role.IsEqual(Leader) && term == rf.currentTerm.ReadNoLock() {
			rf.computeCommitCond.L.Lock()
			if !rf.computeCommit {
				rf.computeCommitCond.Wait()
			}
			rf.computeCommit = false
			rf.computeCommitCond.L.Unlock()

			for !atomic.CompareAndSwapInt32(&rf.muCommit, 1, 0) {
				time.Sleep(CASSleepTime)
			}

			// double check
			if term != rf.currentTerm.ReadNoLock() {
				atomic.AddInt32(&rf.muCommit, 1)
				return
			}

			copyData := make([]int, len(rf.peers))
			copy(copyData, rf.matchIndex)
			curMaxCommit := tools.Quick_select(copyData, len(rf.peers))

			if curMaxCommit <= rf.commitIndex {
				atomic.AddInt32(&rf.muCommit, 1)
				continue
			}

			rf.commitIndex = curMaxCommit
			tools.Debug(dCommit, "S%v commit entries from previous terms, lastCommit %v\n", rf.me, rf.commitIndex)

			atomic.AddInt32(&rf.muCommit, 1)
		}
	}()
}

type RequestVoteArgs struct {
	Term         int32 // candidate's Term
	CandidateId  int32 // candidate requesting vote
	LastLogIndex int   // index of candidate's last log entry
	LastLogTerm  int32 // Term of candidate's last log entry
}

type RequestVoteReply struct {
	Term        int32 // currentTerm, for candidate to update itself
	VoteGranted bool  // True means candidate received vote
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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

// RequestVote
// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// 1. Reply false if Term < currentTerm
	if args.Term < rf.currentTerm.ReadNoLock() {
		reply.Term = rf.currentTerm.ReadNoLock()
		reply.VoteGranted = false
		return
	}

	if oldTerm, ok := rf.currentTerm.SmallerAndSet(args.Term); ok {
		rf.toFollowerByTermUpgrade(oldTerm)
	}

	// 2. If votedFor is null or CandidateId, and candidate's log is at least as up-to-date as receiver's log, grant vote
	rf.currentTerm.Lock()
	rf.role.Lock()
	rf.muLog.Lock()
	defer rf.currentTerm.Unlock()
	defer rf.role.Unlock()
	defer rf.muLog.Unlock()

	reply.Term = rf.currentTerm.ReadNoLock()
	if (args.LastLogTerm > rf.log[len(rf.log)-1].Term ||
		(args.LastLogTerm == rf.log[len(rf.log)-1].Term && args.LastLogIndex >= rf.log[len(rf.log)-1].Index)) &&
		(atomic.CompareAndSwapInt32(&rf.votedFor, -1, args.CandidateId) || rf.votedFor == args.CandidateId) {
		rf.role.WriteNoLock(Follower)
		rf.alive = true
		rf.persist()
		reply.VoteGranted = true
		tools.Debug(dVote, "S%v Granting Vote to S%v at T%v\n", rf.me, args.CandidateId, rf.currentTerm.ReadNoLock())
	} else {
		reply.VoteGranted = false
	}
}

// AppendEntriesArgs
// Invoked by leader to replicated log Entries; also used as heartbeat
type AppendEntriesArgs struct {
	Term         int32 // leader's Term
	LeaderId     int32
	PrevLogIndex int        // index of log entry immediately preceding new ones
	PrevLogTerm  int32      // Term of PrevLogIndex entry
	Entries      []LogEntry // log Command to store (empty for heartbeat; may send more than one for efficiency)
	LeaderCommit int        // leader's commitIndex
}

// AppendEntriesReply
// correspond to AppendEntriesArgs
type AppendEntriesReply struct {
	ConflictIndex int   // conflict Index, always sit together with Term
	Term          int32 // currentTerm, for leader to update itself
	Success       bool  // true if follower contained entry matching PrevLogIndex and PrevLogTerm
}

func (rf *Raft) sendAppendEntries(server int, tp int, term int32) {
	switch tp {
	case HEARTBEAT:
		go func() {
			for !rf.killed() && rf.role.ReadNoLock() == Leader && term == rf.currentTerm.ReadNoLock() {
				tools.Debug(dTImer, "S%v -> S%v checking Heartbeats T%v\n", rf.me, server, term)
				args := AppendEntriesArgs{
					LeaderId:     rf.me,
					Term:         term,
					LeaderCommit: rf.commitIndex}

				reply := AppendEntriesReply{}

				go func() {
					rpcOk := rf.peers[server].Call("Raft.AppendEntries", &args, &reply)
					if rpcOk {
						if oldTerm, ok := rf.currentTerm.SmallerAndSet(reply.Term); ok {
							rf.toFollowerByTermUpgrade(oldTerm)
						}
					} else {
						tools.Debug(dTImer, "S%v -> S%v heartBeat encounter Network Crash\n", rf.me, server)
					}
				}()

				time.Sleep(HeartBeatPeriod)
			}
		}()
	case APPENDENTRIES:
		for !rf.killed() && rf.role.ReadNoLock() == Leader {
			appendRPC := make(chan bool, 1)

			if rf.nextIndex[server] <= rf.lastIncludedIndex {
				rf.sendInstallSnapshot(server)
			}

			rf.muLog.Lock()
			if rf.nextIndex[server] <= rf.lastIncludedIndex {
				rf.muLog.Unlock()
				continue
			}

			args := AppendEntriesArgs{term,
				rf.me,
				rf.nextIndex[server] - 1,
				rf.log[rf.nextIndex[server]-1-rf.lastIncludedIndex].Term,
				rf.log[rf.nextIndex[server]-rf.lastIncludedIndex:],
				rf.commitIndex}
			rf.muLog.Unlock()

			if args.Entries == nil || len(args.Entries) == 0 {
				break
			}

			reply := AppendEntriesReply{}

			// Asynchronous send RPC
			go func() {
				tools.Debug(dLeader, "S%v -> S%v Append PLI: %v PLT: %v N: %v LC: %v - %v\n", rf.me, server, args.PrevLogIndex, args.PrevLogTerm, len(args.Entries), args.LeaderCommit, args.Entries[len(args.Entries)-1])
				appendRPC <- rf.peers[server].Call("Raft.AppendEntries", &args, &reply)
				close(appendRPC)
			}()

			select {
			case ok := <-appendRPC:
				if !ok {
					time.Sleep(NetworkCrashTimeout)
					continue
				} else {
					for !atomic.CompareAndSwapInt32(&rf.muAppend, 1, 0) {
						time.Sleep(CASSleepTime)
					}

					if term != rf.currentTerm.ReadNoLock() /* prevent the stale leader*/ {
						atomic.AddInt32(&rf.muAppend, 1)
						return
					}

					if reply.Success {
						rf.nextIndex[server] = args.Entries[len(args.Entries)-1].Index + 1
						rf.matchIndex[server] = rf.nextIndex[server] - 1
						tools.Debug(dLeader, "S%v <- S%v Append success NextIndex: %v MatchIndex %v\n", rf.me, server, rf.nextIndex[server], rf.matchIndex[server])

						rf.computeCommitCond.L.Lock()
						rf.computeCommit = true
						rf.computeCommitCond.Signal()
						rf.computeCommitCond.L.Unlock()

						atomic.AddInt32(&rf.muAppend, 1)
						break
					} else {
						if oldTerm, ok := rf.currentTerm.SmallerAndSet(reply.Term); ok == true {
							rf.toFollowerByTermUpgrade(oldTerm)
						} else {
							tools.Debug(dLeader, "S%v <- S%v CFI: %v CFT: %v\n", rf.me, server, reply.ConflictIndex, reply.Term)
							rf.backNextIndex(&rf.nextIndex[server], &reply)
							tools.Debug(dLeader, "S%v Resetting S%v NextIndex %v\n", rf.me, server, rf.nextIndex[server])
						}
					}
					atomic.AddInt32(&rf.muAppend, 1)
				}
			case <-time.After(AppendRPCTimeout):
				continue
			}
		}
	}
}

// AppendEntries
// AppendEntries RPC handler.
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	tools.Debug(dTrace, "S%v <- S%v Append  T: %v PLI: %v PLT: %v N: %v LC: %v\n", rf.me, args.LeaderId, args.Term, args.PrevLogIndex, args.PrevLogTerm, len(args.Entries), args.LeaderCommit)
	// 1. Reply false if Term < currentTerm
	if args.Term < rf.currentTerm.ReadNoLock() {
		reply.Term = rf.currentTerm.ReadNoLock()
		reply.Success = false
		return
	}

	if oldTerm, ok := rf.currentTerm.SmallerAndSet(args.Term); ok {
		rf.toFollowerByTermUpgrade(oldTerm)
	} else {
		rf.role.WriteNoLock(Follower)
	}
	rf.alive = true

	switch args.Entries {
	case nil:
		// heartBeats
		rf.muLog.Lock()
		if rf.log[len(rf.log)-1].Index >= args.LeaderCommit && args.LeaderCommit-rf.lastIncludedIndex >= 0 && rf.log[args.LeaderCommit-rf.lastIncludedIndex].Term == args.Term {
			if rf.commitIndex < args.LeaderCommit {
				rf.commitIndex = args.LeaderCommit
				tools.Debug(dCommit, "S%v commit entries from previous terms, lastCommit %v (Follower)\n", rf.me, rf.commitIndex)
			}
		}
		rf.muLog.Unlock()
	default:
		// 2. Reply false if log doesn't contain an entry at PrevLogIndex whose Term matches preLogTerm
		conflictIndex, term, has := rf.ifConflict(args)
		if has {
			reply.ConflictIndex = conflictIndex
			reply.Term = term
			reply.Success = false
			return
		}

		// 3. If an existing entry conflicts with a new one (same index but different terms), delete the existing entry and all that follow it
		// 4. Append any new Entries not already in the log
		rf.currentTerm.Lock()
		rf.muLog.Lock()
		if args.Term >= rf.currentTerm.ReadNoLock() /* prevent stale send */ {
			rf.log = rf.log[:args.PrevLogIndex+1-rf.lastIncludedIndex]
			rf.log = append(rf.log, args.Entries[:]...)
			rf.persist()
		} else {
			reply.Term = rf.currentTerm.ReadNoLock()
			reply.Success = false
		}
		rf.muLog.Unlock()
		rf.currentTerm.Unlock()

		// 5. If LeaderCommit > commitIndex, set commitIndex = min(LeaderCommit, index of last new entry)
		if args.LeaderCommit > rf.commitIndex {
			logLastIndex := rf.log[len(rf.log)-1].Index
			if args.LeaderCommit > logLastIndex {
				rf.commitIndex = logLastIndex
			} else {
				rf.commitIndex = args.LeaderCommit
			}

			tools.Debug(dCommit, "S%v commit entries from previous terms, lastCommit %v (Follower)\n", rf.me, rf.commitIndex)
		}
		reply.Success = true
	}
}

type InstallSnapshotArgs struct {
	Term              int32  // leader's term
	LastIncludedIndex int    // the snapshot replaces all entries up through and including this index
	LastIncludedTerm  int32  // term of lastIncludedIndex
	Data              []byte // raw bytes of the snapshot
}

type InstallSnapshotReply struct {
	Term int32 // for leader to update itself
}

func (rf *Raft) sendInstallSnapshot(server int) {
	rf.currentTerm.Lock()
	rf.muLog.Lock()
	lastIncludedIndex := rf.lastIncludedIndex
	args := InstallSnapshotArgs{
		Term:              rf.currentTerm.ReadNoLock(),
		LastIncludedIndex: lastIncludedIndex,
		LastIncludedTerm:  rf.lastIncludedTerm,
		Data:              rf.persister.ReadSnapshot(),
	}
	rf.muLog.Unlock()
	rf.currentTerm.Unlock()

	reply := InstallSnapshotReply{}

	snapshotRPC := make(chan bool, 1)
	go func() {
		tools.Debug(dSnap, "S%v -> S%v Snapshot LII: %v LIT: %v\n", rf.me, server, args.LastIncludedIndex, args.LastIncludedTerm)
		snapshotRPC <- rf.peers[server].Call("Raft.InstallSnapshot", &args, &reply)
		close(snapshotRPC)
	}()

	select {
	case ok := <-snapshotRPC:
		if !ok {
			time.Sleep(NetworkCrashTimeout)
		} else {
			if oldTerm, ok := rf.currentTerm.SmallerAndSet(reply.Term); ok == true {
				rf.toFollowerByTermUpgrade(oldTerm)
			} else {
				rf.nextIndex[server] = lastIncludedIndex + 1
				rf.matchIndex[server] = rf.nextIndex[server] - 1
				tools.Debug(dLeader, "S%v <- S%v InstallSnapshot success NextIndex: %v MatchIndex: %v\n", rf.me, server, rf.nextIndex[server], rf.matchIndex[server])
			}
		}
	case <-time.After(SnapshotRPCTimeout):
		return
	}

}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.muApply.Lock()
	rf.muLog.Lock()
	defer rf.muApply.Unlock()

	if args.Term < rf.currentTerm.ReadNoLock() {
		return
	}

	if args.LastIncludedIndex <= rf.lastIncludedIndex {
		return
	}

	rf.snapshot(args.LastIncludedIndex, args.LastIncludedTerm, args.Data)
	rf.muLog.Unlock()

	rf.applyCh <- ApplyMsg{
		CommandValid:  false,
		Command:       nil,
		CommandIndex:  0,
		SnapshotValid: true,
		Snapshot:      args.Data,
		SnapshotTerm:  int(args.LastIncludedTerm),
		SnapshotIndex: args.LastIncludedIndex,
	}
	rf.lastApplied = args.LastIncludedIndex

	reply.Term = rf.currentTerm.ReadNoLock()
}

func (rf *Raft) applyTicker() {
	for !rf.killed() {
		for rf.lastApplied < rf.commitIndex {
			rf.muApply.Lock()
			rf.muLog.Lock()

			if rf.lastApplied >= rf.commitIndex {
				rf.muLog.Unlock()
				rf.muApply.Unlock()
				break
			}

			rf.lastApplied++
			command := rf.log[rf.lastApplied-rf.lastIncludedIndex].Command

			rf.muLog.Unlock()

			rf.applyCh <- ApplyMsg{
				CommandValid:  true,
				Command:       command,
				CommandIndex:  rf.lastApplied,
				SnapshotValid: false,
				Snapshot:      nil,
				SnapshotTerm:  0,
				SnapshotIndex: 0,
			}

			rf.muApply.Unlock()
			tools.Debug(dApply, "S%v apply Entry{index: %v, command: %v}\n", rf.me, rf.lastApplied, rf.log[rf.lastApplied-rf.lastIncludedIndex].Command)
		}
		time.Sleep(ApplyPeriod)
	}
}

// GetState
// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.currentTerm.Lock()
	rf.role.Lock()

	defer rf.currentTerm.Unlock()
	defer rf.role.Unlock()

	var term int
	var isleader bool

	term = int(rf.currentTerm.ReadNoLock())
	isleader = rf.role.ReadNoLock() == Leader

	return term, isleader
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
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) toFollowerByTermUpgrade(oldTerm int32) {
	rf.currentTerm.Lock()
	rf.role.Lock()
	rf.muLog.Lock()
	defer rf.currentTerm.Unlock()
	defer rf.role.Unlock()
	defer rf.muLog.Unlock()

	tools.Debug(dTerm, "S%v upgrade Term (%v > %v)\n", rf.me, rf.currentTerm.ReadNoLock(), oldTerm)
	atomic.StoreInt32(&rf.votedFor, -1)
	rf.role.WriteNoLock(Follower)
	rf.persist()
}

// ifConflict
// return conflictIndex, term, has
func (rf *Raft) ifConflict(args *AppendEntriesArgs) (int, int32, bool) {
	rf.muLog.Lock()
	defer rf.muLog.Unlock()

	logLastIndex := rf.log[len(rf.log)-1].Index
	if args.PrevLogIndex > logLastIndex {
		return logLastIndex + 1, -1, true
	} else if args.PrevLogTerm != rf.log[args.PrevLogIndex-rf.lastIncludedIndex].Term {
		firstIndexOfTerm := args.PrevLogIndex - 1
		term := rf.log[firstIndexOfTerm+1-rf.lastIncludedIndex].Term
		for firstIndexOfTerm-rf.lastIncludedIndex > 0 && rf.log[firstIndexOfTerm-rf.lastIncludedIndex].Term == term {
			firstIndexOfTerm--
		}
		return rf.log[firstIndexOfTerm+1-rf.lastIncludedIndex].Index, term, true
	}
	return 0, 0, false
}

func (rf *Raft) backNextIndex(nextIndex *int, reply *AppendEntriesReply) {
	rf.muLog.Lock()
	defer rf.muLog.Unlock()

	if reply.Term == -1 || reply.ConflictIndex <= rf.lastIncludedIndex {
		*nextIndex = reply.ConflictIndex
	} else {
		if rf.log[reply.ConflictIndex-rf.lastIncludedIndex].Term != reply.Term {
			*nextIndex = reply.ConflictIndex
		} else {
			next := reply.ConflictIndex + 1
			for rf.log[next-rf.lastIncludedIndex].Term == reply.Term {
				next++
			}
			*nextIndex = next
		}
	}
}

// persist
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
func (rf *Raft) persist() {
	data := rf.encode()
	rf.persister.SaveRaftState(data)
	tools.Debug(dPersist, "S%v Saved State T:%v VF:%v\n", rf.me, rf.currentTerm.ReadNoLock(), rf.votedFor)
	tools.Debug(dLog1, "S%v saved LE - %v\n", rf.me, rf.log[len(rf.log)-1])
}

func (rf *Raft) encode() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm.ReadNoLock())
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	e.Encode(rf.lastIncludedIndex)
	e.Encode(rf.lastIncludedTerm)
	data := w.Bytes()
	return data
}

// readPersist
// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int32
	var votedFor int32
	var logEntries = new([]LogEntry)
	var lastIncludedIndex int
	var lastIncludedTerm int32
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(logEntries) != nil ||
		d.Decode(&lastIncludedIndex) != nil ||
		d.Decode(&lastIncludedTerm) != nil {
		log.Fatal("readPersist Decode(v) error")
	} else {
		rf.currentTerm = tools.NewConcurrentVarInt32(currentTerm)
		rf.votedFor = votedFor
		rf.log = *logEntries
		rf.lastIncludedIndex = lastIncludedIndex
		rf.lastIncludedTerm = lastIncludedTerm

		rf.lastApplied = rf.lastIncludedIndex
	}
	tools.Debug(dPersist, "S%v restore T:%v VF:%v LLE - %v LII: %v LIT: %v\n", rf.me, rf.currentTerm.ReadNoLock(), rf.votedFor, rf.log[len(rf.log)-1], rf.lastIncludedIndex, rf.lastIncludedTerm)
}

// CondInstallSnapshot
// Deprecated API, just for Test(which is out of date)
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {
	return true
}

// Snapshot
// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	rf.muLog.Lock()
	defer rf.muLog.Unlock()

	if index <= rf.lastIncludedIndex {
		return
	}

	rf.snapshot(index, rf.log[index-rf.lastIncludedIndex].Term, snapshot)
}

func (rf *Raft) snapshot(index int, term int32, snapshot []byte) {
	tailLog := make([]LogEntry, 0)
	if index >= rf.log[len(rf.log)-1].Index {
		rf.log = append(tailLog, LogEntry{
			Index:   index,
			Term:    term,
			Command: nil,
		})
	} else {
		rf.log = append(tailLog, rf.log[index-rf.lastIncludedIndex:]...)
	}
	rf.lastIncludedIndex = index
	rf.lastIncludedTerm = term

	// Save both Raft state and K/V snapshot as a single atomic action, to help avoid them getting out of sync.
	rf.persister.SaveStateAndSnapshot(rf.encode(), snapshot)

	tools.Debug(dSnap, "S%v Snapshot T: %v LE: %v LII: %v LIT: %v\n", rf.me, rf.currentTerm.ReadNoLock(), rf.log[len(rf.log)-1], index, term)
}
