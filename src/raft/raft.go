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
	"fmt"
	"math/rand"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"

	"6.824/labgob"
	"6.824/labrpc"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
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

// // Added: A struct for holding Entries
type Entry struct {
	Term    int
	Command interface{}
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	currentTerm int
	votedFor    int // Which server did this server voted for
	log         []Entry

	commitIndex     int // index of highest log entry known to be committed
	lastApplied     int
	nextIndex       []int // nextIndex[follower] Checking for Inconsistencies
	matchIndex      []int // matchIndex[follower], Checking for Inconsistencies
	numVotes        int
	startTime       time.Time
	timeOutInterval time.Duration

	state      string // Three states of server: follower,candidate,leader
	applyCh    chan ApplyMsg
	applyChArr []chan ApplyMsg

	// status string
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).

	rf.mu.Lock()
	term = rf.currentTerm
	if rf.state == "leader" {
		isleader = true
	}
	rf.mu.Unlock()

	return term, isleader
}

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

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

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

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var log []Entry
	if d.Decode(&currentTerm) != nil || d.Decode(&votedFor) != nil ||
		d.Decode(&log) != nil {
		fmt.Printf("error: %v\n", "Decoding failure")
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.log = make([]Entry, 0)
		rf.log = append(rf.log, log...)
	}
}

// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
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

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	Term         int
	CandidateId  int
	LastlogIndex int
	LastlogTerm  int
	// Your data here (2A, 2B).
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

type AppendEntriesArgs struct {
	// Heartbeat function
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []Entry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term             int
	Success          bool
	MatchingTerm     int
	MatchingIndex    int // First Index Appeared to match
	ConflictingTerm  int
	ConflictingIndex int
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {

	rf.mu.Lock()
	currentTerm := rf.currentTerm

	if args.Term < currentTerm {
		reply.VoteGranted = false
		reply.Term = currentTerm
	} else {
		if args.Term > currentTerm {
			reply.Term = args.Term

			rf.currentTerm = args.Term
			rf.votedFor = -1
			rf.numVotes = 0
			rf.state = "follower"
		}

		state := rf.state
		votedFor := rf.votedFor
		//// Questionable
		if state != "leader" {
			if votedFor == -1 || votedFor == args.CandidateId {
				receiverlastLogIndex := len(rf.log) - 1
				receiverlastLogTerm := rf.log[receiverlastLogIndex].Term

				if args.LastlogTerm > receiverlastLogTerm {
					reply.VoteGranted = true
					rf.startTime = time.Now()
					rf.votedFor = args.CandidateId
				} else if args.LastlogTerm == receiverlastLogTerm {
					if args.LastlogIndex >= receiverlastLogIndex {
						reply.VoteGranted = true
						rf.startTime = time.Now()
						rf.votedFor = args.CandidateId
					} else {
						reply.VoteGranted = false
					}
				} else {
					reply.VoteGranted = false
				}
			}
		}

	}
	// If votedFor is null or candidateId, and candidate’s log is at
	// least as up-to-date as receiver’s log, grant vote (§5.2, §5.4)

	// Raft determines which of two logs is more up-to-date by comparing
	// the index and term of the last entries in the logs. If the logs
	// have last entries with different terms, then the log with the later
	// term is more up-to-date. If the logs end with the same term, then
	// whichever log is longer is more up-to-date.

	rf.persist()
	rf.mu.Unlock()

}

// // Wrapper Function for RequestVote
func (rf *Raft) requestVoteWrapper(server int, args RequestVoteArgs, reply RequestVoteReply) {

	if rf.sendRequestVote(server, &args, &reply) {
		rf.mu.Lock()
		currentTerm := rf.currentTerm
		state := rf.state
		lastLogIndex := len(rf.log) - 1

		if args.Term == currentTerm {
			if reply.Term > currentTerm {
				rf.currentTerm = reply.Term
				rf.state = "follower"
				rf.votedFor = -1
				rf.numVotes = 0
			}

			if reply.Term == currentTerm {
				if state == "candidate" {
					if reply.VoteGranted {
						rf.numVotes++
						numVotes := rf.numVotes
						if numVotes >= int(len(rf.peers)/2)+1 {
							rf.state = "leader"
							for i := 0; i < len(rf.peers); i++ {
								rf.nextIndex[i] = lastLogIndex + 1
								rf.matchIndex[i] = 0
							}
						}
					}
				}
			}
		}

		rf.persist()
		rf.mu.Unlock()

	}
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

// AppendEntries Heartbeat

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {

	// time.Sleep(10 * time.Millisecond)

	rf.mu.Lock()
	currentTerm := rf.currentTerm
	if args.Term < currentTerm {
		reply.Success = false
		reply.Term = currentTerm
	} else {
		rf.startTime = time.Now()

		if args.Term == currentTerm {
			reply.Term = currentTerm

			if rf.state != "follower" {
				rf.state = "follower"
			}
		}

		if args.Term > currentTerm {
			rf.currentTerm = args.Term
			rf.state = "follower"
			rf.votedFor = -1
			rf.numVotes = 0
			reply.Term = args.Term
		}

		state := rf.state

		//// Replicating Log Entries from Leader
		if state == "follower" {
			lastLogIndex := len(rf.log) - 1
			prev_entry_exist := args.PrevLogIndex <= lastLogIndex

			if !prev_entry_exist {
				reply.Success = false
				reply.ConflictingIndex = len(rf.log)
				reply.ConflictingTerm = -1
			} else {
				receiver_entry := rf.log[args.PrevLogIndex]

				if receiver_entry.Term != args.PrevLogTerm {
					reply.Success = false
					reply.ConflictingTerm = receiver_entry.Term

					for i := 1; i <= args.PrevLogIndex; i++ {
						if rf.log[i].Term == reply.ConflictingTerm {
							reply.ConflictingIndex = i
							break
						}
					}

				} else {
					reply.Success = true
				}
			}

			//// Rejecting the one whose prev_entry don't exist or rf.log[args.PrevLogIndex].Term != args.PrevLogTerm
			if reply.Success {
				startIndex := args.PrevLogIndex + 1

				conflictingEntry := false
				nonexistingEntry := false

				entries := make([]Entry, 0)
				entries = append(entries, args.Entries...)

				for leaderIndex := 0; leaderIndex < len(entries); leaderIndex++ {
					followerIndex := leaderIndex + startIndex

					//// If the entries do not exist in the follower log
					if followerIndex > len(rf.log)-1 {
						entries = entries[leaderIndex:]
						nonexistingEntry = true
						break
					}

					//// If the entries have index but conflicting terms
					if rf.log[followerIndex].Term != entries[leaderIndex].Term {
						rf.log = rf.log[:followerIndex]
						entries = entries[leaderIndex:]
						conflictingEntry = true
						break
					}
				}

				if nonexistingEntry || conflictingEntry {
					rf.log = append(rf.log, entries...)
				}

				indexOfLastNewEntry := args.PrevLogIndex + len(args.Entries)

				if args.LeaderCommit > rf.commitIndex {
					if args.LeaderCommit < indexOfLastNewEntry {
						rf.commitIndex = args.LeaderCommit
					} else {
						rf.commitIndex = indexOfLastNewEntry
					}
				}
			}
		}
	}

	rf.persist()
	rf.mu.Unlock()
}

// If desired, the protocol can be optimized to reduce the number of rejected AppendEntries RPCs.
// For example, when rejecting an AppendEntries request, the follower can include the term of the
// conflicting entry and the first index it stores for that term. With this information, the leader
// can decrement nextIndex to bypass all of the conflicting entries in that term; one AppendEntries
//
//	RPC will be required for each term with conflicting entries, rather than one RPC per entry.
//
// In practice, we doubt this optimization is necessary, since failures happen infrequently and
// it is unlikely that there will be many inconsistent entries.
func (rf *Raft) appendEntriesWrapper(server int, args AppendEntriesArgs, reply AppendEntriesReply) {

	//// Differentiate Heartbeat and Log Replicates (How to differentiate)
	if rf.sendAppendEntries(server, &args, &reply) {

		rf.mu.Lock()
		currentTerm := rf.currentTerm
		retry_args := AppendEntriesArgs{}
		retry_reply := AppendEntriesReply{}
		retry := false

		if args.Term == currentTerm {
			if reply.Term > currentTerm {
				rf.currentTerm = reply.Term
				rf.state = "follower"
				rf.votedFor = -1
				rf.numVotes = 0
			} else if reply.Term == currentTerm {
				if reply.Success {
					updatedNextIndex := args.PrevLogIndex + 1 + len(args.Entries)

					if updatedNextIndex > rf.nextIndex[server] {
						rf.nextIndex[server] = updatedNextIndex
						rf.matchIndex[server] = updatedNextIndex - 1

						matchIndexCopyArr := make([]int, len(rf.peers))

						copy(matchIndexCopyArr, rf.matchIndex)
						sort.Ints(matchIndexCopyArr)
						maxMatchIndex := matchIndexCopyArr[int(len(matchIndexCopyArr)/2)]

						if maxMatchIndex > rf.commitIndex && rf.log[maxMatchIndex].Term == rf.currentTerm {
							rf.commitIndex = maxMatchIndex
						}
					}

				} else {
					retry = true
					updatedNextIndex := reply.ConflictingIndex
					foundConflictingTerm := false

					if reply.ConflictingTerm > 0 {
						for j := 1; j < len(rf.log); j++ {
							LogTerm := rf.log[j].Term

							if LogTerm == reply.ConflictingTerm {
								foundConflictingTerm = true
								updatedNextIndex = j

								for k := j + 1; k < len(rf.log); k++ {
									LogTerm := rf.log[k].Term

									if LogTerm == reply.ConflictingTerm {
										updatedNextIndex = k
									} else {
										break
									}
								}
								break
							}
						}
					}

					if foundConflictingTerm {
						updatedNextIndex = updatedNextIndex + 1
					} else {
						updatedNextIndex = reply.ConflictingIndex
					}

					if updatedNextIndex == 0 {
						updatedNextIndex = 1
					}

					rf.nextIndex[server] = updatedNextIndex
					lastLogIndex := len(rf.log) - 1
					follower_nextIndex := rf.nextIndex[server]
					retry_args.Entries = make([]Entry, 0)
					retry_args.Entries = append(retry_args.Entries, rf.log[follower_nextIndex:lastLogIndex+1]...)
					retry_args.LeaderId = rf.me
					retry_args.PrevLogIndex = follower_nextIndex - 1
					retry_args.PrevLogTerm = rf.log[retry_args.PrevLogIndex].Term
					retry_args.Term = rf.currentTerm
				}

			}
		}
		rf.persist()
		rf.mu.Unlock()

		if retry {
			go rf.appendEntriesWrapper(server, retry_args, retry_reply)
		}

	}
}

// Send AppendEntries Heartbeat

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

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
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).
	rf.mu.Lock()
	index = len(rf.log)
	term = rf.currentTerm
	isLeader = rf.state == "leader"

	// fmt.Println("Start(): The log is for current leader id before update: ", rf.me, " is: ")
	// fmt.Println(rf.log)

	if isLeader {

		// fmt.Println("Start(): The log is for current leader id before update: ", rf.me, " is: ")
		// fmt.Println(rf.log)

		rf.log = append(rf.log, Entry{Term: term, Command: command})
		rf.nextIndex[rf.me] = len(rf.log)
		rf.matchIndex[rf.me] = rf.nextIndex[rf.me] - 1

		// fmt.Println("The next index is: ", index)
		// fmt.Println("Start(): The log is for current leader id: ", rf.me, " is: ")
		// fmt.Println(rf.log)
	}
	// fmt.Println("The next index is: ", index)
	// fmt.Println("Start(): The log is for current leader id: ", rf.me, " is: ")
	// fmt.Println(rf.log)

	killed := rf.killed()
	rf.persist()
	rf.mu.Unlock()

	if killed {
		// fmt.Println("Is killed!!!")
		return index, term, isLeader
	}

	if isLeader {
		for i := 0; i < len(rf.peers); i++ {
			if i != rf.me {
				rf.mu.Lock()
				args := AppendEntriesArgs{}
				reply := AppendEntriesReply{}
				follower_nextIndex := rf.nextIndex[i]
				lastLogIndex := len(rf.log) - 1
				args.Entries = make([]Entry, 0)
				args.Entries = append(args.Entries, rf.log[follower_nextIndex:lastLogIndex+1]...)
				args.PrevLogIndex = follower_nextIndex - 1
				args.PrevLogTerm = rf.log[args.PrevLogIndex].Term
				args.LeaderCommit = rf.commitIndex
				args.LeaderId = rf.me
				args.Term = rf.currentTerm
				rf.mu.Unlock()

				if lastLogIndex >= follower_nextIndex {
					go rf.appendEntriesWrapper(i, args, reply)
				}
			}
		}
	}

	return index, term, isLeader
}

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
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.

// State Machine Safety: if a server has applied a log entry at a given index to its state machine,
// no other server will ever apply a different log entry for the same index. §5.4.3

// If commitIndex > lastApplied: increment lastApplied, apply log[lastApplied] to state machine (§5.3)

// If command received from client: append entry to local log, respond after entry applied to state machine

// // Wrapper Function for RPC Hanlder
func (rf *Raft) applyStateMachineWrapper() {
	for rf.killed() == false {
		time.Sleep(10 * time.Millisecond)

		rf.mu.Lock()
		commitIndex := rf.commitIndex
		lastApplied := rf.lastApplied
		applyMsg := ApplyMsg{}
		applyMsgArr := make([]ApplyMsg, 0)

		// fmt.Println("The log is: ", rf.log)

		// if rf.state == "leader" {
		// 	fmt.Println("The log is for current leader id: ", rf.me, " is: ")
		// 	fmt.Println(rf.log)
		// }

		for i := lastApplied; i < commitIndex; i++ {
			rf.lastApplied++
			applyMsg = ApplyMsg{}
			applyMsg.Command = rf.log[rf.lastApplied].Command
			applyMsg.CommandIndex = rf.lastApplied
			applyMsg.CommandValid = true
			applyMsgArr = append(applyMsgArr, applyMsg)
		}

		rf.mu.Unlock()

		// startTime := time.Now()
		for i := 0; i < len(applyMsgArr); i++ {
			rf.applyCh <- applyMsgArr[i]
		}
		// fmt.Println("The interval is: ", time.Since(startTime))
	}
}

// Wrapper Function for SendVote
func (rf *Raft) sendHeartbeat() {
	for rf.killed() == false {
		time.Sleep(100 * time.Millisecond)

		rf.mu.Lock()
		currentState := rf.state
		rf.mu.Unlock()

		if currentState == "leader" {
			for i := 0; i < len(rf.peers); i++ {
				if i != rf.me {
					args := AppendEntriesArgs{}
					reply := AppendEntriesReply{}

					rf.mu.Lock()
					follower_nextIndex := rf.nextIndex[i]
					args.PrevLogIndex = follower_nextIndex - 1
					args.PrevLogTerm = rf.log[args.PrevLogIndex].Term
					args.Entries = make([]Entry, 0)
					args.LeaderCommit = rf.commitIndex
					args.LeaderId = rf.me
					args.Term = rf.currentTerm
					lastLogIndex := len(rf.log) - 1
					follower_nextIndex = rf.nextIndex[i]

					if lastLogIndex >= follower_nextIndex {
						args.Entries = append(args.Entries, rf.log[follower_nextIndex:lastLogIndex+1]...)
					}

					rf.mu.Unlock()

					go rf.appendEntriesWrapper(i, args, reply)
				}
			}
		}
	}
}

/*
Ticker() --> Periodically call while detecting possible incoming heartbeat from AppendEntries to see if ticker
# Once AppendEntries received timeout will be pushed back
Only the elected one will be sent to the server
*/
func (rf *Raft) ticker() {
	rand.Seed(time.Now().UnixNano())
	timeOutInterval := time.Duration(150+rand.Intn(150)) * time.Millisecond

	for rf.killed() == false {

		time.Sleep(10 * time.Millisecond)
		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep()

		//// Send Heartbeat
		rf.mu.Lock()
		currentState := rf.state
		startTime := rf.startTime
		timedOut := false

		if currentState != "leader" && time.Since(startTime) > timeOutInterval {
			timedOut = true
			rf.state = "candidate"
			rf.currentTerm += 1
			rf.votedFor = rf.me
			rf.numVotes = 1
			rf.startTime = time.Now()
			timeOutInterval = time.Duration(150+rand.Intn(150)) * time.Millisecond
		}
		rf.mu.Unlock()

		if timedOut {
			for i := 0; i < len(rf.peers); i++ {
				if i != rf.me {
					rf.mu.Lock()
					args := RequestVoteArgs{}
					reply := RequestVoteReply{}
					args.Term = rf.currentTerm
					args.CandidateId = rf.me
					args.LastlogIndex = len(rf.log) - 1
					args.LastlogTerm = rf.log[args.LastlogIndex].Term
					rf.mu.Unlock()

					go rf.requestVoteWrapper(i, args, reply)
				}
			}
		}
	}
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

	// Your initialization code here (2A, 2B, 2C).

	//// When there is no bytes from persistor, it means the server is initalized.

	//// Mentioned in the paper for persistent state.
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.log = make([]Entry, 1)

	//// Not mentioned in the paper yet releated to be persistent

	rf.numVotes = 0
	rf.state = "follower"
	rf.timeOutInterval = time.Duration(150+rand.Intn(150)) * time.Millisecond

	rf.commitIndex = 0
	rf.lastApplied = 0

	rf.applyCh = applyCh

	bytes := rf.persister.ReadRaftState()
	rf.readPersist(bytes)

	labgob.Register(Entry{})

	for i := 0; i < len(rf.peers); i++ {
		rf.nextIndex = append(rf.nextIndex, 1)
		rf.matchIndex = append(rf.matchIndex, 0)
	}

	rf.startTime = time.Now()

	go rf.ticker()
	go rf.sendHeartbeat()
	go rf.applyStateMachineWrapper()

	return rf
}
