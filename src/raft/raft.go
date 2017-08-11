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

import "sync"
import (
	"labrpc"
	//"log"
	"time"
	"math/rand"
)

// import "bytes"
// import "encoding/gob"

const (
	STATUS_LEADER int = iota
	STATUS_CANDIDATE
	STATUS_FOLLOWER
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
//
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}

//
// A Go object implementing a single Raft peer.
//
type Log struct {
	Commond		interface{}
	Term 		int
}

type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	currentTerm int
	votedFor 	int
	log 		[]Log
	commitIndex	int
	lastApplied int
	nextIndex	[]int
	matchIndex  []int

	status         int
	voteNum        int
	appendEntryCH  chan bool
	becomeLeaderCH chan bool
	lowerTermCH    chan bool
	requestVoteCH  chan bool
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	term = rf.currentTerm
	isleader = (rf.status == STATUS_LEADER)
	return term, isleader
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
	// e := gob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := gob.NewDecoder(r)
	// d.Decode(&rf.xxx)
	// d.Decode(&rf.yyy)
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
}




//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	Term 			int
	CandidateId		int
	LastLogIndex 	int
	LastLogTerm		int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	Term 			int
	VoteGranted 	bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	if rf.currentTerm > args.Term {
		// Reply false if term < currentTerm
		rf.replyRequestVoteNo(args, reply)
		DPrintf("#%d reject candidate #%d, term %d>%d",
			rf.me, args.CandidateId, rf.currentTerm, args.Term)
	} else {
		if rf.currentTerm < args.Term {
			//DPrintf("#%d'term(%d) < #%d'term(%d)",
			//	rf.me, rf.currentTerm, args.CandidateId, args.Term)
			rf.currentTerm = args.Term
			rf.votedFor = -1
			rf.status = STATUS_FOLLOWER
		}
		//rf.checkTerm(args.CandidateId, args.Term)
		isUpToDate := false
		if len(rf.log) > 0 {
			lastIdx := len(rf.log)-1
			lastTerm := rf.log[lastIdx].Term
			//DPrintf("%d?%d, %d?%d", lastTerm, args.LastLogTerm, lastIdx, args.LastLogIndex)
			if lastTerm < args.LastLogTerm || (lastTerm == args.LastLogTerm && lastIdx <= args.LastLogIndex) {
				isUpToDate = true
			}
		} else {
			isUpToDate = true
		}
		if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && isUpToDate {
			rf.replyRequestVoteYes(args, reply)
		} else {
			rf.replyRequestVoteNo(args, reply)
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

func (rf *Raft) broadcastRequestVote() {
	lastLogIndex := len(rf.log)-1
	args := RequestVoteArgs{
		Term: rf.currentTerm,
		CandidateId: rf.me,
		LastLogIndex: -1,
		LastLogTerm: -1,
	}
	if lastLogIndex >= 0 {
		args.LastLogIndex = lastLogIndex
		args.LastLogTerm = rf.log[lastLogIndex].Term
	}

	isAlreadyLeader := false
	for i := 0; i < len(rf.peers); i++ {
		if rf.me != i {
			reply := RequestVoteReply{}
			go func(i int) {
				ok := rf.sendRequestVote(i, &args, &reply)
				if ok {
					if reply.VoteGranted {
						rf.mu.Lock()
						rf.voteNum++
						rf.mu.Unlock()
						if rf.voteNum > len(rf.peers)/2 && !isAlreadyLeader {
							rf.becomeLeaderCH <- true
							isAlreadyLeader = true
						}
					}
					if rf.currentTerm < reply.Term {
						DPrintf("#%d candidate -> follower, term(%d) < vote request reply'term(%d)",
							rf.me, rf.currentTerm, reply.Term)
						rf.currentTerm = reply.Term
						rf.votedFor = -1
						rf.status = STATUS_FOLLOWER
					}
				}
			}(i)
		}
	}
}

//
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
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := rf.commitIndex
	term := rf.currentTerm
	var isLeader bool

	if rf.status == STATUS_LEADER {
		isLeader = true
		//go func() {
			entry := Log{
				Commond: command,
				Term: 	rf.currentTerm,
			}

			rf.mu.Lock()
			rf.log = append(rf.log, entry)
			index = len(rf.log) - 1
			//DPrintf("Leader #%d update index to %d", rf.me, index)
			rf.nextIndex[rf.me] = index
			rf.mu.Unlock()
			DPrintf("Leader #%d receive a command=%d, len(log)=%d", rf.me, command, len(rf.log))

			rf.broadcastAppendEntries()
		//}()
	} else {
		isLeader = false
	}
	//DPrintf("Leader #%d return index %d", rf.me, index)
	return index, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
}

func (rf *Raft) Loop() {
	for {
		switch rf.status {
		case STATUS_LEADER:
			rf.Leader()
		case STATUS_FOLLOWER:
			rf.Follower()
		case STATUS_CANDIDATE:
			rf.Candidate()
		}
	}
}

func (rf *Raft) Candidate() {
	timeout := rand.Intn(150) + 150

	rf.mu.Lock()
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.voteNum = 1
	rf.mu.Unlock()

	rf.broadcastRequestVote()

	select {
	case <- time.After(time.Duration(timeout) * time.Millisecond):
	case <- rf.becomeLeaderCH:
		if rf.status == STATUS_CANDIDATE {
			rf.mu.Lock()
			DPrintf("#%d candidate -> leader", rf.me)
			rf.status = STATUS_LEADER
			rf.nextIndex = []int{}
			rf.matchIndex = []int{}
			for i := 0; i < len(rf.peers); i++ {
				rf.nextIndex = append(rf.nextIndex, len(rf.log)-1)
				rf.matchIndex = append(rf.matchIndex, 0)
			}
			rf.mu.Unlock()
		}
	case <- rf.appendEntryCH:
		rf.mu.Lock()
		DPrintf("#%d candidate -> follower", rf.me)
		rf.status = STATUS_FOLLOWER
		rf.mu.Unlock()
	case <- rf.requestVoteCH:
	case <- rf.lowerTermCH:
		rf.mu.Lock()
		DPrintf("#%d candidate -> follower for lower term", rf.me)
		rf.status = STATUS_FOLLOWER
		rf.mu.Unlock()
	}
}

func (rf *Raft) Follower() {
	timeout := rand.Intn(300) + 500
	//DPrintf("follower #%d start loop", rf.me)
	select {
	case <- rf.appendEntryCH:
		//DPrintf("#%d in select case heartbeat", rf.me)
	//case <- rf.lowerTermCH:
	case <- rf.requestVoteCH:
	case <- time.After(time.Duration(timeout) * time.Millisecond):
		rf.mu.Lock()
		DPrintf("#%d follower -> candidate", rf.me)
		rf.status = STATUS_CANDIDATE
		rf.mu.Unlock()
	}
	//DPrintf("follower #%d end loop", rf.me)
}

type AppendEntriesArgs struct {
	Term 	 int
	LeaderId		int
	PreLogIndex		int
	PreLogTerm		int
	Entries			[]Log
	LeaderCommit 	int
}

type AppendEntriesReply struct {
	Term 			int
	Success			bool
	NextIndex		int
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// Reply false if term < currentTerm
	if rf.currentTerm > args.Term {
		rf.replyAppendEntriesNo(args, reply)
	} else {
		if rf.currentTerm < args.Term {
			rf.replyAppendEntriesYes(args, reply)
			//rf.checkTerm(args.LeaderId, args.Term)
			if rf.currentTerm < args.Term {
				DPrintf("#%d term(%d) < Leader #%d'term(%d)",
					rf.me, rf.currentTerm, args.LeaderId, args.Term)
				rf.currentTerm = args.Term
				rf.votedFor = -1
				rf.status = STATUS_FOLLOWER
			}
		} else {
			// Reply false if log doesn’t contain an entry at prevLogIndex
			// whose term matches prevLogTerm
			if args.PreLogIndex >= 0 && (len(rf.log) <= args.PreLogIndex ||
				(len(rf.log) > args.PreLogIndex && rf.log[args.PreLogIndex].Term != args.PreLogTerm)) {
				if args.PreLogIndex < len(rf.log) {
					tmp := args.PreLogIndex
					//fmt.Printf("%d\n", tmp)
					for tmp >= 0 && rf.log[tmp].Term != args.PreLogTerm {
						tmp--
						//fmt.Printf("%d\n", tmp)
					}
					reply.NextIndex = tmp
				} else {
					reply.NextIndex = 0
				}
				rf.replyAppendEntriesNo(args, reply)
			} else {
				rf.replyAppendEntriesYes(args, reply)
			}
		}

	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool  {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) broadcastAppendEntries() {
	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me && rf.status == STATUS_LEADER {
			args := AppendEntriesArgs{
				Term: rf.currentTerm,
				LeaderId: rf.me,
				PreLogIndex: rf.nextIndex[i],
				//PreLogTerm: rf.matchIndex[i],
				//Entries: rf.log[rf.nextIndex[i]+1:],
				LeaderCommit: rf.commitIndex,
			}
			//fmt.Printf("%d, %d", len(rf.log), rf.nextIndex[i])
			args.PreLogIndex = rf.nextIndex[i]
			if rf.nextIndex[i] == -1 {
				args.PreLogTerm = 0
			} else {
				args.PreLogTerm = rf.log[rf.nextIndex[i]].Term
			}
			if len(rf.log) > 0 {
				args.Entries = rf.log[rf.nextIndex[i]+1:]
			}

			reply := AppendEntriesReply{}
			go func(i int) {
				ok := rf.sendAppendEntries(i, &args, &reply)
				if ok {
					rf.checkTerm(i, reply.Term)
					if len(args.Entries) > 0 {
						if reply.Success {
							//rf.nextIndex[i]++
							rf.nextIndex[i] = reply.NextIndex
							rf.matchIndex[i] = max(rf.matchIndex[i], rf.nextIndex[i])
						} else {
							if rf.nextIndex[i] > 0 {
								rf.nextIndex[i] = reply.NextIndex
							}
						}
					}
				}
			}(i)
		}
	}
}


func (rf *Raft) Leader() {
	rf.broadcastAppendEntries()

	select {
	case <-rf.lowerTermCH:
		rf.mu.Lock()
		DPrintf("No.%d leader to follower for lower term", rf.me)
		rf.status = STATUS_FOLLOWER
		rf.mu.Unlock()
	case <-time.After(150 * time.Millisecond):
	}
}

func (rf *Raft) checkTerm(id int, term int) {
	if rf.currentTerm < term {
		rf.lowerTermCH <- true
		//DPrintf("#%d and #%d term: %d < %d", rf.me, id, rf.currentTerm, term)
		rf.setTerm(term)
	}
}

func (rf *Raft) setTerm(term int) {
	rf.mu.Lock()
	rf.currentTerm = term
	rf.votedFor = -1
	rf.voteNum = 0
	rf.mu.Unlock()
}


func (rf *Raft) replyRequestVoteYes(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.requestVoteCH <- true
	reply.Term = rf.currentTerm
	reply.VoteGranted = true
	rf.mu.Lock()
	DPrintf("#%d vote #%d in term %d, previous vote for %d",
		rf.me, args.CandidateId, rf.currentTerm, rf.votedFor)
	rf.votedFor = args.CandidateId
	rf.mu.Unlock()
}

func (rf *Raft) replyRequestVoteNo(args *RequestVoteArgs, reply *RequestVoteReply) {
	reply.Term = rf.currentTerm
	reply.VoteGranted = false
	//DPrintf("#%d reject candidate #%d",
	//	rf.me, args.CandidateId)
}

func (rf *Raft) replyAppendEntriesYes(args *AppendEntriesArgs, reply *AppendEntriesReply)  {
	rf.appendEntryCH <- true
	reply.Success = true
	reply.Term = rf.currentTerm

	isConflict := false
	for i := 0; i < len(args.Entries); i++ {
		tmpIdx := args.PreLogIndex +1+i
		if tmpIdx >= len(rf.log) || rf.log[tmpIdx] != args.Entries[i] {
			isConflict = true
		}
	}
	if isConflict {
		rf.mu.Lock()
		rf.log = rf.log[:args.PreLogIndex+1]
		for i := 0; i < len(args.Entries); i++ {
			rf.log = append(rf.log, args.Entries[i])
		}
		rf.mu.Unlock()
		//DPrintf("#%d accept append entries request from #%d," +
		//	" len([]log)=%d, commitIndex=%d", rf.me, args.LeaderId, len(rf.log), rf.commitIndex)
	}

	reply.NextIndex = len(rf.log)-1
	rf.mu.Lock()
	rf.votedFor = args.LeaderId
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, len(rf.log)-1)
	}
	rf.mu.Unlock()

}

func (rf *Raft) replyAppendEntriesNo(args *AppendEntriesArgs, reply *AppendEntriesReply)  {
	reply.Success = false
	reply.Term = rf.currentTerm
	//DPrintf("#%d reject append entries request from #%d", rf.me, args.LeaderId)
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
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.status = STATUS_FOLLOWER
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.appendEntryCH = make(chan bool)
	rf.becomeLeaderCH = make(chan bool)
	rf.lowerTermCH = make(chan bool)
	rf.requestVoteCH = make(chan bool)
	rf.log = []Log{}
	rf.nextIndex = []int{}
	rf.matchIndex = []int{}
	for i := 0; i < len(rf.peers); i++ {
		rf.nextIndex = append(rf.nextIndex, len(rf.log)-1)
		rf.matchIndex = append(rf.matchIndex, 0)
	}

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	go rf.Loop();
	go rf.feedStateMachine(applyCh)

	return rf
}

func (rf *Raft) feedStateMachine(applyCh chan ApplyMsg) {
	for {
		rf.mu.Lock()
		if rf.status == STATUS_LEADER {
			N := rf.commitIndex
			for i := rf.commitIndex + 1; i <= len(rf.log) - 1; i++ {
				num := 1
				for j := range rf.peers {
					if j != rf.me && rf.matchIndex[j] >= i && rf.log[i].Term == rf.currentTerm {
						num++
					}
				}
				if 2*num > len(rf.peers) {
					N = i
				}
			}
			if N != rf.commitIndex {
				//DPrintf("Leader #%d commitIndex: %d -> %d", rf.me, rf.commitIndex, N)
				rf.commitIndex = N
			}
			//newCommit := rf.commitIndex
			//cnt := 0
			//for i := 0; i < len(rf.nextIndex); i++ {
			//	if rf.nextIndex[i] > rf.commitIndex {
			//		cnt ++
			//		if newCommit == rf.commitIndex || newCommit > rf.nextIndex[i] {
			//			newCommit = rf.nextIndex[i]
			//		}
			//	}
			//}
			//if cnt > len(rf.peers)/2 && rf.status == STATUS_LEADER {
			//	rf.commitIndex = newCommit
			//}
		}
		rf.mu.Unlock()
		time.Sleep(30 * time.Millisecond)
		if rf.lastApplied < rf.commitIndex {
			go func() {
				rf.mu.Lock()
				oldApplied := rf.lastApplied
				commitIdx := rf.commitIndex
				rf.lastApplied = commitIdx
				rf.mu.Unlock()
				if len(rf.log) - 1 < commitIdx {
					return
				}
				time.Sleep(10*time.Millisecond)
				for i:=oldApplied+1; i<=commitIdx; i++ {
					msg := new(ApplyMsg)
					msg.Index = i
					msg.Command = rf.log[i].Commond
					DPrintf("#%d feed state machine log[%d]", rf.me, i)
					applyCh <- *msg
				}
			}()
		}
	}
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func max(a, b int) int {
	if a < b {
		return b
	}
	return a
}