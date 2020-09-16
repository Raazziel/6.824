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
	"fmt"
	"math/rand"
	"sync"
	"time"
)
import "sync/atomic"
import "../labrpc"

// import "bytes"
// import "../labgob"

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//

const (
	follower = iota
	candidate
	leader
)

const (
	HBtime      = 20
	HBtimeout   = 50
	Votetimeout = 100
)

type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

type followerStatus struct {
	votedFor      int
	updatedWithHB bool
	done          chan struct{}
}

type candidateStatus struct {
	stopVote bool
	voters   map[int]bool
	votes	int
	done     chan struct{}
}

type leaderStatus struct {
	done chan struct{}
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
	term        int
	preLogIndex int
	preLogTerm  int
	role        int
	leader		int

	fs followerStatus
	cs candidateStatus
	ls leaderStatus
}


func (rf *Raft) watchdog() {
	if rf.role != follower {
		return
	}
	for {
		timeInterval := time.Duration(HBtimeout + rand.Int()%13)
		select {
		case <-time.After(timeInterval * time.Millisecond):
			rf.mu.Lock()
			if !rf.fs.updatedWithHB {
				rf.term = rf.term + 1
				rf.roleChange(candidate)
				rf.mu.Unlock()
				return
			} else {
				rf.fs.updatedWithHB = false
			}
			rf.mu.Unlock()
		case <-rf.fs.done:
			return
		}
	}
}

func (rf *Raft) tryAppendEntry(who, startAt int) {
	for {
		if startAt != rf.term || rf.role != leader {
			return
		}
		args := &AppendEntryArgs{startAt, rf.me}
		reply := &AppendEntryReply{}
		if rf.peers[who].Call("Raft.AppendEntry", args, reply) {
			//case 1,reply bigger term
			if !reply.Ok && reply.Term > rf.term {
				rf.mu.Lock()
				rf.term = reply.Term
				rf.roleChange(follower)
				rf.mu.Unlock()
				return
			}
			//case 2,reply smaller term and prelog unmatched
			//case 3,reply ok
		}
	}
}

func (rf *Raft) broadcast() {
	if rf.role != leader {
		return
	}
	for {
		timeInterval := time.Duration(HBtime + rand.Int()%7)
		select {
		case <-time.After(timeInterval * time.Millisecond):
			for i, _ := range rf.peers {
				if i == rf.me {
					continue
				}
				go rf.tryAppendEntry(i, rf.term)
			}
		case <-rf.ls.done:
			return
		}
	}
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	// Your code here (2A).
	return rf.term, rf.role == leader
}

func (rf *Raft) roleChange(to int) {
	if to == rf.role {
		return
	}

	if rf.role == leader {
		close(rf.ls.done)
	} else if rf.role == candidate {
		close(rf.cs.done)
	} else if rf.role == follower {
		close(rf.fs.done)
	}

	rf.role = to
	if to == follower {
		rf.fs = followerStatus{
			votedFor:      -1,
			updatedWithHB: false,
			done:          make(chan struct{}),
		}
		go rf.watchdog()
	} else if to == candidate {
		fmt.Printf("%d becomes a candidate\n",rf.me)
		rf.cs.done=make(chan struct{})
		rf.cs.votes=0
		for i := 0; i < len(rf.peers); i++ {
			rf.cs.voters[i] = false
		}
		go rf.startVote()
	} else if to == leader {
		fmt.Printf("finally we got %d as a leader with term:%d\n",rf.me,rf.term)
		rf.ls = leaderStatus{
			done: make(chan struct{}),
		}
		go rf.broadcast()
	}
	return

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
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	Term        int
	ID          int
	PreLogIndex int
	PreLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
// appendentry implement
type RequestVoteReply struct {
	Term int
	Ok   bool
}

type AppendEntryArgs struct {
	Term int
	ID   int
}

type AppendEntryReply struct {
	Term int
	Ok   bool
}

func (rf *Raft) AppendEntry(args *AppendEntryArgs, reply *AppendEntryReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.term {
		*reply = AppendEntryReply{rf.term, false}
		return
	}
	// need fix in 2b
	*reply = AppendEntryReply{rf.term, true}
	if rf.role == follower {
		rf.fs.updatedWithHB = true
	}

	if rf.role == candidate && args.Term == rf.term {
		rf.roleChange(follower)
		return
	}

	termBigger := args.Term > rf.term
	if termBigger {
		rf.term = args.Term
		if rf.role == follower {
			rf.fs.votedFor = -1
		} else {
			rf.roleChange(follower)
		}
	}

	return
}

//
// example RequestVote RPC handler.
// this is called when asked for vote...
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	fmt.Printf("%d:%d is asked for vote from %d:%d\n",rf.me,rf.term,args.ID,args.Term)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.role==follower{
		rf.fs.updatedWithHB=true
	}
	if args.Term < rf.term {
		*reply = RequestVoteReply{Term: rf.term, Ok: false}
		return
	}
	var ok bool
	if args.Term == rf.term {
		if rf.role == follower {
			ok = (rf.fs.votedFor == -1) || (rf.fs.votedFor == args.ID)
			if ok {
				rf.fs.votedFor = args.ID
			}
		}
	} else if args.Term > rf.term {
		ok=true
		rf.term = args.Term
		rf.roleChange(follower)
		rf.fs.votedFor=args.ID
	}
	*reply=RequestVoteReply{rf.term,ok}
	if ok{
		fmt.Printf("%d in term:%d has voted for %d\n",rf.me,rf.term,args.ID)
	}
}

func (rf *Raft) startVote() {
	if rf.role != candidate {
		return
	}
	interval := time.Duration(Votetimeout + rand.Int()%17)
	for i, _ := range rf.peers {
		go rf.askSingleVote(i, rf.term)
	}
	select {
	case <-time.After(interval * time.Millisecond):
		fmt.Printf("%d's vote timed out,start another round with term:%d\n",rf.me,rf.term+1)
		rf.term++
		for k, _ := range rf.cs.voters {
			rf.cs.voters[k] = false
		}
		rf.startVote()
	case <-rf.cs.done:
		return
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
func (rf *Raft) askSingleVote(who, startAt int) {
	if rf.role != candidate ||who==rf.me{
		return
	}
	for {
		if startAt != rf.term {
			return
		}
		args := &RequestVoteArgs{
			Term:        startAt,
			ID:          rf.me,
			PreLogIndex: 0,
			PreLogTerm:  0,
		}
		reply := &RequestVoteReply{}
		if rf.peers[who].Call("Raft.RequestVote", args, reply) {
			rf.mu.Lock()
			defer rf.mu.Unlock()
			if startAt != rf.term {
				return
			}
			fmt.Sprintf("%d get a vote reply from %d\n",rf.me,args.ID)
			termBigger := reply.Term > rf.term
			if termBigger {
				rf.term = args.Term
				rf.roleChange(follower)
				fmt.Printf("with bigger term %d from %d,%d becomes a follower\n",reply.Term,args.ID,rf.me)
				return
			}
			if reply.Ok && !rf.cs.voters[who] {
				rf.cs.votes++
				if rf.cs.votes+1 > len(rf.peers)/2 {
					rf.roleChange(leader)
					return
				}
			}
			if reply.Ok {
				rf.cs.voters[who] = true
			}
			return
		}
	}
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
	rf.term=0

	// Your initialization code here (2A, 2B, 2C).
	rf.cs.voters=make(map[int]bool,len(rf.peers))
	for i:=0;i<len(rf.peers);i++{
		rf.cs.voters[i]=false
	}


	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.fs = followerStatus{
		votedFor:      -1,
		updatedWithHB: false,
		done:          make(chan struct{}),
	}
	go rf.watchdog()
	return rf
}
