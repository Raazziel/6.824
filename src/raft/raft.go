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
	"math/rand"
	"time"
)
import "sync/atomic"
import "../labrpc"

// import "bytes"
// import "../labgob"

const (
	follower=iota
	candidate
	leader
)
const(
	HBInterval=100
	HBTimeout=150
	ElectionTimeout=300
)

type followerstatus struct {
	votedTo int
	done    chan struct{}
	updatedWithHB bool

}

type candidatestatus struct {
	done  chan struct{}
}

type leaderstatus struct {
	nextIndex []int
	matchIndex []int
	done chan struct{}
}
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
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	DLock         // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	nPeer	 int
	term     int
	role 	 int

	fs		followerstatus
	cs		candidatestatus
	ls		leaderstatus
}
func (rf *Raft)refreshFS(){
	rf.fs.votedTo=-1
	rf.fs.updatedWithHB=false
	rf.fs.done=make(chan struct{})
}

func (rf *Raft)refreshCS(){
	rf.cs.done=make(chan struct{})
}

func (rf *Raft)refreshLS(){
	rf.ls.done=make(chan struct{})
}


// 应该由caller上锁...
func (rf *Raft)changeRole(to int){

	if rf.role==follower{
		close(rf.fs.done)
	}else if rf.role==candidate{
		close(rf.cs.done)
	}else{
		close(rf.ls.done)
	}

	rf.role=to

	if to==follower{
		rf.refreshFS()
		go rf.isAlive()
	}else if to==candidate{
		rf.refreshCS()
		go rf.doRequestVote()
	}else if to==leader{
		rf.refreshLS()
		go rf.keepAlive()
	}
}
//需要外部加锁使用...
func (rf *Raft)hasMajority(nr int)bool{
	return  nr+1>rf.nPeer/2
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.Lock("get state lock")
	defer rf.Unlock()
	// Your code here (2A).
	return rf.term,rf.role==leader
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
	// Your data here (2A, 2B).
	Term int
	From int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term int
	Granted bool
}

type AppendEntryArgs struct {
	Term int
	From int
}

type AppendEntryReply struct {
	Term int
	Success bool
}
//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.Lock("rv lock")
	defer rf.Unlock()
	rf.fs.updatedWithHB=true
	*reply=RequestVoteReply{
		Term:    rf.term,
		Granted: false,
	}
	if args.Term<rf.term{
		return
	}else if args.Term==rf.term && rf.role==follower{
		if rf.fs.votedTo!=-1 && rf.fs.votedTo!=args.From{
			return
		}
		rf.fs.votedTo=args.From
		reply.Granted=true
	}else{
		rf.term=args.Term
		rf.fs.votedTo=args.From
		rf.changeRole(follower)
		reply.Granted=true
	}
	reply.Term=rf.term
	return
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
//func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
//	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
//	return ok
//}

//加锁
func (rf *Raft)requestSingleVote(to int,res chan bool){
	rf.Lock("rsv lock")
	args:=&RequestVoteArgs{rf.term,rf.me}
	rf.Unlock()
	reply:=&RequestVoteReply{}
	for {
		ok:=rf.peers[to].Call("Raft.RequestVote", args, reply)
		if ok{
			//case1 成功获得vote
			if reply.Granted{
				res<-true
			}
			//case2 对方term更大
			rf.Lock("rsv middle lock")
			if reply.Term>rf.term{
				rf.term=reply.Term
				rf.changeRole(follower)
			}
			rf.Unlock()
			return
		}
		time.Sleep(10*time.Millisecond)
	}
}

func (rf *Raft)doRequestVote(){
	votes:=0
	res:=make(chan bool)
	for i:=0;i<rf.nPeer;i++{
		if i==rf.me{
			continue
		}
		go rf.requestSingleVote(i,res)
	}
	for{
		select{
		case <-res:
			votes++
			if rf.hasMajority(votes){
				rf.Lock("become leader lock")
				rf.changeRole(leader)
				rf.Unlock()
				return
			}
		case <-time.After(time.Duration(ElectionTimeout+rand.Intn(61))*time.Millisecond):
			//之后重构用atomic实现...
			rf.Lock("term++ lock")
			rf.term++
			rf.Unlock()
			go rf.doRequestVote()
			return
		case <-rf.cs.done:
			return
		}
	}
}

func (rf *Raft)AppendEntry(args *AppendEntryArgs,reply *AppendEntryReply){
	rf.Lock("ae lock")
	defer rf.Unlock()
	rf.fs.updatedWithHB=true
	*reply=AppendEntryReply{
		Term:    rf.term,
		Success: false,
	}
	if args.Term<rf.term{
		return
	}else if args.Term==rf.term && rf.role==candidate{
		reply.Success=true
		rf.changeRole(follower)
	}else{
		rf.term=args.Term
		reply.Success=true
		rf.changeRole(follower)
	}
	reply.Term=rf.term
	return
}

func (rf *Raft) appendSingleEntry(to int,res chan bool){
	args:=&AppendEntryArgs{rf.term,rf.me}
	reply:=&AppendEntryReply{}
	for {
		ok:=rf.peers[to].Call("Raft.AppendEntry", args, reply)
		if ok {
			if reply.Success{
				res<-true
			}
			rf.Lock("ase lock")
			if reply.Term>rf.term{
				rf.term=reply.Term
				rf.changeRole(follower)
			}
			rf.Unlock()
			return
		}
		time.Sleep(10*time.Millisecond)
	}
}
func (rf *Raft)doAppendEntry(){
	res:=make(chan bool)
	for i:=0;i<rf.nPeer;i++{
		if i==rf.me{
			continue
		}
		go rf.appendSingleEntry(i,res)
	}
}

func (rf *Raft)keepAlive(){
	for{
		select {
		case <-time.After(time.Duration(HBInterval+rand.Int()%41)*time.Millisecond):
			rf.doAppendEntry()
		case <-rf.ls.done:
			return
		}
	}
}

func (rf *Raft)isAlive(){
	for {
		select {
		case <-time.After(time.Duration(HBTimeout+rand.Int()%53) * time.Millisecond):
			rf.Lock("is alive")
			if !rf.fs.updatedWithHB {
				rf.term++
				rf.changeRole(candidate)
				rf.Unlock()
				return
			}
			rf.fs.updatedWithHB = false
			rf.Unlock()
		case <-rf.fs.done:
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


func (rf *Raft)Start(command interface{})(int, int, bool) {
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
	rf.nPeer=len(rf.peers)
	rf.term=0
	// Your initialization code here (2A, 2B, 2C).
	rf.refreshFS()
	rf.isAlive()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())


	return rf
}
