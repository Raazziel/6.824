package raft
// 重构 重构 还是重构!!!
// 屎一样的raft代码和raft味的屎
//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(Command interface{}) (index, Term, isleader)
//   start agreement on a new log entry
// rf.GetState() (Term, isLeader)
//   ask a Raft for its current Term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"bytes"
	"math/rand"
	"time"
	"sync/atomic"
	"../labrpc"
	"../labgob"
)


// import "bytes"
// import "../labgob"

const (
	follower = iota
	candidate
	leader
)
const (
	HBInterval      = 100
	HBTimeout       = 150
	ElectionTimeout = 300
)

type followerstatus struct {
	votedTo       int
	done          chan struct{}
	updatedWithHB bool
}

type candidatestatus struct {
	done chan struct{}
}

type leaderstatus struct {
	nextIndex  []int
	matchIndex []int
	done       chan struct{}
}
//为了弥补我的愚行而使用的dirty hack
var firstLeader bool=true
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
type Log struct {
	Term    int
	Command interface{}
}

type Raft struct {
	DLock                         // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()
	ac		  chan ApplyMsg
	InKV		bool

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	nPeer       int
	term        int
	role        int
	logs        []Log
	CommitIndex int
	LastApplied int


	fs followerstatus
	cs candidatestatus
	ls leaderstatus
}

func (rf *Raft) refreshFS() {
	rf.fs.votedTo = -1
	rf.fs.updatedWithHB = false
	rf.fs.done = make(chan struct{})
}

func (rf *Raft) refreshCS() {
	rf.cs.done = make(chan struct{})
}

func (rf *Raft) refreshLS() {
	if firstLeader{
		rf.ac<-ApplyMsg{
			CommandValid: true,
			Command:      0,
			CommandIndex: 0,
		}
		firstLeader=false
	}
	rf.ls.done = make(chan struct{})
	ii:=len(rf.logs)
	for i:=0;i<rf.nPeer;i++{
		rf.ls.nextIndex[i]=ii
		rf.ls.matchIndex[i]=0
	}
}

// 应该由caller上锁...
func (rf *Raft)getLastIndexTerm()(index,term int){
	index=len(rf.logs)-1
	term=rf.logs[index].Term
	return
}

// 应该由caller上锁...
func (rf *Raft) changeRole(to int) {

	if rf.role == follower {
		close(rf.fs.done)
	} else if rf.role == candidate {
		close(rf.cs.done)
	} else {
		close(rf.ls.done)
	}

	rf.role = to

	if to == follower {
		rf.refreshFS()
		go rf.isAlive()
	} else if to == candidate {
		rf.refreshCS()
		go rf.doRequestVote()
	} else if to == leader {
		rf.refreshLS()
		go rf.keepAlive()
	}
}

//需要外部加锁使用...
func (rf *Raft) hasMajority(nr int) bool {
	return nr+1 > rf.nPeer/2
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.Lock("get state lock")
	defer rf.Unlock()
	// Your code here (2A).
	return rf.term, rf.role == leader
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

	w:=new(bytes.Buffer)
	e:=labgob.NewEncoder(w)
	e.Encode(rf.term)
	e.Encode(len(rf.logs))
	e.Encode(rf.logs)
	data:=w.Bytes()
	rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func virtualP(data []byte){
	if len(data)<1||data==nil{
		return
	}
	r:=bytes.NewBuffer(data)
	d:=labgob.NewDecoder(r)
	term:=0
	e:=d.Decode(&term)
	if e!=nil{
		panic(e.Error())
	}
	lenLogs:=0
	d.Decode(&lenLogs)
	logs:=make([]Log,lenLogs)
	e=d.Decode(&logs)
	DPrintf("decode result:%d,%d,%+v\n",term,lenLogs,logs)
}
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
	rf.Lock("readPersist")
	defer rf.Unlock()
	r:=bytes.NewBuffer(data)
	d:=labgob.NewDecoder(r)
	term:=0
	e:=d.Decode(&term)
	if e!=nil{
		panic(e.Error())
	}
	lenLogs:=0
	d.Decode(&lenLogs)
	logs:=make([]Log,lenLogs)
	e=d.Decode(&logs)
	rf.term=term
	rf.logs=logs[:]
	DPrintf("readpersist:decode term:%d logs size :%d,%+v\n",term,lenLogs,logs)
	if e!=nil{
		panic(e.Error())
	}
}
func printP(data []byte){
	r:=bytes.NewBuffer(data)
	d:=labgob.NewDecoder(r)
	term:=0
	e:=d.Decode(&term)
	if e!=nil{
		panic(e.Error())
	}
	lenLogs:=0
	d.Decode(&lenLogs)
	logs:=make([]Log,lenLogs)
	e=d.Decode(&logs)
	DPrintf("printP:decode logs size :%d,%+v\n",len(logs),logs)
}
//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//




//
// example RequestVote RPC handler.
//




func (rf *Raft) keepAlive() {
	rf.doAppendEntry()
	for {
		select {
		case <-time.After(time.Duration(HBInterval) * time.Millisecond):
			rf.doAppendEntry()
		case <-rf.ls.done:
			return
		}
	}
}

func (rf *Raft) isAlive() {
	for {
		interval:=HBTimeout+rand.Intn(61)
		t:=time.After(time.Duration(interval) * time.Millisecond)
		select {
		case <-t:
			rf.Lock("is alive")
			if !rf.fs.updatedWithHB {
				DPrintf("%d:%d becomes candidate\n", rf.me, rf.term)
				rf.term++
				rf.persist()
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
// agreement on the next Command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// Command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the Command will appear at
// if it's ever committed. the second return value is the current
// Term. the third return value is true if this server believes it is
// the leader.
//

func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.Lock("start")
	defer rf.Unlock()
	index := len(rf.logs)
	term := rf.term
	isLeader := rf.role==leader

	// Your code here (2B).
	if isLeader{
		rf.logs=append(rf.logs,Log{
			Term:    term,
			Command: command,
		})
		DPrintf("%d:%d update log to %d,%+v\n",rf.me,term,len(rf.logs),command)
		rf.persist()
	}
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
	rf.Lock("kill you!")
	defer rf.Unlock()
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.

	switch rf.role {
	case follower:
		close(rf.fs.done)
	case candidate:
		close(rf.cs.done)
	case leader:
		close(rf.ls.done)
	}
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
	rf.nPeer = len(rf.peers)
	rf.term = 0
	rf.CommitIndex =0
	rf.LastApplied =0
	rf.ac=applyCh
	rf.logs=[]Log{{
		0,
		0,
	}}
	rf.ls.nextIndex=make([]int,rf.nPeer)
	rf.ls.matchIndex=make([]int,rf.nPeer)
	// Your initialization code here (2A, 2B, 2C).
	rf.refreshFS()
	go rf.isAlive()
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}
