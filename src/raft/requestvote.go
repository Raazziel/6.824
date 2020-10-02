package raft

import (
	"math/rand"
	"time"
)

type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term int
	From int
	LastLogIndex int
	LastLogTerm	 int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term    int
	Granted bool
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.Lock("rv lock")
	defer rf.Unlock()
	defer DPrintf("%d:%d is asked voting for %+v,%+v", rf.me, rf.term, args, reply)
	*reply = RequestVoteReply{
		Term:    rf.term,
		Granted: false,
	}

	logOK:=true
	index,term:=rf.getLastIndexTerm()
	if term>args.LastLogTerm{
		logOK=false
	}else if term==args.LastLogTerm&&index>args.LastLogIndex{
		logOK=false
	}


	if args.Term < rf.term {
		return
	} else if args.Term == rf.term {
		if rf.role == follower {
			if rf.fs.votedTo != -1 && rf.fs.votedTo != args.From {
				return
			}
			if !logOK{
				return
			}
			rf.fs.votedTo = args.From
			rf.fs.updatedWithHB = true
			reply.Granted = true
		}
		return
	} else {
		DPrintf("%d:%d increase Term with %d:%d\n", rf.me, rf.term, args.From, args.Term)
		rf.term = args.Term
		rf.persist()
		reply.Term = rf.term
		rf.changeRole(follower)
		if !logOK{
			return
		}
		rf.fs.votedTo = args.From
		reply.Granted = true
		return
	}
}



func (rf *Raft) requestSingleVote(to int, res chan bool) {
	rf.Lock("rsv lock")
	index,term:=rf.getLastIndexTerm()
	args := &RequestVoteArgs{rf.term, rf.me,index,term}
	rf.Unlock()
	reply := &RequestVoteReply{}
	for {
		ok := rf.peers[to].Call("Raft.RequestVote", args, reply)
		if ok {
			//case1 成功获得vote
			if reply.Granted {
				res <- true
			}
			//case2 对方term更大
			rf.Lock("rsv middle lock")
			if reply.Term > rf.term {
				DPrintf("%d:%d increase Term with %d:%d\n", rf.me, rf.term, args.From, reply.Term)
				rf.term = reply.Term
				rf.persist()
				rf.changeRole(follower)
			}
			rf.Unlock()
			return
		}
		if rf.killed(){
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func (rf *Raft) doRequestVote() {
	votes := 0
	res := make(chan bool)
	for i := 0; i < rf.nPeer; i++ {
		if i == rf.me {
			continue
		}
		go rf.requestSingleVote(i, res)
	}
	for {
		interval:=ElectionTimeout+rand.Intn(ElectionTimeout)
		select {
		case <-res:
			votes++
			if rf.hasMajority(votes) {
				rf.Lock("become leader lock")
				DPrintf("%d:%d becomes leader\n", rf.me, rf.term)
				rf.changeRole(leader)
				rf.Unlock()
				return
			}
		case <-time.After(time.Duration(interval) * time.Millisecond):
			//TODO:重构用atomic实现...
			DPrintf("%d retry vote after %d ms",rf.me,interval)
			rf.Lock("Term++ lock")
			rf.term++
			rf.persist()
			DPrintf("%d:%d election time out\n", rf.me, rf.term)
			rf.Unlock()
			go rf.doRequestVote()
			return
		case <-rf.Cs.done:
			return
		}
	}
}