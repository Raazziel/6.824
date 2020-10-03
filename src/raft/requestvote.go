package raft

import (
	"math/rand"
	"time"
)

type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	From         int
	LastLogIndex int
	LastLogTerm  int
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
	defer func() {
		if reply.Granted {
			rf.fs.updatedWithHB = true
		}
		reply.Term = rf.term
		DPrintf("%d:%d is asked voting for %+v,%+v", rf.me, rf.term, args, reply)
		rf.persist()
		rf.Unlock()
	}()

	reply.Granted = false

	logOK := true
	index, term := rf.getLastIndexTerm()
	if term > args.LastLogTerm {
		logOK = false
	} else if term == args.LastLogTerm && index > args.LastLogIndex {
		logOK = false
	}

	if args.Term < rf.term {
		return
	} else if args.Term == rf.term {
		if rf.role == follower {
			if rf.fs.votedTo != -1 && rf.fs.votedTo != args.From {
				return
			}
			if !logOK {
				return
			}
			rf.fs.votedTo = args.From
			reply.Granted = true
		}
		return
	} else {
		rf.withBiggerTerm(args.Term, args)
		if !logOK {
			return
		}
		rf.fs.votedTo = args.From
		reply.Granted = true
		return
	}
}

func (rf *Raft) requestSingleVote(to int, res chan bool) {
	rf.Lock("rsv lock")
	index, term := rf.getLastIndexTerm()
	args := &RequestVoteArgs{rf.term, rf.me, index, term}
	reply := &RequestVoteReply{}
	rf.Unlock()
	for {
		if rf.killed() {
			return
		}
		ok := rf.peers[to].Call("Raft.RequestVote", args, reply)
		if ok {
			//case1 成功获得vote
			if reply.Granted {
				res <- true
			}
			//case2 对方term更大
			rf.Lock("rsv middle lock")
			if reply.Term > rf.term {
				rf.withBiggerTerm(reply.Term, reply)
				rf.persist()
			}
			rf.Unlock()
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func (rf *Raft) doRequestVote() {
	votes := 0
	c := make(chan bool)
	for i := 0; i < rf.nPeer; i++ {
		if i == rf.me {
			continue
		}
		go rf.requestSingleVote(i, c)
	}
	for {
		interval := ElectionTimeout + rand.Intn(ElectionTimeout)
		select {
		case <-c:
			votes++
			if rf.hasMajority(votes) {
				rf.Lock("become leader lock")
				DPrintf("%d:%d becomes leader\n", rf.me, rf.term)
				rf.changeRole(leader)
				rf.Unlock()
				return
			}
		case <-time.After(time.Duration(interval) * time.Millisecond):
			rf.Lock("Term++ lock")
			rf.term++
			rf.persist()
			DPrintf("%d:%d election time out\n", rf.me, rf.term)
			rf.Unlock()
			go rf.doRequestVote()
			return
		case <-rf.Cs.done:
			//这里channel内存泄漏
			return
		}
	}
}
