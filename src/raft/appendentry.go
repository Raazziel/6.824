package raft

import "time"

type AppendEntryArgs struct {
	Term int
	From int
	PrevLogIndex int
	PrevLogTerm  int
	Entry		 []Log
	LeaderCommit int
}

type AppendEntryReply struct {
	Term    int
	Success bool
}
func (rf *Raft) AppendEntry(args *AppendEntryArgs, reply *AppendEntryReply) {
	rf.Lock("ae lock")
	defer DPrintf("%d:%d is asked appending entry %+v,%+v", rf.me, rf.term, args, reply)
	defer rf.Unlock()
	defer rf.persist()
	rf.fs.updatedWithHB = true
	*reply = AppendEntryReply{
		Term:    rf.term,
		Success: false,
	}
	if args.Term < rf.term {
		return
	}

	index,_:=rf.getLastIndexTerm()
	logOk:=true
	if index<args.PrevLogIndex {
		logOk=false
	}else {
		if rf.logs[args.PrevLogIndex].Term != args.PrevLogTerm{
			DPrintf("%d:%d delete log\n", rf.me, rf.term)
			rf.logs = rf.logs[:args.PrevLogIndex]
			logOk = false
		}
	}
	if logOk{
		rf.logs=rf.logs[:args.PrevLogIndex+1]
		rf.logs=append(rf.logs,args.Entry...)
	}

	if args.LeaderCommit>rf.CommitIndex && logOk {
		if newIndex:=len(rf.logs)-1;args.LeaderCommit>newIndex{
			rf.changeCommitIndex(newIndex)
		}else{
			rf.changeCommitIndex(args.LeaderCommit)
		}
	}

	if args.Term == rf.term {
		reply.Success = logOk
		if rf.role == candidate {
			rf.changeRole(follower)
		}
		return
	} else {
		DPrintf("%d:%d increase Term with %d:%d\n", rf.me, rf.term, args.From, args.Term)
		rf.term = args.Term
		reply.Term = rf.term
		reply.Success = logOk
		rf.changeRole(follower)
		return
	}
}

func (rf *Raft) appendSingleEntry(to int, res chan bool) {
	for {
		select {
		case <-rf.Ls.Done:
			return
		default:
			rf.Lock("ase lock1")
			if rf.role!=leader||rf.killed(){
				rf.Unlock()
				return
			}
			prevIndex:=rf.Ls.nextIndex[to]-1
			prevTerm:=rf.logs[prevIndex].Term
			logs:=rf.logs[prevIndex+1:]
			args := &AppendEntryArgs{rf.term, rf.me,prevIndex,prevTerm,logs,rf.CommitIndex}
			rf.Unlock()
			reply := &AppendEntryReply{}
			ok := rf.peers[to].Call("Raft.AppendEntry", args, reply)
			if ok {
				rf.Lock("ase lock2")
				if reply.Term > rf.term {
					DPrintf("%d:%d increase Term with %d:%d\n", rf.me, rf.term, args.From, reply.Term)
					rf.term = reply.Term
					rf.persist()
					rf.changeRole(follower)
					rf.Unlock()
					return
				}
				if rf.role!=leader{
					rf.Unlock()
					return
				}
				if reply.Success {
					rf.Ls.nextIndex[to]=prevIndex+len(logs)+1
					rf.Ls.matchIndex[to]=prevIndex+len(logs)
					rf.eternalChecker()
					rf.Unlock()
					return
				}else{
					rf.Ls.nextIndex[to]--
					rf.Unlock()
					continue
				}

			}
			time.Sleep(10 * time.Millisecond)
		}
	}
}
func (rf *Raft) doAppendEntry() {
	res := make(chan bool)
	for i := 0; i < rf.nPeer; i++ {
		if i == rf.me {
			continue
		}
		go rf.appendSingleEntry(i, res)
	}
}
//需要由caller加锁..
func (rf *Raft)eternalChecker(){
	for i:=len(rf.logs)-1;i>rf.LastApplied;i--{
		// 对于不是当前term的log需要藉由其他log间接提交
		if rf.logs[i].Term!=rf.term{
			DPrintf("%d:%d has log %d created in previous term \n",rf.me,rf.term,i)
			continue
		}
		votes:=0
		for j:=0;j<rf.nPeer;j++{
			if j==rf.me{
				continue
			}
			if rf.Ls.matchIndex[j]>=i {
				votes++
				if rf.hasMajority(votes){
					DPrintf("great leader %d:%d commit log from %d to %d \n",rf.me,rf.term,rf.LastApplied+1,i)
					for k:=rf.LastApplied +1;k<=i;k++{
						rf.ac<-ApplyMsg{
							CommandValid: true,
							Command:      rf.logs[k].Command,
							CommandIndex: k,
						}
					}
					time.Sleep(time.Duration(10)*time.Millisecond)
					rf.CommitIndex =i
					rf.LastApplied =i
					return
				}
			}
		}
	}
}

func (rf *Raft)changeCommitIndex(to int){
	for i:=rf.CommitIndex;i<=to;i++{
		rf.ac<-ApplyMsg{
			CommandValid: true,
			Command:      rf.logs[i].Command,
			CommandIndex: i,
		}
		DPrintf("%d:%d apply log,%d:%d",rf.me,rf.term,rf.logs[i].Command,i)
	}
	time.Sleep(time.Millisecond*time.Duration(10))
	rf.CommitIndex =to
	rf.LastApplied =to
}