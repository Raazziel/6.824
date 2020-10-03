package raft

import "time"

type AppendEntryArgs struct {
	Term         int
	From         int
	PrevLogIndex int
	PrevLogTerm  int
	Entry        []Log
	LeaderCommit int
}

type AppendEntryReply struct {
	Term    int
	Success bool
}

func (rf *Raft) AppendEntry(args *AppendEntryArgs, reply *AppendEntryReply) {
	rf.Lock("ae lock")
	defer func() {
		if reply.Success {
			rf.fs.updatedWithHB = true
		}
		reply.Term = rf.term
		rf.persist()
		rf.Unlock()
		DPrintf("%d:%d is asked appending entry %+v,%+v", rf.me, rf.term, args, reply)
	}()

	reply.Success = false
	if args.Term < rf.term {
		return
	}
	DPrintf("%d:%d is asked appending entry %+v", rf.me, rf.term, args)

	index, _ := rf.getLastIndexTerm()
	matched := true

	if index < args.PrevLogIndex {
		matched = false
	} else if rf.logs[args.PrevLogIndex].Term != args.PrevLogTerm {
		DPrintf("%d:%d delete log\n", rf.me, rf.term)
		rf.logs = rf.logs[:args.PrevLogIndex]
		matched = false
	}

	if matched {
		//心跳包不检查
		if len(args.Entry)!=0{
			// RPC是乱序的,可能会收到旧的RPC,需要进一步检查
			shouldConcat := true
			nowSize := len(rf.logs)
			newSize := args.PrevLogIndex + len(args.Entry) + 1

			if nowSize > newSize {
				argsLastTerm := args.Entry[len(args.Entry)-1].Term
				if rf.logs[newSize-1].Term == argsLastTerm {
					shouldConcat = false
				}
			}

			if shouldConcat {
				rf.logs = rf.logs[:args.PrevLogIndex+1]
			}

			//if shouldConcat || len(rf.logs)-1==args.PrevLogIndex{
			//	rf.logs = append(rf.logs, args.Entry...)
			//}
			rf.logs = append(rf.logs, args.Entry...)
		}
		reply.Success = true
	}

	if matched && args.LeaderCommit > rf.CommitIndex {
		//DPrintf("%d:%d,leader commit :%d",rf.me,rf.term,args.LeaderCommit)
		if newIndex := len(rf.logs) - 1; args.LeaderCommit > newIndex {
			rf.commitLog(newIndex)
		} else {
			rf.commitLog(args.LeaderCommit)
		}
	}

	if args.Term == rf.term {
		if rf.role == candidate {
			rf.changeRole(follower)
		}
		return
	} else {
		rf.withBiggerTerm(args.Term, args)
		return
	}
}

func (rf *Raft) appendSingleEntry(to int) {
	for {
		select {
		case <-rf.Ls.Done:
			return
		default:
			rf.Lock("ase lock1")
			if rf.role != leader || rf.killed() {
				rf.Unlock()
				return
			}
			prevIndex := rf.Ls.nextIndex[to] - 1
			prevTerm := rf.logs[prevIndex].Term
			logs := rf.logs[prevIndex+1:]
			args := &AppendEntryArgs{rf.term, rf.me, prevIndex, prevTerm, logs, rf.CommitIndex}
			reply := &AppendEntryReply{}
			rf.Unlock()

			ok := rf.peers[to].Call("Raft.AppendEntry", args, reply)
			if ok {
				rf.Lock("ase lock2")
				if reply.Term > rf.term {
					rf.withBiggerTerm(reply.Term, reply)
					rf.Unlock()
					return
				}
				if rf.role != leader || rf.killed() {
					rf.Unlock()
					return
				}
				if reply.Success {
					rf.Ls.nextIndex[to] = prevIndex + len(logs) + 1
					rf.Ls.matchIndex[to] = prevIndex + len(logs)
					rf.eternalChecker()
					rf.Unlock()
					return
				} else {
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
	for i := 0; i < rf.nPeer; i++ {
		if i == rf.me {
			continue
		}
		go rf.appendSingleEntry(i)
	}
}

//需要由caller加锁..
func (rf *Raft) eternalChecker() {
	for i := len(rf.logs) - 1; i > rf.LastApplied; i-- {
		// 对于不是当前term的log需要藉由其他log间接提交
		if rf.logs[i].Term != rf.term {
			DPrintf("%d:%d has log %d created in previous term \n", rf.me, rf.term, i)
			continue
		}
		votes := 0
		for j := 0; j < rf.nPeer; j++ {
			if j == rf.me {
				continue
			}
			if rf.Ls.matchIndex[j] >= i {
				votes++
				if rf.hasMajority(votes) {
					DPrintf("great leader %d:%d commit log from %d to %d \n", rf.me, rf.term, rf.LastApplied+1, i)
					for k := rf.LastApplied + 1; k <= i; k++ {
						rf.ac <- ApplyMsg{
							CommandValid: true,
							Command:      rf.logs[k].Command,
							CommandIndex: k,
						}
					}
					time.Sleep(time.Duration(10) * time.Millisecond)
					rf.CommitIndex = i
					rf.LastApplied = i
					return
				}
			}
		}
	}
}

func (rf *Raft) commitLog(to int) {
	for i := rf.CommitIndex; i <= to; i++ {
		rf.ac <- ApplyMsg{
			CommandValid: true,
			Command:      rf.logs[i].Command,
			CommandIndex: i,
		}
		DPrintf("%d:%d apply log,%d:%d", rf.me, rf.term, rf.logs[i].Command, i)
	}
	time.Sleep(time.Millisecond * time.Duration(10))
	rf.CommitIndex = to
	rf.LastApplied = to
}

func (rf *Raft) withBiggerTerm(term int, args interface{}) {
	DPrintf("%d:%d increase Term with %+v\n", rf.me, rf.term, args)
	rf.term = term
	rf.changeRole(follower)
}
