package raft

import "time"

type AppendEntryArgs struct {
	Term         int
	From         int
	PrevLogIndex int
	PrevLogTerm  int
	Entry        []Log
	LeaderCommit int
	Snapshot     []byte
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
		DPrintf("%d:%d is asked appending entry %+v,%+v", rf.Me, rf.term, args, reply)
	}()

	reply.Success = false
	if args.Term < rf.term {
		return
	}

	index, _ := rf.getLastIndexTerm()
	matched,covered:=true,false

	if hasSnapshot(args.Snapshot) {
		if args.PrevLogIndex < rf.LastApplied {
			DPrintf("refuse:%d<%d",args.PrevLogIndex,rf.LastApplied)
			matched = false
		} else {
			reply.Success = true
			if args.PrevLogIndex > rf.LastApplied {
				covered = true
			} else {
				nowSize := len(rf.logs) + rf.LastIncludedIndex + 1
				newSize := args.PrevLogIndex + len(args.Entry) + 1
				if nowSize < newSize {
					covered = true
				}
			}
			if covered {
				rf.installSnapshot(args.Snapshot, args.PrevLogIndex, args.PrevLogTerm)
				rf.LastApplied = args.PrevLogIndex
				rf.CommitIndex = args.PrevLogIndex
				rf.logs = args.Entry
				rf.Chsnapshot <- args.Snapshot
				time.Sleep(50*time.Millisecond)
			}
		}
	} else {
		localIndex := args.PrevLogIndex - 1 - rf.LastIncludedIndex
		if localIndex >= len(rf.logs) || localIndex < 0 {
			DPrintf("debug:%+v\n%+v", rf,args)
		}
		if index < args.PrevLogIndex {
			matched = false
		} else if rf.logs[localIndex].Term != args.PrevLogTerm {
			DPrintf("%d:%d delete log\n", rf.Me, rf.term)
			rf.logs = rf.logs[:localIndex]
			matched = false
		}
		if matched {
			//心跳包不检查
			if len(args.Entry) != 0 {
				// RPC是乱序的,可能会收到旧的RPC,需要进一步检查
				shouldConcat := true
				nowSize := len(rf.logs) + rf.LastIncludedIndex + 1
				newSize := args.PrevLogIndex + len(args.Entry) + 1

				if nowSize > newSize {
					argsLastTerm := args.Entry[len(args.Entry)-1].Term
					i:=newSize-(rf.LastIncludedIndex+1)-1
					if rf.logs[i].Term == argsLastTerm {
						shouldConcat = false
					}
				}

				if shouldConcat {
					rf.logs = rf.logs[:localIndex+1]
					rf.logs = append(rf.logs, args.Entry...)
				}
			}
			reply.Success = true
		}
	}

	if matched && args.LeaderCommit > rf.CommitIndex {
		if newIndex := len(rf.logs) + rf.LastIncludedIndex; args.LeaderCommit > newIndex {
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
	start := rf.term
	for {
		select {
		case <-rf.Ls.Done:
			return
		default:
			rf.Lock("ase lock1")
			if rf.role != leader || rf.killed() ||rf.term!=start{
				rf.Unlock()
				return
			}
			prevIndex := rf.Ls.nextIndex[to] - 1
			args := &AppendEntryArgs{
				PrevLogIndex: prevIndex,
				Term:         rf.term,
				From:         rf.Me,
				LeaderCommit: rf.CommitIndex,
			}

			if prevIndex <= rf.LastIncludedIndex {
				args.PrevLogIndex = rf.LastIncludedIndex
				args.PrevLogTerm = rf.LastIncludedTerm
				args.Entry = rf.logs[:]
				DPrintf("%d,ask install snapshot to %d:%+v", args.From, to, args)
				args.Snapshot = rf.Persister.ReadSnapshot()
			} else {
				local := prevIndex - 1 - rf.LastIncludedIndex
				args.PrevLogTerm = rf.logs[local].Term
				args.Entry = rf.logs[local+1:]
			}

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
				if rf.role != leader || rf.killed()||rf.term!=start {
					rf.Unlock()
					return
				}
				if reply.Success {
					rf.Ls.nextIndex[to] = prevIndex + len(args.Entry) + 1
					rf.Ls.matchIndex[to] = prevIndex + len(args.Entry)
					rf.eternalChecker()
					rf.Unlock()
					return
				} else {

					if rf.Ls.nextIndex[to] > 1+rf.LastIncludedIndex && rf.Ls.nextIndex[to] == args.PrevLogIndex+1 {
						rf.Ls.nextIndex[to]--
						if rf.Ls.nextIndex[to] == 0 {
							DPrintf("indexxxxxxxxxx:%+v,%+v", args, reply)
						}
					}
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
		if i == rf.Me {
			continue
		}
		go rf.appendSingleEntry(i)
	}
}

//需要由caller加锁..
func (rf *Raft) eternalChecker() {
	rf.PrevLog = false
	for i := len(rf.logs) - 1; ; i-- {
		// 对于不是当前term的log需要藉由其他log间接提交
		trueI := i + rf.LastIncludedIndex + 1
		if trueI == rf.LastApplied || i == -1 {
			return
		}
		if rf.logs[i].Term != rf.term {
			rf.PrevLog = true
			DPrintf("%d:%d has log %d created in previous term \n", rf.Me, rf.term, i)
			continue
		}
		votes := 0

		for j := 0; j < rf.nPeer; j++ {
			if j == rf.Me {
				continue
			}
			if rf.Ls.matchIndex[j] >= trueI {
				votes++
				if rf.hasMajority(votes) {
					DPrintf("great leader %d:%d commit log from %d to %d \n", rf.Me, rf.term, rf.LastApplied+1, trueI)
					for k := rf.LastApplied + 1; k <= trueI; k++ {
						trueK := k - rf.LastIncludedIndex - 1
						if trueK >= len(rf.logs) || trueK < 0 {
							DPrintf("debug:%+v", rf)
						}
						rf.ac <- ApplyMsg{
							CommandValid: true,
							Command:      rf.logs[trueK].Command,
							CommandIndex: k,
						}
					}
					//time.Sleep(time.Duration(10) * time.Millisecond)
					rf.CommitIndex = trueI
					rf.LastApplied = trueI
					return
				}
			}
		}
	}
}

func (rf *Raft) commitLog(to int) {

	for i := rf.CommitIndex + 1; i <= to; i++ {
		index := i - rf.LastIncludedIndex - 1
		if index >= len(rf.logs) || index < 0 {
			DPrintf("debug:%+v", rf)
		}
		rf.ac <- ApplyMsg{
			CommandValid: true,
			Command:      rf.logs[index].Command,
			CommandIndex: i,
		}
		//DPrintf("%d:%d apply log,%d:%d", rf.Me, rf.term, rf.logs[index].Command, i)
	}
	time.Sleep(time.Millisecond * time.Duration(10))
	rf.CommitIndex = to
	rf.LastApplied = to
}

func (rf *Raft) withBiggerTerm(term int, args interface{}) {
	DPrintf("%d:%d increase Term with %+v\n", rf.Me, rf.term, args)
	rf.term = term
	rf.changeRole(follower)
}

func (rf *Raft) installSnapshot(b []byte, index, term int) {
	if index == rf.LastIncludedIndex {
		DPrintf("###duplicated snapshot###")
		return
	}
	rf.LastIncludedIndex = index
	rf.LastIncludedTerm = term
	rs := rf.SerializeRaftState()
	rf.Persister.SaveStateAndSnapshot(rs, b)
}
func hasSnapshot(b []byte) bool{
	return len(b)>0
}