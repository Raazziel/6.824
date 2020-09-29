package kvraft

import (
	"../labrpc"
	"sync/atomic"
	rr "math/rand"
)
import "crypto/rand"
import "math/big"

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	nServers int

	who      int
	cmdIndex int32
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.nServers = len(servers)
	ck.who=rr.Int()
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {
	// You will have to modify this function.
	args := GetArgs{Key: key}
	reply := GetReply{}
	//for i:=0;i<ck.nServers;i++{
	//	for{
	//		if ok:=ck.servers[i].Call("KVServer.Get", &args, &reply);!ok{
	//			continue
	//		}
	//		if reply.Err==OK {
	//			DPrintf("put call done")
	//			return reply.Value
	//		}else if reply.Err==ErrNoKey{
	//			return ""
	//		}
	//		break
	//	}
	//}
	//return ""
	leaderIndex := 0

	for {
		for {
			if ok := ck.servers[leaderIndex].Call("KVServer.Get", &args, &reply); !ok {
				continue
			}
			if reply.Err == OK {
				DPrintf("put call done")
				return reply.Value
			} else if reply.Err == ErrNoKey {
				return ""
			}
			break
		}
		leaderIndex = (leaderIndex + 1) % ck.nServers
	}
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//

func (ck *Clerk) PutAppend(key string, value string, op string) {
	index:=atomic.AddInt32(&ck.cmdIndex,1)
	// You will have to modify this function.
	args := PutAppendArgs{key, value, op,index,ck.who}
	reply := PutAppendReply{}
	leaderIndex := 0
	for {
		for {
			if ok := ck.servers[leaderIndex].Call("KVServer.PutAppend", &args, &reply); !ok {
				continue
			}
			if reply.Err == OK {
				DPrintf("put call done")
				return
			}
			break
		}
		leaderIndex = (leaderIndex + 1) % ck.nServers
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
