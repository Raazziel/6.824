package kvraft

import (
	"../labrpc"
	rr "math/rand"
	"sync/atomic"
	"time"
	"crypto/rand"
	"math/big"
	"../raft"
)


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

//append不是幂等的,要避免重复的rpc
func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.nServers = len(servers)
	ck.who = rr.Int()
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
	DPrintf("%d,get call,%s", ck.who, key)
	args := GetArgs{Key: key}
	reply := GetReply{}
	for {
		for i := 0; i < ck.nServers; i++ {
			DPrintf("get call start")
			c := make(chan string)
			go func() {
				ck.servers[i].Call("KVServer.Get", &args, &reply)
				if reply.Err == OK {
					c <- reply.Value
				}
			}()
			select {
			case val := <-c:
				return val
			case <-time.After(100 * time.Millisecond):
				continue
			}
		}

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
	DPrintf("%d,put call,%s,%s", ck.who, key, value)
	index := atomic.AddInt32(&ck.cmdIndex, 1)
	args := PutAppendArgs{key, value, op, index, ck.who}
	reply := PutAppendReply{}
	done := false
	for {
		for i := 0; i < ck.nServers; i++ {
			DPrintf("put call start")
			ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
			if reply.Err == OK {
				DPrintf("put call done")
				done = true
			}
		}
		if done {
			// 等待leader把commitindex同步到follower...
			time.Sleep(raft.HBInterval*time.Millisecond)
			return
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	DPrintf("put call %s %s", key, value)
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	DPrintf("apd call %s %s", key, value)
	ck.PutAppend(key, value, "Append")
}
