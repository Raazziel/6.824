package raft

import (
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

// Debugging
const Debug = 1

func DPrintf(format string, a ...interface{}) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}
type DLock struct {
	mu sync.Mutex
	lid int32
}

func (m *DLock)Lock(desc string){
	r:=atomic.AddInt32(&m.lid,2)
	go func(){
		time.Sleep(time.Duration(10)*time.Millisecond)
		if atomic.LoadInt32(&m.lid)==r{
			fmt.Println("deadlock detected",desc)
		}
	}()
	m.mu.Lock()
}
func (m *DLock)	Unlock(){
	atomic.AddInt32(&m.lid,7)
	m.mu.Unlock()
}