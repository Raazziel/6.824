package raft

import (
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

// Debugging
const Debug = 0

func DPrintf(format string, a ...interface{}) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type DLock struct {
	mu  sync.Mutex
	lid int32
}

func (m *DLock) Lock(desc string) {
	m.mu.Lock()
	if Debug > 0 {
		r := atomic.AddInt32(&m.lid, 2)
		go func() {
			time.Sleep(time.Duration(100) * time.Millisecond)
			if atomic.LoadInt32(&m.lid) == r {
				fmt.Println("deadlock detected", desc, "+", m.lid)
			}
		}()
	}
}
func (m *DLock) Unlock() {
	atomic.AddInt32(&m.lid, 7)
	m.mu.Unlock()
}
