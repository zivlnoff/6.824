package tools

import (
	"sync"
	"sync/atomic"
)

type ConcurrentVar32 struct {
	sync.Mutex
	val int32
}

func (rv *ConcurrentVar32) read() int32 {
	return atomic.LoadInt32(&rv.val)
}

func (rv *ConcurrentVar32) write(v int32) {
	rv.Lock()
	defer rv.Unlock()

	rv.val = v
}
