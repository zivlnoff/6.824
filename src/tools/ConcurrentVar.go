package tools

import (
	"sync"
	"sync/atomic"
)

type ConcurrentVar32 struct {
	sync.Mutex
	val int32
}

func (rv *ConcurrentVar32) Read() int32 {
	return atomic.LoadInt32(&rv.val)
}

func (rv *ConcurrentVar32) Write(v int32) {
	rv.Lock()
	defer rv.Unlock()

	rv.val = v
}

func (rv *ConcurrentVar32) SmallerAndSet(v int32) bool {
	rv.Lock()
	defer rv.Unlock()

	if rv.val < v {
		rv.val = v
		return true
	}

	return false
}

func (rv *ConcurrentVar32) IsEqual(v int32) bool {
	rv.Lock()
	defer rv.Unlock()

	if rv.val == v {
		return true
	}

	return false
}
