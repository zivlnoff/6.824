package tools

import (
	"sync"
	"sync/atomic"
)

type ConcurrentVarInt32 struct {
	sync.Mutex
	val int32
}

func (rvInt32 *ConcurrentVarInt32) Read() int32 {
	return atomic.LoadInt32(&rvInt32.val)
}

func (rvInt32 *ConcurrentVarInt32) Write(v int32) {
	rvInt32.Lock()
	defer rvInt32.Unlock()

	rvInt32.val = v
}

func (rvInt32 *ConcurrentVarInt32) SmallerAndSet(v int32) bool {
	rvInt32.Lock()
	defer rvInt32.Unlock()

	if rvInt32.val < v {
		rvInt32.val = v
		return true
	}

	return false
}

func (rvInt32 *ConcurrentVarInt32) IsEqual(v int32) bool {
	rvInt32.Lock()
	defer rvInt32.Unlock()

	if rvInt32.val == v {
		return true
	}

	return false
}

type ConcurrentVarInt struct {
	sync.Mutex
	Val int
}

func (rvInt *ConcurrentVarInt) Lock() {
	rvInt.Lock()
}

func (rvInt *ConcurrentVarInt) UnLock() {
	rvInt.Unlock()
}
