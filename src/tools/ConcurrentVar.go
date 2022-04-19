package tools

import (
	"sync"
	"sync/atomic"
)

type ConcurrentVarInt32 struct {
	sync.Mutex
	val int32
}

func NewConcurrentVarInt32(v int32) *ConcurrentVarInt32 {
	return &ConcurrentVarInt32{
		Mutex: sync.Mutex{},
		val:   v,
	}
}

func (rvInt32 *ConcurrentVarInt32) Read() int32 {
	return atomic.LoadInt32(&rvInt32.val)
}

func (rvInt32 *ConcurrentVarInt32) Write(v int32) {
	rvInt32.Lock()
	defer rvInt32.Unlock()

	rvInt32.val = v
}

func (rvInt32 *ConcurrentVarInt32) SmallerAndSet(v int32) (int32, bool) {
	rvInt32.Lock()
	defer rvInt32.Unlock()

	old := rvInt32.val
	if old < v {
		rvInt32.val = v
		return old, true
	}

	return old, false
}

func (rvInt32 *ConcurrentVarInt32) BiggerAndSet(v int32) bool {
	rvInt32.Lock()
	defer rvInt32.Unlock()

	if rvInt32.val > v {
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

func (rvInt32 *ConcurrentVarInt32) AddOne() int32 {
	return atomic.AddInt32(&rvInt32.val, 1)
}

type ConcurrentVarInt struct {
	sync.RWMutex
	val int
}

func NewConcurrentVarInt(v int) *ConcurrentVarInt {
	return &ConcurrentVarInt{
		RWMutex: sync.RWMutex{},
		val:     v,
	}
}

func (rvInt *ConcurrentVarInt) Lock() {
	rvInt.RWMutex.Lock()
}

func (rvInt *ConcurrentVarInt) UnLock() {
	rvInt.RWMutex.Unlock()
}

func (rvInt *ConcurrentVarInt) RLock() {
	rvInt.RWMutex.RLock()
}

func (rvInt *ConcurrentVarInt) RUnLock() {
	rvInt.RWMutex.RUnlock()
}
func (rvInt *ConcurrentVarInt) Read() int {
	rvInt.RLock()
	defer rvInt.RUnLock()

	return rvInt.val
}

func (rvInt *ConcurrentVarInt) Write(v int) {
	rvInt.Lock()
	defer rvInt.Unlock()

	rvInt.val = v
}

func (rvInt *ConcurrentVarInt) AddOne() int {
	rvInt.Lock()
	defer rvInt.Unlock()

	rvInt.val++

	return rvInt.val
}
