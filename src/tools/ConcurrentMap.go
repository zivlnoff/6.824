package tools

import (
	"sync"
)

type any interface{}

type ConcurrentMap struct {
	sync.RWMutex
	size  int
	entry map[any]any
}

func NewConcurrentMap() *ConcurrentMap {
	return &ConcurrentMap{
		entry: make(map[any]any),
	}
}

func (rm *ConcurrentMap) load(key any) (value any, ok bool) {
	rm.RLock()
	result, ok := rm.entry[key]
	rm.RUnlock()
	return result, ok
}

func (rm *ConcurrentMap) delete(key any) {
	rm.Lock()
	delete(rm.entry, key)
	rm.size--
	rm.Unlock()
}

func (rm *ConcurrentMap) store(key any, value any) {
	rm.Lock()
	rm.entry[key] = value
	rm.size++
	rm.Unlock()
}

func (rm *ConcurrentMap) Load(key any) (value any, ok bool) {
	return rm.load(key)
}

func (rm *ConcurrentMap) Delete(key any) {
	rm.delete(key)
}

func (rm *ConcurrentMap) Store(key any, value any) {
	rm.store(key, value)
}

func (rm *ConcurrentMap) Size() int {
	// forget add Lock...
	rm.RLock()
	size := rm.size
	rm.RUnlock()
	return size
}

func (rm *ConcurrentMap) Random() any {
	for k, _ := range rm.entry {
		return k
	}
	return nil
}
