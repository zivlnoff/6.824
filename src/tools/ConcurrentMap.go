package tools

import (
	"sync"
)

type any interface{}

type ConcurrentMap struct {
	sync.RWMutex
	entry map[any]any
}

func NewConcurrentMap() *ConcurrentMap {
	return &ConcurrentMap{
		entry: make(map[any]any),
	}
}

func (rm *ConcurrentMap) load(key any) (value any, ok bool) {
	rm.RLock()
	defer rm.RUnlock()

	result, ok := rm.entry[key]
	return result, ok
}

func (rm *ConcurrentMap) delete(key any) {
	rm.Lock()
	defer rm.Unlock()

	delete(rm.entry, key)
}

func (rm *ConcurrentMap) store(key any, value any) {
	rm.Lock()
	defer rm.Unlock()

	rm.entry[key] = value
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
	defer rm.RUnlock()

	return len(rm.entry)
}

func (rm *ConcurrentMap) Random() any {
	rm.Lock()
	defer rm.Unlock()
	for k, _ := range rm.entry {
		return k
	}

	return nil
}
