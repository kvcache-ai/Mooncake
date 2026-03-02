package common

import (
	"sync"
	"sync/atomic"
)

type SyncMap[K any, V any] struct {
	m   sync.Map
	len atomic.Int32
}

func (sm *SyncMap[K, V]) Delete(key K) {
	sm.LoadAndDelete(key)
}

func (sm *SyncMap[K, V]) Load(key K) (typedVal V, ok bool) {
	value, ok := sm.m.Load(key)
	if ok {
		typedVal = value.(V)
	}
	return
}

func (sm *SyncMap[K, V]) LoadAndDelete(key K) (typedVal V, loaded bool) {
	value, loaded := sm.m.LoadAndDelete(key)
	if loaded {
		typedVal = value.(V)
		sm.len.Add(-1)
	}
	return
}

func (sm *SyncMap[K, V]) LoadOrStore(key K, value V) (V, bool) {
	actual, loaded := sm.m.LoadOrStore(key, value)
	if !loaded {
		sm.len.Add(1)
	}
	return actual.(V), loaded
}

func (sm *SyncMap[K, V]) Range(f func(key K, value V) bool) {
	sm.m.Range(func(key, value any) bool {
		return f(key.(K), value.(V))
	})
}

func (sm *SyncMap[K, V]) Keys() []K {
	k := make([]K, 0, sm.Len())
	sm.m.Range(func(key, value any) bool {
		k = append(k, key.(K))
		return true
	})
	return k
}

func (sm *SyncMap[K, V]) Values() []V {
	v := make([]V, 0, sm.Len())
	sm.m.Range(func(key, value any) bool {
		v = append(v, value.(V))
		return true
	})
	return v
}

func (sm *SyncMap[K, V]) Store(key K, value V) {
	sm.Swap(key, value)
}

func (sm *SyncMap[K, V]) Swap(key K, value V) (V, bool) {
	old, loaded := sm.m.Swap(key, value)
	if !loaded {
		var ret V
		sm.len.Add(1)
		return ret, loaded
	}
	return old.(V), loaded
}

func (sm *SyncMap[K, V]) Len() int {
	return int(sm.len.Load())
}
