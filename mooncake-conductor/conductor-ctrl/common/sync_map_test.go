package common

import (
	"fmt"
	"sync"
	"testing"
)

func TestSyncMap(t *testing.T) {
	sm := &SyncMap[string, int]{}

	// test Store and Load
	sm.Store("key1", 1)
	val, ok := sm.Load("key1")
	if !ok || val != 1 {
		t.Errorf("Expected Load('key1') to return (1, true), got (%d, %v)", val, ok)
	}

	// test Len
	if sm.Len() != 1 {
		t.Errorf("Expected Len() to return 1, got %d", sm.Len())
	}

	// test Load non-existent key
	val, ok = sm.Load("key2")
	if ok {
		t.Errorf("Expected Load('key2') to return (0, false), got (%d, %v)", val, ok)
	}

	// test LoadOrStore with non-existent key
	val, loaded := sm.LoadOrStore("key2", 2)
	if loaded || val != 2 {
		t.Errorf("Expected LoadOrStore('key2', 2) to return (2, false), got (%d, %v)", val, loaded)
	}
	if sm.Len() != 2 {
		t.Errorf("Expected Len() to return 2, got %d", sm.Len())
	}

	// test LoadOrStore with existing key
	val, loaded = sm.LoadOrStore("key1", 10)
	if !loaded || val != 1 {
		t.Errorf("Expected LoadOrStore('key1', 10) to return (1, true), got (%d, %v)", val, loaded)
	}

	// test Swap with existing key
	val, loaded = sm.Swap("key1", 3)
	if !loaded || val != 1 {
		t.Errorf("Expected Swap('key1', 3) to return (1, true), got (%d, %v)", val, loaded)
	}
	val, ok = sm.Load("key1")
	if !ok || val != 3 {
		t.Errorf("Expected Load('key1') after Swap to return (3, true), got (%d, %v)", val, ok)
	}

	// test Swap with non-existent key
	val, loaded = sm.Swap("key3", 4)
	if loaded || val != 0 {
		t.Errorf("Expected Swap('key3', 4) to return (0, false), got (%d, %v)", val, loaded)
	}
	if sm.Len() != 3 {
		t.Errorf("Expected Len() to return 3, got %d", sm.Len())
	}

	// test Keys
	keys := sm.Keys()
	if len(keys) != 3 {
		t.Errorf("Expected Keys() to return 3 keys, got %d", len(keys))
	}

	// test Keys is exists
	keyMap := make(map[string]bool)
	for _, k := range keys {
		keyMap[k] = true
	}
	expectedKeys := []string{"key1", "key2", "key3"}
	for _, k := range expectedKeys {
		if !keyMap[k] {
			t.Errorf("Expected key '%s' in Keys() result", k)
		}
	}

	values := sm.Values()
	if len(values) != 3 {
		t.Errorf("Expected Values() to return 3 values, got %d", len(values))
	}

	// test Range
	var rangeCount int
	sm.Range(func(key string, value int) bool {
		rangeCount++
		return true
	})
	if rangeCount != 3 {
		t.Errorf("Expected Range to iterate over 3 items, got %d", rangeCount)
	}

	// test Range with early termination
	var earlyTerminateCount int
	sm.Range(func(key string, value int) bool {
		earlyTerminateCount++
		return earlyTerminateCount < 2 // only iterate over the first two elements
	})
	if earlyTerminateCount != 2 {
		t.Errorf("Expected Range with early termination to iterate over 2 items, got %d", earlyTerminateCount)
	}

	// test LoadAndDelete
	val, loaded = sm.LoadAndDelete("key2")
	if !loaded || val != 2 {
		t.Errorf("Expected LoadAndDelete('key2') to return (2, true), got (%d, %v)", val, loaded)
	}
	if sm.Len() != 2 {
		t.Errorf("Expected Len() after LoadAndDelete to return 2, got %d", sm.Len())
	}

	// test Delete
	sm.Delete("key1")
	if sm.Len() != 1 {
		t.Errorf("Expected Len() after Delete to return 1, got %d", sm.Len())
	}
	val, ok = sm.Load("key1")
	if ok {
		t.Errorf("Expected Load('key1') after Delete to return (0, false), got (%d, %v)", val, ok)
	}

	// test concurrent safety
	var wg sync.WaitGroup
	concurrency := 100
	wg.Add(concurrency)
	for i := 0; i < concurrency; i++ {
		go func(i int) {
			defer wg.Done()
			key := fmt.Sprintf("concurrent_key_%d", i)
			sm.Store(key, i)
			val, ok := sm.Load(key)
			if !ok || val != i {
				t.Errorf("Concurrent test failed: Expected Load('%s') to return (%d, true), got (%d, %v)", key, i, val, ok)
			}
		}(i)
	}
	wg.Wait()
	if sm.Len() != concurrency+1 { // +1 for key3 still present
		t.Errorf("Expected Len() after concurrent operations to return %d, got %d", concurrency+1, sm.Len())
	}
}
