// Copyright 2026 KVCache.AI
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package p2pstore

import (
	"errors"
	"sync"
	"testing"
	"time"
)

type blockingMemoryEngine struct {
	registerStarted   chan uintptr
	unregisterStarted chan uintptr
	allowRegister     chan struct{}
	allowUnregister   chan struct{}
	registerErr       error
	mu                sync.Mutex
	events            []string
}

func (e *blockingMemoryEngine) registerLocalMemory(addr uintptr, _ uint64, _ string) error {
	if e.registerStarted != nil {
		e.registerStarted <- addr
	}
	if e.allowRegister != nil {
		<-e.allowRegister
	}
	e.mu.Lock()
	e.events = append(e.events, "register")
	e.mu.Unlock()
	return e.registerErr
}

func (e *blockingMemoryEngine) unregisterLocalMemory(addr uintptr) error {
	if e.unregisterStarted != nil {
		e.unregisterStarted <- addr
	}
	if e.allowUnregister != nil {
		<-e.allowUnregister
	}
	e.mu.Lock()
	e.events = append(e.events, "unregister")
	e.mu.Unlock()
	return nil
}

func asyncAdd(memory *RegisteredMemory, addr uintptr) chan error {
	result := make(chan error, 1)
	go func() { result <- memory.Add(addr, 4096, 4096, "test") }()
	return result
}

func asyncRemove(memory *RegisteredMemory, addr uintptr) chan error {
	result := make(chan error, 1)
	go func() { result <- memory.Remove(addr, 4096, 4096) }()
	return result
}

func assertBlocked(t *testing.T, result chan error) {
	t.Helper()
	select {
	case err := <-result:
		t.Fatalf("operation returned early with %v", err)
	case <-time.After(20 * time.Millisecond):
	}
}

func TestAddWaitsForRegistrationRollback(t *testing.T) {
	injected := errors.New("injected registration failure")
	engine := &blockingMemoryEngine{
		registerStarted: make(chan uintptr, 1),
		allowRegister:   make(chan struct{}),
		registerErr:     injected,
	}
	memory := &RegisteredMemory{engine: engine, maxChunkSize: 4096}
	first := asyncAdd(memory, 0x1000)
	<-engine.registerStarted
	second := asyncAdd(memory, 0x1000)
	assertBlocked(t, second)
	close(engine.allowRegister)
	if err := <-first; !errors.Is(err, injected) {
		t.Fatalf("first Add error = %v, want %v", err, injected)
	}
	if err := <-second; !errors.Is(err, injected) {
		t.Fatalf("second Add error = %v, want %v", err, injected)
	}
	if len(memory.bufferList) != 0 {
		t.Fatalf("bufferList contains %d entries after rollback, want 0", len(memory.bufferList))
	}

	engine.registerStarted = nil
	engine.registerErr = nil
	if err := memory.Add(0x1000, 4096, 4096, "test"); err != nil {
		t.Fatalf("Add after rollback failed: %v", err)
	}
	if len(memory.bufferList) != 1 || memory.bufferList[0].refCount != 1 {
		t.Fatalf("unexpected entries after retry: %+v", memory.bufferList)
	}
}

func TestAddWaitsForFinalUnregister(t *testing.T) {
	engine := &blockingMemoryEngine{}
	memory := &RegisteredMemory{engine: engine, maxChunkSize: 4096}
	if err := memory.Add(0x2000, 4096, 4096, "test"); err != nil {
		t.Fatalf("initial Add failed: %v", err)
	}
	engine.unregisterStarted = make(chan uintptr)
	engine.allowUnregister = make(chan struct{})
	removeResult := asyncRemove(memory, 0x2000)
	<-engine.unregisterStarted
	addResult := asyncAdd(memory, 0x2000)
	assertBlocked(t, addResult)
	close(engine.allowUnregister)
	if err := <-removeResult; err != nil {
		t.Fatalf("Remove failed: %v", err)
	}
	if err := <-addResult; err != nil {
		t.Fatalf("Add after unregister failed: %v", err)
	}
	if len(memory.bufferList) != 1 || memory.bufferList[0].refCount != 1 {
		t.Fatalf("unexpected entries after re-registration: %+v", memory.bufferList)
	}
	if len(engine.events) != 3 || engine.events[0] != "register" ||
		engine.events[1] != "unregister" || engine.events[2] != "register" {
		t.Fatalf("unexpected operation order: %v", engine.events)
	}
}

func TestIndependentRegionProgressesDuringUnregister(t *testing.T) {
	engine := &blockingMemoryEngine{}
	memory := &RegisteredMemory{engine: engine, maxChunkSize: 4096}
	if err := memory.Add(0x3000, 4096, 4096, "test"); err != nil {
		t.Fatalf("Add first region failed: %v", err)
	}
	if err := memory.Add(0x4000, 4096, 4096, "test"); err != nil {
		t.Fatalf("Add second region failed: %v", err)
	}
	engine.unregisterStarted = make(chan uintptr)
	engine.allowUnregister = make(chan struct{})
	removeResult := asyncRemove(memory, 0x3000)
	<-engine.unregisterStarted
	addResult := asyncAdd(memory, 0x5000)
	select {
	case err := <-addResult:
		if err != nil {
			t.Fatalf("independent Add failed: %v", err)
		}
	case <-time.After(100 * time.Millisecond):
		t.Fatal("independent Add was blocked by another region's unregister")
	}
	close(engine.allowUnregister)
	if err := <-removeResult; err != nil {
		t.Fatalf("Remove failed: %v", err)
	}
}
