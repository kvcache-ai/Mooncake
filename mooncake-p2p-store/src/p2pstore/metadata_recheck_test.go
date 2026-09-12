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
	"context"
	"errors"
	"sync"
	"testing"
)

type metadataResult struct {
	payload  *Payload
	revision int64
	err      error
}

type scriptedMetadata struct {
	results     []metadataResult
	getCalls    int
	updateCalls int
}

func (m *scriptedMetadata) Close() error { return nil }
func (m *scriptedMetadata) Create(context.Context, string, *Payload) error {
	return nil
}
func (m *scriptedMetadata) Put(context.Context, string, *Payload) error { return nil }
func (m *scriptedMetadata) List(context.Context, string) ([]*Payload, error) {
	return nil, nil
}
func (m *scriptedMetadata) Get(context.Context, string) (*Payload, int64, error) {
	if m.getCalls >= len(m.results) {
		return nil, -1, errors.New("unexpected metadata Get")
	}
	result := m.results[m.getCalls]
	m.getCalls++
	return result.payload, result.revision, result.err
}
func (m *scriptedMetadata) Update(context.Context, string, *Payload, int64) (bool, error) {
	m.updateCalls++
	return true, nil
}

type successfulTransferEngine struct {
	mu     sync.Mutex
	nextID BatchID
	freed  int
	reg    int
	unreg  int
	events []string
}

func (f *successfulTransferEngine) Close() {}
func (f *successfulTransferEngine) GetLocalIpAndPort() (string, error) {
	return "test", nil
}
func (f *successfulTransferEngine) registerLocalMemory(uintptr, uint64, string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.reg++
	f.events = append(f.events, "register")
	return nil
}
func (f *successfulTransferEngine) unregisterLocalMemory(uintptr) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.unreg++
	f.events = append(f.events, "unregister")
	return nil
}
func (f *successfulTransferEngine) allocateBatchID(int) (BatchID, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.nextID++
	return f.nextID, nil
}
func (f *successfulTransferEngine) openSegment(string, bool) (int64, error) {
	return 1, nil
}
func (f *successfulTransferEngine) submitTransfer(BatchID, []TransferRequest) error {
	return nil
}
func (f *successfulTransferEngine) getTransferStatus(BatchID, int) (int, uint64, error) {
	return STATUS_COMPLETED, 0, nil
}
func (f *successfulTransferEngine) freeBatchID(BatchID) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.freed++
	return nil
}

func payloadWithLayout(size uint64, sources ...Location) *Payload {
	shards := make([]Shard, 0, (size+4095)/4096)
	for remaining := size; remaining > 0; {
		length := min(remaining, uint64(4096))
		shards = append(shards, Shard{Length: length, Gold: sources})
		remaining -= length
	}
	return &Payload{
		Name:         "payload",
		Size:         size,
		SizeList:     []uint64{size},
		MaxShardSize: 4096,
		Shards:       shards,
	}
}

func newMetadataRecheckStore(metadata metadataStore) (*P2PStore, *successfulTransferEngine) {
	engine := &successfulTransferEngine{}
	return &P2PStore{
		localServerName: "replica",
		catalog:         NewCatalog(),
		memory: &RegisteredMemory{
			engine:       engine,
			maxChunkSize: 4096,
		},
		metadata: metadata,
		transfer: engine,
	}, engine
}

func TestReconcilePayloadAfterTransfer(t *testing.T) {
	sourceA := Location{SegmentName: "source-a", Offset: 0}
	sourceB := Location{SegmentName: "source-b", Offset: 0}
	original := payloadWithLayout(4096, sourceA, sourceB)

	t.Run("removed source retries latest snapshot", func(t *testing.T) {
		latest := payloadWithLayout(4096, sourceB)
		payload, revision, retry, err := reconcilePayloadAfterTransfer(original, 10, latest, 11)
		if err != nil {
			t.Fatalf("reconcile failed: %v", err)
		}
		if !retry || payload != latest || revision != 11 {
			t.Fatalf("retry=%v payload=%p revision=%d, want latest payload at revision 11", retry, payload, revision)
		}
	})

	t.Run("added source keeps completed transfer", func(t *testing.T) {
		latest := payloadWithLayout(4096, sourceA, sourceB,
			Location{SegmentName: "source-c", Offset: 0})
		payload, revision, retry, err := reconcilePayloadAfterTransfer(original, 10, latest, 11)
		if err != nil {
			t.Fatalf("reconcile failed: %v", err)
		}
		if retry || payload != latest || revision != 11 {
			t.Fatalf("retry=%v payload=%p revision=%d, want completed latest payload at revision 11", retry, payload, revision)
		}
	})

	t.Run("deleted payload returns not found", func(t *testing.T) {
		_, _, retry, err := reconcilePayloadAfterTransfer(original, 10, nil, -1)
		if !errors.Is(err, ErrPayloadNotFound) || retry {
			t.Fatalf("retry=%v err=%v, want ErrPayloadNotFound without retry", retry, err)
		}
	})
}

func TestGetReplicaReleasesRegistrationBeforeRetry(t *testing.T) {
	sourceA := Location{SegmentName: "source-a", Offset: 0}
	sourceB := Location{SegmentName: "source-b", Offset: 0}
	original := payloadWithLayout(4096, sourceA, sourceB)
	latest := payloadWithLayout(4096, sourceB)
	metadata := &scriptedMetadata{results: []metadataResult{
		{payload: original, revision: 10},
		{payload: latest, revision: 11},
		{payload: latest, revision: 11},
	}}
	store, engine := newMetadataRecheckStore(metadata)

	err := store.GetReplica(context.Background(), "payload", []uintptr{0x1000}, []uint64{4096})
	if err != nil {
		t.Fatalf("GetReplica failed: %v", err)
	}
	if len(store.memory.bufferList) != 1 || store.memory.bufferList[0].refCount != 1 {
		t.Fatalf("registered memory = %+v, want one catalog-owned reference", store.memory.bufferList)
	}
	if engine.reg != 2 || engine.unreg != 1 {
		t.Fatalf("memory registrations=%d unregistrations=%d, want 2 and 1", engine.reg, engine.unreg)
	}
	wantEvents := []string{"register", "unregister", "register"}
	if len(engine.events) != len(wantEvents) {
		t.Fatalf("memory events=%v, want %v", engine.events, wantEvents)
	}
	for i := range wantEvents {
		if engine.events[i] != wantEvents[i] {
			t.Fatalf("memory events=%v, want %v", engine.events, wantEvents)
		}
	}
	if metadata.updateCalls != 1 {
		t.Fatalf("metadata updates=%d, want 1", metadata.updateCalls)
	}
}

func TestGetReplicaRejectsLayoutChangesAndReleasesRegistration(t *testing.T) {
	source := Location{SegmentName: "source", Offset: 0}
	tests := []struct {
		name   string
		latest *Payload
	}{
		{name: "payload growth", latest: payloadWithLayout(8192, source)},
		{name: "payload shrinkage", latest: payloadWithLayout(2048, source)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			original := payloadWithLayout(4096, source)
			metadata := &scriptedMetadata{results: []metadataResult{
				{payload: original, revision: 10},
				{payload: tt.latest, revision: 11},
			}}
			store, engine := newMetadataRecheckStore(metadata)

			err := store.GetReplica(context.Background(), "payload", []uintptr{0x1000}, []uint64{4096})
			if !errors.Is(err, ErrInvalidArgument) {
				t.Fatalf("GetReplica error=%v, want ErrInvalidArgument", err)
			}
			if len(store.memory.bufferList) != 0 {
				t.Fatalf("registered memory leaked: %+v", store.memory.bufferList)
			}
			if engine.reg != 1 || engine.unreg != 1 {
				t.Fatalf("memory registrations=%d unregistrations=%d, want 1 and 1", engine.reg, engine.unreg)
			}
			if metadata.updateCalls != 0 {
				t.Fatalf("metadata updates=%d, want 0", metadata.updateCalls)
			}
		})
	}
}
