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
	"testing"
)

func payloadWithSources(sources ...Location) *Payload {
	return &Payload{Shards: []Shard{{Length: 4096, Gold: sources}}}
}

func TestReconcilePayloadAfterTransfer(t *testing.T) {
	original := payloadWithSources(
		Location{SegmentName: "source-a", Offset: 0},
		Location{SegmentName: "source-b", Offset: 0},
	)

	t.Run("removed source retries latest snapshot", func(t *testing.T) {
		latest := payloadWithSources(Location{SegmentName: "source-b", Offset: 0})
		payload, revision, retry, err := reconcilePayloadAfterTransfer(original, 10, latest, 11)
		if err != nil {
			t.Fatalf("reconcile failed: %v", err)
		}
		if !retry {
			t.Fatal("source removal did not request a retry")
		}
		if payload != latest || revision != 11 {
			t.Fatalf("retry kept stale snapshot: payload=%p revision=%d", payload, revision)
		}
	})

	t.Run("added source keeps completed transfer", func(t *testing.T) {
		latest := payloadWithSources(
			Location{SegmentName: "source-a", Offset: 0},
			Location{SegmentName: "source-b", Offset: 0},
			Location{SegmentName: "source-c", Offset: 0},
		)
		payload, revision, retry, err := reconcilePayloadAfterTransfer(original, 10, latest, 11)
		if err != nil {
			t.Fatalf("reconcile failed: %v", err)
		}
		if retry {
			t.Fatal("source addition unnecessarily requested a retry")
		}
		if payload != latest || revision != 11 {
			t.Fatalf("finalization kept stale snapshot: payload=%p revision=%d", payload, revision)
		}
	})

	t.Run("deleted payload returns not found", func(t *testing.T) {
		_, _, retry, err := reconcilePayloadAfterTransfer(original, 10, nil, -1)
		if !errors.Is(err, ErrPayloadNotFound) {
			t.Fatalf("err = %v, want ErrPayloadNotFound", err)
		}
		if retry {
			t.Fatal("deleted payload requested a retry")
		}
	})
}

func TestReleaseRetryRegistrationsKeepsCatalogReference(t *testing.T) {
	const (
		addr             = uintptr(0x1000)
		length           = uint64(4096)
		maxShardSize     = uint64(4096)
		successfulRounds = 3
	)
	memory := &RegisteredMemory{
		bufferList:   []BufferHandle{{addr: addr, length: length, refCount: successfulRounds}},
		maxChunkSize: maxShardSize,
	}
	store := &P2PStore{memory: memory}

	store.releaseRetryRegistrations(
		[]Buffer{{addr: addr, size: length}}, maxShardSize, successfulRounds)

	if got := memory.bufferList[0].refCount; got != 1 {
		t.Fatalf("registration refcount = %d, want one catalog-owned reference", got)
	}
}
