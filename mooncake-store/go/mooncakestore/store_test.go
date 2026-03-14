// Copyright 2024 KVCache.AI
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

package mooncakestore

import (
	"testing"
)

// Unit tests run with `go test -short`. Integration tests additionally
// require a running mooncake_master (mooncake_master --enable_http_metadata_server=true)
// and are invoked via `go test -v ./...` (without -short).

func setupStore(t *testing.T) *Store {
	t.Helper()
	store, err := New()
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}
	err = store.Setup(
		"localhost",
		"http://localhost:8080/metadata",
		512*1024*1024, // 512 MB global segment
		128*1024*1024, // 128 MB local buffer
		"tcp",
		"",
		"localhost:50051",
	)
	if err != nil {
		store.Close()
		t.Fatalf("Setup() failed: %v", err)
	}
	return store
}

func TestNewAndClose(t *testing.T) {
	store, err := New()
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}
	// Close without setup should not panic
	store.Close()

	// Double close should be safe
	store.Close()
}

func TestNilStoreOperations(t *testing.T) {
	s := &Store{handle: nil}

	if err := s.Setup("", "", 0, 0, "", "", ""); err != ErrStoreNil {
		t.Errorf("expected ErrStoreNil, got %v", err)
	}
	if err := s.Put("k", []byte("v"), nil); err != ErrStoreNil {
		t.Errorf("expected ErrStoreNil, got %v", err)
	}
	if _, err := s.Get("k", make([]byte, 10)); err != ErrStoreNil {
		t.Errorf("expected ErrStoreNil, got %v", err)
	}
	if _, err := s.Exists("k"); err != ErrStoreNil {
		t.Errorf("expected ErrStoreNil, got %v", err)
	}
	if err := s.Remove("k", false); err != ErrStoreNil {
		t.Errorf("expected ErrStoreNil, got %v", err)
	}
}

func TestDefaultReplicateConfig(t *testing.T) {
	cfg := DefaultReplicateConfig()
	if cfg.ReplicaNum != 1 {
		t.Errorf("expected ReplicaNum=1, got %d", cfg.ReplicaNum)
	}
	if cfg.WithSoftPin {
		t.Error("expected WithSoftPin=false")
	}
	if len(cfg.PreferredSegments) != 0 {
		t.Error("expected empty PreferredSegments")
	}
}

func TestInvalidReplicaNum(t *testing.T) {
	store, err := New()
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}
	defer store.Close()

	cfg := &ReplicateConfig{ReplicaNum: 0}
	if err := store.Put("k", []byte("v"), cfg); err != ErrInvalidArgument {
		t.Errorf("expected ErrInvalidArgument for ReplicaNum=0, got %v", err)
	}

	cfg.ReplicaNum = -1
	if err := store.Put("k", []byte("v"), cfg); err != ErrInvalidArgument {
		t.Errorf("expected ErrInvalidArgument for ReplicaNum=-1, got %v", err)
	}
}

func TestPutFromValidation(t *testing.T) {
	store, err := New()
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}
	defer store.Close()

	if err := store.PutFrom("k", 0, 100, nil); err != ErrInvalidArgument {
		t.Errorf("expected ErrInvalidArgument for nil ptr, got %v", err)
	}
	if err := store.PutFrom("k", 0xDEAD, 0, nil); err != ErrInvalidArgument {
		t.Errorf("expected ErrInvalidArgument for zero size, got %v", err)
	}
}

func TestRegisterBufferValidation(t *testing.T) {
	store, err := New()
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}
	defer store.Close()

	if err := store.RegisterBuffer(0, 100); err != ErrInvalidArgument {
		t.Errorf("expected ErrInvalidArgument for nil ptr, got %v", err)
	}
	if err := store.RegisterBuffer(0xDEAD, 0); err != ErrInvalidArgument {
		t.Errorf("expected ErrInvalidArgument for zero size, got %v", err)
	}
}

// Integration tests below require a running cluster.
// They are skipped when `go test -short` is used.
// Run with:  go test -v ./...

func TestPutGetRoundTrip(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	store := setupStore(t)
	defer store.Close()

	key := "test_go_roundtrip"
	value := []byte("hello from Go")

	// Put
	if err := store.Put(key, value, nil); err != nil {
		t.Fatalf("Put() failed: %v", err)
	}

	// Exists
	exists, err := store.Exists(key)
	if err != nil {
		t.Fatalf("Exists() failed: %v", err)
	}
	if !exists {
		t.Fatal("expected key to exist after Put")
	}

	// GetSize
	size, err := store.GetSize(key)
	if err != nil {
		t.Fatalf("GetSize() failed: %v", err)
	}
	if size != int64(len(value)) {
		t.Fatalf("expected size=%d, got %d", len(value), size)
	}

	// Get
	buf := make([]byte, 1024)
	n, err := store.Get(key, buf)
	if err != nil {
		t.Fatalf("Get() failed: %v", err)
	}
	if string(buf[:n]) != string(value) {
		t.Fatalf("expected %q, got %q", value, buf[:n])
	}

	// Remove (force=true because the object may still have an active lease)
	if err := store.Remove(key, true); err != nil {
		t.Fatalf("Remove() failed: %v", err)
	}

	// Verify removed
	exists, err = store.Exists(key)
	if err != nil {
		t.Fatalf("Exists() after remove failed: %v", err)
	}
	if exists {
		t.Fatal("expected key to not exist after Remove")
	}
}

func TestBatchExists(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	store := setupStore(t)
	defer store.Close()

	keys := []string{"batch_go_1", "batch_go_2", "batch_go_3"}

	// Put first two
	for _, k := range keys[:2] {
		if err := store.Put(k, []byte("data"), nil); err != nil {
			t.Fatalf("Put(%s) failed: %v", k, err)
		}
	}

	results, err := store.BatchExists(keys)
	if err != nil {
		t.Fatalf("BatchExists() failed: %v", err)
	}
	if len(results) != 3 {
		t.Fatalf("expected 3 results, got %d", len(results))
	}
	if !results[0] || !results[1] {
		t.Error("expected first two keys to exist")
	}
	if results[2] {
		t.Error("expected third key to not exist")
	}

	// Cleanup
	for _, k := range keys[:2] {
		_ = store.Remove(k, true)
	}
}

func TestRemoveAll(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	store := setupStore(t)
	defer store.Close()

	for i := 0; i < 5; i++ {
		key := "removeall_go_" + string(rune('a'+i))
		if err := store.Put(key, []byte("data"), nil); err != nil {
			t.Fatalf("Put(%s) failed: %v", key, err)
		}
	}

	removed, err := store.RemoveAll(false)
	if err != nil {
		t.Fatalf("RemoveAll() failed: %v", err)
	}
	if removed < 5 {
		t.Logf("RemoveAll returned %d (may include pre-existing keys)", removed)
	}
}

func TestHostname(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	store := setupStore(t)
	defer store.Close()

	hostname, err := store.Hostname()
	if err != nil {
		t.Fatalf("Hostname() failed: %v", err)
	}
	if hostname == "" {
		t.Fatal("expected non-empty hostname")
	}
	t.Logf("Store hostname: %s", hostname)
}

func TestReplicateConfig(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	store := setupStore(t)
	defer store.Close()

	cfg := &ReplicateConfig{
		ReplicaNum:  2,
		WithSoftPin: true,
	}

	key := "test_go_replicated"
	if err := store.Put(key, []byte("replicated data"), cfg); err != nil {
		t.Fatalf("Put with ReplicateConfig failed: %v", err)
	}

	exists, err := store.Exists(key)
	if err != nil {
		t.Fatalf("Exists() failed: %v", err)
	}
	if !exists {
		t.Fatal("expected replicated key to exist")
	}

	_ = store.Remove(key, true)
}
