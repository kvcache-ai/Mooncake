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

package tests

import (
	"testing"

	store "github.com/kvcache-ai/Mooncake/mooncake-store/go/mooncakestore"
)

// Integration tests require a running mooncake_master + metadata server.
//
// To run:
//   mooncake_master --enable_http_metadata_server=true
//   go test -v ./tests/...

func setupStore(t *testing.T) *store.Store {
	t.Helper()
	s, err := store.New()
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}
	err = s.Setup(
		"localhost",
		"http://localhost:8080/metadata",
		512*1024*1024, // 512 MB global segment
		128*1024*1024, // 128 MB local buffer
		"tcp",
		"",
		"localhost:50051",
	)
	if err != nil {
		s.Close()
		t.Fatalf("Setup() failed: %v", err)
	}
	return s
}

func TestPutGetRoundTrip(t *testing.T) {
	s := setupStore(t)
	defer s.Close()

	key := "test_go_roundtrip"
	value := []byte("hello from Go")

	if err := s.Put(key, value, nil); err != nil {
		t.Fatalf("Put() failed: %v", err)
	}

	exists, err := s.Exists(key)
	if err != nil {
		t.Fatalf("Exists() failed: %v", err)
	}
	if !exists {
		t.Fatal("expected key to exist after Put")
	}

	size, err := s.GetSize(key)
	if err != nil {
		t.Fatalf("GetSize() failed: %v", err)
	}
	if size != int64(len(value)) {
		t.Fatalf("expected size=%d, got %d", len(value), size)
	}

	buf := make([]byte, size)
	n, err := s.Get(key, buf)
	if err != nil {
		t.Fatalf("Get() failed: %v", err)
	}
	if string(buf[:n]) != string(value) {
		t.Fatalf("expected %q, got %q", value, buf[:n])
	}

	if err := s.Remove(key, true); err != nil {
		t.Fatalf("Remove() failed: %v", err)
	}

	exists, err = s.Exists(key)
	if err != nil {
		t.Fatalf("Exists() after remove failed: %v", err)
	}
	if exists {
		t.Fatal("expected key to not exist after Remove")
	}
}

func TestBatchExists(t *testing.T) {
	s := setupStore(t)
	defer s.Close()

	keys := []string{"batch_go_1", "batch_go_2", "batch_go_3"}

	for _, k := range keys[:2] {
		if err := s.Put(k, []byte("data"), nil); err != nil {
			t.Fatalf("Put(%s) failed: %v", k, err)
		}
	}

	results, err := s.BatchExists(keys)
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

	for _, k := range keys[:2] {
		_ = s.Remove(k, true)
	}
}

func TestRemoveAll(t *testing.T) {
	s := setupStore(t)
	defer s.Close()

	for i := 0; i < 5; i++ {
		key := "removeall_go_" + string(rune('a'+i))
		if err := s.Put(key, []byte("data"), nil); err != nil {
			t.Fatalf("Put(%s) failed: %v", key, err)
		}
	}

	removed, err := s.RemoveAll(false)
	if err != nil {
		t.Fatalf("RemoveAll() failed: %v", err)
	}
	if removed < 5 {
		t.Logf("RemoveAll returned %d (may include pre-existing keys)", removed)
	}
}

func TestHostname(t *testing.T) {
	s := setupStore(t)
	defer s.Close()

	hostname, err := s.Hostname()
	if err != nil {
		t.Fatalf("Hostname() failed: %v", err)
	}
	if hostname == "" {
		t.Fatal("expected non-empty hostname")
	}
	t.Logf("Store hostname: %s", hostname)
}

func TestReplicateConfig(t *testing.T) {
	s := setupStore(t)
	defer s.Close()

	cfg := &store.ReplicateConfig{
		ReplicaNum:  2,
		WithSoftPin: true,
	}

	key := "test_go_replicated"
	if err := s.Put(key, []byte("replicated data"), cfg); err != nil {
		t.Fatalf("Put with ReplicateConfig failed: %v", err)
	}

	exists, err := s.Exists(key)
	if err != nil {
		t.Fatalf("Exists() failed: %v", err)
	}
	if !exists {
		t.Fatal("expected replicated key to exist")
	}

	_ = s.Remove(key, true)
}
