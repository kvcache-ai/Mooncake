package prefixindex

import (
	"testing"

	"conductor/common"
)

func TestComputePrefixHash(t *testing.T) {
	p := NewPrefixCacheTable()

	tests := []struct {
		name       string
		modelCtx   *ModelContext
		tokenIds   []int32
		cacheSalt  uint64
		wantLen    int
	}{
		{
			name: "single block",
			modelCtx: &ModelContext{
				ModelName:      "test-model",
				LoraName:       "none",
				BlockSize:      4,
				AdditionalSalt: "test-salt",
				TenantID:       "default",
			},
			tokenIds:  []int32{1, 2, 3, 4},
			cacheSalt: 0,
			wantLen:   1,
		},
		{
			name: "multiple blocks",
			modelCtx: &ModelContext{
				ModelName:      "test-model",
				LoraName:       "none",
				BlockSize:      4,
				AdditionalSalt: "test-salt",
				TenantID:       "default",
			},
			tokenIds:  []int32{1, 2, 3, 4, 5, 6, 7, 8},
			cacheSalt: 0,
			wantLen:   2,
		},
		{
			name: "partial block not counted",
			modelCtx: &ModelContext{
				ModelName:      "test-model",
				LoraName:       "none",
				BlockSize:      4,
				AdditionalSalt: "test-salt",
				TenantID:       "default",
			},
			tokenIds:  []int32{1, 2, 3, 4, 5},
			cacheSalt: 0,
			wantLen:   1,
		},
		{
			name: "empty token ids",
			modelCtx: &ModelContext{
				ModelName:      "test-model",
				LoraName:       "none",
				BlockSize:      4,
				AdditionalSalt: "test-salt",
				TenantID:       "default",
			},
			tokenIds:  []int32{},
			cacheSalt: 0,
			wantLen:   0,
		},
		{
			name: "with non-zero cache salt",
			modelCtx: &ModelContext{
				ModelName:      "test-model",
				LoraName:       "none",
				BlockSize:      4,
				AdditionalSalt: "test-salt",
				TenantID:       "default",
			},
			tokenIds:  []int32{1, 2, 3, 4},
			cacheSalt: 12345,
			wantLen:   1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := p.ComputePrefixHash(tt.modelCtx, tt.tokenIds, tt.cacheSalt)
			if len(got) != tt.wantLen {
				t.Errorf("ComputePrefixHash() returned %d hashes, want %d", len(got), tt.wantLen)
			}
			// Verify hashes are non-zero for non-empty inputs
			if tt.wantLen > 0 {
				for i, h := range got {
					if h == 0 {
						t.Errorf("ComputePrefixHash() returned zero hash at index %d", i)
					}
				}
			}
		})
	}
}

func TestProcessStoreEvent(t *testing.T) {
	p := NewPrefixCacheTable()

	event := common.StoredEvent{
		BlockHashes:     []uint64{100, 200},
		BlockSize:       4,
		ModelName:       "test-model",
		LoraName:        "none",
		InstanceID:      "instance-1",
		ParentBlockHash: 0,
		TokenIds:        []int32{1, 2, 3, 4, 5, 6, 7, 8},
		Medium:          "cpu",
	}

	err := p.ProcessStoreEvent(event, 0, "instance-1")
	if err != nil {
		t.Errorf("ProcessStoreEvent() error = %v", err)
	}

	// Verify that context data was created
	modelCtx := &ModelContext{
		ModelName:      "test-model",
		LoraName:       "none",
		BlockSize:      4,
		TenantID:       "default",
		AdditionalSalt: "",
	}

	value, exists := p.contextMap.Load(*modelCtx)
	if !exists {
		t.Fatal("ProcessStoreEvent() did not create context data")
	}

	contextData := value.(*ContextData)
	if contextData.instanceID != "instance-1" {
		t.Errorf("ProcessStoreEvent() instanceID = %v, want %v", contextData.instanceID, "instance-1")
	}
}

func TestProcessStoreEvent_EmptyBlockHashes(t *testing.T) {
	p := NewPrefixCacheTable()

	event := common.StoredEvent{
		BlockHashes:     []uint64{},
		BlockSize:       4,
		ModelName:       "test-model",
		LoraName:        "none",
		InstanceID:      "instance-1",
		ParentBlockHash: 0,
		TokenIds:        []int32{1, 2, 3, 4},
		Medium:          "cpu",
	}

	err := p.ProcessStoreEvent(event, 0, "instance-1")
	if err != nil {
		t.Errorf("ProcessStoreEvent() with empty BlockHashes should not error, got %v", err)
	}
}

func TestProcessRemoveEvent(t *testing.T) {
	p := NewPrefixCacheTable()

	// First store an event
	storeEvent := common.StoredEvent{
		BlockHashes:     []uint64{100, 200},
		BlockSize:       4,
		ModelName:       "test-model",
		LoraName:        "none",
		InstanceID:      "instance-1",
		ParentBlockHash: 0,
		TokenIds:        []int32{1, 2, 3, 4, 5, 6, 7, 8},
		Medium:          "cpu",
	}

	err := p.ProcessStoreEvent(storeEvent, 0, "instance-1")
	if err != nil {
		t.Fatalf("ProcessStoreEvent() error = %v", err)
	}

	// Now remove the event
	removeEvent := common.RemovedEvent{
		BlockHashes: []uint64{100, 200},
		ModelName:   "test-model",
		LoraName:    "none",
		InstanceID:  "instance-1",
		BlockSize:   4,
		Medium:      "cpu",
	}

	err = p.ProcessRemoveEvent(removeEvent, 0, "instance-1")
	if err != nil {
		t.Errorf("ProcessRemoveEvent() error = %v", err)
	}

	// Verify that proxyHashMapping was cleared
	modelCtx := &ModelContext{
		ModelName:      "test-model",
		LoraName:       "none",
		BlockSize:      4,
		TenantID:       "default",
		AdditionalSalt: "",
	}

	value, exists := p.contextMap.Load(*modelCtx)
	if !exists {
		t.Fatal("ProcessRemoveEvent() removed context data unexpectedly")
	}

	contextData := value.(*ContextData)
	contextData.hashmapMu.Lock()
	defer contextData.hashmapMu.Unlock()

	if len(contextData.proxyHashMapping) != 0 {
		t.Errorf("ProcessRemoveEvent() proxyHashMapping len = %d, want 0", len(contextData.proxyHashMapping))
	}
}

func TestProcessRemoveEvent_EmptyBlockHashes(t *testing.T) {
	p := NewPrefixCacheTable()

	removeEvent := common.RemovedEvent{
		BlockHashes: []uint64{},
		ModelName:   "test-model",
		LoraName:    "none",
		InstanceID:  "instance-1",
		BlockSize:   4,
		Medium:      "cpu",
	}

	err := p.ProcessRemoveEvent(removeEvent, 0, "instance-1")
	if err != nil {
		t.Errorf("ProcessRemoveEvent() with empty BlockHashes should not error, got %v", err)
	}
}

func TestCacheHitCompute(t *testing.T) {
	p := NewPrefixCacheTable()

	modelCtx := &ModelContext{
		ModelName:      "test-model",
		LoraName:       "none",
		BlockSize:      4,
		TenantID:       "default",
		AdditionalSalt: "", // Must match what ProcessStoreEvent uses internally
	}

	// Store an event first
	storeEvent := common.StoredEvent{
		BlockHashes:     []uint64{100},
		BlockSize:       4,
		ModelName:       "test-model",
		LoraName:       "none",
		InstanceID:      "instance-1",
		ParentBlockHash: 0,
		TokenIds:        []int32{1, 2, 3, 4},
		Medium:          "cpu",
	}

	err := p.ProcessStoreEvent(storeEvent, 0, "instance-1")
	if err != nil {
		t.Fatalf("ProcessStoreEvent() error = %v", err)
	}

	// Now compute cache hit - using same tokenIds to get matching prefix
	tokenIds := []int32{1, 2, 3, 4}
	result := p.CacheHitCompute(modelCtx, tokenIds, "instance-1")

	if result == nil {
		t.Fatal("CacheHitCompute() returned nil")
	}

	if result.LongestMatchTokens == 0 {
		t.Error("CacheHitCompute() expected non-zero LongestMatchTokens after storing data")
	}

	if result.CPU == 0 {
		t.Error("CacheHitCompute() expected non-zero CPU after storing cpu medium")
	}
}

func TestCacheHitCompute_NonExistentContext(t *testing.T) {
	p := NewPrefixCacheTable()

	modelCtx := &ModelContext{
		ModelName:      "non-existent-model",
		LoraName:       "none",
		BlockSize:      4,
		TenantID:       "default",
		AdditionalSalt: "test-salt",
	}

	// Use tokenIds that don't match any stored data
	tokenIds := []int32{1, 2, 3, 4}
	result := p.CacheHitCompute(modelCtx, tokenIds, "instance-1")

	if result == nil {
		t.Fatal("CacheHitCompute() returned nil for non-existent context")
	}

	// Should return zero values for non-existent context
	if result.LongestMatchTokens != 0 {
		t.Errorf("CacheHitCompute() LongestMatchTokens = %d, want 0 for non-existent context", result.LongestMatchTokens)
	}

	if result.CPU != 0 || result.GPU != 0 {
		t.Errorf("CacheHitCompute() expected zero CPU/GPU for non-existent context, got CPU=%d GPU=%d", result.CPU, result.GPU)
	}
}
