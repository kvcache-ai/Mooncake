package zmq_test

import (
	"testing"
	"time"

	"conductor/zmq"

	msgpack "github.com/shamaton/msgpack/v2"
)

func TestDecodeMooncakeEventBatch(t *testing.T) {
	timestamp := int64(1700000000)
	event := []interface{}{
		"BlockStoreEvent",
		"mooncake-key-123",
		[][]interface{}{
			[]interface{}{"replica1", "replica2"},
			[]interface{}{"replica3"},
		},
		nil,                                     // index 3 is not used
		int64(1024),                             // BlockSize at index 4
		[]interface{}{uint64(100), uint64(200)}, // BlockHashes at index 5
		uint64(50),                              // ParentBlockHash at index 6
		[]interface{}{int32(1), int32(2), int32(3)}, // TokenIDs at index 7
	}
	events := []interface{}{event}
	batch := []interface{}{timestamp, events}

	data, err := msgpack.Marshal(batch)
	if err != nil {
		t.Fatalf("Failed to marshal test data: %v", err)
	}

	result, err := zmq.DecodeMooncakeEventBatch(data)
	if err != nil {
		t.Fatalf("DecodeMooncakeEventBatch failed: %v", err)
	}

	if result.Source != zmq.SourceMooncake {
		t.Errorf("Expected source %v, got %v", zmq.SourceMooncake, result.Source)
	}

	if len(result.Events) != 1 {
		t.Fatalf("Expected 1 event, got %d", len(result.Events))
	}

	blockEvent, ok := result.Events[0].(*zmq.BlockStoredEvent)
	if !ok {
		t.Fatalf("Expected BlockStoredEvent, got %T", result.Events[0])
	}

	if blockEvent.Type != zmq.EventTypeBlockStored {
		t.Errorf("Expected type %v, got %v", zmq.EventTypeBlockStored, blockEvent.Type)
	}

	if blockEvent.MooncakeKey != "mooncake-key-123" {
		t.Errorf("Expected MooncakeKey 'mooncake-key-123', got '%s'", blockEvent.MooncakeKey)
	}

	if blockEvent.BlockSize != 1024 {
		t.Errorf("Expected BlockSize 1024, got %d", blockEvent.BlockSize)
	}

	if len(blockEvent.BlockHashes) != 2 {
		t.Fatalf("Expected 2 block hashes, got %d", len(blockEvent.BlockHashes))
	}

	if blockEvent.BlockHashes[0] != 100 || blockEvent.BlockHashes[1] != 200 {
		t.Errorf("Expected block hashes [100, 200], got %v", blockEvent.BlockHashes)
	}

	if blockEvent.ParentBlockHash != 50 {
		t.Errorf("Expected ParentBlockHash 50, got %d", blockEvent.ParentBlockHash)
	}

	if len(blockEvent.TokenIDs) != 3 {
		t.Fatalf("Expected 3 token IDs, got %d", len(blockEvent.TokenIDs))
	}

	if len(blockEvent.ReplicaList) != 2 {
		t.Fatalf("Expected 2 replica lists, got %d", len(blockEvent.ReplicaList))
	}
}

func TestDecodeVllmEventBatch(t *testing.T) {
	timestamp := int64(1700000000)
	event := []interface{}{
		"BlockStored",
		[]interface{}{uint64(100), uint64(200)},            // BlockHashes
		uint64(5000000000),                                 // ParentBlockHash
		[]interface{}{int32(10000000), int32(2), int32(3)}, // TokenIDs
		int64(1024),                                        // BlockSize
	}
	events := []interface{}{event}
	status := "ok"
	batch := []interface{}{timestamp, events, status}

	data, err := msgpack.Marshal(batch)
	if err != nil {
		t.Fatalf("Failed to marshal test data: %v", err)
	}

	result, err := zmq.DecodeVllmEventBatch(data)
	if err != nil {
		t.Fatalf("DecodeVllmEventBatch failed: %v", err)
	}

	if result.Source != zmq.SourceVLLM {
		t.Errorf("Expected source %v, got %v", zmq.SourceVLLM, result.Source)
	}

	if len(result.Events) != 1 {
		t.Fatalf("Expected 1 event, got %d", len(result.Events))
	}

	blockEvent, ok := result.Events[0].(*zmq.BlockStoredEvent)
	if !ok {
		t.Fatalf("Expected BlockStoredEvent, got %T", result.Events[0])
	}

	if blockEvent.Type != zmq.EventTypeBlockStored {
		t.Errorf("Expected type %v, got %v", zmq.EventTypeBlockStored, blockEvent.Type)
	}

	expectedTime := time.Unix(1700000000, 0).UTC()
	if !blockEvent.Timestamp.Equal(expectedTime) {
		t.Errorf("Expected timestamp %v, got %v", expectedTime, blockEvent.Timestamp)
	}

	if len(blockEvent.BlockHashes) != 2 {
		t.Fatalf("Expected 2 block hashes, got %d", len(blockEvent.BlockHashes))
	}

	if blockEvent.BlockHashes[0] != 100 || blockEvent.BlockHashes[1] != 200 {
		t.Errorf("Expected block hashes [100, 200], got %v", blockEvent.BlockHashes)
	}

	if blockEvent.ParentBlockHash != 5000000000 {
		t.Errorf("Expected ParentBlockHash 50, got %d", blockEvent.ParentBlockHash)
	}

	if blockEvent.BlockSize != 1024 {
		t.Errorf("Expected BlockSize 1024, got %d", blockEvent.BlockSize)
	}

	if len(blockEvent.TokenIDs) != 3 {
		t.Fatalf("Expected 3 token IDs, got %d", len(blockEvent.TokenIDs))
	}
}

func TestDecodeMooncakeEventBatch_InvalidData(t *testing.T) {
	// Test with invalid array length
	invalidBatch := []interface{}{int64(1700000000)} // Missing events
	data, err := msgpack.Marshal(invalidBatch)
	if err != nil {
		t.Fatalf("Failed to marshal test data: %v", err)
	}

	_, err = zmq.DecodeMooncakeEventBatch(data)
	if err == nil {
		t.Error("Expected error for invalid array length, got nil")
	}
}

func TestDecodeVllmEventBatch_InvalidData(t *testing.T) {
	// Test with invalid array length
	invalidBatch := []interface{}{int64(1700000000)} // Missing events and status
	data, err := msgpack.Marshal(invalidBatch)
	if err != nil {
		t.Fatalf("Failed to marshal test data: %v", err)
	}

	_, err = zmq.DecodeVllmEventBatch(data)
	if err == nil {
		t.Error("Expected error for invalid array length, got nil")
	}
}
