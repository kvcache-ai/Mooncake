package zmq_test

import (
	"testing"

	"conductor/zmq"

	msgpack "github.com/shamaton/msgpack/v2"
)

func TestDecodeVllmBlockStoredEvent(t *testing.T) {
	timestamp := int64(1700000000)
	event := []interface{}{
		"BlockStored",
		[]interface{}{uint64(100), uint64(200)},
		uint64(5000000000),
		[]interface{}{int32(1), int32(2), int32(3)},
		int64(1024),
		nil,
		"memory",
	}
	events := []interface{}{event}
	batch := []interface{}{timestamp, events, int64(0)}

	data, err := msgpack.Marshal(batch)
	if err != nil {
		t.Fatalf("Failed to marshal: %v", err)
	}

	result, err := zmq.DecodeVllmEventBatch(data)
	if err != nil {
		t.Fatalf("DecodeVllmEventBatch failed: %v", err)
	}

	if result.Source != zmq.SourceVLLM {
		t.Errorf("Expected source vllm, got %s", result.Source)
	}

	blockEvent, ok := result.Events[0].(*zmq.BlockStoredEvent)
	if !ok {
		t.Fatalf("Expected BlockStoredEvent, got %T", result.Events[0])
	}

	if len(blockEvent.BlockHashes) != 2 {
		t.Errorf("Expected 2 block hashes, got %d", len(blockEvent.BlockHashes))
	}
	if blockEvent.BlockHashes[0] != 100 || blockEvent.BlockHashes[1] != 200 {
		t.Errorf("Expected [100, 200], got %v", blockEvent.BlockHashes)
	}
}

func TestDecodeVllmBlockRemovedEvent(t *testing.T) {
	timestamp := int64(1700000000)
	event := []interface{}{
		"BlockRemoved",
		[]interface{}{uint64(100), uint64(200)},
	}
	events := []interface{}{event}
	batch := []interface{}{timestamp, events, int64(0)}

	data, err := msgpack.Marshal(batch)
	if err != nil {
		t.Fatalf("Failed to marshal: %v", err)
	}

	result, err := zmq.DecodeVllmEventBatch(data)
	if err != nil {
		t.Fatalf("DecodeVllmEventBatch failed: %v", err)
	}

	removedEvent, ok := result.Events[0].(*zmq.BlockRemovedEvent)
	if !ok {
		t.Fatalf("Expected BlockRemovedEvent, got %T", result.Events[0])
	}

	if len(removedEvent.BlockHashes) != 2 {
		t.Errorf("Expected 2 block hashes, got %d", len(removedEvent.BlockHashes))
	}
}

func TestDecodeVllmEventBatch_InvalidLength(t *testing.T) {
	invalidBatch := []interface{}{int64(1700000000)}
	data, err := msgpack.Marshal(invalidBatch)
	if err != nil {
		t.Fatalf("Failed to marshal: %v", err)
	}

	_, err = zmq.DecodeVllmEventBatch(data)
	if err == nil {
		t.Error("Expected error for invalid length, got nil")
	}
}

func TestDecodeVllmEventBatch_EmptyEvents(t *testing.T) {
	timestamp := int64(1700000000)
	batch := []interface{}{timestamp, []interface{}{}, int64(0)}

	data, err := msgpack.Marshal(batch)
	if err != nil {
		t.Fatalf("Failed to marshal: %v", err)
	}

	result, err := zmq.DecodeVllmEventBatch(data)
	if err != nil {
		t.Fatalf("DecodeVllmEventBatch failed: %v", err)
	}

	if len(result.Events) != 0 {
		t.Errorf("Expected 0 events, got %d", len(result.Events))
	}
}

func TestDecodeEventBatch_UnknownTopic(t *testing.T) {
	_, err := zmq.DecodeEventBatch("unknown", []byte{})
	if err == nil {
		t.Error("Expected error for unknown topic, got nil")
	}
}
