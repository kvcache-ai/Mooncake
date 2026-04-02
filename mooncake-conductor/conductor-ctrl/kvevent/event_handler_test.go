package kvevent

import (
	"context"
	"sync"
	"testing"
	"time"

	"conductor/prefixindex"
	"conductor/zmq"
)

func newTestEventManager() *EventManager {
	return &EventManager{
		ctx:     context.Background(),
		mu:      sync.RWMutex{},
		stopped: false,
		indexer: prefixindex.NewPrefixCacheTable(),
	}
}

func newTestKVEventHandler(manager *EventManager) *KVEventHandler {
	return &KVEventHandler{
		manager:    manager,
		tenantID:   "test-tenant",
		modelName:  "test-model",
		loraName:   "test-lora",
		instanceID: "test-instance",
		blockSize:  1024,
	}
}

func TestKVEventHandler_HandleEvent_BlockStored(t *testing.T) {
	manager := newTestEventManager()
	handler := newTestKVEventHandler(manager)

	event := &zmq.BlockStoredEvent{
		Type:            zmq.EventTypeBlockStored,
		Timestamp:       time.Now(),
		BlockHashes:     []uint64{1, 2, 3},
		TokenIDs:        []int32{10, 20, 30},
		ParentBlockHash: 0,
		BlockSize:       1024,
		Medium:          "test-medium",
	}

	err := handler.HandleEvent(event, 0)
	if err != nil {
		t.Fatalf("HandleEvent returned error for BlockStored: %v", err)
	}
}

func TestKVEventHandler_HandleEvent_BlockRemoved(t *testing.T) {
	manager := newTestEventManager()
	handler := newTestKVEventHandler(manager)

	event := &zmq.BlockRemovedEvent{
		Type:        zmq.EventTypeBlockRemoved,
		Timestamp:   time.Now(),
		BlockHashes: []uint64{1, 2, 3},
		ModelName:   "test-model",
		PodName:     "test-pod",
	}

	err := handler.HandleEvent(event, 0)
	if err != nil {
		t.Fatalf("HandleEvent returned error for BlockRemoved: %v", err)
	}
}

func TestKVEventHandler_HandleEvent_UnknownType(t *testing.T) {
	manager := newTestEventManager()
	handler := newTestKVEventHandler(manager)

	// Unknown event type - create a custom struct that implements KVEvent
	unknownEvent := &unknownTestEvent{
		eventType: "UnknownType",
	}

	err := handler.HandleEvent(unknownEvent, 0)
	// Expected behavior: HandleEvent returns nil for unknown event types,
	// as the event is simply ignored without error.
	if err != nil {
		t.Fatalf("HandleEvent returned unexpected error for UnknownType: %v", err)
	}
}

func TestKVEventHandler_HandleEvent_ManagerStopped(t *testing.T) {
	manager := &EventManager{
		ctx:     context.Background(),
		mu:      sync.RWMutex{},
		stopped: true,
		indexer: prefixindex.NewPrefixCacheTable(),
	}
	handler := newTestKVEventHandler(manager)

	event := &zmq.BlockStoredEvent{
		Type:        zmq.EventTypeBlockStored,
		Timestamp:   time.Now(),
		BlockHashes: []uint64{1, 2, 3},
		BlockSize:   1024,
	}

	err := handler.HandleEvent(event, 0)
	if err == nil {
		t.Error("HandleEvent should return error when manager is stopped")
	}
}

// unknownTestEvent is a test event type that is not recognized by HandleEvent
type unknownTestEvent struct {
	eventType zmq.EventType
}

func (e *unknownTestEvent) GetType() zmq.EventType {
	return e.eventType
}

func (e *unknownTestEvent) GetTimestamp() time.Time {
	return time.Now()
}
