package kvevent

import (
	"context"
	"fmt"
	"time"

	"conductor/common"
	"conductor/zmq"
	"log/slog"
)

// KVEventHandler adapts the generic EventHandler interface for EventManager.
// It is instantiated in event_manager.go but implemented here to keep files clean.
type KVEventHandler struct {
	manager   *EventManager
	tenant_id string
	// svcName   string
	modelName      string
	loraName       string
	instanceID     string
	blockSize      int64
	additionalSalt string
}

func (h *KVEventHandler) HandleEvent(event zmq.KVEvent, dpRank int64) error {
	h.manager.mu.RLock()
	if h.manager.stopped {
		h.manager.mu.RUnlock()
		return fmt.Errorf("manager stopped")
	}
	h.manager.mu.RUnlock()

	// Create context for processing
	ctx, cancel := context.WithTimeout(h.manager.ctx, 10*time.Second)
	defer cancel()

	// Dispatch event
	switch e := event.(type) {
	case *zmq.BlockStoredEvent:
		slog.Debug("BlockStored",
			"instance_id", h.instanceID,
			"dpRank", dpRank,
			"blocks", len(e.BlockHashes),
		)
		return h.handleBlockStored(ctx, e, dpRank)
	case *zmq.BlockRemovedEvent:
		slog.Debug("BlockRemoved",
			"instance_id", h.instanceID,
			"dpRank", dpRank,
			"blocks", len(e.BlockHashes),
		)
		return h.handleBlockRemoved(ctx, e, dpRank)

	default:
		slog.Warn("Unknown event type",
			"type", fmt.Sprintf("%T", event),
		)
		return nil
	}
}

func (h *KVEventHandler) handleBlockStored(ctx context.Context, event *zmq.BlockStoredEvent, dpRank int64) error {

	// Convert to kvindexer event
	conductorEvent := common.StoredEvent{
		BlockHashes:     event.BlockHashes,
		BlockSize:       event.BlockSize,
		ModelName:       h.modelName,
		LoraName:        h.loraName,
		InstanceID:      h.instanceID,
		ParentBlockHash: event.ParentBlockHash,
		TokenIds:        event.TokenIDs,
		Medium:          event.Medium,
	}

	indexer := h.manager.getIndexer()
	er := indexer.ProcessStoreEvent(conductorEvent, dpRank, h.instanceID)
	// TODO support mooncake_key map
	if er != nil {
		slog.Error("process store event failed.", "error", er)
	}

	slog.Debug("in handleBlockStored", "conductorEvent", conductorEvent)

	return nil
}

func (h *KVEventHandler) handleBlockRemoved(ctx context.Context, event *zmq.BlockRemovedEvent, dpRank int64) error {
	// Convert to conductor event
	conductorEvent := common.RemovedEvent{
		BlockHashes: event.BlockHashes,
		ModelName:   h.modelName,
		LoraName:    h.loraName,
		InstanceID:  h.instanceID,
		BlockSize:   h.blockSize,
	}
	indexer := h.manager.getIndexer()
	er := indexer.ProcessRemoveEvent(conductorEvent, dpRank, h.instanceID)
	if er != nil {
		slog.Error("process store event failed.")
	}
	slog.Debug("in handleBlockRemoved", "conductorEvent", conductorEvent)

	return nil
}

// TODO support mooncake update kv event
