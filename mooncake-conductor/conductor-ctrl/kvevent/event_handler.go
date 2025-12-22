package kvevent

import (
	"context"
	"encoding/binary"
	"fmt"
	"time"

	"log/slog"

	"conductor/common"
	"conductor/zmq"
)

// KVEventHandler adapts the generic EventHandler interface for EventManager.
// It is instantiated in event_manager.go but implemented here to keep files clean.
type KVEventHandler struct {
	manager   *EventManager
	svcName   string
	modelName string
	loraID    int64
}

func (h *KVEventHandler) HandleEvent(event zmq.KVEvent) error {
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
		slog.Info("[%s] BlockStored: %d blocks", h.svcName, len(e.BlockHashes))
		return h.handleBlockStored(ctx, e)
	case *zmq.BlockRemovedEvent:
		slog.Info("[%s] BlockRemoved: %d blocks", h.svcName, len(e.BlockHashes))
		return h.handleBlockRemoved(ctx, e)

	default:
		slog.Warn("Unknown event type: %T", event)
		return nil
	}
}

func (h *KVEventHandler) handleBlockStored(ctx context.Context, event *zmq.BlockStoredEvent) error {

	// Convert to conductor event
	conductorEvent := common.StoredEvent{
		BlockHashes:     event.BlockHashes,
		ModelName:       h.modelName,
		LoraID:          h.loraID,
		EngineIp:        h.svcName,
		ParentBlockHash: event.ParentBlockHash,
		TokenIds:        event.TokenIDs,
	}
	indexer := h.manager.getIndexer()
	er := indexer.ProcessStoreEvent(conductorEvent)
	// TODO support mooncake_key map
	if er != nil {
		slog.Error("process store event failed.")
	}

	slog.Debug("event generated",
		"model", conductorEvent.ModelName,
		"lora_id", conductorEvent.LoraID,
	)

	return nil
}

func (h *KVEventHandler) handleBlockRemoved(ctx context.Context, event *zmq.BlockRemovedEvent) error {
	// Convert to conductor event
	conductorEvent := common.RemovedEvent{
		BlockHashes: event.BlockHashes,
		ModelName:   h.modelName,
		LoraID:      h.loraID,
		SourcePod:   h.svcName,
	}
	indexer := h.manager.getIndexer()
	er := indexer.ProcessRemoveEvent(conductorEvent)
	if er != nil {
		slog.Error("process store event failed.")
	}
	slog.Debug("event generated",
		"model", conductorEvent.ModelName,
		"lora_id", conductorEvent.LoraID,
	)

	return nil
}

func convertTokenIDs(tokenIDs [][]int32) [][]byte {
	result := make([][]byte, len(tokenIDs))
	for i, ids := range tokenIDs {
		result[i] = tokenIDsToBytes(ids)
	}
	return result
}

func tokenIDsToBytes(tokenIDs []int32) []byte {
	bytes := make([]byte, len(tokenIDs)*4)
	for i, id := range tokenIDs {
		binary.BigEndian.PutUint32(bytes[i*4:], uint32(id))
	}
	return bytes
}
