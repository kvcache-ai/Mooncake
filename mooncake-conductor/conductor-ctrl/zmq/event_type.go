package zmq

import "time"

type EventType string

const (
	EventTypeBlockStored  EventType = "BlockStored"
	EventTypeBlockRemoved EventType = "BlockRemoved"

	// EventTypeBlockUpdate indicates that blocks have been updated from the KV cache
	EventTypeBlockUpdate EventType = "BlockUpdate"
	EventTypeAllCleared  EventType = "AllBlocksCleared"
)

type KVEvent interface {
	GetType() EventType
	GetTimestamp() time.Time
}

type BlockStoredEvent struct {
	Type            EventType
	Timestamp       time.Time
	BlockHashes     []uint64
	TokenIDs        []int32
	ParentBlockHash uint64
	BlockSize       int64
	MooncakeKey     string
	ReplicaList     [][]string
	ModelName       string
	PodName         string
}

func (e *BlockStoredEvent) GetType() EventType {
	return e.Type
}

func (e *BlockStoredEvent) GetTimestamp() time.Time {
	return e.Timestamp
}

type BlockRemovedEvent struct {
	Type        EventType
	Timestamp   time.Time
	BlockHashes []uint64
	ModelName   string
	PodName     string
}

func (e *BlockRemovedEvent) GetType() EventType {
	return e.Type
}

func (e *BlockRemovedEvent) GetTimestamp() time.Time {
	return e.Timestamp
}

type AllBlocksClearedEvent struct {
	Type      EventType
	Timestamp time.Time
	ModelName string
	PodName   string
}

func (e *AllBlocksClearedEvent) GetType() EventType {
	return e.Type
}

func (e *AllBlocksClearedEvent) GetTimestamp() time.Time {
	return e.Timestamp
}

type BlockUpdateEvent struct {
	Type            EventType
	Timestamp       time.Time
	BlockHashes     []uint64
	TokenIDs        []int32
	ParentBlockHash uint64
	ModelName       string
	PodName         string
	BlockSize       int64
}

// GetType returns the event type
func (e *BlockUpdateEvent) GetType() EventType {
	return e.Type
}

func (e *BlockUpdateEvent) GetTimestamp() time.Time {
	return e.Timestamp
}

const (
	SourceMooncake string = "mooncake"
	SourceVLLM     string = "vllm"
)

type EventBatch struct {
	Source string // indicates the origin of the event batch
	Events []KVEvent
}
