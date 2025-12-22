package zmq

import "time"

type EventType string

// Note on Token Representation:
// - vLLM sends token IDs as []int32 arrays
// - Gateway expects tokens as []byte for hashing
// - Conversion: Each int32 is encoded as 4 bytes in big-endian format
// - Example: []int32{1, 2} becomes []byte{0, 0, 0, 1, 0, 0, 0, 2}

const (
	// EventTypeBlockStored indicates that blocks have been stored in the KV cache
	EventTypeBlockStored EventType = "BlockStored"

	// EventTypeBlockRemoved indicates that blocks have been removed from the KV cache
	EventTypeBlockRemoved EventType = "BlockRemoved"

	// EventTypeBlockUpdate indicates that blocks have been updated from the KV cache
	EventTypeBlockUpdate EventType = "BlockUpdate"

	// EventTypeAllCleared indicates that all blocks have been cleared from the cache
	EventTypeAllCleared EventType = "AllBlocksCleared"
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

type BlockUpdataEvent struct {
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
func (e *BlockUpdataEvent) GetType() EventType {
	return e.Type
}

func (e *BlockUpdataEvent) GetTimestamp() time.Time {
	return e.Timestamp
}

type EventSource string

const (
	SourceMooncake EventSource = "mooncake"
	SourceVLLM     EventSource = "vllm"
)

type EventBatch struct {
	Source EventSource // 新增来源标识
	Events []KVEvent
}
