package zmq

import "time"

type EventType string

const (
	EventTypeBlockStored  EventType = "BlockStored"
	EventTypeBlockRemoved EventType = "BlockRemoved"
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
	ModelName       string
	LoraID          int64
	LoraName        string
	PodName         string
	Medium          string
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

const (
	SourceVLLM string = "vllm"
)

type EventBatch struct {
	Source           string // indicates the origin of the event batch
	Events           []KVEvent
	DataParallelRank int64
}
