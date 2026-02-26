package common

const (
	ServiceTypeVLLM     string = "vLLM"
	ServiceTypeMooncake string = "Mooncake"
)

type ServiceConfig struct {
	Endpoint       string // kv publisher endpoint address
	ReplayEndpoint string // optional
	Type           string // kv publisher type
	ModelName      string // Model name hosted by the service
	LoraName       string // (optinoal)
	TenantID       string // (optional), default use 'default'
	InstanceID     string // (optional), default use 'vllm-prefill-node1'
	BlockSize      int
	DPRank         int
	AdditionalSalt string // (optional), default use 'w8a8,etc..'
}

// TODO combine with /zmq/event_type
type StoredEvent struct {
	BlockHashes     []uint64
	ModelName       string
	LoraID          int64
	EngineIp        string
	ParentBlockHash uint64
	TokenIds        []int32
}

type RemovedEvent struct {
	BlockHashes []uint64
	ModelName   string
	LoraID      int64
	SourcePod   string
}
