package common

const (
	ServiceTypeVLLM     string = "vLLM"
	ServiceTypeMooncake string = "Mooncake"
)

type ServiceConfig struct {
	Name      string // Unique identifier (e.g., "vllm-worker-0")
	IP        string // Service IP address
	Port      int    // ZMQ publisher port
	Type      string // Service type (vLLM/Mooncake)
	ModelName string // Model name hosted by the service
	LoraID    int64  // LoRA ID (-1 if not applicable)
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
