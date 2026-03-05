package common

const (
	ServiceTypeVLLM     string = "vLLM"
	ServiceTypeMooncake string = "Mooncake"
)

type ServiceConfig struct {
	Endpoint       string // kv publisher endpoint
	ReplayEndpoint string // (optional)
	Type           string // kv publisher type, support: vLLM,Mooncake
	ModelName      string // Model name hosted by the service
	LoraName       string
	TenantID       string // (optional), default use 'default'
	InstanceID     string // required
	BlockSize      int64
	DPRank         int
	AdditionalSalt string // (optional), default use empty string
}

type StoredEvent struct {
	BlockHashes     []uint64
	BlockSize       int64
	ModelName       string
	LoraName        string
	InstanceID      string
	ParentBlockHash uint64
	TokenIds        []int32
	Medium          string
}

type RemovedEvent struct {
	BlockHashes []uint64
	ModelName   string
	LoraName    string
	InstanceID  string
	BlockSize   int64
	Medium      string
}
