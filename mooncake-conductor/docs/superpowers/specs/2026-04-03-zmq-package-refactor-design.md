# ZMQ Package Refactor Design

**Date**: 2026-04-03
**Goal**: Refactor zmq package - delete mooncake code, keep vLLM only, add parser registry for extensibility

---

## 1. Overview

The zmq package handles ZMQ subscription to vLLM's KV event stream. This refactoring:
- Removes all mooncake-related parser and decoder code
- Keeps only vLLM-related code
- Adds a parser registry for future extensibility (e.g., SGLang support)
- Reimplements unit tests with simple mocked approach

---

## 2. Architecture

### Parser Registry Pattern

```go
var parsers = map[string]func([]byte) (*EventBatch, error){
    "vllm": DecodeVllmEventBatch,
}
```

Adding new parsers (e.g., SGLang) just requires:
```go
func init() {
    RegisterParser("sglang", DecodeSglangEventBatch)
}
```

### Extensibility Design

The `EventParser` interface already exists but is kept for potential future use. The registry approach is chosen for simplicity:
- Topic string → Decoder function
- No interface implementation needed for new parsers
- Decoder signature: `func([]byte) (*EventBatch, error)`

---

## 3. Files

### types.go

**Keep:**
```go
type EventType string

const (
    EventTypeBlockStored  EventType = "BlockStored"
    EventTypeBlockRemoved EventType = "BlockRemoved"
)

const (
    SourceVLLM string = "vllm"
)
```

**Delete:**
- `SourceMooncake` constant
- `EventTypeBlockUpdate`
- `EventTypeAllCleared`
- `BlockUpdateEvent` struct
- `AllBlocksClearedEvent` struct
- `BlockStoredEvent.MooncakeKey` field
- `BlockStoredEvent.ReplicaList` field

**Keep:**
```go
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

type BlockRemovedEvent struct {
    Type        EventType
    Timestamp   time.Time
    BlockHashes []uint64
    ModelName   string
    PodName     string
}

type EventBatch struct {
    Source           string
    Events           []KVEvent
    DataParallelRank int64
}
```

### msg_decoder.go

**Delete:**
- `mooncakeParser` struct and methods
- `newMooncakeParser()` function
- `DecodeMooncakeEventBatch()` function
- `parseMooncakeBlockStored()` function
- `parseMooncakeUint64()` function
- `parseMooncakeParentUint64()` function
- `convertToReplicaList()` function

**Keep:**
- `vllmParser` struct and methods
- `newVLLMParser()` function
- `DecodeVllmEventBatch()` function
- `parseVllmBlockStored()` function
- Helper functions: `safeGetString`, `parseTimestamp`, `parseInt64`, `parseUint64`, `parseUint64Array`, `parseInt32Array`

**Add:**
```go
var parsers = map[string]func([]byte) (*EventBatch, error){
    "vllm": DecodeVllmEventBatch,
}

func DecodeEventBatch(topic string, data []byte) (*EventBatch, error) {
    decoder, ok := parsers[topic]
    if !ok {
        return nil, fmt.Errorf("unknown event topic: %s", topic)
    }
    return decoder(data)
}
```

### zmq_client.go

**Change (line 266-271):**

FROM:
```go
switch string(topic) {
case "mooncake":
    batch, err = DecodeMooncakeEventBatch(payload)
default:
    batch, err = DecodeVllmEventBatch(payload)
}
```

TO:
```go
batch, err = DecodeEventBatch(string(topic), payload)
if err != nil {
    return fmt.Errorf("decode failed: %w", err)
}
```

### msg_decoder_test.go

DELETE all content, reimplement with:

| Test | Description |
|------|-------------|
| `TestDecodeVllmBlockStoredEvent` | Valid BlockStored event decodes correctly |
| `TestDecodeVllmBlockRemovedEvent` | Valid BlockRemoved event decodes correctly |
| `TestDecodeVllmEventBatch_InvalidLength` | Invalid array length returns error |
| `TestDecodeVllmEventBatch_EmptyEvents` | Empty events array handled |
| `TestDecodeVllmEventBatch_UnknownEventType` | Unknown event type returns error |
| `TestDecodeEventBatch_UnknownTopic` | Unknown topic returns error |

### zmq_client_test.go

DELETE all content, reimplement with mocked approach (no real ZMQ):

| Test | Description |
|------|-------------|
| `TestZMQClientConfig_Validate_Success` | Valid config passes validation |
| `TestZMQClientConfig_Validate_MissingEndpoint` | Empty endpoint fails validation |
| `TestZMQClient_NewZMQClient` | Client created with correct initial state |
| `TestZMQClient_Start_InvalidEndpoint` | Start fails gracefully with invalid endpoint |
| `TestZMQClient_Stop_WithoutStart` | Stop works even if not started |

Note: Real ZMQ socket tests not included per user requirement for simple mocked UT.

---

## 4. Exclusions

- No integration tests with real ZMQ sockets
- No SGLang parser implementation (future extension point)
- No TODO items resolved

---

## 5. Acceptance Criteria

- [ ] All mooncake code removed from zmq package
- [ ] Parser registry pattern implemented
- [ ] Only vLLM events supported
- [ ] New UT added with mocked approach
- [ ] Build passes
- [ ] UT passes
