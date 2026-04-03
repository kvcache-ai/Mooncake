# ZMQ Package Refactor Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Refactor zmq package - delete mooncake code, keep vLLM only, add parser registry for extensibility

**Architecture:** Use a parser registry map (topic string → decoder function) for extensibility. Delete all mooncake-related parsers, decoders, and event types. Keep only vLLM BlockStoredEvent and BlockRemovedEvent.

**Tech Stack:** Go, msgpack, ZMQ4

---

## Task 1: Refactor types.go

**Files:**
- Modify: `conductor-ctrl/zmq/types.go`

Remove from types.go:
- `SourceMooncake` constant (line ~97)
- `EventTypeBlockUpdate` and `EventTypeAllCleared` constants
- `BlockUpdateEvent` struct
- `AllBlocksClearedEvent` struct
- `BlockStoredEvent.MooncakeKey` field
- `BlockStoredEvent.ReplicaList` field

Keep in types.go:
- `EventType` interface and constants
- `KVEvent` interface
- `BlockStoredEvent` (simplified)
- `BlockRemovedEvent`
- `EventBatch`
- `SourceVLLM` constant

**After refactor, BlockStoredEvent should be:**
```go
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
```

---

## Task 2: Refactor msg_decoder.go - Remove Mooncake Code

**Files:**
- Modify: `conductor-ctrl/zmq/msg_decoder.go`

Delete from msg_decoder.go:
- `mooncakeParser` struct and all methods
- `newMooncakeParser()` function
- `DecodeMooncakeEventBatch()` function
- `parseMooncakeBlockStored()` function
- `parseMooncakeUint64()` function
- `parseMooncakeParentUint64()` function
- `convertToReplicaList()` function

Keep in msg_decoder.go:
- `vllmParser` struct and methods
- `newVLLMParser()` function
- `DecodeVllmEventBatch()` function
- `parseVllmBlockStored()` function
- Helper functions: `safeGetString`, `parseTimestamp`, `parseInt64`, `parseUint64`, `parseUint64Array`, `parseInt32Array`
- `decodeCommonEventBatch()` function (simplified - remove parser parameter)

**Add parser registry:**
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

---

## Task 3: Update zmq_client.go to Use Registry

**Files:**
- Modify: `conductor-ctrl/zmq/zmq_client.go:266-271`

Change:
```go
// FROM:
switch string(topic) {
case "mooncake":
    batch, err = DecodeMooncakeEventBatch(payload)
default:
    batch, err = DecodeVllmEventBatch(payload)
}

// TO:
batch, err = DecodeEventBatch(string(topic), payload)
```

---

## Task 4: Reimplement msg_decoder_test.go

**Files:**
- Delete: `conductor-ctrl/zmq/msg_decoder_test.go`
- Create: `conductor-ctrl/zmq/msg_decoder_test.go`

Write tests for vLLM decoder only:

- [ ] **Step 1: Write test for BlockStored decode**

```go
func TestDecodeVllmBlockStoredEvent(t *testing.T) {
    timestamp := int64(1700000000)
    event := []interface{}{
        "BlockStored",
        []interface{}{uint64(100), uint64(200)},
        uint64(5000000000),
        []interface{}{int32(1), int32(2), int32(3)},
        int64(1024),
    }
    events := []interface{}{event}
    batch := []interface{}{timestamp, events, "ok"}

    data, err := msgpack.Marshal(batch)
    if err != nil {
        t.Fatalf("Failed to marshal: %v", err)
    }

    result, err := zmq.DecodeVllmEventBatch(data)
    if err != nil {
        t.Fatalf("DecodeVllmEventBatch failed: %v", err)
    }

    if result.Source != zmq.SourceVLLM {
        t.Errorf("Expected source vllm, got %s", result.Source)
    }

    blockEvent, ok := result.Events[0].(*zmq.BlockStoredEvent)
    if !ok {
        t.Fatalf("Expected BlockStoredEvent, got %T", result.Events[0])
    }

    if len(blockEvent.BlockHashes) != 2 {
        t.Errorf("Expected 2 block hashes, got %d", len(blockEvent.BlockHashes))
    }
    if blockEvent.BlockHashes[0] != 100 || blockEvent.BlockHashes[1] != 200 {
        t.Errorf("Expected [100, 200], got %v", blockEvent.BlockHashes)
    }
}
```

- [ ] **Step 2: Write test for BlockRemoved decode**

```go
func TestDecodeVllmBlockRemovedEvent(t *testing.T) {
    timestamp := int64(1700000000)
    event := []interface{}{
        "BlockRemoved",
        []interface{}{uint64(100), uint64(200)},
    }
    events := []interface{}{event}
    batch := []interface{}{timestamp, events, "ok"}

    data, err := msgpack.Marshal(batch)
    if err != nil {
        t.Fatalf("Failed to marshal: %v", err)
    }

    result, err := zmq.DecodeVllmEventBatch(data)
    if err != nil {
        t.Fatalf("DecodeVllmEventBatch failed: %v", err)
    }

    removedEvent, ok := result.Events[0].(*zmq.BlockRemovedEvent)
    if !ok {
        t.Fatalf("Expected BlockRemovedEvent, got %T", result.Events[0])
    }

    if len(removedEvent.BlockHashes) != 2 {
        t.Errorf("Expected 2 block hashes, got %d", len(removedEvent.BlockHashes))
    }
}
```

- [ ] **Step 3: Write test for invalid array length**

```go
func TestDecodeVllmEventBatch_InvalidLength(t *testing.T) {
    invalidBatch := []interface{}{int64(1700000000)}
    data, err := msgpack.Marshal(invalidBatch)
    if err != nil {
        t.Fatalf("Failed to marshal: %v", err)
    }

    _, err = zmq.DecodeVllmEventBatch(data)
    if err == nil {
        t.Error("Expected error for invalid length, got nil")
    }
}
```

- [ ] **Step 4: Write test for empty events**

```go
func TestDecodeVllmEventBatch_EmptyEvents(t *testing.T) {
    timestamp := int64(1700000000)
    batch := []interface{}{timestamp, []interface{}{}, "ok"}

    data, err := msgpack.Marshal(batch)
    if err != nil {
        t.Fatalf("Failed to marshal: %v", err)
    }

    result, err := zmq.DecodeVllmEventBatch(data)
    if err != nil {
        t.Fatalf("DecodeVllmEventBatch failed: %v", err)
    }

    if len(result.Events) != 0 {
        t.Errorf("Expected 0 events, got %d", len(result.Events))
    }
}
```

- [ ] **Step 5: Write test for unknown topic in registry**

```go
func TestDecodeEventBatch_UnknownTopic(t *testing.T) {
    _, err := zmq.DecodeEventBatch("unknown", []byte{})
    if err == nil {
        t.Error("Expected error for unknown topic, got nil")
    }
    if err != nil && err.Error() != "unknown event topic: unknown" {
        t.Errorf("Unexpected error message: %v", err)
    }
}
```

- [ ] **Step 6: Run tests**

Run: `go test ./conductor-ctrl/zmq/... -v -run "TestDecode"`
Expected: All tests pass

- [ ] **Step 7: Commit**

```bash
git add conductor-ctrl/zmq/msg_decoder_test.go
git commit -m "test(zmq): add msg_decoder tests for vLLM events"
```

---

## Task 5: Reimplement zmq_client_test.go

**Files:**
- Delete: `conductor-ctrl/zmq/zmq_client_test.go`
- Create: `conductor-ctrl/zmq/zmq_client_test.go`

Write tests using simple mocked approach (no real ZMQ):

- [ ] **Step 1: Write config validation test**

```go
func TestZMQClientConfig_Validate_Success(t *testing.T) {
    config := &zmq.ZMQClientConfig{
        CachePoolKey:   "test-pod",
        Endpoint:       "tcp://127.0.0.1:5557",
        ReplayEndpoint: "tcp://127.0.0.1:5558",
        ModelName:      "test-model",
        PollTimeout:    100 * time.Millisecond,
        ReplayTimeout:  5 * time.Second,
        ReconnectDelay: 1 * time.Second,
    }

    err := zmq.ValidateConfig(config)
    if err != nil {
        t.Errorf("Expected no error, got %v", err)
    }
}

func TestZMQClientConfig_Validate_MissingEndpoint(t *testing.T) {
    config := &zmq.ZMQClientConfig{
        CachePoolKey: "test-pod",
        // Missing Endpoint
    }

    err := zmq.ValidateConfig(config)
    if err == nil {
        t.Error("Expected error for missing endpoint, got nil")
    }
}
```

- [ ] **Step 2: Write NewZMQClient test**

```go
func TestZMQClient_NewZMQClient(t *testing.T) {
    config := &zmq.ZMQClientConfig{
        CachePoolKey:   "test-pod",
        Endpoint:       "tcp://127.0.0.1:5557",
        ReconnectDelay: 1 * time.Second,
    }

    handler := &mockHandler{}
    client := zmq.NewZMQClient(config, handler)

    if client == nil {
        t.Fatal("Expected non-nil client")
    }
}

type mockHandler struct{}

func (m *mockHandler) HandleEvent(event zmq.KVEvent, dpRank int64) error {
    return nil
}
```

- [ ] **Step 3: Write Start with invalid endpoint test**

```go
func TestZMQClient_Start_InvalidEndpoint(t *testing.T) {
    config := &zmq.ZMQClientConfig{
        CachePoolKey:   "test-pod",
        Endpoint:       "tcp://invalid:9999",
        ReconnectDelay: 10 * time.Millisecond,
    }

    handler := &mockHandler{}
    client := zmq.NewZMQClient(config, handler)

    err := client.Start()
    // Should fail due to invalid endpoint
    // We just verify it doesn't panic
    _ = err

    client.Stop()
}
```

- [ ] **Step 4: Write Stop without Start test**

```go
func TestZMQClient_Stop_WithoutStart(t *testing.T) {
    config := &zmq.ZMQClientConfig{
        CachePoolKey:   "test-pod",
        Endpoint:       "tcp://127.0.0.1:5557",
        ReconnectDelay: 1 * time.Second,
    }

    handler := &mockHandler{}
    client := zmq.NewZMQClient(config, handler)

    // Stop without Start should not panic
    client.Stop()
}
```

- [ ] **Step 5: Run tests**

Run: `go test ./conductor-ctrl/zmq/... -v -run "TestZMQClient|TestDecode"`
Expected: All tests pass

- [ ] **Step 6: Commit**

```bash
git add conductor-ctrl/zmq/zmq_client_test.go
git commit -m "test(zmq): add zmq_client tests with mocked approach"
```

---

## Task 6: Final Verification

- [ ] **Step 1: Run all tests**

Run: `go test ./conductor-ctrl/... -v`
Expected: All tests pass

- [ ] **Step 2: Build verification**

Run: `go build ./conductor-ctrl/...`
Expected: Build succeeds

- [ ] **Step 3: Verify no mooncake references remain**

Run: `grep -r "mooncake" conductor-ctrl/zmq/`
Expected: No output (no mooncake references)

- [ ] **Step 4: Commit**

```bash
git add -A
git commit -m "refactor(zmq): remove mooncake code, keep vLLM only with parser registry"
```

---

## Execution Options

**1. Subagent-Driven (recommended)** - Dispatch fresh subagent per task with two-stage review

**2. Inline Execution** - Execute tasks in this session using executing-plans
