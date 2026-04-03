package zmq

import (
	"fmt"
	"log/slog"
	"time"

	msgpack "github.com/shamaton/msgpack/v2"
)

type EventParser interface {
	ParseEvent(raw []interface{}, timestamp interface{}) (KVEvent, error)
	EventMappings() map[string]EventType
	Source() string
}

type vllmParser struct{}

func (p *vllmParser) Source() string { return SourceVLLM }

func (p *vllmParser) EventMappings() map[string]EventType {
	return map[string]EventType{
		"BlockStored":  EventTypeBlockStored,
		"BlockRemoved": EventTypeBlockRemoved,
	}
}

func (p *vllmParser) ParseEvent(raw []interface{}, timestamp interface{}) (KVEvent, error) {
	eventTypeStr, ok := raw[0].(string)
	if !ok {
		return nil, fmt.Errorf("invalid event type format: %T", raw[0])
	}

	eventType, exists := p.EventMappings()[eventTypeStr]
	if !exists {
		return nil, fmt.Errorf("unknown vllm event type: %s", eventTypeStr)
	}

	switch eventType {
	case EventTypeBlockStored:
		return parseVllmBlockStored(raw, timestamp)
	case EventTypeBlockRemoved:
		return parseVllmBlockRemoved(raw, timestamp)
	default:
		return nil, fmt.Errorf("unhandled event: %s", eventType)
	}
}

func newVLLMParser() EventParser {
	return &vllmParser{}
}

// Parser registry for event batch decoding
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

func DecodeVllmEventBatch(data []byte) (*EventBatch, error) {
	return decodeCommonEventBatch(data)
}

func decodeCommonEventBatch(data []byte) (*EventBatch, error) {
	const expectedLength = 3

	if len(data) > 0 {
		slog.Debug("First byte of payload", "hex", fmt.Sprintf("%02x", data[0]))
	}

	var arr []interface{}
	if err := msgpack.Unmarshal(data, &arr); err != nil {
		return nil, fmt.Errorf("failed to unmarshal event batch: %w", err)
	}

	if len(arr) != expectedLength {
		return nil, fmt.Errorf("expected %d-element array, got %d", expectedLength, len(arr))
	}

	events, ok := arr[1].([]interface{})
	if !ok {
		return nil, fmt.Errorf("invalid events type: %T", arr[1])
	}

	timestamp := arr[0]

	if len(events) == 0 {
		slog.Warn("Received empty event list")
	}

	dpRank, err := parseInt64(arr[2])
	if err != nil {
		return nil, fmt.Errorf("failed to parse dpRank: %w", err)
	}

	batch := &EventBatch{
		Source:           SourceVLLM,
		Events:           make([]KVEvent, 0, len(events)),
		DataParallelRank: dpRank,
	}

	parser := newVLLMParser()

	for i, rawEvent := range events {
		eventSlice, ok := rawEvent.([]interface{})
		if !ok {
			return nil, fmt.Errorf("event at index %d is not a slice: %T", i, rawEvent)
		}
		event, err := parser.ParseEvent(eventSlice, timestamp)
		if err != nil {
			return nil, fmt.Errorf("failed to parse event at index %d: %w", i, err)
		}
		batch.Events = append(batch.Events, event)
	}

	return batch, nil
}

// parseBlockStoredEvent parses a BlockStoredEvent from raw data
func parseVllmBlockStored(data []interface{}, timestamp interface{}) (*BlockStoredEvent, error) {
	event := &BlockStoredEvent{
		Type: EventTypeBlockStored,
	}

	for i, elem := range data {
		slog.Debug("in parseVllmBlockStored:", "index", i, "type", fmt.Sprintf("%T", elem), "value", elem)
	}

	slog.Debug("in parseVllmBlockStored:", "timestamp", timestamp)
	// Parse timestamp
	if ts, err := parseTimestamp(timestamp); err == nil {
		event.Timestamp = ts
	} else {
		return nil, fmt.Errorf("failed to parse timestamp: %w", err)
	}

	// // Parse block hashes
	if hashes, err := parseUint64Array(data[1]); err == nil {
		event.BlockHashes = hashes
	} else {
		return nil, fmt.Errorf("failed to parse block_hashes: %w", err)
	}

	// // Parse token IDs (array of arrays)
	if tokenIDsRaw, ok := data[3].([]interface{}); ok {
		tokens, err := parseInt32Array(tokenIDsRaw)
		if err != nil {
			return nil, fmt.Errorf("failed to parse token_ids at index %w", err)
		}
		event.TokenIDs = tokens
	} else {
		return nil, fmt.Errorf("missing or invalid token_ids")
	}

	var parentHash uint64
	if data[2] == nil {
		parentHash = uint64(0)
	} else {
		hash := data[2]
		// fmt.Printf("Type of hash>>>>: %T\n", hash)
		if h, ok := hash.(uint64); ok {
			parentHash = h
		} else {
			return nil, fmt.Errorf("expected uint64, got %T", hash)
		}
	}
	event.ParentBlockHash = parentHash

	if blocksize, err := parseInt64(data[4]); err == nil {
		event.BlockSize = blocksize
	} else {
		return nil, fmt.Errorf("failed to parse field at index 4 as 'block_size': %w", err)
	}

	if medium, err := safeGetString(data[6]); err == nil {
		event.Medium = medium
	} else {
		return nil, fmt.Errorf("failed to parse 'medium' from field at index 6: %w", err)
	}

	return event, nil
}

func parseVllmBlockRemoved(data []interface{}, timestamp interface{}) (*BlockRemovedEvent, error) {
	event := &BlockRemovedEvent{
		Type: EventTypeBlockRemoved,
	}

	// Parse timestamp
	if ts, err := parseTimestamp(timestamp); err == nil {
		event.Timestamp = ts
	} else {
		return nil, fmt.Errorf("failed to parse timestamp: %w", err)
	}

	// Parse block hashes
	if hashes, err := parseUint64Array(data[1]); err == nil {
		event.BlockHashes = hashes
	} else {
		return nil, fmt.Errorf("failed to parse block_hashes: %w", err)
	}

	return event, nil
}

func safeGetString(val interface{}) (string, error) {
	switch v := val.(type) {
	case string:
		return v, nil
	case []byte:
		return string(v), nil
	case fmt.Stringer:
		return v.String(), nil
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64, float32, float64:
		return fmt.Sprintf("%v", v), nil
	case nil:
		return "", nil
	default:
		slog.Warn("Unexpected type in string field",
			"type", fmt.Sprintf("%T", v),
			"value", v)
		return fmt.Sprintf("%v", v), nil
	}
}

// Helper functions for parsing common types
func parseTimestamp(v interface{}) (time.Time, error) {
	switch t := v.(type) {
	case time.Time:
		return t, nil
	case int64:
		return time.Unix(t, 0).UTC(), nil
	case int:
		return time.Unix(int64(t), 0).UTC(), nil
	case int32:
		return time.Unix(int64(t), 0).UTC(), nil
	case uint32:
		return time.Unix(int64(t), 0).UTC(), nil
	case uint64:
		return time.Unix(int64(t), 0).UTC(), nil
	case float64:
		sec := int64(t)
		nsec := int64((t - float64(sec)) * 1e9)
		return time.Unix(sec, nsec).UTC().Truncate(time.Microsecond), nil
	case float32:
		f64 := float64(t)
		sec := int64(f64)
		nsec := int64((f64 - float64(sec)) * 1e9)
		return time.Unix(sec, nsec).UTC().Truncate(time.Microsecond), nil
	case string:
		// Try to parse RFC3339 format
		return time.Parse(time.RFC3339, t)
	default:
		return time.Time{}, fmt.Errorf("unsupported timestamp type: %T", v)
	}
}

func parseInt64(v interface{}) (int64, error) {
	switch n := v.(type) {
	case int64:
		return n, nil
	case int:
		return int64(n), nil
	case int32:
		return int64(n), nil
	case int16:
		return int64(n), nil
	case int8:
		return int64(n), nil
	case uint:
		return int64(n), nil
	case uint64:
		return int64(n), nil
	case uint32:
		return int64(n), nil
	case uint16:
		return int64(n), nil
	case uint8:
		return int64(n), nil
	case float64:
		return int64(n), nil
	case float32:
		return int64(n), nil
	default:
		return 0, fmt.Errorf("unsupported int64 type: %T", v)
	}
}

func parseUint64(v interface{}) (uint64, error) {
	switch n := v.(type) {
	case uint64:
		return n, nil
	case int:
		return uint64(n), nil
	case int32:
		return uint64(n), nil
	case int16:
		return uint64(n), nil
	case int8:
		return uint64(n), nil
	case uint:
		return uint64(n), nil
	case int64:
		return uint64(n), nil
	case uint32:
		return uint64(n), nil
	case uint16:
		return uint64(n), nil
	case uint8:
		return uint64(n), nil
	case float64:
		return uint64(n), nil
	case float32:
		return uint64(n), nil
	default:
		return 0, fmt.Errorf("unsupported int64 type: %T", v)
	}
}

func parseUint64Array(v interface{}) ([]uint64, error) {
	arr, ok := v.([]interface{})
	if !ok {
		return nil, fmt.Errorf("expected array, got %T", v)
	}

	result := make([]uint64, 0, len(arr))
	for i, item := range arr {
		val, err := parseUint64(item)
		if err != nil {
			return nil, fmt.Errorf("failed to parse element at index %d: %w", i, err)
		}
		result = append(result, val)
	}
	return result, nil
}

func parseInt32Array(v interface{}) ([]int32, error) {
	arr, ok := v.([]interface{})
	if !ok {
		return nil, fmt.Errorf("expected array, got %T", v)
	}

	result := make([]int32, 0, len(arr))
	for i, item := range arr {
		switch n := item.(type) {
		case int32:
			result = append(result, n)
		case int:
			result = append(result, int32(n))
		case int64:
			result = append(result, int32(n))
		case int16:
			result = append(result, int32(n))
		case int8:
			result = append(result, int32(n))
		case uint:
			result = append(result, int32(n))
		case uint64:
			result = append(result, int32(n))
		case uint32:
			result = append(result, int32(n))
		case uint16:
			result = append(result, int32(n))
		case uint8:
			result = append(result, int32(n))
		case float64:
			result = append(result, int32(n))
		case float32:
			result = append(result, int32(n))
		default:
			return nil, fmt.Errorf("unsupported int32 type at index %d: %T", i, item)
		}
	}
	return result, nil
}
