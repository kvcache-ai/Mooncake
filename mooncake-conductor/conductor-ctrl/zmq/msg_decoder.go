package zmq

import (
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"time"

	msgpack "github.com/shamaton/msgpack/v2"
)

type EventParser interface {
	ParseEvent(raw []interface{}, timestamp interface{}) (KVEvent, error)
	EventMappings() map[string]EventType
	Source() string
}

type mooncakeParser struct{}

func (p *mooncakeParser) Source() string { return SourceMooncake }

func (p *mooncakeParser) EventMappings() map[string]EventType {
	return map[string]EventType{
		"BlockStoreEvent":  EventTypeBlockStored,
		"BlockUpdateEvent": EventTypeBlockUpdate,
		"RemoveAllEvent":   EventTypeAllCleared,
	}
}

func (p *mooncakeParser) ParseEvent(raw []interface{}, timestamp interface{}) (KVEvent, error) {
	eventTypeStr, ok := raw[0].(string)
	if !ok {
		return nil, fmt.Errorf("invalid event type format: %T", raw[0])
	}

	eventType, exists := p.EventMappings()[eventTypeStr]
	if !exists {
		return nil, fmt.Errorf("unknown mooncake event type: %s", eventTypeStr)
	}

	switch eventType {
	case EventTypeBlockStored:
		return parseMooncakeBlockStored(raw, timestamp)
	default:
		return nil, fmt.Errorf("unhandled event: %s", eventType)
	}
}

func decodeCommonEventBatch(
	data []byte,
	expectedLength int,
	extractEvents func([]interface{}) ([]interface{}, interface{}, error),
	parser EventParser,
) (*EventBatch, error) {

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

	events, timestamp, err := extractEvents(arr)
	if err != nil {
		return nil, err
	}

	if len(events) == 0 {
		slog.Warn("Received empty event list")
	}

	batch := &EventBatch{
		Source: parser.Source(),
		Events: make([]KVEvent, 0, len(events)),
	}

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

func newMooncakeParser() EventParser {
	return &mooncakeParser{}
}

func DecodeMooncakeEventBatch(data []byte) (*EventBatch, error) {
	return decodeCommonEventBatch(
		data,
		2,
		func(arr []interface{}) ([]interface{}, interface{}, error) {
			events, ok := arr[1].([]interface{})
			if !ok {
				return nil, nil, fmt.Errorf("invalid events type: %T", arr[1])
			}
			return events, arr[0], nil
		},
		newMooncakeParser(),
	)
}

type vllmParser struct{}

func (p *vllmParser) Source() string { return SourceVLLM }

func (p *vllmParser) EventMappings() map[string]EventType {
	return map[string]EventType{
		"BlockStored":      EventTypeBlockStored,
		"BlockRemoved":     EventTypeBlockRemoved,
		"AllBlocksCleared": EventTypeAllCleared,
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
	default:
		return nil, fmt.Errorf("unhandled event: %s", eventType)
	}
}

func newVLLMParser() EventParser {
	return &vllmParser{}
}

func DecodeVllmEventBatch(data []byte) (*EventBatch, error) {
	return decodeCommonEventBatch(
		data,
		3,
		func(arr []interface{}) ([]interface{}, interface{}, error) {
			events, ok := arr[1].([]interface{})
			if !ok {
				return nil, nil, fmt.Errorf("invalid events type: %T", arr[1])
			}
			return events, arr[0], nil
		},
		newVLLMParser(),
	)
}

func parseMooncakeBlockStored(data []interface{}, timestamp interface{}) (*BlockStoredEvent, error) {
	event := &BlockStoredEvent{
		Type: EventTypeBlockStored,
	}

	if mooncakekey, err := safeGetString(data[1]); err == nil {
		event.MooncakeKey = mooncakekey
	} else {
		return nil, fmt.Errorf("failed to parse MooncakeKey from field at index 1: %w", err)
	}

	if replicalist, err := convertToReplicaList(data[2]); err == nil {
		event.ReplicaList = replicalist
		slog.Debug("ReplicaList:", "ReplicaList", event.ReplicaList)
	} else {
		return nil, fmt.Errorf("failed to parse ReplicaList from field at index 2: %w", err)
	}

	if blocksize, err := parseInt64(data[4]); err == nil {
		event.BlockSize = blocksize
		slog.Debug("BlockSize:", "BlockSize", event.BlockSize)
	} else {
		return nil, fmt.Errorf("failed to parse BlockSize from field at index 4: %w", err)
	}

	if blockhash, err := parseMooncakeParentUint64(data[5]); err == nil {
		event.BlockHashes = blockhash
		slog.Debug("BlockHashes:", "BlockHashes", event.BlockHashes)
	} else {
		return nil, fmt.Errorf("failed to parse BlockHashes from field at index 5: %w", err)
	}

	if parentblockhash, err := parseMooncakeUint64(data[6]); err == nil {
		event.ParentBlockHash = parentblockhash
		slog.Debug("ParentBlockHash:", "ParentBlockHash", event.ParentBlockHash)
	} else {
		return nil, fmt.Errorf("failed to parse ParentBlockHash from field at index 6: %w", err)
	}

	if tokenIDsRaw, ok := data[7].([]interface{}); ok {
		tokens, err := parseInt32Array(tokenIDsRaw)
		if err != nil {
			return nil, fmt.Errorf("failed to parse TokenIDs from field at index 7: %w", err)
		}
		event.TokenIDs = tokens
		slog.Debug("TokenIDs:", "TokenIDs", event.TokenIDs)
	} else {
		return nil, fmt.Errorf("missing or invalid token_ids")
	}

	return event, nil
}

// parseBlockStoredEvent parses a BlockStoredEvent from raw data
func parseVllmBlockStored(data []interface{}, timestamp interface{}) (*BlockStoredEvent, error) {
	event := &BlockStoredEvent{
		Type: EventTypeBlockStored,
	}

	slog.Debug("success parseBlockStoredEvent")
	for i, elem := range data {
		slog.Debug("Array element", "index", i, "type", fmt.Sprintf("%T", elem), "value", elem)
	}

	slog.Debug("Raw eventType bytes:", "timestamp", timestamp)
	// Parse timestamp
	if ts, err := parseTimestamp(timestamp); err == nil {
		slog.Debug("timestamp:", "timestamp", ts)
		event.Timestamp = ts
	} else {
		return nil, fmt.Errorf("failed to parse timestamp: %w", err)
	}

	// // Parse block hashes
	if hashes, err := parseUint64Array(data[1]); err == nil {
		slog.Debug("BlockHashes:", "BlockHashes", hashes)
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
		slog.Debug("TokenIDs:", "TokenIDs", event.TokenIDs)
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
		return nil, fmt.Errorf("failed to parse field at index 4 as integer: %w", err)
	}

	return event, nil
}

func convertToReplicaList(raw interface{}) ([][]string, error) {
	list, ok := raw.([]interface{})
	if !ok {
		return nil, fmt.Errorf("expected []interface{}, got %T", raw)
	}

	result := make([][]string, len(list))
	for i, item := range list {
		subList, ok := item.([]interface{})
		if !ok {
			return nil, fmt.Errorf("item %d is not []interface{}, got %T", i, item)
		}
		result[i] = make([]string, len(subList))
		for j, v := range subList {
			str, ok := v.(string)
			if !ok {
				return nil, fmt.Errorf("element [%d][%d] is not string, got %T", i, j, v)
			}
			result[i][j] = str
		}
	}
	return result, nil
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

func parseMooncakeUint64(v interface{}) (uint64, error) {
	var s string
	switch val := v.(type) {
	case string:
		s = val
	case nil:
		s = ""
	case uint64:
		return val, nil
	case int:
		if val < 0 {
			return 0, fmt.Errorf("negative value %d", val)
		}
		return uint64(val), nil
	case int8:
		if val < 0 {
			return 0, fmt.Errorf("negative value %d", val)
		}
		return uint64(val), nil
	case int16:
		if val < 0 {
			return 0, fmt.Errorf("negative value %d", val)
		}
		return uint64(val), nil
	case int32:
		if val < 0 {
			return 0, fmt.Errorf("negative value %d", val)
		}
		return uint64(val), nil
	case int64:
		if val < 0 {
			return 0, fmt.Errorf("negative value %d", val)
		}
		return uint64(val), nil
	case uint:
		return uint64(val), nil
	case uint8:
		return uint64(val), nil
	case uint16:
		return uint64(val), nil
	case uint32:
		return uint64(val), nil
	default:
		s = fmt.Sprint(v)
	}
	if s == "" {
		return 0, nil
	}
	return strconv.ParseUint(s, 10, 64)
}

func parseMooncakeParentUint64(v interface{}) ([]uint64, error) {
	switch val := v.(type) {
	case nil:
		return []uint64{}, nil

	case string:
		if val == "" {
			return []uint64{}, nil
		}
		parts := strings.FieldsFunc(val, func(r rune) bool {
			return r == ',' || r == ' ' || r == '\t' || r == '\n'
		})
		result := make([]uint64, 0, len(parts))
		for _, part := range parts {
			if part == "" {
				continue
			}
			u, err := strconv.ParseUint(part, 10, 64)
			if err != nil {
				return nil, fmt.Errorf("failed to parse uint64 from string %q: %w", part, err)
			}
			result = append(result, u)
		}
		return result, nil

	case []interface{}:
		result := make([]uint64, 0, len(val))
		for _, item := range val {
			u, err := parseSingleUint64(item)
			if err != nil {
				return nil, fmt.Errorf("failed to parse element %v: %w", item, err)
			}
			result = append(result, u)
		}
		return result, nil

	case []uint64:
		// already correct type
		return val, nil

	default:
		// try parse as single uint64
		u, err := parseSingleUint64(val)
		if err != nil {
			return nil, err
		}
		return []uint64{u}, nil
	}
}

func parseSingleUint64(v interface{}) (uint64, error) {
	switch val := v.(type) {
	case uint64:
		return val, nil
	case int:
		if val < 0 {
			return 0, fmt.Errorf("negative int %d cannot convert to uint64", val)
		}
		return uint64(val), nil
	case int8:
		if val < 0 {
			return 0, fmt.Errorf("negative int8 %d cannot convert to uint64", val)
		}
		return uint64(val), nil
	case int16:
		if val < 0 {
			return 0, fmt.Errorf("negative int16 %d cannot convert to uint64", val)
		}
		return uint64(val), nil
	case int32:
		if val < 0 {
			return 0, fmt.Errorf("negative int32 %d cannot convert to uint64", val)
		}
		return uint64(val), nil
	case int64:
		if val < 0 {
			return 0, fmt.Errorf("negative int64 %d cannot convert to uint64", val)
		}
		return uint64(val), nil
	case uint:
		return uint64(val), nil
	case uint8:
		return uint64(val), nil
	case uint16:
		return uint64(val), nil
	case uint32:
		return uint64(val), nil
	case float32:
		f := float64(val)
		if f < 0 || f != float64(uint64(f)) {
			return 0, fmt.Errorf("float32 %v invalid for uint64", val)
		}
		return uint64(f), nil
	case float64:
		if val < 0 || val != float64(uint64(val)) {
			return 0, fmt.Errorf("float64 %v invalid for uint64", val)
		}
		return uint64(val), nil
	case string:
		if val == "" {
			return 0, fmt.Errorf("empty string cannot be parsed as uint64")
		}
		return strconv.ParseUint(val, 10, 64)
	case nil:
		return 0, fmt.Errorf("nil cannot be parsed as uint64")
	default:
		return 0, fmt.Errorf("unsupported type %T for uint64 conversion", v)
	}
}
