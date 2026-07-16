#include "conductor/zmq/msg_decoder.h"

#include <msgpack.hpp>

#include <cmath>
#include <cstdint>
#include <limits>
#include <map>
#include <optional>
#include <set>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace conductor {
namespace zmq {

namespace {

using msgpack::object;
using msgpack::type::object_type;

template <typename T>
struct ValueResult {
    std::optional<T> value;
    std::string error;

    static ValueResult Ok(T value) {
        return {.value = std::move(value), .error = ""};
    }
    static ValueResult Err(std::string error) {
        return {.value = std::nullopt, .error = std::move(error)};
    }
};

std::string TypeName(const object& value) {
    switch (value.type) {
        case object_type::NIL:
            return "nil";
        case object_type::BOOLEAN:
            return "boolean";
        case object_type::POSITIVE_INTEGER:
            return "positive integer";
        case object_type::NEGATIVE_INTEGER:
            return "negative integer";
        case object_type::FLOAT32:
        case object_type::FLOAT64:
            return "float";
        case object_type::STR:
            return "string";
        case object_type::BIN:
            return "binary";
        case object_type::ARRAY:
            return "array";
        case object_type::MAP:
            return "map";
        case object_type::EXT:
            return "extension";
    }
    return "unknown";
}

class MapReader {
   public:
    MapReader(const object& value,
              const std::set<std::string_view>& recognized_fields) {
        if (value.type != object_type::MAP) {
            error_ = "expected event map, got " + TypeName(value);
            return;
        }
        for (uint32_t index = 0; index < value.via.map.size; ++index) {
            const auto& item = value.via.map.ptr[index];
            if (item.key.type != object_type::STR) {
                error_ = "event map key at index " + std::to_string(index) +
                         " must be a string";
                return;
            }
            const std::string key(item.key.via.str.ptr, item.key.via.str.size);
            if (!recognized_fields.contains(key)) {
                continue;
            }
            if (!fields_.emplace(key, &item.val).second) {
                error_ = "duplicate recognized key: " + key;
                return;
            }
        }
    }

    const std::string& error() const { return error_; }

    const object* Get(std::string_view name) const {
        auto it = fields_.find(std::string(name));
        return it == fields_.end() ? nullptr : it->second;
    }

   private:
    std::map<std::string, const object*> fields_;
    std::string error_;
};

ValueResult<std::string> ParseString(const object& value) {
    if (value.type != object_type::STR) {
        return ValueResult<std::string>::Err("expected string, got " +
                                             TypeName(value));
    }
    return ValueResult<std::string>::Ok(
        std::string(value.via.str.ptr, value.via.str.size));
}

ValueResult<std::optional<std::string>> ParseNullableString(
    const object& value) {
    if (value.type == object_type::NIL) {
        return ValueResult<std::optional<std::string>>::Ok(std::nullopt);
    }
    auto parsed = ParseString(value);
    if (!parsed.value.has_value()) {
        return ValueResult<std::optional<std::string>>::Err(parsed.error);
    }
    return ValueResult<std::optional<std::string>>::Ok(
        std::move(*parsed.value));
}

ValueResult<uint64_t> ParseUint64(const object& value) {
    if (value.type != object_type::POSITIVE_INTEGER) {
        return ValueResult<uint64_t>::Err("expected unsigned integer, got " +
                                          TypeName(value));
    }
    return ValueResult<uint64_t>::Ok(value.via.u64);
}

ValueResult<int64_t> ParseInt64(const object& value) {
    if (value.type == object_type::NEGATIVE_INTEGER) {
        return ValueResult<int64_t>::Ok(value.via.i64);
    }
    if (value.type == object_type::POSITIVE_INTEGER &&
        value.via.u64 <=
            static_cast<uint64_t>(std::numeric_limits<int64_t>::max())) {
        return ValueResult<int64_t>::Ok(static_cast<int64_t>(value.via.u64));
    }
    return ValueResult<int64_t>::Err("expected signed 64-bit integer, got " +
                                     TypeName(value));
}

ValueResult<std::optional<int64_t>> ParseNullableInt64(const object& value) {
    if (value.type == object_type::NIL) {
        return ValueResult<std::optional<int64_t>>::Ok(std::nullopt);
    }
    auto parsed = ParseInt64(value);
    if (!parsed.value.has_value()) {
        return ValueResult<std::optional<int64_t>>::Err(parsed.error);
    }
    return ValueResult<std::optional<int64_t>>::Ok(*parsed.value);
}

ValueResult<std::optional<uint64_t>> ParseNullableUint64(const object& value) {
    if (value.type == object_type::NIL) {
        return ValueResult<std::optional<uint64_t>>::Ok(std::nullopt);
    }
    auto parsed = ParseUint64(value);
    if (!parsed.value.has_value()) {
        return ValueResult<std::optional<uint64_t>>::Err(parsed.error);
    }
    return ValueResult<std::optional<uint64_t>>::Ok(*parsed.value);
}

ValueResult<ExternalHash> ParseExternalHash(const object& value) {
    if (value.type == object_type::POSITIVE_INTEGER) {
        return ValueResult<ExternalHash>::Ok(value.via.u64);
    }
    if (value.type == object_type::BIN) {
        const auto* begin = reinterpret_cast<const uint8_t*>(value.via.bin.ptr);
        return ValueResult<ExternalHash>::Ok(
            std::vector<uint8_t>(begin, begin + value.via.bin.size));
    }
    return ValueResult<ExternalHash>::Err(
        "expected unsigned integer or binary hash, got " + TypeName(value));
}

ValueResult<std::optional<ExternalHash>> ParseNullableExternalHash(
    const object& value) {
    if (value.type == object_type::NIL) {
        return ValueResult<std::optional<ExternalHash>>::Ok(std::nullopt);
    }
    auto parsed = ParseExternalHash(value);
    if (!parsed.value.has_value()) {
        return ValueResult<std::optional<ExternalHash>>::Err(parsed.error);
    }
    return ValueResult<std::optional<ExternalHash>>::Ok(
        std::move(*parsed.value));
}

template <typename T, typename Parser>
ValueResult<std::vector<T>> ParseArray(const object& value, Parser parser) {
    if (value.type != object_type::ARRAY) {
        return ValueResult<std::vector<T>>::Err("expected array, got " +
                                                TypeName(value));
    }
    std::vector<T> result;
    result.reserve(value.via.array.size);
    for (uint32_t index = 0; index < value.via.array.size; ++index) {
        auto parsed = parser(value.via.array.ptr[index]);
        if (!parsed.value.has_value()) {
            return ValueResult<std::vector<T>>::Err(
                "element " + std::to_string(index) + ": " + parsed.error);
        }
        result.push_back(std::move(*parsed.value));
    }
    return ValueResult<std::vector<T>>::Ok(std::move(result));
}

ValueResult<std::vector<ExternalHash>> ParseExternalHashes(
    const object& value) {
    return ParseArray<ExternalHash>(value, ParseExternalHash);
}

ValueResult<std::vector<uint64_t>> ParseUint64Array(const object& value) {
    return ParseArray<uint64_t>(value, ParseUint64);
}

ValueResult<std::vector<int32_t>> ParseInt32Array(const object& value) {
    return ParseArray<int32_t>(value, [](const object& item) {
        auto parsed = ParseInt64(item);
        if (!parsed.value.has_value()) {
            return ValueResult<int32_t>::Err(parsed.error);
        }
        if (*parsed.value < std::numeric_limits<int32_t>::min() ||
            *parsed.value > std::numeric_limits<int32_t>::max()) {
            return ValueResult<int32_t>::Err("integer is outside int32 range");
        }
        return ValueResult<int32_t>::Ok(static_cast<int32_t>(*parsed.value));
    });
}

ValueResult<std::optional<std::vector<int32_t>>> ParseNullableInt32Array(
    const object& value) {
    if (value.type == object_type::NIL) {
        return ValueResult<std::optional<std::vector<int32_t>>>::Ok(
            std::nullopt);
    }
    auto parsed = ParseInt32Array(value);
    if (!parsed.value.has_value()) {
        return ValueResult<std::optional<std::vector<int32_t>>>::Err(
            parsed.error);
    }
    return ValueResult<std::optional<std::vector<int32_t>>>::Ok(
        std::move(*parsed.value));
}

template <typename T, typename Parser>
bool ParseRequired(const MapReader& reader, std::string_view field,
                   Parser parser, T* output, std::string* error) {
    const object* value = reader.Get(field);
    if (value == nullptr) {
        *error = "missing required key: " + std::string(field);
        return false;
    }
    auto parsed = parser(*value);
    if (!parsed.value.has_value()) {
        *error = "invalid " + std::string(field) + ": " + parsed.error;
        return false;
    }
    *output = std::move(*parsed.value);
    return true;
}

template <typename T, typename Parser>
bool ParseOptional(const MapReader& reader, std::string_view field,
                   Parser parser, std::optional<T>* output,
                   std::string* error) {
    const object* value = reader.Get(field);
    if (value == nullptr) {
        output->reset();
        return true;
    }
    auto parsed = parser(*value);
    if (!parsed.value.has_value()) {
        *error = "invalid " + std::string(field) + ": " + parsed.error;
        return false;
    }
    *output = std::move(*parsed.value);
    return true;
}

template <typename T, typename Parser>
bool ParseOptionalNullable(const MapReader& reader, std::string_view field,
                           Parser parser, std::optional<T>* output,
                           std::string* error) {
    const object* value = reader.Get(field);
    if (value == nullptr) {
        output->reset();
        return true;
    }
    auto parsed = parser(*value);
    if (!parsed.value.has_value()) {
        *error = "invalid " + std::string(field) + ": " + parsed.error;
        return false;
    }
    *output = std::move(*parsed.value);
    return true;
}

const std::set<std::string_view> kVllmFields = {
    "type",      "block_hashes",       "parent_block_hash",
    "token_ids", "block_size",         "lora_id",
    "medium",    "lora_name",          "extra_keys",
    "group_idx", "kv_cache_spec_kind", "kv_cache_spec_sliding_window",
};

const std::set<std::string_view> kVllmRemovedFields = {"type", "block_hashes",
                                                       "medium", "group_idx"};

std::string ValidateVllmExtraKeys(const object& value, size_t block_count,
                                  bool* present) {
    if (value.type == object_type::NIL) {
        *present = false;
        return "";
    }
    if (value.type != object_type::ARRAY) {
        return "expected array or nil, got " + TypeName(value);
    }
    if (value.via.array.size != block_count) {
        return "expected one entry per block hash, got " +
               std::to_string(value.via.array.size) + " entries for " +
               std::to_string(block_count) + " block hashes";
    }
    for (uint32_t index = 0; index < value.via.array.size; ++index) {
        const object& item = value.via.array.ptr[index];
        if (item.type != object_type::NIL && item.type != object_type::ARRAY) {
            return "element " + std::to_string(index) +
                   ": expected array or nil, got " + TypeName(item);
        }
    }
    *present = true;
    return "";
}

ValueResult<VllmEvent> ParseVllmEvent(const object& raw) {
    MapReader reader(raw, kVllmFields);
    if (!reader.error().empty()) {
        return ValueResult<VllmEvent>::Err(reader.error());
    }

    std::string error;
    std::string type;
    if (!ParseRequired(reader, "type", ParseString, &type, &error)) {
        return ValueResult<VllmEvent>::Err(error);
    }

    if (type == "BlockStored") {
        VllmStoredEvent event;
        if (!ParseRequired(reader, "block_hashes", ParseExternalHashes,
                           &event.block_hashes, &error) ||
            !ParseRequired(reader, "parent_block_hash",
                           ParseNullableExternalHash, &event.parent_block_hash,
                           &error) ||
            !ParseRequired(reader, "token_ids", ParseNullableInt32Array,
                           &event.token_ids, &error) ||
            !ParseRequired(reader, "block_size", ParseInt64, &event.block_size,
                           &error) ||
            !ParseRequired(reader, "lora_id", ParseNullableInt64,
                           &event.lora_id, &error) ||
            !ParseRequired(reader, "medium", ParseNullableString, &event.medium,
                           &error) ||
            !ParseRequired(reader, "lora_name", ParseNullableString,
                           &event.lora_name, &error) ||
            !ParseOptionalNullable(reader, "group_idx", ParseNullableInt64,
                                   &event.group_idx, &error) ||
            !ParseOptionalNullable(reader, "kv_cache_spec_kind",
                                   ParseNullableString,
                                   &event.kv_cache_spec_kind, &error) ||
            !ParseOptionalNullable(
                reader, "kv_cache_spec_sliding_window", ParseNullableInt64,
                &event.kv_cache_spec_sliding_window, &error)) {
            return ValueResult<VllmEvent>::Err(error);
        }
        if (const object* extra_keys = reader.Get("extra_keys");
            extra_keys != nullptr) {
            if (std::string extra_keys_error = ValidateVllmExtraKeys(
                    *extra_keys, event.block_hashes.size(),
                    &event.extra_keys_present);
                !extra_keys_error.empty()) {
                return ValueResult<VllmEvent>::Err("invalid extra_keys: " +
                                                   extra_keys_error);
            }
        }
        return ValueResult<VllmEvent>::Ok(std::move(event));
    }

    if (type == "BlockRemoved") {
        for (std::string_view field : kVllmFields) {
            if (!kVllmRemovedFields.contains(field) &&
                reader.Get(field) != nullptr) {
                return ValueResult<VllmEvent>::Err(
                    "BlockRemoved contains recognized key: " +
                    std::string(field));
            }
        }
        VllmRemovedEvent event;
        if (!ParseRequired(reader, "block_hashes", ParseExternalHashes,
                           &event.block_hashes, &error) ||
            !ParseRequired(reader, "medium", ParseNullableString, &event.medium,
                           &error) ||
            !ParseOptionalNullable(reader, "group_idx", ParseNullableInt64,
                                   &event.group_idx, &error)) {
            return ValueResult<VllmEvent>::Err(error);
        }
        return ValueResult<VllmEvent>::Ok(std::move(event));
    }

    if (type == "AllBlocksCleared") {
        for (std::string_view field : kVllmFields) {
            if (field != "type" && reader.Get(field) != nullptr) {
                return ValueResult<VllmEvent>::Err(
                    "AllBlocksCleared contains recognized key: " +
                    std::string(field));
            }
        }
        return ValueResult<VllmEvent>::Ok(VllmClearedEvent{});
    }

    return ValueResult<VllmEvent>::Err("unknown vLLM event tag: " + type);
}

const std::set<std::string_view> kMooncakeFields = {
    "event_id",
    "timestamp",
    "event_type",
    "type",
    "model_name",
    "block_size",
    "additional_salt",
    "lora_name",
    "tenant_id",
    "backend_id",
    "medium",
    "dp_rank",
    "group_id",
    "object_key",
    "connector_block_hash",
    "cache_prefix",
    "tp_rank",
    "head_or_tp_rank",
    "pcp_rank",
    "dcp_rank",
    "pp_rank",
    "layer_id",
    "seq_hashes",
    "block_hashes",
    "base_block_idx",
    "parent_hash",
    "token_ids",
    "parent_block_hash",
};

bool ParseMooncakeCommon(const MapReader& reader, int64_t batch_timestamp,
                         std::string_view event_type,
                         MooncakeEventFields* fields, std::string* error) {
    if (!ParseRequired(reader, "event_id", ParseUint64, &fields->event_id,
                       error) ||
        !ParseRequired(reader, "timestamp", ParseInt64,
                       &fields->timestamp_milliseconds, error) ||
        !ParseRequired(reader, "model_name", ParseNullableString,
                       &fields->model_name, error) ||
        !ParseRequired(reader, "block_size", ParseNullableInt64,
                       &fields->block_size, error) ||
        !ParseRequired(reader, "additional_salt", ParseNullableString,
                       &fields->additional_salt, error) ||
        !ParseRequired(reader, "lora_name", ParseNullableString,
                       &fields->lora_name, error) ||
        !ParseRequired(reader, "tenant_id", ParseString, &fields->tenant_id,
                       error) ||
        !ParseRequired(reader, "backend_id", ParseString, &fields->backend_id,
                       error) ||
        !ParseRequired(reader, "medium", ParseNullableString, &fields->medium,
                       error) ||
        !ParseRequired(reader, "dp_rank", ParseInt64,
                       &fields->data_parallel_rank, error)) {
        return false;
    }
    if (fields->timestamp_milliseconds != batch_timestamp) {
        *error = "event timestamp conflicts with batch timestamp";
        return false;
    }
    if (fields->data_parallel_rank < 0) {
        *error = "event dp_rank must be non-negative";
        return false;
    }

    if (const object* legacy_type = reader.Get("type");
        legacy_type != nullptr) {
        auto parsed = ParseString(*legacy_type);
        if (!parsed.value.has_value()) {
            *error = "invalid type: " + parsed.error;
            return false;
        }
        const std::string_view expected =
            event_type == "stored"
                ? "BlockStored"
                : (event_type == "removed" ? "BlockRemoved"
                                           : "AllBlocksCleared");
        if (*parsed.value != expected) {
            *error = "legacy type conflicts with event_type";
            return false;
        }
    }
    return true;
}

bool ParseMooncakeObject(const MapReader& reader, MooncakeObjectFields* object,
                         std::string* error) {
    if (!ParseRequired(reader, "group_id", ParseNullableString,
                       &object->group_id, error) ||
        !ParseRequired(reader, "seq_hashes", ParseUint64Array,
                       &object->seq_hashes, error) ||
        !ParseRequired(reader, "base_block_idx", ParseNullableInt64,
                       &object->base_block_idx, error) ||
        !ParseOptional(reader, "object_key", ParseString, &object->object_key,
                       error) ||
        !ParseOptional(reader, "connector_block_hash", ParseString,
                       &object->connector_block_hash, error) ||
        !ParseOptional(reader, "cache_prefix", ParseString,
                       &object->cache_prefix, error) ||
        !ParseOptionalNullable(reader, "tp_rank", ParseNullableInt64,
                               &object->tp_rank, error) ||
        !ParseOptionalNullable(reader, "head_or_tp_rank", ParseNullableInt64,
                               &object->head_or_tp_rank, error) ||
        !ParseOptionalNullable(reader, "pcp_rank", ParseNullableInt64,
                               &object->pcp_rank, error) ||
        !ParseOptionalNullable(reader, "dcp_rank", ParseNullableInt64,
                               &object->dcp_rank, error) ||
        !ParseOptionalNullable(reader, "pp_rank", ParseNullableInt64,
                               &object->pp_rank, error) ||
        !ParseOptionalNullable(reader, "layer_id", ParseNullableInt64,
                               &object->layer_id, error) ||
        !ParseOptional(reader, "block_hashes", ParseUint64Array,
                       &object->legacy_block_hashes, error)) {
        return false;
    }
    if (object->legacy_block_hashes.has_value() &&
        *object->legacy_block_hashes != object->seq_hashes) {
        *error = "legacy block_hashes conflicts with seq_hashes";
        return false;
    }
    return true;
}

ValueResult<MooncakeEvent> ParseMooncakeEvent(const object& raw,
                                              int64_t batch_timestamp) {
    MapReader reader(raw, kMooncakeFields);
    if (!reader.error().empty()) {
        return ValueResult<MooncakeEvent>::Err(reader.error());
    }
    std::string error;
    std::string event_type;
    if (!ParseRequired(reader, "event_type", ParseString, &event_type,
                       &error)) {
        return ValueResult<MooncakeEvent>::Err(error);
    }
    if (event_type != "stored" && event_type != "removed" &&
        event_type != "cleared") {
        return ValueResult<MooncakeEvent>::Err("unknown Mooncake event tag: " +
                                               event_type);
    }

    MooncakeEventFields fields;
    if (!ParseMooncakeCommon(reader, batch_timestamp, event_type, &fields,
                             &error)) {
        return ValueResult<MooncakeEvent>::Err(error);
    }

    if (event_type == "cleared") {
        static const std::set<std::string_view> kObjectOnlyFields = {
            "group_id",          "object_key",  "connector_block_hash",
            "cache_prefix",      "tp_rank",     "head_or_tp_rank",
            "pcp_rank",          "dcp_rank",    "pp_rank",
            "layer_id",          "seq_hashes",  "block_hashes",
            "base_block_idx",    "parent_hash", "token_ids",
            "parent_block_hash",
        };
        for (std::string_view field : kObjectOnlyFields) {
            if (reader.Get(field) != nullptr) {
                return ValueResult<MooncakeEvent>::Err(
                    "cleared event contains recognized object key: " +
                    std::string(field));
            }
        }
        return ValueResult<MooncakeEvent>::Ok(
            MooncakeClearedEvent{.fields = std::move(fields)});
    }

    MooncakeObjectFields object_fields;
    if (!ParseMooncakeObject(reader, &object_fields, &error)) {
        return ValueResult<MooncakeEvent>::Err(error);
    }

    if (event_type == "removed") {
        if (reader.Get("parent_hash") != nullptr ||
            reader.Get("token_ids") != nullptr ||
            reader.Get("parent_block_hash") != nullptr) {
            return ValueResult<MooncakeEvent>::Err(
                "removed event contains recognized stored-only key");
        }
        return ValueResult<MooncakeEvent>::Ok(MooncakeRemovedEvent{
            .fields = std::move(fields), .object = std::move(object_fields)});
    }

    MooncakeStoredEvent event{.fields = std::move(fields),
                              .object = std::move(object_fields)};
    if (!ParseRequired(reader, "parent_hash", ParseNullableUint64,
                       &event.parent_hash, &error) ||
        !ParseRequired(reader, "token_ids", ParseNullableInt32Array,
                       &event.token_ids, &error)) {
        return ValueResult<MooncakeEvent>::Err(error);
    }
    if (const object* legacy_parent = reader.Get("parent_block_hash");
        legacy_parent != nullptr) {
        auto parsed = ParseNullableUint64(*legacy_parent);
        if (!parsed.value.has_value()) {
            return ValueResult<MooncakeEvent>::Err(
                "invalid parent_block_hash: " + parsed.error);
        }
        if (*parsed.value != event.parent_hash) {
            return ValueResult<MooncakeEvent>::Err(
                "parent_block_hash conflicts with parent_hash");
        }
    }
    return ValueResult<MooncakeEvent>::Ok(std::move(event));
}

template <typename Batch>
BatchDecodeResult<Batch> EnvelopeError(std::string error) {
    return {.ok = false, .batch = {}, .error = std::move(error)};
}

ValueResult<msgpack::object_handle> UnpackOne(const char* data, size_t len) {
    if (data == nullptr || len == 0) {
        return ValueResult<msgpack::object_handle>::Err("empty payload");
    }
    try {
        size_t offset = 0;
        auto handle = msgpack::unpack(data, len, offset);
        if (offset != len) {
            return ValueResult<msgpack::object_handle>::Err(
                "trailing bytes after MessagePack value");
        }
        return ValueResult<msgpack::object_handle>::Ok(std::move(handle));
    } catch (const std::exception& error) {
        return ValueResult<msgpack::object_handle>::Err(error.what());
    }
}

bool ValidateEnvelopeShape(const object& root, const object** timestamp,
                           const object** events, const object** dp_rank,
                           std::string* error) {
    if (root.type != object_type::ARRAY) {
        *error = "expected three-element array envelope, got " + TypeName(root);
        return false;
    }
    if (root.via.array.size != 3) {
        *error = "expected three-element array envelope, got " +
                 std::to_string(root.via.array.size) + " elements";
        return false;
    }
    *timestamp = &root.via.array.ptr[0];
    *events = &root.via.array.ptr[1];
    *dp_rank = &root.via.array.ptr[2];
    if ((*events)->type != object_type::ARRAY) {
        *error = "events must be an array, got " + TypeName(**events);
        return false;
    }
    return true;
}

ValueResult<std::optional<int64_t>> ParseBatchDpRank(const object& value) {
    auto parsed = ParseNullableInt64(value);
    if (!parsed.value.has_value()) {
        return parsed;
    }
    if (parsed.value->has_value() && **parsed.value < 0) {
        return ValueResult<std::optional<int64_t>>::Err(
            "data_parallel_rank must be non-negative or nil");
    }
    return parsed;
}

}  // namespace

VllmEventBatchResult DecodeVllmEventBatch(const char* data, size_t len) {
    auto unpacked = UnpackOne(data, len);
    if (!unpacked.value.has_value()) {
        return EnvelopeError<VllmEventBatch>(
            "failed to decode vLLM envelope: " + unpacked.error);
    }
    const object* timestamp = nullptr;
    const object* events = nullptr;
    const object* dp_rank = nullptr;
    std::string error;
    if (!ValidateEnvelopeShape(unpacked.value->get(), &timestamp, &events,
                               &dp_rank, &error)) {
        return EnvelopeError<VllmEventBatch>(error);
    }
    if (timestamp->type != object_type::FLOAT32 &&
        timestamp->type != object_type::FLOAT64) {
        return EnvelopeError<VllmEventBatch>(
            "vLLM timestamp must be a float, got " + TypeName(*timestamp));
    }
    if (!std::isfinite(timestamp->via.f64)) {
        return EnvelopeError<VllmEventBatch>("vLLM timestamp must be finite");
    }
    auto parsed_rank = ParseBatchDpRank(*dp_rank);
    if (!parsed_rank.value.has_value()) {
        return EnvelopeError<VllmEventBatch>(
            "invalid vLLM data_parallel_rank: " + parsed_rank.error);
    }

    VllmEventBatch batch{.timestamp_seconds = timestamp->via.f64,
                         .events = {},
                         .data_parallel_rank = *parsed_rank.value};
    batch.events.reserve(events->via.array.size);
    for (uint32_t index = 0; index < events->via.array.size; ++index) {
        auto parsed = ParseVllmEvent(events->via.array.ptr[index]);
        if (parsed.value.has_value()) {
            batch.events.push_back(
                {.event = std::move(*parsed.value), .error = ""});
        } else {
            batch.events.push_back({.event = std::nullopt,
                                    .error = "event " + std::to_string(index) +
                                             ": " + parsed.error});
        }
    }
    return {.ok = true, .batch = std::move(batch), .error = ""};
}

MooncakeEventBatchResult DecodeMooncakeEventBatch(const char* data,
                                                  size_t len) {
    auto unpacked = UnpackOne(data, len);
    if (!unpacked.value.has_value()) {
        return EnvelopeError<MooncakeEventBatch>(
            "failed to decode Mooncake envelope: " + unpacked.error);
    }
    const object* timestamp = nullptr;
    const object* events = nullptr;
    const object* dp_rank = nullptr;
    std::string error;
    if (!ValidateEnvelopeShape(unpacked.value->get(), &timestamp, &events,
                               &dp_rank, &error)) {
        return EnvelopeError<MooncakeEventBatch>(error);
    }
    auto parsed_timestamp = ParseInt64(*timestamp);
    if (!parsed_timestamp.value.has_value() || *parsed_timestamp.value < 0) {
        return EnvelopeError<MooncakeEventBatch>(
            "Mooncake timestamp must be a non-negative integer");
    }
    auto parsed_rank = ParseBatchDpRank(*dp_rank);
    if (!parsed_rank.value.has_value()) {
        return EnvelopeError<MooncakeEventBatch>(
            "invalid Mooncake data_parallel_rank: " + parsed_rank.error);
    }

    MooncakeEventBatch batch{
        .timestamp_milliseconds = *parsed_timestamp.value,
        .events = {},
        .data_parallel_rank = *parsed_rank.value,
    };
    batch.events.reserve(events->via.array.size);
    for (uint32_t index = 0; index < events->via.array.size; ++index) {
        auto parsed = ParseMooncakeEvent(events->via.array.ptr[index],
                                         batch.timestamp_milliseconds);
        if (parsed.value.has_value()) {
            batch.events.push_back(
                {.event = std::move(*parsed.value), .error = ""});
        } else {
            batch.events.push_back({.event = std::nullopt,
                                    .error = "event " + std::to_string(index) +
                                             ": " + parsed.error});
        }
    }
    return {.ok = true, .batch = std::move(batch), .error = ""};
}

}  // namespace zmq
}  // namespace conductor
