#include "msgpack_reader.h"

#include <limits>

namespace mooncake::conductor::zmq::detail {

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

MapReader::MapReader(const object& value,
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

const object* MapReader::Get(std::string_view name) const {
    auto it = fields_.find(std::string(name));
    return it == fields_.end() ? nullptr : it->second;
}

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

}  // namespace mooncake::conductor::zmq::detail
