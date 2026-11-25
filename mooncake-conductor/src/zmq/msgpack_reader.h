// Shared MessagePack map-reading helpers for the ZMQ event decoders.
//
// This is an internal header: it backs msg_decoder.cpp and its unit test, and
// is deliberately not published under include/conductor.  The helpers know
// nothing about publisher kinds, storage tiers, LoRA policy, object keys or
// index mutations; each engine decoder keeps its own field rules on top.
#pragma once

#include <msgpack.hpp>

#include <cstdint>
#include <map>
#include <optional>
#include <set>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace mooncake::conductor::zmq::detail {

using msgpack::object;
using msgpack::type::object_type;

// A parsed value or the reason it could not be parsed.  An empty `value` with
// an empty `error` is never produced by these helpers.
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

// MessagePack type name as it appears in decoder error messages.
std::string TypeName(const object& value);

// Indexes the recognized string keys of a MessagePack map.
//
// Unrecognized keys are skipped so publishers can add fields without breaking
// older consumers; a repeated *recognized* key is an error because the intended
// value would be ambiguous.  Views returned by Get() point into the object
// tree, so a reader and its results must not outlive the owning object_handle.
class MapReader {
   public:
    MapReader(const object& value,
              const std::set<std::string_view>& recognized_fields);

    const std::string& error() const { return error_; }

    // The value for `name`, or nullptr when the key was absent.  An explicit
    // nil is a present value: absence and nil stay distinguishable.
    const object* Get(std::string_view name) const;

   private:
    std::map<std::string, const object*> fields_;
    std::string error_;
};

ValueResult<std::string> ParseString(const object& value);
ValueResult<std::optional<std::string>> ParseNullableString(
    const object& value);
ValueResult<uint64_t> ParseUint64(const object& value);
ValueResult<std::optional<uint64_t>> ParseNullableUint64(const object& value);
ValueResult<int64_t> ParseInt64(const object& value);
ValueResult<std::optional<int64_t>> ParseNullableInt64(const object& value);

// Parses every element of a MessagePack array with `parser`, reporting the
// index of the first element that fails.
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

// Integer array narrowed to int32, rejecting values outside its range.
ValueResult<std::vector<int32_t>> ParseInt32Array(const object& value);
ValueResult<std::optional<std::vector<int32_t>>> ParseNullableInt32Array(
    const object& value);

// Reads a key that must be present.  A nullable field is still required when
// the publisher always emits it: nullable does not imply omittable.
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

// Reads a key that may be absent, leaving `output` empty in that case.  The
// parser still runs on an explicit nil, so a nil-rejecting parser reports it.
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

}  // namespace mooncake::conductor::zmq::detail
