#include "conductor/zmq/msg_decoder.h"

#include <glog/logging.h>
#include <msgpack.hpp>

#include <charconv>
#include <cmath>
#include <cstdint>
#include <ctime>
#include <functional>
#include <sstream>
#include <string>
#include <vector>

namespace conductor {
namespace zmq {

namespace {

using msgpack::object;
using msgpack::type::object_type;

// ---------------------------------------------------------------------
// Small result helpers carrying either a value or an error string.

template <typename T>
struct Result {
    bool ok = false;
    T value{};
    std::string error;

    static Result Ok(T v) { return {true, std::move(v), ""}; }
    static Result Err(std::string e) { return {false, T{}, std::move(e)}; }
};

// A neutral type name for a msgpack-decoded value (msgpack-cxx erases
// integer width, so all integers report as "integer"). Only used in
// error/log text — tests match on stable keywords, not type names.
std::string MsgpackTypeName(const object& v) {
    switch (v.type) {
        case object_type::NIL:
            return "nil";
        case object_type::BOOLEAN:
            return "bool";
        case object_type::POSITIVE_INTEGER:
            return "integer";
        case object_type::NEGATIVE_INTEGER:
            return "integer";
        case object_type::FLOAT32:
            return "float";
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
            return "ext";
    }
    return "unknown";
}

// Shortest round-trip float formatting ('g' with minimal digits),
// suitable for the numeric-string fallback paths below.
std::string FormatFloatShortest(double f) {
    char buf[64];
    auto [ptr, ec] = std::to_chars(buf, buf + sizeof(buf), f);
    if (ec != std::errc()) return "?";
    return std::string(buf, ptr);
}

// Strict base-10 uint64 parse: digits only (no sign, no spaces).
Result<uint64_t> ParseUintStrict(const std::string& s) {
    if (s.empty()) {
        return Result<uint64_t>::Err("invalid syntax");
    }
    uint64_t out = 0;
    auto [ptr, ec] = std::from_chars(s.data(), s.data() + s.size(), out, 10);
    if (ec != std::errc() || ptr != s.data() + s.size()) {
        return Result<uint64_t>::Err("invalid syntax");
    }
    return Result<uint64_t>::Ok(out);
}

// ---------------------------------------------------------------------
// Lenient numeric conversion helpers.

Result<int64_t> ParseInt64(const object& v) {
    switch (v.type) {
        case object_type::POSITIVE_INTEGER:
            return Result<int64_t>::Ok(static_cast<int64_t>(v.via.u64));
        case object_type::NEGATIVE_INTEGER:
            return Result<int64_t>::Ok(v.via.i64);
        case object_type::FLOAT32:
        case object_type::FLOAT64:
            return Result<int64_t>::Ok(static_cast<int64_t>(v.via.f64));
        default:
            return Result<int64_t>::Err("unsupported integer type: " +
                                        MsgpackTypeName(v));
    }
}

Result<uint64_t> ParseUint64(const object& v) {
    switch (v.type) {
        case object_type::POSITIVE_INTEGER:
            return Result<uint64_t>::Ok(v.via.u64);
        case object_type::NEGATIVE_INTEGER:
            // Cast: int64 to uint64 wraps two's-complement.
            return Result<uint64_t>::Ok(static_cast<uint64_t>(v.via.i64));
        case object_type::FLOAT32:
        case object_type::FLOAT64: {
            const double f = v.via.f64;
            if (f < 0) {
                // Negative floats wrap through the signed conversion
                // (fixed conversion contract).
                return Result<uint64_t>::Ok(
                    static_cast<uint64_t>(static_cast<int64_t>(f)));
            }
            return Result<uint64_t>::Ok(static_cast<uint64_t>(f));
        }
        default:
            return Result<uint64_t>::Err("unsupported integer type: " +
                                         MsgpackTypeName(v));
    }
}

Result<std::vector<uint64_t>> ParseUint64Array(const object& v) {
    if (v.type != object_type::ARRAY) {
        return Result<std::vector<uint64_t>>::Err("expected array, got " +
                                                  MsgpackTypeName(v));
    }
    std::vector<uint64_t> result;
    result.reserve(v.via.array.size);
    for (uint32_t i = 0; i < v.via.array.size; ++i) {
        auto item = ParseUint64(v.via.array.ptr[i]);
        if (!item.ok) {
            return Result<std::vector<uint64_t>>::Err(
                "failed to parse element at index " + std::to_string(i) + ": " +
                item.error);
        }
        result.push_back(item.value);
    }
    return Result<std::vector<uint64_t>>::Ok(std::move(result));
}

Result<std::vector<int32_t>> ParseInt32Array(const object& v) {
    if (v.type != object_type::ARRAY) {
        return Result<std::vector<int32_t>>::Err("expected array, got " +
                                                 MsgpackTypeName(v));
    }
    std::vector<int32_t> result;
    result.reserve(v.via.array.size);
    for (uint32_t i = 0; i < v.via.array.size; ++i) {
        const object& item = v.via.array.ptr[i];
        switch (item.type) {
            case object_type::POSITIVE_INTEGER:
                // int32(uint64) truncates to the low 32 bits.
                result.push_back(
                    static_cast<int32_t>(static_cast<uint32_t>(item.via.u64)));
                break;
            case object_type::NEGATIVE_INTEGER:
                result.push_back(
                    static_cast<int32_t>(static_cast<uint32_t>(item.via.i64)));
                break;
            case object_type::FLOAT32:
            case object_type::FLOAT64:
                result.push_back(
                    static_cast<int32_t>(static_cast<int64_t>(item.via.f64)));
                break;
            default:
                return Result<std::vector<int32_t>>::Err(
                    "unsupported integer type at index " + std::to_string(i) +
                    ": " + MsgpackTypeName(item));
        }
    }
    return Result<std::vector<int32_t>>::Ok(std::move(result));
}

// SafeGetString never returns an error — every branch produces some
// string (with a warning for unexpected types).
std::string SafeGetString(const object& v) {
    switch (v.type) {
        case object_type::STR:
            return std::string(v.via.str.ptr, v.via.str.size);
        case object_type::BIN:
            return std::string(v.via.bin.ptr, v.via.bin.size);
        case object_type::POSITIVE_INTEGER:
            return std::to_string(v.via.u64);
        case object_type::NEGATIVE_INTEGER:
            return std::to_string(v.via.i64);
        case object_type::FLOAT32:
        case object_type::FLOAT64:
            return FormatFloatShortest(v.via.f64);
        case object_type::NIL:
            return "";
        default:
            LOG(WARNING) << "Unexpected type in string field type="
                         << MsgpackTypeName(v);
            if (v.type == object_type::BOOLEAN) {
                return v.via.boolean ? "true" : "false";
            }
            // Fall back to a generic stream stringification;
            // arrays/maps never occur in string fields with real
            // publishers.
            std::ostringstream oss;
            oss << v;
            return oss.str();
    }
}

// Timestamp in unix microseconds (float timestamps are truncated to
// microsecond precision).
Result<int64_t> ParseTimestampMicro(const object& v) {
    switch (v.type) {
        case object_type::EXT: {
            // msgpack timestamp extension (type -1) — decoded as a
            // point in time.
            if (v.via.ext.type() != -1) {
                return Result<int64_t>::Err("unsupported timestamp type: ext");
            }
            const char* p = v.via.ext.data();
            const uint32_t size = v.via.ext.size;
            auto be32 = [](const char* b) {
                return (uint32_t(uint8_t(b[0])) << 24) |
                       (uint32_t(uint8_t(b[1])) << 16) |
                       (uint32_t(uint8_t(b[2])) << 8) | uint32_t(uint8_t(b[3]));
            };
            int64_t sec = 0;
            int64_t nsec = 0;
            if (size == 4) {
                sec = be32(p);
            } else if (size == 8) {
                const uint64_t data64 =
                    (uint64_t(be32(p)) << 32) | uint64_t(be32(p + 4));
                nsec = static_cast<int64_t>(data64 >> 34);
                sec = static_cast<int64_t>(data64 & 0x3FFFFFFFFULL);
            } else if (size == 12) {
                nsec = be32(p);
                sec = static_cast<int64_t>((uint64_t(be32(p + 4)) << 32) |
                                           uint64_t(be32(p + 8)));
            } else {
                return Result<int64_t>::Err("unsupported timestamp type: ext");
            }
            return Result<int64_t>::Ok(sec * 1000000 + nsec / 1000);
        }
        case object_type::POSITIVE_INTEGER:
            return Result<int64_t>::Ok(static_cast<int64_t>(v.via.u64) *
                                       1000000);
        case object_type::NEGATIVE_INTEGER:
            return Result<int64_t>::Ok(v.via.i64 * 1000000);
        case object_type::FLOAT32:
        case object_type::FLOAT64: {
            const double t = v.via.f64;
            const int64_t sec = static_cast<int64_t>(t);
            const int64_t nsec =
                static_cast<int64_t>((t - static_cast<double>(sec)) * 1e9);
            // Unix timestamp (sec, nsec) truncated to microseconds.
            return Result<int64_t>::Ok(sec * 1000000 + nsec / 1000);
        }
        case object_type::STR: {
            // Minimal RFC3339 timestamp parsing:
            // "2006-01-02T15:04:05Z" / "±hh:mm" offsets, optional
            // fractional seconds.
            const std::string s(v.via.str.ptr, v.via.str.size);
            std::tm tm{};
            const char* rest = strptime(s.c_str(), "%Y-%m-%dT%H:%M:%S", &tm);
            if (rest == nullptr) {
                return Result<int64_t>::Err("cannot parse timestamp \"" + s +
                                            "\"");
            }
            int64_t micro = 0;
            if (*rest == '.') {
                ++rest;
                int64_t frac = 0;
                int digits = 0;
                while (*rest >= '0' && *rest <= '9') {
                    if (digits < 6) {
                        frac = frac * 10 + (*rest - '0');
                        ++digits;
                    }
                    ++rest;
                }
                while (digits < 6) {
                    frac *= 10;
                    ++digits;
                }
                micro = frac;
            }
            int64_t offset_sec = 0;
            if (*rest == 'Z' || *rest == 'z') {
                // UTC
            } else if (*rest == '+' || *rest == '-') {
                const int sign = (*rest == '-') ? -1 : 1;
                int hh = 0, mm = 0;
                if (sscanf(rest + 1, "%2d:%2d", &hh, &mm) != 2) {
                    return Result<int64_t>::Err("cannot parse timestamp \"" +
                                                s + "\"");
                }
                offset_sec = sign * (hh * 3600 + mm * 60);
            } else {
                return Result<int64_t>::Err("cannot parse timestamp \"" + s +
                                            "\"");
            }
            const int64_t sec = timegm(&tm) - offset_sec;
            return Result<int64_t>::Ok(sec * 1000000 + micro);
        }
        default:
            return Result<int64_t>::Err("unsupported timestamp type: " +
                                        MsgpackTypeName(v));
    }
}

// Strict scalar conversion used for Mooncake block hash array elements.
// Negatives and non-integral floats error.
Result<uint64_t> ParseSingleUint64(const object& v) {
    switch (v.type) {
        case object_type::POSITIVE_INTEGER:
            return Result<uint64_t>::Ok(v.via.u64);
        case object_type::NEGATIVE_INTEGER:
            return Result<uint64_t>::Err("negative value " +
                                         std::to_string(v.via.i64) +
                                         " cannot convert to unsigned integer");
        case object_type::FLOAT32:
        case object_type::FLOAT64: {
            const double f = v.via.f64;
            if (f < 0 || f != static_cast<double>(static_cast<uint64_t>(f))) {
                return Result<uint64_t>::Err("float " + FormatFloatShortest(f) +
                                             " invalid for unsigned integer");
            }
            return Result<uint64_t>::Ok(static_cast<uint64_t>(f));
        }
        case object_type::STR: {
            const std::string s(v.via.str.ptr, v.via.str.size);
            if (s.empty()) {
                return Result<uint64_t>::Err(
                    "empty string cannot be parsed as integer");
            }
            auto parsed = ParseUintStrict(s);
            if (!parsed.ok) {
                return Result<uint64_t>::Err("invalid integer string \"" + s +
                                             "\"");
            }
            return parsed;
        }
        case object_type::NIL:
            return Result<uint64_t>::Err("nil cannot be parsed as integer");
        default:
            return Result<uint64_t>::Err("unsupported type " +
                                         MsgpackTypeName(v) +
                                         " for integer conversion");
    }
}

// Parent-hash field of BlockStoreEvent. Accepts unsigned ints,
// non-negative signed ints, nil (-> 0), decimal strings, and (via a
// generic stringify fallback) integral floats.
Result<uint64_t> ParseMooncakeUint64(const object& v) {
    std::string s;
    switch (v.type) {
        case object_type::STR:
            s = std::string(v.via.str.ptr, v.via.str.size);
            break;
        case object_type::NIL:
            s = "";
            break;
        case object_type::POSITIVE_INTEGER:
            return Result<uint64_t>::Ok(v.via.u64);
        case object_type::NEGATIVE_INTEGER:
            return Result<uint64_t>::Err("negative value " +
                                         std::to_string(v.via.i64));
        default:
            // Default branch: stringify the value then parse as uint.
            // Integral floats whose shortest formatting is plain digits
            // parse fine; "1e+06", "1.5", "true", "[...]" all fail.
            if (v.type == object_type::FLOAT32 ||
                v.type == object_type::FLOAT64) {
                s = FormatFloatShortest(v.via.f64);
            } else if (v.type == object_type::BOOLEAN) {
                s = v.via.boolean ? "true" : "false";
            } else {
                std::ostringstream oss;
                oss << v;
                s = oss.str();
            }
            break;
    }
    if (s.empty()) {
        return Result<uint64_t>::Ok(0);
    }
    auto parsed = ParseUintStrict(s);
    if (!parsed.ok) {
        return Result<uint64_t>::Err("invalid integer string \"" + s + "\"");
    }
    return parsed;
}

// Block-hashes field of BlockStoreEvent. Accepts nil,
// comma/space-separated decimal strings, arrays of scalars, or a bare
// scalar.
Result<std::vector<uint64_t>> ParseMooncakeUint64List(const object& v) {
    using R = Result<std::vector<uint64_t>>;
    switch (v.type) {
        case object_type::NIL:
            return R::Ok({});
        case object_type::STR: {
            const std::string s(v.via.str.ptr, v.via.str.size);
            if (s.empty()) {
                return R::Ok({});
            }
            std::vector<uint64_t> result;
            std::string part;
            auto flush = [&]() -> std::string {
                if (part.empty()) return "";
                auto parsed = ParseUintStrict(part);
                if (!parsed.ok) {
                    return "failed to parse integer from string \"" + part +
                           "\": invalid integer string \"" + part + "\"";
                }
                result.push_back(parsed.value);
                part.clear();
                return "";
            };
            for (const char c : s) {
                if (c == ',' || c == ' ' || c == '\t' || c == '\n') {
                    if (auto err = flush(); !err.empty()) return R::Err(err);
                } else {
                    part.push_back(c);
                }
            }
            if (auto err = flush(); !err.empty()) return R::Err(err);
            return R::Ok(std::move(result));
        }
        case object_type::ARRAY: {
            std::vector<uint64_t> result;
            result.reserve(v.via.array.size);
            for (uint32_t i = 0; i < v.via.array.size; ++i) {
                auto item = ParseSingleUint64(v.via.array.ptr[i]);
                if (!item.ok) {
                    return R::Err("failed to parse element: " + item.error);
                }
                result.push_back(item.value);
            }
            return R::Ok(std::move(result));
        }
        default: {
            // try parse as single uint64
            auto single = ParseSingleUint64(v);
            if (!single.ok) {
                return R::Err(single.error);
            }
            return R::Ok({single.value});
        }
    }
}

Result<std::vector<std::vector<std::string>>> ConvertToReplicaList(
    const object& v) {
    using R = Result<std::vector<std::vector<std::string>>>;
    if (v.type != object_type::ARRAY) {
        return R::Err("expected array, got " + MsgpackTypeName(v));
    }
    std::vector<std::vector<std::string>> result;
    result.reserve(v.via.array.size);
    for (uint32_t i = 0; i < v.via.array.size; ++i) {
        const object& item = v.via.array.ptr[i];
        if (item.type != object_type::ARRAY) {
            return R::Err("item " + std::to_string(i) +
                          " is not an array, got " + MsgpackTypeName(item));
        }
        std::vector<std::string> sub;
        sub.reserve(item.via.array.size);
        for (uint32_t j = 0; j < item.via.array.size; ++j) {
            const object& elem = item.via.array.ptr[j];
            if (elem.type != object_type::STR) {
                return R::Err("element [" + std::to_string(i) + "][" +
                              std::to_string(j) + "] is not string, got " +
                              MsgpackTypeName(elem));
            }
            sub.emplace_back(elem.via.str.ptr, elem.via.str.size);
        }
        result.push_back(std::move(sub));
    }
    return R::Ok(std::move(result));
}

// ---------------------------------------------------------------------
// Event parsers.

// Bounds guard for fixed-index array access: when an event array is
// shorter than the schema requires, we return a decode error instead of
// reading out of range.
bool CheckLen(const object& arr, uint32_t need, const char* what,
              std::string* err) {
    if (arr.via.array.size < need) {
        *err = std::string("event array too short for ") + what + ": need " +
               std::to_string(need) + " elements, got " +
               std::to_string(arr.via.array.size) +
               " (returns error; caller handles)";
        return false;
    }
    return true;
}

Result<KVEvent> ParseVllmBlockStored(const object& data,
                                     const object& timestamp) {
    using R = Result<KVEvent>;
    BlockStoredEvent event;
    event.type = kEventTypeBlockStored;

    std::string len_err;
    if (!CheckLen(data, 7, "vLLM BlockStored", &len_err)) {
        return R::Err(len_err);
    }
    const object* f = data.via.array.ptr;

    // Field order below is fixed so the *first* error surfaced for a
    // multi-error payload is deterministic.
    auto ts = ParseTimestampMicro(timestamp);
    if (!ts.ok) {
        return R::Err("failed to parse timestamp: " + ts.error);
    }
    event.timestamp_unix_micro = ts.value;

    auto hashes = ParseUint64Array(f[1]);
    if (!hashes.ok) {
        return R::Err("failed to parse block_hashes: " + hashes.error);
    }
    event.block_hashes = std::move(hashes.value);

    if (f[3].type == object_type::ARRAY) {
        auto tokens = ParseInt32Array(f[3]);
        if (!tokens.ok) {
            return R::Err("failed to parse token_ids at index " + tokens.error);
        }
        event.token_ids = std::move(tokens.value);
    } else {
        return R::Err("missing or invalid token_ids");
    }

    if (f[2].type == object_type::NIL) {
        event.parent_block_hash = 0;
    } else if (f[2].type == object_type::POSITIVE_INTEGER &&
               f[2].via.u64 > 0xFFFFFFFFULL) {
        // Require a full 8-byte integer marker: minimal-width encoders
        // only emit this width for values above 2**32.
        event.parent_block_hash = f[2].via.u64;
    } else {
        return R::Err("expected integer, got " + MsgpackTypeName(f[2]));
    }

    auto block_size = ParseInt64(f[4]);
    if (!block_size.ok) {
        return R::Err("failed to parse field at index 4 as 'block_size': " +
                      block_size.error);
    }
    event.block_size = block_size.value;

    // f[5] is lora_id in the vLLM schema; it is intentionally skipped.
    event.medium = SafeGetString(f[6]);

    return R::Ok(std::move(event));
}

Result<KVEvent> ParseVllmBlockRemoved(const object& data) {
    using R = Result<KVEvent>;
    BlockRemovedEvent event;
    event.type = kEventTypeBlockRemoved;

    std::string len_err;
    if (!CheckLen(data, 3, "vLLM BlockRemoved", &len_err)) {
        return R::Err(len_err);
    }
    const object* f = data.via.array.ptr;

    auto hashes = ParseUint64Array(f[1]);
    if (!hashes.ok) {
        return R::Err("failed to parse block_hashes: " + hashes.error);
    }
    event.block_hashes = std::move(hashes.value);

    event.medium = SafeGetString(f[2]);
    // NOTE: no timestamp assigned for removal events.
    return R::Ok(std::move(event));
}

Result<KVEvent> ParseMooncakeBlockStored(const object& data,
                                         const object& /*timestamp*/) {
    // NOTE: batch timestamp ignored for removal event items.
    using R = Result<KVEvent>;
    BlockStoredEvent event;
    event.type = kEventTypeBlockStored;

    std::string len_err;
    if (!CheckLen(data, 8, "Mooncake BlockStoreEvent", &len_err)) {
        return R::Err(len_err);
    }
    const object* f = data.via.array.ptr;

    event.mooncake_key = SafeGetString(f[1]);

    auto replicas = ConvertToReplicaList(f[2]);
    if (!replicas.ok) {
        return R::Err("failed to parse ReplicaList from field at index 2: " +
                      replicas.error);
    }
    event.replica_list = std::move(replicas.value);

    auto block_size = ParseInt64(f[4]);
    if (!block_size.ok) {
        return R::Err("failed to parse BlockSize from field at index 4: " +
                      block_size.error);
    }
    event.block_size = block_size.value;

    auto hashes = ParseMooncakeUint64List(f[5]);
    if (!hashes.ok) {
        return R::Err("failed to parse BlockHashes from field at index 5: " +
                      hashes.error);
    }
    event.block_hashes = std::move(hashes.value);

    auto parent = ParseMooncakeUint64(f[6]);
    if (!parent.ok) {
        return R::Err(
            "failed to parse ParentBlockHash from field at index 6: " +
            parent.error);
    }
    event.parent_block_hash = parent.value;

    if (f[7].type == object_type::ARRAY) {
        auto tokens = ParseInt32Array(f[7]);
        if (!tokens.ok) {
            return R::Err("failed to parse TokenIDs from field at index 7: " +
                          tokens.error);
        }
        event.token_ids = std::move(tokens.value);
    } else {
        return R::Err("missing or invalid token_ids");
    }

    // BUG: Medium never assigned for Mooncake-sourced blocks; queries always
    // miss.
    return R::Ok(std::move(event));
}

// ---------------------------------------------------------------------
// Parser dispatch — one implementation per publisher schema.

struct ParserSpec {
    const char* source;
    // Maps event-name string to the EventType string; nullptr = unknown.
    const char* (*map_event)(const std::string&);
    Result<KVEvent> (*parse)(const std::string& event_type, const object& raw,
                             const object& timestamp);
    const char* unknown_fmt;  // "unknown vllm event type: " etc.
};

const char* MooncakeEventMapping(const std::string& name) {
    if (name == "BlockStoreEvent") return kEventTypeBlockStored;
    if (name == "BlockUpdateEvent") return kEventTypeBlockUpdate;
    if (name == "RemoveAllEvent") return kEventTypeAllCleared;
    return nullptr;
}

Result<KVEvent> MooncakeParse(const std::string& event_type, const object& raw,
                              const object& timestamp) {
    if (event_type == kEventTypeBlockStored) {
        return ParseMooncakeBlockStored(raw, timestamp);
    }
    // BUG: BlockUpdateEvent / RemoveAllEvent not handled (dead code on main
    // branch — schema pending upstream alignment, see docs/KNOWN_ISSUES.md
    // C.9).
    return Result<KVEvent>::Err("unhandled event: " + event_type);
}

const char* VllmEventMapping(const std::string& name) {
    if (name == "BlockStored") return kEventTypeBlockStored;
    if (name == "BlockRemoved") return kEventTypeBlockRemoved;
    if (name == "AllBlocksCleared") return kEventTypeAllCleared;
    return nullptr;
}

Result<KVEvent> VllmParse(const std::string& event_type, const object& raw,
                          const object& timestamp) {
    if (event_type == kEventTypeBlockStored) {
        return ParseVllmBlockStored(raw, timestamp);
    }
    if (event_type == kEventTypeBlockRemoved) {
        return ParseVllmBlockRemoved(raw);
    }
    return Result<KVEvent>::Err("unhandled event: " + event_type);
}

EventBatchResult DecodeCommonEventBatch(const char* data, size_t len,
                                        uint32_t expected_length,
                                        const ParserSpec& parser) {
    EventBatchResult result;

    if (len > 0) {
        VLOG(1) << "First byte of payload hex="
                << static_cast<int>(static_cast<unsigned char>(data[0]));
    }

    msgpack::object_handle oh;
    try {
        oh = msgpack::unpack(data, len);
    } catch (const std::exception& e) {
        result.error = std::string("failed to decode event batch: ") + e.what();
        return result;
    }
    const object& root = oh.get();
    if (root.type != object_type::ARRAY) {
        // The event batch must be an array; non-arrays are rejected.
        result.error =
            "failed to decode event batch: msgpack data is not "
            "an array (got " +
            MsgpackTypeName(root) + ")";
        return result;
    }

    if (root.via.array.size != expected_length) {
        result.error = "expected " + std::to_string(expected_length) +
                       "-element array, got " +
                       std::to_string(root.via.array.size);
        return result;
    }

    const object& timestamp = root.via.array.ptr[0];
    const object& events_obj = root.via.array.ptr[1];
    if (events_obj.type != object_type::ARRAY) {
        result.error = "invalid events type: " + MsgpackTypeName(events_obj);
        return result;
    }

    if (events_obj.via.array.size == 0) {
        LOG(WARNING) << "Received empty event list";
    }

    int64_t dp_rank = -1;
    if (expected_length == 3) {
        auto parsed = ParseInt64(root.via.array.ptr[2]);
        if (!parsed.ok) {
            result.error = "failed to parse dpRank: " + parsed.error;
            return result;
        }
        dp_rank = parsed.value;
    }

    result.batch.source = parser.source;
    result.batch.data_parallel_rank = dp_rank;
    result.batch.events.reserve(events_obj.via.array.size);
    LOG(INFO) << "Receive batched kv-event source=" << parser.source
              << " dpRank=" << dp_rank;

    for (uint32_t i = 0; i < events_obj.via.array.size; ++i) {
        const object& raw_event = events_obj.via.array.ptr[i];
        if (raw_event.type != object_type::ARRAY) {
            result.error = "event at index " + std::to_string(i) +
                           " is not an array: " + MsgpackTypeName(raw_event);
            return result;
        }
        if (raw_event.via.array.size == 0 ||
            raw_event.via.array.ptr[0].type != object_type::STR) {
            result.error = "failed to parse event at index " +
                           std::to_string(i) + ": invalid event type format: " +
                           (raw_event.via.array.size == 0
                                ? "<empty>"
                                : MsgpackTypeName(raw_event.via.array.ptr[0]));
            return result;
        }
        const object& name_obj = raw_event.via.array.ptr[0];
        const std::string name(name_obj.via.str.ptr, name_obj.via.str.size);

        const char* event_type = parser.map_event(name);
        if (event_type == nullptr) {
            result.error = "failed to parse event at index " +
                           std::to_string(i) + ": " + parser.unknown_fmt + name;
            return result;
        }

        auto parsed = parser.parse(event_type, raw_event, timestamp);
        if (!parsed.ok) {
            result.error = "failed to parse event at index " +
                           std::to_string(i) + ": " + parsed.error;
            return result;
        }
        result.batch.events.push_back(std::move(parsed.value));
    }

    result.ok = true;
    return result;
}

}  // namespace

EventBatchResult DecodeMooncakeEventBatch(const char* data, size_t len) {
    static const ParserSpec kMooncakeParser = {
        kSourceMooncake, MooncakeEventMapping, MooncakeParse,
        "unknown mooncake event type: "};
    return DecodeCommonEventBatch(data, len, 2, kMooncakeParser);
}

EventBatchResult DecodeVllmEventBatch(const char* data, size_t len) {
    static const ParserSpec kVllmParser = {
        kSourceVLLM, VllmEventMapping, VllmParse, "unknown vllm event type: "};
    return DecodeCommonEventBatch(data, len, 3, kVllmParser);
}

}  // namespace zmq
}  // namespace conductor
