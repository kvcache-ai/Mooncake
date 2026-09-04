#pragma once

#include <array>
#include <cstddef>
#include <cstdint>
#include <span>
#include <string>
#include <string_view>

#include "ylt/util/tl/expected.hpp"

namespace mooncake::logstructured {

enum class RecordKind : uint16_t {
    kValue = 1,
    kTombstone = 2,
    kCompactionCopy = 3,
};

enum class DecodeError {
    kNeedMoreData,
    kInvalidMagic,
    kUnsupportedVersion,
    kInvalidFlags,
    kInvalidLength,
    kHeaderChecksumMismatch,
    kPayloadChecksumMismatch,
    kFooterMismatch,
};

struct ObjectIncarnation {
    uint64_t high{0};
    uint64_t low{0};

    bool operator==(const ObjectIncarnation&) const = default;
};

struct RecordIdentity {
    std::string tenant_id;
    std::string object_key;
    ObjectIncarnation incarnation;

    bool operator==(const RecordIdentity&) const = default;
};

struct RecordHeader {
    RecordKind kind{RecordKind::kValue};
    uint64_t total_length{0};
    uint64_t sequence{0};
    ObjectIncarnation incarnation;
    uint32_t tenant_length{0};
    uint32_t key_length{0};
    uint64_t value_length{0};
};

struct EncodedRecordEnvelope {
    std::array<char, 64> header{};
    std::array<char, 24> footer{};
    uint64_t total_length{0};
    uint64_t padding_length{0};
};

struct DecodedRecord {
    RecordIdentity identity;
    RecordKind kind{RecordKind::kValue};
    uint64_t sequence{0};
    std::string value;
    uint64_t total_length{0};
};

inline constexpr uint32_t kRecordMagic = 0x474C434D;
inline constexpr uint64_t kRecordCommitMagic = 0x54494D4D4F43434DULL;
inline constexpr uint16_t kRecordFormatVersion = 1;
inline constexpr size_t kRecordHeaderSize = 64;
inline constexpr size_t kRecordFooterSize = 24;
inline constexpr size_t kRecordAlignment = 8;
inline constexpr uint32_t kMaxTenantLength = 1U << 20;
inline constexpr uint32_t kMaxKeyLength = 16U << 20;

uint64_t AlignedRecordSize(uint32_t tenant_length, uint32_t key_length,
                           uint64_t value_length);

tl::expected<EncodedRecordEnvelope, DecodeError> EncodeRecordEnvelope(
    const RecordIdentity& identity, std::string_view value, RecordKind kind,
    uint64_t sequence);

tl::expected<std::string, DecodeError> EncodeRecord(
    const RecordIdentity& identity, std::string_view value, RecordKind kind,
    uint64_t sequence);

tl::expected<RecordHeader, DecodeError> DecodeRecordHeader(
    std::span<const char> bytes);

tl::expected<DecodedRecord, DecodeError> DecodeRecord(
    std::span<const char> bytes);

}  // namespace mooncake::logstructured
