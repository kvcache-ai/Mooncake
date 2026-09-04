#include "storage/local/log_structured/record_format.h"

#include <array>
#include <cstring>
#include <limits>
#include <type_traits>

#include "crc32c.h"

namespace mooncake::logstructured {
namespace {

constexpr size_t kMagicOffset = 0;
constexpr size_t kVersionOffset = 4;
constexpr size_t kKindOffset = 6;
constexpr size_t kTotalLengthOffset = 8;
constexpr size_t kSequenceOffset = 16;
constexpr size_t kIncarnationHighOffset = 24;
constexpr size_t kIncarnationLowOffset = 32;
constexpr size_t kTenantLengthOffset = 40;
constexpr size_t kKeyLengthOffset = 44;
constexpr size_t kValueLengthOffset = 48;
constexpr size_t kHeaderChecksumOffset = 56;
constexpr size_t kHeaderReservedOffset = 60;
constexpr size_t kHeaderChecksumInputSize = kHeaderChecksumOffset;

constexpr size_t kFooterLengthOffset = 0;
constexpr size_t kFooterPayloadChecksumOffset = 8;
constexpr size_t kFooterChecksumOffset = 12;
constexpr size_t kFooterMagicOffset = 16;
constexpr size_t kFooterChecksumInputSize = kFooterChecksumOffset;

template <typename T>
void WriteLittleEndian(char* output, T value) {
    using Unsigned = std::make_unsigned_t<T>;
    Unsigned unsigned_value = static_cast<Unsigned>(value);
    for (size_t i = 0; i < sizeof(T); ++i) {
        output[i] = static_cast<char>((unsigned_value >> (i * 8)) & 0xff);
    }
}

template <typename T>
T ReadLittleEndian(const char* input) {
    using Unsigned = std::make_unsigned_t<T>;
    Unsigned value = 0;
    for (size_t i = 0; i < sizeof(T); ++i) {
        value |= static_cast<Unsigned>(static_cast<unsigned char>(input[i]))
                 << (i * 8);
    }
    return static_cast<T>(value);
}

bool IsKnownKind(RecordKind kind) {
    return kind == RecordKind::kValue || kind == RecordKind::kTombstone ||
           kind == RecordKind::kCompactionCopy;
}

tl::expected<uint64_t, DecodeError> CheckedRecordSize(uint32_t tenant_length,
                                                      uint32_t key_length,
                                                      uint64_t value_length) {
    constexpr uint64_t kFixedSize = kRecordHeaderSize + kRecordFooterSize;
    if (value_length > std::numeric_limits<uint64_t>::max() - kFixedSize ||
        tenant_length >
            std::numeric_limits<uint64_t>::max() - kFixedSize - value_length ||
        key_length > std::numeric_limits<uint64_t>::max() - kFixedSize -
                         value_length - tenant_length) {
        return tl::unexpected(DecodeError::kInvalidLength);
    }
    const uint64_t unaligned =
        kRecordHeaderSize + tenant_length + key_length + value_length;
    if (unaligned > std::numeric_limits<uint64_t>::max() -
                        (kRecordAlignment - 1) - kRecordFooterSize) {
        return tl::unexpected(DecodeError::kInvalidLength);
    }
    const uint64_t aligned =
        (unaligned + kRecordAlignment - 1) & ~(kRecordAlignment - 1);
    return aligned + kRecordFooterSize;
}

}  // namespace

uint64_t AlignedRecordSize(uint32_t tenant_length, uint32_t key_length,
                           uint64_t value_length) {
    auto result = CheckedRecordSize(tenant_length, key_length, value_length);
    return result ? result.value() : 0;
}

tl::expected<EncodedRecordEnvelope, DecodeError> EncodeRecordEnvelope(
    const RecordIdentity& identity, std::string_view value, RecordKind kind,
    uint64_t sequence) {
    if (!IsKnownKind(kind)) {
        return tl::unexpected(DecodeError::kInvalidFlags);
    }
    if (identity.tenant_id.size() > kMaxTenantLength ||
        identity.object_key.size() > kMaxKeyLength ||
        (kind == RecordKind::kTombstone && !value.empty())) {
        return tl::unexpected(DecodeError::kInvalidLength);
    }

    const uint32_t tenant_length =
        static_cast<uint32_t>(identity.tenant_id.size());
    const uint32_t key_length =
        static_cast<uint32_t>(identity.object_key.size());
    auto size_result =
        CheckedRecordSize(tenant_length, key_length, value.size());
    if (!size_result) {
        return tl::unexpected(size_result.error());
    }

    EncodedRecordEnvelope envelope;
    envelope.total_length = size_result.value();
    envelope.padding_length =
        envelope.total_length - kRecordHeaderSize - identity.tenant_id.size() -
        identity.object_key.size() - value.size() - kRecordFooterSize;

    char* header = envelope.header.data();
    WriteLittleEndian<uint32_t>(header + kMagicOffset, kRecordMagic);
    WriteLittleEndian<uint16_t>(header + kVersionOffset, kRecordFormatVersion);
    WriteLittleEndian<uint16_t>(header + kKindOffset,
                                static_cast<uint16_t>(kind));
    WriteLittleEndian<uint64_t>(header + kTotalLengthOffset,
                                envelope.total_length);
    WriteLittleEndian<uint64_t>(header + kSequenceOffset, sequence);
    WriteLittleEndian<uint64_t>(header + kIncarnationHighOffset,
                                identity.incarnation.high);
    WriteLittleEndian<uint64_t>(header + kIncarnationLowOffset,
                                identity.incarnation.low);
    WriteLittleEndian<uint32_t>(header + kTenantLengthOffset, tenant_length);
    WriteLittleEndian<uint32_t>(header + kKeyLengthOffset, key_length);
    WriteLittleEndian<uint64_t>(header + kValueLengthOffset, value.size());
    WriteLittleEndian<uint32_t>(header + kHeaderChecksumOffset,
                                Crc32cValue(header, kHeaderChecksumInputSize));
    WriteLittleEndian<uint32_t>(header + kHeaderReservedOffset, 0);

    Crc32c payload_crc;
    payload_crc.Extend(identity.tenant_id.data(), identity.tenant_id.size());
    payload_crc.Extend(identity.object_key.data(), identity.object_key.size());
    payload_crc.Extend(value.data(), value.size());

    char* footer = envelope.footer.data();
    WriteLittleEndian<uint64_t>(footer + kFooterLengthOffset,
                                envelope.total_length);
    WriteLittleEndian<uint32_t>(footer + kFooterPayloadChecksumOffset,
                                payload_crc.Final());
    WriteLittleEndian<uint32_t>(footer + kFooterChecksumOffset,
                                Crc32cValue(footer, kFooterChecksumInputSize));
    WriteLittleEndian<uint64_t>(footer + kFooterMagicOffset,
                                kRecordCommitMagic);
    return envelope;
}

tl::expected<std::string, DecodeError> EncodeRecord(
    const RecordIdentity& identity, std::string_view value, RecordKind kind,
    uint64_t sequence) {
    auto envelope = EncodeRecordEnvelope(identity, value, kind, sequence);
    if (!envelope) {
        return tl::unexpected(envelope.error());
    }
    if (envelope->total_length >
        static_cast<uint64_t>(std::numeric_limits<size_t>::max())) {
        return tl::unexpected(DecodeError::kInvalidLength);
    }

    std::string encoded(static_cast<size_t>(envelope->total_length), '\0');
    size_t cursor = 0;
    std::memcpy(encoded.data() + cursor, envelope->header.data(),
                envelope->header.size());
    cursor += envelope->header.size();
    std::memcpy(encoded.data() + cursor, identity.tenant_id.data(),
                identity.tenant_id.size());
    cursor += identity.tenant_id.size();
    std::memcpy(encoded.data() + cursor, identity.object_key.data(),
                identity.object_key.size());
    cursor += identity.object_key.size();
    std::memcpy(encoded.data() + cursor, value.data(), value.size());
    std::memcpy(encoded.data() + encoded.size() - envelope->footer.size(),
                envelope->footer.data(), envelope->footer.size());
    return encoded;
}

tl::expected<RecordHeader, DecodeError> DecodeRecordHeader(
    std::span<const char> bytes) {
    if (bytes.size() < kRecordHeaderSize) {
        return tl::unexpected(DecodeError::kNeedMoreData);
    }
    if (ReadLittleEndian<uint32_t>(bytes.data() + kMagicOffset) !=
        kRecordMagic) {
        return tl::unexpected(DecodeError::kInvalidMagic);
    }
    if (ReadLittleEndian<uint16_t>(bytes.data() + kVersionOffset) !=
        kRecordFormatVersion) {
        return tl::unexpected(DecodeError::kUnsupportedVersion);
    }

    const auto kind = static_cast<RecordKind>(
        ReadLittleEndian<uint16_t>(bytes.data() + kKindOffset));
    if (!IsKnownKind(kind)) {
        return tl::unexpected(DecodeError::kInvalidFlags);
    }
    const uint32_t expected_header_checksum =
        ReadLittleEndian<uint32_t>(bytes.data() + kHeaderChecksumOffset);
    if (Crc32cValue(bytes.data(), kHeaderChecksumInputSize) !=
        expected_header_checksum) {
        return tl::unexpected(DecodeError::kHeaderChecksumMismatch);
    }
    if (ReadLittleEndian<uint32_t>(bytes.data() + kHeaderReservedOffset) != 0) {
        return tl::unexpected(DecodeError::kInvalidFlags);
    }

    RecordHeader header;
    header.kind = kind;
    header.total_length =
        ReadLittleEndian<uint64_t>(bytes.data() + kTotalLengthOffset);
    header.sequence =
        ReadLittleEndian<uint64_t>(bytes.data() + kSequenceOffset);
    header.incarnation.high =
        ReadLittleEndian<uint64_t>(bytes.data() + kIncarnationHighOffset);
    header.incarnation.low =
        ReadLittleEndian<uint64_t>(bytes.data() + kIncarnationLowOffset);
    header.tenant_length =
        ReadLittleEndian<uint32_t>(bytes.data() + kTenantLengthOffset);
    header.key_length =
        ReadLittleEndian<uint32_t>(bytes.data() + kKeyLengthOffset);
    header.value_length =
        ReadLittleEndian<uint64_t>(bytes.data() + kValueLengthOffset);

    if (header.tenant_length > kMaxTenantLength ||
        header.key_length > kMaxKeyLength) {
        return tl::unexpected(DecodeError::kInvalidLength);
    }
    auto expected_size = CheckedRecordSize(
        header.tenant_length, header.key_length, header.value_length);
    if (!expected_size || expected_size.value() != header.total_length ||
        (header.kind == RecordKind::kTombstone && header.value_length != 0)) {
        return tl::unexpected(DecodeError::kInvalidLength);
    }
    return header;
}

tl::expected<DecodedRecord, DecodeError> DecodeRecord(
    std::span<const char> bytes) {
    auto header_result = DecodeRecordHeader(bytes);
    if (!header_result) {
        return tl::unexpected(header_result.error());
    }
    const RecordHeader& header = header_result.value();
    if (header.total_length > bytes.size()) {
        return tl::unexpected(DecodeError::kNeedMoreData);
    }

    const char* footer = bytes.data() + header.total_length - kRecordFooterSize;
    if (ReadLittleEndian<uint64_t>(footer + kFooterLengthOffset) !=
            header.total_length ||
        ReadLittleEndian<uint64_t>(footer + kFooterMagicOffset) !=
            kRecordCommitMagic ||
        Crc32cValue(footer, kFooterChecksumInputSize) !=
            ReadLittleEndian<uint32_t>(footer + kFooterChecksumOffset)) {
        return tl::unexpected(DecodeError::kFooterMismatch);
    }

    size_t cursor = kRecordHeaderSize;
    DecodedRecord record;
    record.identity.tenant_id.assign(bytes.data() + cursor,
                                     header.tenant_length);
    cursor += header.tenant_length;
    record.identity.object_key.assign(bytes.data() + cursor, header.key_length);
    cursor += header.key_length;
    record.value.assign(bytes.data() + cursor,
                        static_cast<size_t>(header.value_length));
    record.identity.incarnation = header.incarnation;
    record.kind = header.kind;
    record.sequence = header.sequence;
    record.total_length = header.total_length;

    Crc32c payload_crc;
    payload_crc.Extend(record.identity.tenant_id.data(),
                       record.identity.tenant_id.size());
    payload_crc.Extend(record.identity.object_key.data(),
                       record.identity.object_key.size());
    payload_crc.Extend(record.value.data(), record.value.size());
    if (payload_crc.Final() !=
        ReadLittleEndian<uint32_t>(footer + kFooterPayloadChecksumOffset)) {
        return tl::unexpected(DecodeError::kPayloadChecksumMismatch);
    }
    return record;
}

}  // namespace mooncake::logstructured
