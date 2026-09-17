#include "ha/oplog/oplog_batch_binary_codec.h"

#include <cstring>
#include <limits>
#include <vector>

#include "common/base64.h"
#include "ha/oplog/oplog_batch_types.h"
#include "ha/oplog/oplog_types.h"

namespace mooncake {

namespace {

// ---------------------------------------------------------------------------
// MessagePack format markers that this schema is allowed to accept. Only the
// subset needed for the frozen six-element batch projection is recognized;
// everything else is rejected explicitly instead of being skipped.
// ---------------------------------------------------------------------------
constexpr uint8_t kMsgPackPositiveFixIntMax = 0x7F;
constexpr uint8_t kMsgPackFixMapMin = 0x80;
constexpr uint8_t kMsgPackFixArrayMin = 0x90;
constexpr uint8_t kMsgPackFixArrayMax = 0x9F;
constexpr uint8_t kMsgPackFixStrMin = 0xA0;
constexpr uint8_t kMsgPackFixStrMax = 0xBF;
constexpr uint8_t kMsgPackNil = 0xC0;
constexpr uint8_t kMsgPackFalse = 0xC2;
constexpr uint8_t kMsgPackTrue = 0xC3;
constexpr uint8_t kMsgPackBin8 = 0xC4;
constexpr uint8_t kMsgPackBin16 = 0xC5;
constexpr uint8_t kMsgPackBin32 = 0xC6;
constexpr uint8_t kMsgPackExt8 = 0xC7;
constexpr uint8_t kMsgPackExt16 = 0xC8;
constexpr uint8_t kMsgPackExt32 = 0xC9;
constexpr uint8_t kMsgPackFloat32 = 0xCA;
constexpr uint8_t kMsgPackFloat64 = 0xCB;
constexpr uint8_t kMsgPackUint8 = 0xCC;
constexpr uint8_t kMsgPackUint16 = 0xCD;
constexpr uint8_t kMsgPackUint32 = 0xCE;
constexpr uint8_t kMsgPackUint64 = 0xCF;
constexpr uint8_t kMsgPackInt8 = 0xD0;
constexpr uint8_t kMsgPackInt16 = 0xD1;
constexpr uint8_t kMsgPackInt32 = 0xD2;
constexpr uint8_t kMsgPackInt64 = 0xD3;
constexpr uint8_t kMsgPackFixExt1 = 0xD4;
constexpr uint8_t kMsgPackFixExt2 = 0xD5;
constexpr uint8_t kMsgPackFixExt4 = 0xD6;
constexpr uint8_t kMsgPackFixExt8 = 0xD7;
constexpr uint8_t kMsgPackFixExt16 = 0xD8;
constexpr uint8_t kMsgPackStr8 = 0xD9;
constexpr uint8_t kMsgPackStr16 = 0xDA;
constexpr uint8_t kMsgPackStr32 = 0xDB;
constexpr uint8_t kMsgPackArray16 = 0xDC;
constexpr uint8_t kMsgPackArray32 = 0xDD;
constexpr uint8_t kMsgPackMap16 = 0xDE;
constexpr uint8_t kMsgPackMap32 = 0xDF;
constexpr uint8_t kMsgPackNegativeFixIntMin = 0xE0;
constexpr uint8_t kMsgPackMaxDeclaredLength = 0xFF;

constexpr size_t kOpLogBatchRootElementCount = 6;
constexpr size_t kOpLogBatchEntryElementCount = 4;

void SetReason(std::string* reason, const std::string& value) {
    if (reason != nullptr) {
        *reason = value;
    }
}

uint16_t ReadBigEndianUint16(const unsigned char* bytes) {
    return static_cast<uint16_t>((static_cast<uint16_t>(bytes[0]) << 8) |
                                 static_cast<uint16_t>(bytes[1]));
}

uint32_t ReadBigEndianUint32(const unsigned char* bytes) {
    return (static_cast<uint32_t>(bytes[0]) << 24) |
           (static_cast<uint32_t>(bytes[1]) << 16) |
           (static_cast<uint32_t>(bytes[2]) << 8) |
           static_cast<uint32_t>(bytes[3]);
}

void AppendBigEndianUint16(std::string& out, uint16_t value) {
    out.push_back(static_cast<char>((value >> 8) & 0xFF));
    out.push_back(static_cast<char>(value & 0xFF));
}

void AppendBigEndianUint32(std::string& out, uint32_t value) {
    out.push_back(static_cast<char>((value >> 24) & 0xFF));
    out.push_back(static_cast<char>((value >> 16) & 0xFF));
    out.push_back(static_cast<char>((value >> 8) & 0xFF));
    out.push_back(static_cast<char>(value & 0xFF));
}

void AppendBigEndianUint64(std::string& out, uint64_t value) {
    for (int shift = 56; shift >= 0; shift -= 8) {
        out.push_back(static_cast<char>((value >> shift) & 0xFF));
    }
}

// Bounded reader over a MessagePack body. Every length declaration is checked
// against the bytes that actually remain before any allocation happens, so a
// forged array32/str32/bin32 header cannot trigger a huge reserve or copy.
class BoundedMsgPackReader {
   public:
    BoundedMsgPackReader(const unsigned char* begin, const unsigned char* end)
        : cursor_(begin), end_(end) {}

    bool AtEnd() const { return cursor_ == end_; }
    bool Failed() const { return failed_; }
    const std::string& Failure() const { return failure_; }

    void Fail(const std::string& reason) {
        if (!failed_) {
            failed_ = true;
            failure_ = reason;
        }
    }

    size_t Remaining() const { return static_cast<size_t>(end_ - cursor_); }

    bool ReadMarker(uint8_t* marker) {
        if (failed_ || Remaining() < 1) {
            Fail("truncated msgpack value");
            return false;
        }
        *marker = *cursor_;
        ++cursor_;
        return true;
    }

    // Reads an unsigned integer. Signed encodings, floats, booleans, nil, and
    // numeric strings are rejected: the frozen schema only accepts non-negative
    // integers, and a signed value must not silently wrap into a valid enum.
    bool ReadUnsigned(uint64_t* out) {
        uint8_t marker = 0;
        if (!ReadMarker(&marker)) {
            return false;
        }
        if (marker <= kMsgPackPositiveFixIntMax) {
            *out = marker;
            return true;
        }
        switch (marker) {
            case kMsgPackUint8: {
                uint8_t value = 0;
                if (!ReadRaw(&value, 1)) return false;
                *out = value;
                return true;
            }
            case kMsgPackUint16: {
                unsigned char bytes[2];
                if (!ReadRaw(bytes, 2)) return false;
                *out = ReadBigEndianUint16(bytes);
                return true;
            }
            case kMsgPackUint32: {
                unsigned char bytes[4];
                if (!ReadRaw(bytes, 4)) return false;
                *out = ReadBigEndianUint32(bytes);
                return true;
            }
            case kMsgPackUint64: {
                unsigned char bytes[8];
                if (!ReadRaw(bytes, 8)) return false;
                uint64_t value = 0;
                for (int i = 0; i < 8; ++i) {
                    value = (value << 8) | static_cast<uint64_t>(bytes[i]);
                }
                *out = value;
                return true;
            }
            default:
                if (marker >= kMsgPackNegativeFixIntMin ||
                    marker == kMsgPackInt8 || marker == kMsgPackInt16 ||
                    marker == kMsgPackInt32 || marker == kMsgPackInt64) {
                    Fail("msgpack signed integer is not accepted");
                } else if (marker == kMsgPackNil || marker == kMsgPackFalse ||
                           marker == kMsgPackTrue) {
                    Fail("msgpack nil/bool is not accepted");
                } else if (marker == kMsgPackFloat32 ||
                           marker == kMsgPackFloat64) {
                    Fail("msgpack float is not accepted");
                } else if (marker == kMsgPackMap16 || marker == kMsgPackMap32 ||
                           (marker >= kMsgPackFixMapMin &&
                            marker < kMsgPackFixArrayMin)) {
                    Fail("msgpack map is not accepted");
                } else if (marker == kMsgPackExt8 || marker == kMsgPackExt16 ||
                           marker == kMsgPackExt32 ||
                           marker == kMsgPackFixExt1 ||
                           marker == kMsgPackFixExt2 ||
                           marker == kMsgPackFixExt4 ||
                           marker == kMsgPackFixExt8 ||
                           marker == kMsgPackFixExt16) {
                    Fail("msgpack extension is not accepted");
                } else {
                    Fail("msgpack value must be a non-negative integer");
                }
                return false;
        }
    }

    // Reads the declared length of an array without consuming its elements.
    bool ReadArrayHeader(uint64_t* count) {
        uint8_t marker = 0;
        if (!ReadMarker(&marker)) {
            return false;
        }
        if (marker >= kMsgPackFixArrayMin && marker <= kMsgPackFixArrayMax) {
            *count = static_cast<uint64_t>(marker - kMsgPackFixArrayMin);
            return true;
        }
        if (marker == kMsgPackArray16) {
            unsigned char bytes[2];
            if (!ReadRaw(bytes, 2)) return false;
            *count = ReadBigEndianUint16(bytes);
            return true;
        }
        if (marker == kMsgPackArray32) {
            unsigned char bytes[4];
            if (!ReadRaw(bytes, 4)) return false;
            *count = ReadBigEndianUint32(bytes);
            return true;
        }
        Fail("msgpack value must be an array");
        return false;
    }

    // Reads a BIN field. STR, EXT, and fixed-extension markers are rejected so
    // an extension subtype can never masquerade as an opaque payload.
    bool ReadBinary(std::string* out, size_t max_size) {
        const size_t declared = ReadBinLength();
        if (failed_) {
            return false;
        }
        return ReadSizedBytes(out, declared, max_size, "payload");
    }

    // Reads a string field while preserving raw bytes, including embedded NUL
    // and non-UTF-8 sequences; no normalization or replacement is performed.
    bool ReadString(std::string* out, size_t max_size) {
        const size_t declared = ReadStrLength();
        if (failed_) {
            return false;
        }
        return ReadSizedBytes(out, declared, max_size, "string");
    }

    void SkipBytes(size_t count) {
        if (failed_) {
            return;
        }
        if (Remaining() < count) {
            Fail("truncated msgpack value");
            return;
        }
        cursor_ += count;
    }

   private:
    bool ReadRaw(void* destination, size_t count) {
        if (failed_) {
            return false;
        }
        if (Remaining() < count) {
            Fail("truncated msgpack value");
            return false;
        }
        std::memcpy(destination, cursor_, count);
        cursor_ += count;
        return true;
    }

    size_t ReadBinLength() {
        uint8_t marker = 0;
        if (!ReadMarker(&marker)) {
            return 0;
        }
        switch (marker) {
            case kMsgPackBin8: {
                uint8_t value = 0;
                if (!ReadRaw(&value, 1)) return 0;
                return value;
            }
            case kMsgPackBin16: {
                unsigned char bytes[2];
                if (!ReadRaw(bytes, 2)) return 0;
                return ReadBigEndianUint16(bytes);
            }
            case kMsgPackBin32: {
                unsigned char bytes[4];
                if (!ReadRaw(bytes, 4)) return 0;
                return ReadBigEndianUint32(bytes);
            }
            default:
                Fail("payload must be msgpack BIN");
                return 0;
        }
    }

    size_t ReadStrLength() {
        uint8_t marker = 0;
        if (!ReadMarker(&marker)) {
            return 0;
        }
        if (marker >= kMsgPackFixStrMin && marker <= kMsgPackFixStrMax) {
            return static_cast<size_t>(marker - kMsgPackFixStrMin);
        }
        switch (marker) {
            case kMsgPackStr8: {
                uint8_t value = 0;
                if (!ReadRaw(&value, 1)) return 0;
                return value;
            }
            case kMsgPackStr16: {
                unsigned char bytes[2];
                if (!ReadRaw(bytes, 2)) return 0;
                return ReadBigEndianUint16(bytes);
            }
            case kMsgPackStr32: {
                unsigned char bytes[4];
                if (!ReadRaw(bytes, 4)) return 0;
                return ReadBigEndianUint32(bytes);
            }
            default:
                Fail("expected msgpack STR field");
                return 0;
        }
    }

    bool ReadSizedBytes(std::string* out, size_t declared, size_t max_size,
                        const char* field) {
        if (failed_) {
            return false;
        }
        // Reject before allocating: a declared length that exceeds either the
        // remaining input or the field budget never reaches a resize/copy.
        if (declared > max_size) {
            Fail(std::string(field) + " exceeds the maximum allowed size");
            return false;
        }
        if (declared > Remaining()) {
            Fail("truncated msgpack value");
            return false;
        }
        out->assign(reinterpret_cast<const char*>(cursor_), declared);
        cursor_ += declared;
        return true;
    }

    const unsigned char* cursor_;
    const unsigned char* end_;
    bool failed_{false};
    std::string failure_;
};

// ---------------------------------------------------------------------------
// Encoder used by the test-only fixture helper and by the reader tests.
// ---------------------------------------------------------------------------
void AppendMsgPackUnsigned(std::string& out, uint64_t value) {
    if (value <= kMsgPackPositiveFixIntMax) {
        out.push_back(static_cast<char>(value));
    } else if (value <= std::numeric_limits<uint8_t>::max()) {
        out.push_back(static_cast<char>(kMsgPackUint8));
        out.push_back(static_cast<char>(value & 0xFF));
    } else if (value <= std::numeric_limits<uint16_t>::max()) {
        out.push_back(static_cast<char>(kMsgPackUint16));
        AppendBigEndianUint16(out, static_cast<uint16_t>(value));
    } else if (value <= std::numeric_limits<uint32_t>::max()) {
        out.push_back(static_cast<char>(kMsgPackUint32));
        AppendBigEndianUint32(out, static_cast<uint32_t>(value));
    } else {
        out.push_back(static_cast<char>(kMsgPackUint64));
        AppendBigEndianUint64(out, value);
    }
}

void AppendMsgPackArrayHeader(std::string& out, size_t count) {
    if (count <= kMsgPackFixArrayMax - kMsgPackFixArrayMin) {
        out.push_back(static_cast<char>(kMsgPackFixArrayMin + count));
    } else if (count <= std::numeric_limits<uint16_t>::max()) {
        out.push_back(static_cast<char>(kMsgPackArray16));
        AppendBigEndianUint16(out, static_cast<uint16_t>(count));
    } else {
        out.push_back(static_cast<char>(kMsgPackArray32));
        AppendBigEndianUint32(out, static_cast<uint32_t>(count));
    }
}

void AppendMsgPackString(std::string& out, const std::string& value) {
    const size_t size = value.size();
    if (size <= kMsgPackFixStrMax - kMsgPackFixStrMin) {
        out.push_back(static_cast<char>(kMsgPackFixStrMin + size));
    } else if (size <= std::numeric_limits<uint8_t>::max()) {
        out.push_back(static_cast<char>(kMsgPackStr8));
        out.push_back(static_cast<char>(size & 0xFF));
    } else if (size <= std::numeric_limits<uint16_t>::max()) {
        out.push_back(static_cast<char>(kMsgPackStr16));
        AppendBigEndianUint16(out, static_cast<uint16_t>(size));
    } else {
        out.push_back(static_cast<char>(kMsgPackStr32));
        AppendBigEndianUint32(out, static_cast<uint32_t>(size));
    }
    out.append(value);
}

void AppendMsgPackBinary(std::string& out, const std::string& value) {
    const size_t size = value.size();
    if (size <= std::numeric_limits<uint8_t>::max()) {
        out.push_back(static_cast<char>(kMsgPackBin8));
        out.push_back(static_cast<char>(size & 0xFF));
    } else if (size <= std::numeric_limits<uint16_t>::max()) {
        out.push_back(static_cast<char>(kMsgPackBin16));
        AppendBigEndianUint16(out, static_cast<uint16_t>(size));
    } else {
        out.push_back(static_cast<char>(kMsgPackBin32));
        AppendBigEndianUint32(out, static_cast<uint32_t>(size));
    }
    out.append(value);
}

}  // namespace

bool HasOpLogBatchBinaryMagic(std::string_view value) {
    return value.size() >= kOpLogBatchEnvelopeMagicSize &&
           std::memcmp(value.data(), kOpLogBatchEnvelopeMagic,
                       kOpLogBatchEnvelopeMagicSize) == 0;
}

bool IsTruncatedOpLogBatchBinaryMagic(std::string_view value) {
    if (value.empty() || value.size() >= kOpLogBatchEnvelopeMagicSize) {
        return false;
    }
    return std::memcmp(value.data(), kOpLogBatchEnvelopeMagic, value.size()) ==
           0;
}

std::string EncodeOpLogBatchRecordBinaryForTest(const OpLogBatchRecord& batch) {
    OpLogBatchRecord encoded = batch;
    encoded.schema_version = kOpLogBatchRecordSchemaVersion;

    std::string body;
    AppendMsgPackArrayHeader(body, kOpLogBatchRootElementCount);
    AppendMsgPackUnsigned(body, encoded.schema_version);
    AppendMsgPackUnsigned(body, encoded.batch_id);
    AppendMsgPackUnsigned(body, encoded.first_seq);
    AppendMsgPackUnsigned(body, encoded.last_seq);
    AppendMsgPackArrayHeader(body, encoded.entries.size());
    for (const auto& entry : encoded.entries) {
        AppendMsgPackArrayHeader(body, kOpLogBatchEntryElementCount);
        AppendMsgPackUnsigned(body, static_cast<uint8_t>(entry.op_type));
        AppendMsgPackString(body, entry.tenant_id);
        AppendMsgPackString(body, entry.object_key);
        AppendMsgPackBinary(body, entry.payload);
    }
    encoded.checksum = ComputeOpLogBatchRecordChecksum(encoded);
    AppendMsgPackUnsigned(body, encoded.checksum);

    std::string wire;
    wire.reserve(kOpLogBatchEnvelopeHeaderSize + body.size());
    wire.append(kOpLogBatchEnvelopeMagic, kOpLogBatchEnvelopeMagicSize);
    wire.push_back(static_cast<char>(kOpLogBatchEnvelopeVersion));
    wire.push_back(static_cast<char>(kOpLogBatchCodecMessagePack));
    AppendBigEndianUint16(wire, 0);
    AppendBigEndianUint32(wire, static_cast<uint32_t>(body.size()));
    wire.append(body);
    return wire;
}

bool DecodeOpLogBatchRecordBinary(const std::string& value,
                                  OpLogBatchRecord* batch,
                                  std::string* reason) {
    if (reason != nullptr) {
        reason->clear();
    }
    if (batch == nullptr) {
        SetReason(reason, "batch output is null");
        return false;
    }
    // Check the minimum envelope size first so the body_length arithmetic below
    // can never underflow.
    if (value.size() < kOpLogBatchEnvelopeHeaderSize) {
        SetReason(reason, "binary batch record is shorter than its envelope");
        return false;
    }
    const auto* bytes = reinterpret_cast<const unsigned char*>(value.data());
    if (!HasOpLogBatchBinaryMagic(value)) {
        SetReason(reason, "binary batch record magic mismatch");
        return false;
    }
    if (bytes[8] != kOpLogBatchEnvelopeVersion) {
        SetReason(reason, "unsupported envelope_version");
        return false;
    }
    if (bytes[9] != kOpLogBatchCodecMessagePack) {
        SetReason(reason, "unsupported codec_id");
        return false;
    }
    const uint16_t flags = ReadBigEndianUint16(bytes + 10);
    if (flags != 0) {
        SetReason(reason, "unsupported envelope flags");
        return false;
    }
    const uint32_t body_length = ReadBigEndianUint32(bytes + 12);
    // Compare against the remaining size instead of computing 16 + body_length,
    // which could overflow.
    if (static_cast<size_t>(body_length) !=
        value.size() - kOpLogBatchEnvelopeHeaderSize) {
        SetReason(reason, "envelope body_length does not match input size");
        return false;
    }
    if (body_length == 0) {
        SetReason(reason, "envelope body is empty");
        return false;
    }

    BoundedMsgPackReader reader(bytes + kOpLogBatchEnvelopeHeaderSize,
                                bytes + value.size());
    uint64_t root_count = 0;
    if (!reader.ReadArrayHeader(&root_count)) {
        SetReason(reason, reader.Failure());
        return false;
    }
    if (root_count != kOpLogBatchRootElementCount) {
        SetReason(reason, "binary batch record must have six root elements");
        return false;
    }

    OpLogBatchRecord decoded;
    uint64_t schema_version = 0;
    if (!reader.ReadUnsigned(&schema_version)) {
        SetReason(reason, reader.Failure());
        return false;
    }
    if (schema_version != kOpLogBatchRecordSchemaVersion) {
        SetReason(reason, "unsupported batch record schema_version");
        return false;
    }
    decoded.schema_version = static_cast<uint32_t>(schema_version);
    if (!reader.ReadUnsigned(&decoded.batch_id) ||
        !reader.ReadUnsigned(&decoded.first_seq) ||
        !reader.ReadUnsigned(&decoded.last_seq)) {
        SetReason(reason, reader.Failure());
        return false;
    }

    uint64_t entry_count = 0;
    if (!reader.ReadArrayHeader(&entry_count)) {
        SetReason(reason, reader.Failure());
        return false;
    }
    if (entry_count == 0) {
        SetReason(reason, "batch entries must not be empty");
        return false;
    }
    // Sequence continuity plus the entry minimum size bound the declared entry
    // count against the bytes that actually remain, so the reserve below is
    // always justified by the input size.
    if (decoded.last_seq < decoded.first_seq ||
        decoded.last_seq - decoded.first_seq != entry_count - 1) {
        SetReason(reason, "batch sequence range does not match entry count");
        return false;
    }
    if (entry_count > reader.Remaining()) {
        SetReason(reason, "entry count exceeds remaining body bytes");
        return false;
    }

    decoded.entries.reserve(static_cast<size_t>(entry_count));
    for (uint64_t i = 0; i < entry_count; ++i) {
        uint64_t entry_elements = 0;
        if (!reader.ReadArrayHeader(&entry_elements)) {
            SetReason(reason, reader.Failure());
            return false;
        }
        if (entry_elements != kOpLogBatchEntryElementCount) {
            SetReason(reason, "oplog entry must be a four-element array");
            return false;
        }
        uint64_t op_type = 0;
        if (!reader.ReadUnsigned(&op_type)) {
            SetReason(reason, reader.Failure());
            return false;
        }
        // Range-check in a wide integer before narrowing so a large declared
        // op_type cannot wrap into a valid enum value.
        if (op_type == 0 ||
            op_type >= static_cast<uint64_t>(OpType::OP_TYPE_MAX)) {
            SetReason(reason, "oplog entry op_type is outside the enum range");
            return false;
        }

        OpLogEntry entry;
        entry.sequence_id = decoded.first_seq + i;
        entry.timestamp_ms = 0;
        entry.op_type = static_cast<OpType>(op_type);
        if (!reader.ReadString(&entry.tenant_id, kMaxOpLogObjectKeySize) ||
            !reader.ReadString(&entry.object_key, kMaxOpLogObjectKeySize) ||
            !reader.ReadBinary(&entry.payload, kMaxOpLogPayloadSize)) {
            SetReason(reason, reader.Failure());
            return false;
        }
        entry.checksum = ComputeOpLogChecksum(entry.payload);
        entry.prefix_hash = 0;
        if (!ValidateOpLogBatchEntry(entry, reason)) {
            return false;
        }
        decoded.entries.push_back(std::move(entry));
    }

    uint64_t checksum = 0;
    if (!reader.ReadUnsigned(&checksum)) {
        SetReason(reason, reader.Failure());
        return false;
    }
    if (checksum > std::numeric_limits<uint32_t>::max()) {
        SetReason(reason, "batch record checksum is out of range");
        return false;
    }
    decoded.checksum = static_cast<uint32_t>(checksum);

    // The whole body must be consumed: trailing bytes, a second concatenated
    // body, or extra fields are all rejected rather than ignored.
    if (!reader.AtEnd()) {
        SetReason(reason, "binary batch record has trailing bytes");
        return false;
    }
    if (decoded.checksum != ComputeOpLogBatchRecordChecksum(decoded)) {
        SetReason(reason, "batch record checksum mismatch");
        return false;
    }
    if (!ValidateOpLogBatchRecordShape(decoded, reason)) {
        return false;
    }

    *batch = std::move(decoded);
    return true;
}

}  // namespace mooncake
