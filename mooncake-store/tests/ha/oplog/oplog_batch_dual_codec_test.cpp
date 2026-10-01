#include "ha/oplog/oplog_batch_binary_codec.h"
#include "ha/oplog/oplog_batch_codec.h"
#include "ha/oplog/oplog_batch_types.h"

#include <gtest/gtest.h>
#include <xxhash.h>

#include <cstdint>
#include <random>
#include <string>
#include <vector>

namespace mooncake::test {

namespace {

constexpr char kEnvelopeMagic[] = "\x89MCOPLG\n";
constexpr size_t kEnvelopeHeaderSize = 16;

OpLogEntry MakeEntry(uint64_t seq, OpType type = OpType::PUT_END,
                     std::string key = "key", std::string payload = "value") {
    OpLogEntry entry;
    entry.sequence_id = seq;
    entry.timestamp_ms = 1234567890;
    entry.op_type = type;
    entry.tenant_id = "tenant";
    entry.object_key = std::move(key);
    entry.payload = std::move(payload);
    entry.checksum = static_cast<uint32_t>(
        XXH32(entry.payload.data(), entry.payload.size(), 0));
    entry.prefix_hash = static_cast<uint32_t>(
        XXH32(entry.object_key.data(), entry.object_key.size(), 0));
    return entry;
}

OpLogBatchRecord MakeBatch(uint64_t batch_id, std::vector<OpLogEntry> entries) {
    OpLogBatchRecord batch;
    batch.batch_id = batch_id;
    batch.entries = std::move(entries);
    batch.first_seq = batch.entries.front().sequence_id;
    batch.last_seq = batch.entries.back().sequence_id;
    return batch;
}

// Compares the persisted logical batch value. timestamp_ms, prefix_hash, and
// the batch checksum are rebuilt by the codec from the wire, so they are not
// part of the persisted identity and are asserted explicitly where relevant.
bool SameBatch(const OpLogBatchRecord& lhs, const OpLogBatchRecord& rhs) {
    if (lhs.schema_version != rhs.schema_version ||
        lhs.batch_id != rhs.batch_id || lhs.first_seq != rhs.first_seq ||
        lhs.last_seq != rhs.last_seq ||
        lhs.entries.size() != rhs.entries.size()) {
        return false;
    }
    for (size_t i = 0; i < lhs.entries.size(); ++i) {
        const auto& a = lhs.entries[i];
        const auto& b = rhs.entries[i];
        if (a.sequence_id != b.sequence_id || a.op_type != b.op_type ||
            a.tenant_id != b.tenant_id || a.object_key != b.object_key ||
            a.payload != b.payload || a.checksum != b.checksum) {
            return false;
        }
    }
    return true;
}

std::string Wire() {
    return EncodeOpLogBatchRecordBinaryForTest(
        MakeBatch(1, {MakeEntry(1, OpType::PUT_END, "key", "value")}));
}

// Rewrites the 4-byte big-endian body_length field so framing mutations can be
// built without touching the body bytes.
void SetBodyLength(std::string* wire, uint32_t body_length) {
    (*wire)[12] = static_cast<char>((body_length >> 24) & 0xFF);
    (*wire)[13] = static_cast<char>((body_length >> 16) & 0xFF);
    (*wire)[14] = static_cast<char>((body_length >> 8) & 0xFF);
    (*wire)[15] = static_cast<char>(body_length & 0xFF);
}

size_t FirstOffsetOf(const std::string& haystack, const std::string& needle) {
    return haystack.find(needle);
}

// Parses a MessagePack unsigned integer at a fixed offset, mirroring the
// markers the fixture encoder emits. Returns false if the marker is not an
// unsigned encoding.
bool ParseMsgPackUnsignedAt(const std::string& wire, size_t offset,
                            uint64_t* value, size_t* next) {
    if (offset >= wire.size()) {
        return false;
    }
    const auto marker = static_cast<uint8_t>(wire[offset]);
    const auto read_be = [&wire](size_t at, size_t width) -> uint64_t {
        uint64_t result = 0;
        for (size_t i = 0; i < width; ++i) {
            result = (result << 8) | static_cast<uint8_t>(wire[at + i]);
        }
        return result;
    };
    if (marker <= 0x7F) {
        *value = marker;
        *next = offset + 1;
        return true;
    }
    if (marker == 0xCC && offset + 1 < wire.size()) {
        *value = static_cast<uint8_t>(wire[offset + 1]);
        *next = offset + 2;
        return true;
    }
    if (marker == 0xCD && offset + 2 < wire.size()) {
        *value = read_be(offset + 1, 2);
        *next = offset + 3;
        return true;
    }
    if (marker == 0xCE && offset + 4 < wire.size()) {
        *value = read_be(offset + 1, 4);
        *next = offset + 5;
        return true;
    }
    if (marker == 0xCF && offset + 8 < wire.size()) {
        *value = read_be(offset + 1, 8);
        *next = offset + 9;
        return true;
    }
    return false;
}

// Locates the first entry's BIN payload bytes in a single-entry fixture wire.
// The walk follows the frozen schema layout instead of hard-coding byte
// positions, so encoding-width changes do not silently invalidate the test.
bool FindFirstEntryPayload(const std::string& wire, const std::string& payload,
                           size_t* offset) {
    if (wire.size() < 16) {
        return false;
    }
    size_t cursor = 16;
    const auto skip_unsigned = [&wire, &cursor]() {
        uint64_t ignored = 0;
        size_t next = 0;
        const bool ok = ParseMsgPackUnsignedAt(wire, cursor, &ignored, &next);
        if (ok) {
            cursor = next;
        }
        return ok;
    };
    // root array(6)
    if (wire[cursor++] != static_cast<char>(0x96)) {
        return false;
    }
    if (!skip_unsigned() || !skip_unsigned() || !skip_unsigned() ||
        !skip_unsigned()) {
        return false;
    }
    if (wire[cursor++] != static_cast<char>(0x91)) {  // entries array(1)
        return false;
    }
    // entry array(4)
    const auto entry_marker = static_cast<uint8_t>(wire[cursor]);
    if (entry_marker < 0x94 || entry_marker > 0x97) {
        return false;
    }
    cursor += 1;
    if (!skip_unsigned()) {
        return false;
    }
    const auto skip_string = [&wire, &cursor]() {
        const auto marker = static_cast<uint8_t>(wire[cursor]);
        size_t length = 0;
        if (marker >= 0xA0 && marker <= 0xBF) {
            length = marker - 0xA0;
            ++cursor;
        } else if (marker == 0xD9 && cursor + 1 < wire.size()) {
            length = static_cast<uint8_t>(wire[cursor + 1]);
            cursor += 2;
        } else if (marker == 0xDA && cursor + 2 < wire.size()) {
            length =
                (static_cast<size_t>(static_cast<uint8_t>(wire[cursor + 1]))
                 << 8) |
                static_cast<uint8_t>(wire[cursor + 2]);
            cursor += 3;
        } else {
            return false;
        }
        cursor += length;
        return true;
    };
    if (!skip_string() || !skip_string()) {
        return false;
    }
    const auto bin_marker = static_cast<uint8_t>(wire[cursor]);
    size_t declared = 0;
    if (bin_marker == 0xC4 && cursor + 1 < wire.size()) {
        declared = static_cast<uint8_t>(wire[cursor + 1]);
        cursor += 2;
    } else if (bin_marker == 0xC5 && cursor + 2 < wire.size()) {
        declared =
            (static_cast<size_t>(static_cast<uint8_t>(wire[cursor + 1])) << 8) |
            static_cast<uint8_t>(wire[cursor + 2]);
        cursor += 3;
    } else if (bin_marker == 0xC6 && cursor + 4 < wire.size()) {
        declared = 0;
        for (size_t i = 0; i < 4; ++i) {
            declared =
                (declared << 8) | static_cast<uint8_t>(wire[cursor + 1 + i]);
        }
        cursor += 5;
    } else {
        return false;
    }
    if (declared != payload.size() || cursor + declared > wire.size()) {
        return false;
    }
    *offset = cursor;
    return true;
}

}  // namespace

// ---------------------------------------------------------------------------
// L01/L02: the existing JSON encoder must stay byte-for-byte stable.
// ---------------------------------------------------------------------------
TEST(OpLogBatchJsonGoldenTest, SingleEntryGoldenIsStable) {
    const auto encoded = EncodeOpLogBatchRecord(
        MakeBatch(3, {MakeEntry(10, OpType::PUT_END, "key", "value")}));
    EXPECT_NE(std::string::npos, encoded.find(R"([1,3,10,10,)"));
    EXPECT_NE(std::string::npos, encoded.find(R"("tenant","key","dmFsdWU=")"));
    EXPECT_EQ(std::string::npos, encoded.find("schema_version"));
    EXPECT_EQ(std::string::npos, encoded.find("checksum\":"));
}

TEST(OpLogBatchJsonGoldenTest, EmptyPayloadKeepsEmptyBase64) {
    const auto encoded = EncodeOpLogBatchRecord(
        MakeBatch(7, {MakeEntry(21, OpType::REMOVE, "dead-key", "")}));
    EXPECT_NE(std::string::npos, encoded.find(R"([3,"tenant","dead-key",""])"));
}

// Exact wire identity for the production JSON encoder. These bytes were
// captured from the unmodified baseline (commit 553c39a, tree of upstream main
// c4d6328) before the P02 change; reproducing them byte-for-byte is the
// strongest available proof that the legacy encoding is unchanged.
TEST(OpLogBatchJsonGoldenTest, ExactBytesMatchBaselineCapture) {
    EXPECT_EQ(std::string(
                  R"([1,3,10,10,[[1,"tenant","key","dmFsdWU="]],2438585334])"),
              EncodeOpLogBatchRecord(MakeBatch(
                  3, {MakeEntry(10, OpType::PUT_END, "key", "value")})));
    EXPECT_EQ(
        std::string(R"([1,7,21,21,[[3,"tenant","dead-key",""]],282285989])"),
        EncodeOpLogBatchRecord(
            MakeBatch(7, {MakeEntry(21, OpType::REMOVE, "dead-key", "")})));
    EXPECT_EQ(
        std::string(
            R"([1,6,20,22,[[1,"tenant","k1","djE="],[3,"tenant","k2",""],[5,"tenant","k3","AAB6"]],1108712004])"),
        EncodeOpLogBatchRecord(
            MakeBatch(6, {MakeEntry(20, OpType::PUT_END, "k1", "v1"),
                          MakeEntry(21, OpType::REMOVE, "k2", ""),
                          MakeEntry(22, OpType::SEGMENT_MOUNT, "k3",
                                    std::string("\0\0z", 3))})));
    EXPECT_EQ(
        std::string(R"({"batch_id":9,"last_seq":1024,"schema_version":1})"),
        EncodeDurablePrefix({.batch_id = 9, .last_seq = 1024}));
}

TEST(OpLogBatchJsonGoldenTest, BinaryPayloadUsesCanonicalBase64) {
    const std::string payload("\x00\x01\xFE\xFF", 4);
    const auto encoded = EncodeOpLogBatchRecord(
        MakeBatch(9, {MakeEntry(31, OpType::PUT_END, "bin-key", payload)}));

    OpLogBatchRecord decoded;
    std::string reason;
    ASSERT_TRUE(DecodeOpLogBatchRecord(encoded, &decoded, &reason)) << reason;
    ASSERT_EQ(1u, decoded.entries.size());
    EXPECT_EQ(payload, decoded.entries[0].payload);
}

// ---------------------------------------------------------------------------
// B01/B02/B03/B04: binary round-trip, byte fidelity, and logical agreement.
// ---------------------------------------------------------------------------
TEST(OpLogBatchBinaryCodecTest, DecodesEveryValidOpType) {
    for (uint32_t raw = 1; raw < static_cast<uint32_t>(OpType::OP_TYPE_MAX);
         ++raw) {
        const auto op_type = static_cast<OpType>(raw);
        const auto batch =
            MakeBatch(4, {MakeEntry(5, op_type, "key-" + std::to_string(raw),
                                    "payload-" + std::to_string(raw))});
        const auto wire = EncodeOpLogBatchRecordBinaryForTest(batch);

        OpLogBatchRecord decoded;
        std::string reason;
        ASSERT_TRUE(DecodeOpLogBatchRecord(wire, &decoded, &reason))
            << "op_type=" << raw << " reason=" << reason;
        ASSERT_EQ(1u, decoded.entries.size());
        EXPECT_EQ(op_type, decoded.entries[0].op_type);
    }
}

// Characterization of the legacy JSON key domain versus the binary path. The
// JSON writer escapes representable bytes, and it rejects non-UTF-8 byte
// sequences outright; the binary path preserves the raw bytes. Neither behavior
// is changed by P02, and both are locked here so a future encoding change is
// visible.
TEST(OpLogBatchBinaryCodecTest, DocsJsonKeyDomainVersusBinaryKeyFidelity) {
    // Literal NUL in a key is escaped by the JSON writer and round-trips.
    const std::string nul_key("a\0b", 3);
    const auto nul_batch =
        MakeBatch(21, {MakeEntry(50, OpType::PUT_END, nul_key, "v")});
    OpLogBatchRecord nul_from_json;
    std::string nul_json_reason;
    ASSERT_TRUE(DecodeOpLogBatchRecord(EncodeOpLogBatchRecord(nul_batch),
                                       &nul_from_json, &nul_json_reason))
        << nul_json_reason;
    EXPECT_EQ(nul_key, nul_from_json.entries[0].object_key);

    OpLogBatchRecord nul_from_binary;
    std::string nul_binary_reason;
    ASSERT_TRUE(
        DecodeOpLogBatchRecord(EncodeOpLogBatchRecordBinaryForTest(nul_batch),
                               &nul_from_binary, &nul_binary_reason))
        << nul_binary_reason;
    EXPECT_EQ(nul_key, nul_from_binary.entries[0].object_key);

    // Non-UTF-8 bytes: the legacy JSON reader accepts the record but the bytes
    // do not survive the UTF-8 conversion (JsonCpp substitutes or drops them),
    // so the decoded key is not byte-equal. The binary path preserves them
    // byte-exactly. Both behaviors are unchanged by P02.
    const std::string raw_key("\xC9\xFF\x80raw", 5);
    const auto raw_batch =
        MakeBatch(22, {MakeEntry(51, OpType::PUT_END, raw_key, "v")});
    OpLogBatchRecord raw_from_json;
    std::string raw_json_reason;
    const bool raw_json_ok = DecodeOpLogBatchRecord(
        EncodeOpLogBatchRecord(raw_batch), &raw_from_json, &raw_json_reason);
    if (raw_json_ok) {
        EXPECT_NE(raw_key, raw_from_json.entries[0].object_key)
            << "legacy JSON unexpectedly preserved non-UTF-8 key bytes; "
               "update this characterization";
    }

    OpLogBatchRecord raw_from_binary;
    std::string raw_binary_reason;
    ASSERT_TRUE(
        DecodeOpLogBatchRecord(EncodeOpLogBatchRecordBinaryForTest(raw_batch),
                               &raw_from_binary, &raw_binary_reason))
        << raw_binary_reason;
    EXPECT_EQ(raw_key, raw_from_binary.entries[0].object_key);
}

TEST(OpLogBatchBinaryCodecTest, RoundTripsMultiEntryBatch) {
    const auto batch = MakeBatch(6, {MakeEntry(20, OpType::PUT_END, "k1", "v1"),
                                     MakeEntry(21, OpType::REMOVE, "k2", ""),
                                     MakeEntry(22, OpType::SEGMENT_MOUNT, "k3",
                                               std::string("\0\0z", 3))});
    const auto wire = EncodeOpLogBatchRecordBinaryForTest(batch);

    OpLogBatchRecord decoded;
    std::string reason;
    ASSERT_TRUE(DecodeOpLogBatchRecord(wire, &decoded, &reason)) << reason;
    EXPECT_TRUE(SameBatch(batch, decoded));
    EXPECT_EQ(mooncake::ComputeOpLogBatchRecordChecksum(batch),
              decoded.checksum);
    EXPECT_EQ(0u, decoded.entries[0].timestamp_ms);
    EXPECT_EQ(0u, decoded.entries[0].prefix_hash);
    EXPECT_TRUE(VerifyOpLogChecksum(decoded.entries[2]));
}

TEST(OpLogBatchBinaryCodecTest, PreservesAllPayloadBytes) {
    std::string payload;
    payload.reserve(256);
    for (int byte = 0; byte <= 0xFF; ++byte) {
        payload.push_back(static_cast<char>(byte));
    }
    const auto batch =
        MakeBatch(11, {MakeEntry(40, OpType::PUT_END, "all-bytes", payload)});
    const auto wire = EncodeOpLogBatchRecordBinaryForTest(batch);

    OpLogBatchRecord decoded;
    std::string reason;
    ASSERT_TRUE(DecodeOpLogBatchRecord(wire, &decoded, &reason)) << reason;
    ASSERT_EQ(1u, decoded.entries.size());
    EXPECT_EQ(payload, decoded.entries[0].payload);
}

TEST(OpLogBatchBinaryCodecTest, PreservesEmbeddedNulAndMagicInPayload) {
    std::string payload("prefix\0", 7);
    payload.append(kEnvelopeMagic, 8);
    payload.append("\0suffix", 7);
    const auto batch = MakeBatch(
        12, {MakeEntry(41, OpType::PUT_END, std::string("k\0ey", 4), payload)});
    const auto wire = EncodeOpLogBatchRecordBinaryForTest(batch);
    ASSERT_TRUE(HasOpLogBatchBinaryMagic(wire));

    // The magic inside the payload must not trigger a second envelope parse.
    OpLogBatchRecord decoded;
    std::string reason;
    ASSERT_TRUE(DecodeOpLogBatchRecord(wire, &decoded, &reason)) << reason;
    ASSERT_EQ(1u, decoded.entries.size());
    EXPECT_EQ(payload, decoded.entries[0].payload);
    EXPECT_EQ(std::string("k\0ey", 4), decoded.entries[0].object_key);
}

TEST(OpLogBatchBinaryCodecTest, PreservesNonUtf8KeyAndTenantBytes) {
    const std::string key("\xFF\xFE\x80raw", 6);
    auto entry = MakeEntry(42, OpType::PUT_END, key, "payload");
    entry.tenant_id = std::string("\xC3\x28", 2);
    const auto wire =
        EncodeOpLogBatchRecordBinaryForTest(MakeBatch(13, {entry}));

    OpLogBatchRecord decoded;
    std::string reason;
    ASSERT_TRUE(DecodeOpLogBatchRecord(wire, &decoded, &reason)) << reason;
    ASSERT_EQ(1u, decoded.entries.size());
    EXPECT_EQ(key, decoded.entries[0].object_key);
    EXPECT_EQ(entry.tenant_id, decoded.entries[0].tenant_id);
}

TEST(OpLogBatchBinaryCodecTest, MatchesJsonLogicalDecisionForIdenticalBatch) {
    const auto batch = MakeBatch(
        14, {MakeEntry(100, OpType::PUT_END, "k", "v"),
             MakeEntry(101, OpType::REMOVE, "k2", std::string("\x00\xFF", 2))});
    const auto json_wire = EncodeOpLogBatchRecord(batch);
    const auto binary_wire = EncodeOpLogBatchRecordBinaryForTest(batch);

    OpLogBatchRecord from_json;
    OpLogBatchRecord from_binary;
    std::string json_reason;
    std::string binary_reason;
    ASSERT_TRUE(DecodeOpLogBatchRecord(json_wire, &from_json, &json_reason))
        << json_reason;
    ASSERT_TRUE(
        DecodeOpLogBatchRecord(binary_wire, &from_binary, &binary_reason))
        << binary_reason;
    EXPECT_TRUE(SameBatch(from_json, from_binary));
    EXPECT_EQ(from_json.checksum, from_binary.checksum);
}

TEST(OpLogBatchBinaryCodecTest, RoundsUpUint64BoundaryValues) {
    OpLogBatchRecord batch;
    batch.batch_id = 1;
    batch.first_seq = 1;
    batch.last_seq = 2;
    auto first = MakeEntry(1);
    first.payload = std::string(70000, 'x');  // forces bin32/str16 paths
    first.checksum = ComputeOpLogChecksum(first.payload);
    auto second = MakeEntry(2, OpType::REMOVE, std::string(300, 'k'), "");
    batch.entries = {first, second};

    const auto wire = EncodeOpLogBatchRecordBinaryForTest(batch);
    OpLogBatchRecord decoded;
    std::string reason;
    ASSERT_TRUE(DecodeOpLogBatchRecord(wire, &decoded, &reason)) << reason;
    EXPECT_TRUE(SameBatch(batch, decoded));
    EXPECT_EQ(mooncake::ComputeOpLogBatchRecordChecksum(batch),
              decoded.checksum);
}

TEST(OpLogBatchBinaryCodecTest, AcceptsLargeBatchIdNearUint64Max) {
    OpLogBatchRecord batch = MakeBatch(1, {MakeEntry(1)});
    batch.batch_id = UINT64_MAX - 1;
    batch.first_seq = UINT64_MAX - 5;
    batch.last_seq = UINT64_MAX - 5;

    const auto wire = EncodeOpLogBatchRecordBinaryForTest(batch);
    OpLogBatchRecord decoded;
    std::string reason;
    ASSERT_TRUE(DecodeOpLogBatchRecord(wire, &decoded, &reason)) << reason;
    EXPECT_EQ(UINT64_MAX - 1, decoded.batch_id);
    EXPECT_EQ(UINT64_MAX - 5, decoded.first_seq);
}

// ---------------------------------------------------------------------------
// B08/B09/B10: framing, truncation, and strict full consumption.
// ---------------------------------------------------------------------------
TEST(OpLogBatchBinaryEnvelopeTest, RejectsEveryCorruptedHeaderField) {
    const std::string valid = Wire();

    OpLogBatchRecord out;
    std::string reason;
    {
        auto wire = valid;
        wire[0] = static_cast<char>(0x00);
        EXPECT_FALSE(DecodeOpLogBatchRecord(wire, &out, &reason)) << reason;
    }
    {
        auto wire = valid;
        wire[8] = 2;  // envelope_version
        EXPECT_FALSE(DecodeOpLogBatchRecord(wire, &out, &reason));
        EXPECT_NE(std::string::npos, reason.find("envelope_version"));
    }
    {
        auto wire = valid;
        wire[9] = 2;  // codec_id
        EXPECT_FALSE(DecodeOpLogBatchRecord(wire, &out, &reason));
        EXPECT_NE(std::string::npos, reason.find("codec_id"));
    }
    {
        auto wire = valid;
        wire[11] = 1;  // flags
        EXPECT_FALSE(DecodeOpLogBatchRecord(wire, &out, &reason));
        EXPECT_NE(std::string::npos, reason.find("flags"));
    }
    {
        auto wire = valid;
        SetBodyLength(&wire, static_cast<uint32_t>(wire.size() - 15));
        EXPECT_FALSE(DecodeOpLogBatchRecord(wire, &out, &reason));
        EXPECT_NE(std::string::npos, reason.find("body_length"));
    }
    {
        auto wire = valid;
        SetBodyLength(&wire, 0);
        EXPECT_FALSE(DecodeOpLogBatchRecord(wire, &out, &reason));
    }
}

TEST(OpLogBatchBinaryEnvelopeTest, RejectsTrailingByteAndConcatenatedBody) {
    const std::string valid = Wire();

    OpLogBatchRecord out;
    std::string reason;
    {
        auto wire = valid;
        wire.push_back('\x00');
        EXPECT_FALSE(DecodeOpLogBatchRecord(wire, &out, &reason));
        EXPECT_NE(std::string::npos, reason.find("body_length"));
    }
    {
        auto wire = valid;
        SetBodyLength(&wire, static_cast<uint32_t>(wire.size() - 16));
        wire.append(valid);
        EXPECT_FALSE(DecodeOpLogBatchRecord(wire, &out, &reason));
    }
}

TEST(OpLogBatchBinaryEnvelopeTest, FailsAtEveryTruncationPoint) {
    const std::string valid = Wire();
    for (size_t length = 0; length < valid.size(); ++length) {
        const std::string truncated = valid.substr(0, length);
        OpLogBatchRecord out =
            MakeBatch(99, {MakeEntry(99, OpType::REMOVE, "sentinel", "keep")});
        OpLogBatchRecord sentinel = out;
        std::string reason;
        EXPECT_FALSE(DecodeOpLogBatchRecord(truncated, &out, &reason))
            << "length=" << length;
        EXPECT_TRUE(SameBatch(sentinel, out)) << "length=" << length;
    }
}

TEST(OpLogBatchBinaryEnvelopeTest, TruncatedMagicPrefixIsNotJsonOrBinary) {
    OpLogBatchRecord out;
    std::string reason;
    for (size_t length = 1; length < kOpLogBatchEnvelopeMagicSize; ++length) {
        const std::string prefix(kEnvelopeMagic, length);
        EXPECT_TRUE(IsTruncatedOpLogBatchBinaryMagic(prefix));
        EXPECT_FALSE(DecodeOpLogBatchRecord(prefix, &out, &reason))
            << "length=" << length;
        EXPECT_NE(std::string::npos, reason.find("truncated binary"));
    }
    const std::string full(kEnvelopeMagic, kOpLogBatchEnvelopeMagicSize);
    EXPECT_TRUE(HasOpLogBatchBinaryMagic(full));
    EXPECT_FALSE(IsTruncatedOpLogBatchBinaryMagic(full));
}

TEST(OpLogBatchBinaryEnvelopeTest, RejectsExtraBodyField) {
    auto wire = Wire();
    // Append one extra fixint to the body and fix up the declared length so the
    // only defect is the unexpected seventh root element.
    wire.push_back('\x00');
    SetBodyLength(&wire, static_cast<uint32_t>(wire.size() - 16));

    OpLogBatchRecord out;
    std::string reason;
    EXPECT_FALSE(DecodeOpLogBatchRecord(wire, &out, &reason));
    EXPECT_NE(std::string::npos, reason.find("trailing"));
}

// ---------------------------------------------------------------------------
// B05/B06: strict type contract.
// ---------------------------------------------------------------------------
TEST(OpLogBatchBinaryStrictTypeTest, RejectsSignedIntegerEncodedSchema) {
    auto wire = Wire();
    const auto body = kEnvelopeHeaderSize;
    // Replace the schema fixint (first body byte) with int8(-1) plus payload.
    wire.replace(body, 1, std::string("\xD0\xFF", 2));
    SetBodyLength(&wire,
                  static_cast<uint32_t>(wire.size() - kEnvelopeHeaderSize));

    OpLogBatchRecord out;
    std::string reason;
    EXPECT_FALSE(DecodeOpLogBatchRecord(wire, &out, &reason));
    EXPECT_FALSE(reason.empty());
    // The signed-integer marker must be reported as a type error, not as an
    // out-of-range value or a checksum mismatch.
    EXPECT_NE(std::string::npos, reason.find("msgpack"));
}

TEST(OpLogBatchBinaryStrictTypeTest, RejectsFloatBooleanNilAndMapMarkers) {
    const std::string valid = Wire();
    const auto body = kEnvelopeHeaderSize;
    const std::vector<std::pair<std::string, std::string>> rejected = {
        {"float64", std::string("\xCB\x00\x00\x00\x00\x00\x00\x00\x01", 9)},
        {"true", std::string("\xC3", 1)},
        {"nil", std::string("\xC0", 1)},
        {"fixmap", std::string("\x80", 1)},
        {"map16", std::string("\xDE\x00\x01", 3)},
    };
    for (const auto& [name, replacement] : rejected) {
        auto wire = valid;
        wire.replace(body, 1, replacement);
        SetBodyLength(&wire,
                      static_cast<uint32_t>(wire.size() - kEnvelopeHeaderSize));

        OpLogBatchRecord out;
        std::string reason;
        EXPECT_FALSE(DecodeOpLogBatchRecord(wire, &out, &reason)) << name;
        EXPECT_FALSE(reason.empty()) << name;
    }
}

TEST(OpLogBatchBinaryStrictTypeTest, RejectsOutOfRangeOpTypeWithoutWrapping) {
    // op_type = 0x0101 must not wrap to 1 (PUT_END). The fixture entry starts
    // with fixarray(4) followed by the op_type fixint, so the op_type byte is
    // one past the entry array header.
    auto wire = Wire();
    const size_t entries_offset =
        FirstOffsetOf(wire, std::string("\x91\x94\x01\xA6tenant", 9));
    ASSERT_NE(std::string::npos, entries_offset);
    const size_t entry_op = entries_offset + 2;
    wire.replace(entry_op, 1, std::string("\xCD\x01\x01", 3));
    SetBodyLength(&wire,
                  static_cast<uint32_t>(wire.size() - kEnvelopeHeaderSize));

    OpLogBatchRecord out;
    std::string reason;
    EXPECT_FALSE(DecodeOpLogBatchRecord(wire, &out, &reason));
    EXPECT_NE(std::string::npos, reason.find("op_type"));
}

TEST(OpLogBatchBinaryStrictTypeTest, RejectsPayloadEncodedAsString) {
    auto wire = Wire();
    // The payload in the fixture is BIN8 0x05 "value"; turn it into STR8.
    const size_t payload_offset =
        FirstOffsetOf(wire, std::string("\xC4\x05value", 7));
    ASSERT_NE(std::string::npos, payload_offset);
    wire[payload_offset] = static_cast<char>(0xD9);

    OpLogBatchRecord out;
    std::string reason;
    EXPECT_FALSE(DecodeOpLogBatchRecord(wire, &out, &reason));
    EXPECT_NE(std::string::npos, reason.find("BIN"));
}

TEST(OpLogBatchBinaryStrictTypeTest, RejectsPayloadEncodedAsExtension) {
    auto wire = Wire();
    const size_t payload_offset =
        FirstOffsetOf(wire, std::string("\xC4\x05value", 7));
    ASSERT_NE(std::string::npos, payload_offset);
    // bin8(5).'value' -> ext8(len=5, type=1).'value' keeps the body length
    // identical while replacing the payload type.
    wire.replace(payload_offset, 2, std::string("\xC7\x05\x01", 3));
    SetBodyLength(&wire,
                  static_cast<uint32_t>(wire.size() - kEnvelopeHeaderSize));

    OpLogBatchRecord out;
    std::string reason;
    EXPECT_FALSE(DecodeOpLogBatchRecord(wire, &out, &reason));
    EXPECT_NE(std::string::npos, reason.find("BIN"));
}

// ---------------------------------------------------------------------------
// B07/B11: structure, ranges, and checksum.
// ---------------------------------------------------------------------------
TEST(OpLogBatchBinaryValidationTest, RejectsNonContiguousSequenceRange) {
    // last_seq claims 5 while a single entry was encoded. seq_id and last_seq
    // are adjacent uint32 fields, so walk to last_seq instead of pattern
    // matching raw bytes.
    auto wire = Wire();
    size_t cursor = kEnvelopeHeaderSize;
    ASSERT_EQ(static_cast<char>(0x96), wire[cursor]);
    ++cursor;
    uint64_t schema = 0;
    uint64_t batch_id = 0;
    uint64_t first_seq = 0;
    uint64_t last_seq = 0;
    size_t next = 0;
    ASSERT_TRUE(ParseMsgPackUnsignedAt(wire, cursor, &schema, &next));
    cursor = next;
    ASSERT_TRUE(ParseMsgPackUnsignedAt(wire, cursor, &batch_id, &next));
    cursor = next;
    ASSERT_TRUE(ParseMsgPackUnsignedAt(wire, cursor, &first_seq, &next));
    cursor = next;
    const size_t last_seq_offset = cursor;
    ASSERT_TRUE(ParseMsgPackUnsignedAt(wire, cursor, &last_seq, &next));
    ASSERT_EQ(first_seq, last_seq);
    wire[last_seq_offset] = static_cast<char>(first_seq + 4);
    SetBodyLength(&wire,
                  static_cast<uint32_t>(wire.size() - kEnvelopeHeaderSize));

    OpLogBatchRecord out;
    std::string reason;
    EXPECT_FALSE(DecodeOpLogBatchRecord(wire, &out, &reason));
    EXPECT_NE(std::string::npos, reason.find("sequence range"));
}

TEST(OpLogBatchBinaryValidationTest, RejectsEmptyEntryArray) {
    auto batch = MakeBatch(1, {MakeEntry(1)});
    auto wire = EncodeOpLogBatchRecordBinaryForTest(batch);
    // Replace the entry array header (fixarray(1) = 0x91) with fixarray(0).
    const size_t entries_offset =
        FirstOffsetOf(wire, std::string("\x91\x94", 2));
    ASSERT_NE(std::string::npos, entries_offset);
    wire[entries_offset] = static_cast<char>(0x90);
    SetBodyLength(&wire,
                  static_cast<uint32_t>(wire.size() - kEnvelopeHeaderSize));

    OpLogBatchRecord out;
    std::string reason;
    EXPECT_FALSE(DecodeOpLogBatchRecord(wire, &out, &reason));
    EXPECT_FALSE(reason.empty());
}

TEST(OpLogBatchBinaryValidationTest, RejectsCorruptedBatchChecksum) {
    auto wire = Wire();
    // Flip the final checksum byte; the body stays well-formed.
    wire.back() =
        static_cast<char>(static_cast<unsigned char>(wire.back()) ^ 0x01);

    OpLogBatchRecord out;
    std::string reason;
    EXPECT_FALSE(DecodeOpLogBatchRecord(wire, &out, &reason));
    EXPECT_NE(std::string::npos, reason.find("checksum"));
}

TEST(OpLogBatchBinaryValidationTest,
     RejectsPayloadMutationThatKeepsOldChecksum) {
    const auto batch =
        MakeBatch(15, {MakeEntry(60, OpType::PUT_END, "key", "payload-A")});
    auto wire = EncodeOpLogBatchRecordBinaryForTest(batch);
    size_t payload_offset = 0;
    ASSERT_TRUE(FindFirstEntryPayload(wire, "payload-A", &payload_offset))
        << "fixture layout walk failed";
    wire[payload_offset + 7] = 'B';
    ASSERT_EQ(std::string("payloadB"), wire.substr(payload_offset, 8));

    OpLogBatchRecord out;
    std::string reason;
    EXPECT_FALSE(DecodeOpLogBatchRecord(wire, &out, &reason));
    EXPECT_NE(std::string::npos, reason.find("checksum"));
}

TEST(OpLogBatchBinaryValidationTest,
     RejectsAttributeMutationWithStaleChecksum) {
    const auto wire = EncodeOpLogBatchRecordBinaryForTest(
        MakeBatch(16, {MakeEntry(70, OpType::PUT_END, "key", "value")}));

    for (size_t index = kEnvelopeHeaderSize; index < wire.size(); ++index) {
        auto mutated = wire;
        mutated[index] = static_cast<char>(
            static_cast<unsigned char>(mutated[index]) ^ 0x01);
        OpLogBatchRecord out;
        std::string reason;
        // Either a strict type/framing error or a checksum mismatch must reject
        // the record; a successful decode would mean a silently accepted
        // semantic change.
        EXPECT_FALSE(DecodeOpLogBatchRecord(mutated, &out, &reason))
            << "byte index " << index;
    }
}

// ---------------------------------------------------------------------------
// B13/B14: null outputs, sentinel preservation, and allocation bounds.
// ---------------------------------------------------------------------------
TEST(OpLogBatchBinarySafetyTest, NullOutputFailsWithoutCrashing) {
    std::string reason;
    EXPECT_FALSE(DecodeOpLogBatchRecord(Wire(), nullptr, &reason));
    EXPECT_FALSE(reason.empty());

    // A null reason pointer must still be accepted for a valid record.
    OpLogBatchRecord out;
    EXPECT_TRUE(DecodeOpLogBatchRecord(Wire(), &out, nullptr));
    EXPECT_TRUE(DecodeOpLogBatchRecordBinary(Wire(), &out, nullptr));
}

TEST(OpLogBatchBinarySafetyTest, FailureLeavesCallerRecordUnchanged) {
    OpLogBatchRecord out = MakeBatch(
        1234, {MakeEntry(4321, OpType::REMOVE, "sentinel-key", "sentinel")});
    const OpLogBatchRecord sentinel = out;

    auto bad_checksum = Wire();
    bad_checksum.back() = static_cast<char>(
        static_cast<unsigned char>(bad_checksum.back()) ^ 0xFF);
    std::string reason;
    EXPECT_FALSE(DecodeOpLogBatchRecord(bad_checksum, &out, &reason));
    EXPECT_TRUE(SameBatch(sentinel, out));
    EXPECT_FALSE(reason.empty());
    // The reason must stay short and must not embed payload contents.
    EXPECT_LT(reason.size(), 128u);
    EXPECT_EQ(std::string::npos, reason.find("value"));
}

TEST(OpLogBatchBinarySafetyTest, ForgedHugeLengthsFailWithoutHugeAllocation) {
    // A bin32 header declaring ~4 GiB while only a few bytes remain must be
    // rejected by the remaining-input check before any allocation.
    std::string wire(kEnvelopeHeaderSize, '\0');
    wire.replace(0, kOpLogBatchEnvelopeMagicSize, kEnvelopeMagic);
    wire[8] = 1;
    wire[9] = 1;
    wire.append(std::string("\x96\x01\x01\x01\x01\x91\x94\x01\xA1k\xA1k", 13));
    wire.append(std::string("\xC6\xFF\xFF\xFF\xFF", 5));
    SetBodyLength(&wire,
                  static_cast<uint32_t>(wire.size() - kEnvelopeHeaderSize));

    OpLogBatchRecord out;
    std::string reason;
    EXPECT_FALSE(DecodeOpLogBatchRecord(wire, &out, &reason));
    EXPECT_FALSE(reason.empty());
}

TEST(OpLogBatchBinarySafetyTest,
     ForgedHugeEntryCountFailsWithoutHugeAllocation) {
    std::string wire(kEnvelopeHeaderSize, '\0');
    wire.replace(0, kOpLogBatchEnvelopeMagicSize, kEnvelopeMagic);
    wire[8] = 1;
    wire[9] = 1;
    // Six root elements, then array32 declaring ~4 billion entries.
    wire.append(std::string("\x96\x01\x01\x01\x01\xDD\xFF\xFF\xFF\xFF", 10));
    SetBodyLength(&wire,
                  static_cast<uint32_t>(wire.size() - kEnvelopeHeaderSize));

    OpLogBatchRecord out;
    std::string reason;
    EXPECT_FALSE(DecodeOpLogBatchRecord(wire, &out, &reason));
    EXPECT_NE(std::string::npos, reason.find("entry count"));
}

#if defined(__SANITIZE_ADDRESS__) || defined(__SANITIZE_THREAD__)
TEST(OpLogBatchBinarySafetyTest, DeepNestingIsNotReachableThroughTheSchema) {
    // Nested arrays are only accepted at the two schema levels; a third nesting
    // level is a type error, not a recursion, so no stack growth is possible.
    std::string wire = Wire();
    const size_t entries_offset =
        FirstOffsetOf(wire, std::string("\x91\x94", 2));
    ASSERT_NE(std::string::npos, entries_offset);
    wire.replace(entries_offset, 2, std::string("\x91\x91", 2));
    SetBodyLength(&wire,
                  static_cast<uint32_t>(wire.size() - kEnvelopeHeaderSize));

    OpLogBatchRecord out;
    std::string reason;
    EXPECT_FALSE(DecodeOpLogBatchRecord(wire, &out, &reason));
}
#endif

// ---------------------------------------------------------------------------
// Property tests over constrained logical batches with a fixed seed.
// ---------------------------------------------------------------------------
TEST(OpLogBatchBinaryPropertyTest, RoundTripsFixedSeedBatchPopulation) {
    std::mt19937_64 rng(0x5eed1234ULL);
    std::uniform_int_distribution<uint32_t> op_dist(
        1, static_cast<uint32_t>(OpType::OP_TYPE_MAX) - 1);
    std::uniform_int_distribution<size_t> count_dist(1, 8);
    std::uniform_int_distribution<size_t> key_dist(0, 64);
    std::uniform_int_distribution<size_t> payload_dist(0, 512);
    std::uniform_int_distribution<int> byte_dist(0, 255);

    for (int iteration = 0; iteration < 1000; ++iteration) {
        const size_t count = count_dist(rng);
        std::vector<OpLogEntry> entries;
        entries.reserve(count);
        const uint64_t first_seq = 1 + static_cast<uint64_t>(rng() % 1000000);
        for (size_t i = 0; i < count; ++i) {
            // Keys stay within the printable ASCII domain the legacy JSON
            // writer can represent losslessly; payloads deliberately cover all
            // 256 byte values. Key fidelity for arbitrary bytes is
            // characterized separately below.
            std::string key(key_dist(rng), '\0');
            for (auto& ch : key) {
                ch = static_cast<char>(32 + (byte_dist(rng) % 95));
            }
            std::string payload(payload_dist(rng), '\0');
            for (auto& ch : payload) {
                ch = static_cast<char>(byte_dist(rng));
            }
            entries.push_back(MakeEntry(first_seq + i,
                                        static_cast<OpType>(op_dist(rng)),
                                        std::move(key), std::move(payload)));
        }
        const auto batch = MakeBatch(1 + static_cast<uint64_t>(rng() % 100000),
                                     std::move(entries));
        const auto wire = EncodeOpLogBatchRecordBinaryForTest(batch);

        OpLogBatchRecord decoded;
        std::string reason;
        ASSERT_TRUE(DecodeOpLogBatchRecord(wire, &decoded, &reason))
            << "iteration=" << iteration << " reason=" << reason;
        ASSERT_TRUE(SameBatch(batch, decoded)) << "iteration=" << iteration;

        // The binary and JSON readers must agree on the logical batch.
        OpLogBatchRecord from_json;
        std::string json_reason;
        ASSERT_TRUE(DecodeOpLogBatchRecord(EncodeOpLogBatchRecord(batch),
                                           &from_json, &json_reason))
            << "iteration=" << iteration << " reason=" << json_reason;
        ASSERT_TRUE(SameBatch(from_json, decoded)) << "iteration=" << iteration;
    }
}

TEST(OpLogBatchBinaryPropertyTest, RejectsSingleBitFlipsInFixedSeedSample) {
    std::mt19937_64 rng(0xf11a5eedULL);
    for (int iteration = 0; iteration < 200; ++iteration) {
        const auto batch = MakeBatch(
            17,
            {MakeEntry(80, OpType::PUT_END, "stable-key", "stable-payload")});
        auto wire = EncodeOpLogBatchRecordBinaryForTest(batch);
        const size_t index = static_cast<size_t>(rng() % wire.size());
        const auto bit = static_cast<unsigned>(rng() % 8);
        wire[index] = static_cast<char>(
            static_cast<unsigned char>(wire[index]) ^ (1u << bit));

        OpLogBatchRecord out;
        std::string reason;
        const bool accepted = DecodeOpLogBatchRecord(wire, &out, &reason);
        if (accepted) {
            // The only acceptable acceptance is a logically identical batch,
            // which a bit flip cannot produce without contradicting the
            // canonical checksum.
            EXPECT_TRUE(SameBatch(batch, out))
                << "iteration=" << iteration << " index=" << index
                << " bit=" << bit;
        }
    }
}

}  // namespace mooncake::test
