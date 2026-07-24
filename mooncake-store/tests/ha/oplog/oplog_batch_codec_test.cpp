#include "ha/oplog/oplog_batch_codec.h"
#include "ha/oplog/oplog_batch_types.h"

#include <gtest/gtest.h>
#include <xxhash.h>

#include <memory>
#include <sstream>
#include <string>
#include <vector>

#if __has_include(<jsoncpp/json/json.h>)
#include <jsoncpp/json/json.h>
#else
#include <json/json.h>
#endif

namespace mooncake::test {

namespace {

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

Json::Value ParseJson(const std::string& value) {
    Json::Value root;
    Json::CharReaderBuilder builder;
    std::string errors;
    std::istringstream stream(value);
    EXPECT_TRUE(Json::parseFromStream(builder, stream, &root, &errors))
        << errors;
    return root;
}

std::string WriteJson(const Json::Value& root) {
    Json::StreamWriterBuilder builder;
    builder["indentation"] = "";
    return Json::writeString(builder, root);
}

}  // namespace

TEST(OpLogBatchTypesTest, RejectsEmptyEntries) {
    OpLogBatchRecord batch;
    batch.batch_id = 1;
    batch.first_seq = 1;
    batch.last_seq = 1;

    std::string reason;
    EXPECT_FALSE(ValidateOpLogBatchRecordShape(batch, &reason));
    EXPECT_FALSE(reason.empty());
}

TEST(OpLogBatchTypesTest, RejectsFirstSeqMismatch) {
    auto batch = MakeBatch(1, {MakeEntry(2)});
    batch.first_seq = 1;

    std::string reason;
    EXPECT_FALSE(ValidateOpLogBatchRecordShape(batch, &reason));
    EXPECT_FALSE(reason.empty());
}

TEST(OpLogBatchTypesTest, RejectsLastSeqMismatch) {
    auto batch = MakeBatch(1, {MakeEntry(1)});
    batch.last_seq = 2;

    std::string reason;
    EXPECT_FALSE(ValidateOpLogBatchRecordShape(batch, &reason));
    EXPECT_FALSE(reason.empty());
}

TEST(OpLogBatchTypesTest, RejectsNonContiguousEntrySequence) {
    auto batch = MakeBatch(1, {MakeEntry(1), MakeEntry(3)});
    batch.last_seq = 3;

    std::string reason;
    EXPECT_FALSE(ValidateOpLogBatchRecordShape(batch, &reason));
    EXPECT_FALSE(reason.empty());
}

TEST(OpLogBatchTypesTest, RejectsZeroSchemaVersion) {
    auto batch = MakeBatch(1, {MakeEntry(1)});
    batch.schema_version = 0;

    std::string reason;
    EXPECT_FALSE(ValidateOpLogBatchRecordShape(batch, &reason));
    EXPECT_FALSE(reason.empty());
}

TEST(OpLogBatchTypesTest, AcceptsValidContiguousBatch) {
    auto batch = MakeBatch(1, {MakeEntry(1), MakeEntry(2), MakeEntry(3)});

    std::string reason;
    EXPECT_TRUE(ValidateOpLogBatchRecordShape(batch, &reason));
    EXPECT_TRUE(reason.empty());
}

TEST(OpLogBatchKeyLayoutTest, BuildsPaddedBatchRecordKey) {
    EXPECT_EQ("/oplog/clusterA/batches/00000000000000000001",
              BuildBatchRecordKey("clusterA", 1));
}

TEST(OpLogBatchKeyLayoutTest, BuildsDurablePrefixKey) {
    EXPECT_EQ("/oplog/clusterA/durable_prefix",
              BuildDurablePrefixKey("clusterA"));
}

TEST(OpLogBatchKeyLayoutTest, BuildsBatchRangeBounds) {
    auto bounds = BuildBatchRecordRange("clusterA", 7);
    EXPECT_EQ("/oplog/clusterA/batches/00000000000000000008", bounds.begin_key);
    EXPECT_EQ("/oplog/clusterA/batches0", bounds.end_key);
}

TEST(OpLogBatchKeyLayoutTest, MaxBatchRangeIsEmpty) {
    auto bounds = BuildBatchRecordRange("clusterA", UINT64_MAX);
    EXPECT_EQ(bounds.begin_key, bounds.end_key);
}

TEST(OpLogBatchKeyLayoutTest, RejectsInvalidClusterId) {
    std::string reason;
    EXPECT_FALSE(ValidateOpLogBatchClusterId("bad/cluster", &reason));
    EXPECT_FALSE(reason.empty());
    EXPECT_TRUE(BuildBatchRecordKey("bad/cluster", 1).empty());
    EXPECT_TRUE(BuildDurablePrefixKey("bad/cluster").empty());
    auto bounds = BuildBatchRecordRange("bad/cluster", 1);
    EXPECT_TRUE(bounds.begin_key.empty());
    EXPECT_TRUE(bounds.end_key.empty());
}

TEST(OpLogBatchKeyLayoutTest, NormalizesTrailingSlashClusterId) {
    EXPECT_EQ("/oplog/clusterA/batches/00000000000000000001",
              BuildBatchRecordKey("clusterA/", 1));
}

TEST(OpLogDurablePrefixCodecTest, RoundTripsZeroPrefix) {
    DurablePrefix in;
    auto encoded = EncodeDurablePrefix(in);

    DurablePrefix out;
    std::string reason;
    ASSERT_TRUE(DecodeDurablePrefix(encoded, &out, &reason));
    EXPECT_EQ(in.batch_id, out.batch_id);
    EXPECT_EQ(in.last_seq, out.last_seq);
    EXPECT_TRUE(reason.empty());
}

TEST(OpLogDurablePrefixCodecTest, RoundTripsNonZeroPrefix) {
    DurablePrefix in{.batch_id = 9, .last_seq = 1024};
    auto encoded = EncodeDurablePrefix(in);

    DurablePrefix out;
    std::string reason;
    ASSERT_TRUE(DecodeDurablePrefix(encoded, &out, &reason));
    EXPECT_EQ(in.batch_id, out.batch_id);
    EXPECT_EQ(in.last_seq, out.last_seq);
    EXPECT_TRUE(reason.empty());
}

TEST(OpLogDurablePrefixCodecTest, RejectsMalformedPayload) {
    DurablePrefix out;
    std::string reason;
    EXPECT_FALSE(DecodeDurablePrefix("{", &out, &reason));
    EXPECT_FALSE(reason.empty());
}

TEST(OpLogDurablePrefixCodecTest, RejectsMalformedTypedFieldsWithoutThrowing) {
    DurablePrefix out;
    std::string reason;
    EXPECT_NO_THROW(EXPECT_FALSE(DecodeDurablePrefix(
        R"({"schema_version":"x","batch_id":"x","last_seq":"x"})", &out,
        &reason)));
    EXPECT_FALSE(reason.empty());
}

TEST(OpLogDurablePrefixCodecTest, RejectsUnsupportedSchemaVersion) {
    DurablePrefix out;
    std::string reason;
    EXPECT_FALSE(DecodeDurablePrefix(
        R"({"schema_version":2,"batch_id":1,"last_seq":1})", &out, &reason));
    EXPECT_FALSE(reason.empty());
}

TEST(OpLogBatchRecordCodecTest, RoundTripsSingleEntryBatch) {
    auto in = MakeBatch(3, {MakeEntry(10)});
    auto encoded = EncodeOpLogBatchRecord(in);

    OpLogBatchRecord out;
    std::string reason;
    ASSERT_TRUE(DecodeOpLogBatchRecord(encoded, &out, &reason));
    EXPECT_EQ(in.batch_id, out.batch_id);
    EXPECT_EQ(in.first_seq, out.first_seq);
    EXPECT_EQ(in.last_seq, out.last_seq);
    ASSERT_EQ(1u, out.entries.size());
    EXPECT_EQ(in.entries[0].sequence_id, out.entries[0].sequence_id);
    EXPECT_EQ(in.entries[0].payload, out.entries[0].payload);
    EXPECT_TRUE(reason.empty());
}

TEST(OpLogBatchRecordCodecTest, EncodesCompactArraySchema) {
    const auto encoded = EncodeOpLogBatchRecord(MakeBatch(
        3,
        {MakeEntry(10, OpType::PUT_END, "key", std::string("\0binary", 7))}));
    const auto root = ParseJson(encoded);

    ASSERT_TRUE(root.isArray());
    ASSERT_EQ(6u, root.size());
    EXPECT_EQ(kOpLogBatchRecordSchemaVersion, root[0].asUInt());
    EXPECT_EQ(3u, root[1].asUInt64());
    EXPECT_EQ(10u, root[2].asUInt64());
    EXPECT_EQ(10u, root[3].asUInt64());
    ASSERT_TRUE(root[4].isArray());
    ASSERT_EQ(1u, root[4].size());
    ASSERT_TRUE(root[4][0].isArray());
    EXPECT_EQ(4u, root[4][0].size());
    EXPECT_EQ(static_cast<uint8_t>(OpType::PUT_END), root[4][0][0].asUInt());
    EXPECT_EQ("tenant", root[4][0][1].asString());
    EXPECT_EQ("key", root[4][0][2].asString());
    EXPECT_EQ("AGJpbmFyeQ==", root[4][0][3].asString());
    EXPECT_EQ(std::string::npos, encoded.find("timestamp_ms"));
    EXPECT_EQ(std::string::npos, encoded.find("prefix_hash"));
    EXPECT_EQ(std::string::npos, encoded.find("sequence_id"));
}

TEST(OpLogBatchRecordCodecTest, RebuildsNonPersistedEntryFields) {
    auto in = MakeBatch(3, {MakeEntry(10)});
    ASSERT_NE(0u, in.entries[0].timestamp_ms);
    ASSERT_NE(0u, in.entries[0].prefix_hash);

    OpLogBatchRecord out;
    std::string reason;
    ASSERT_TRUE(
        DecodeOpLogBatchRecord(EncodeOpLogBatchRecord(in), &out, &reason));
    ASSERT_EQ(1u, out.entries.size());
    EXPECT_EQ(0u, out.entries[0].timestamp_ms);
    EXPECT_EQ(0u, out.entries[0].prefix_hash);
    EXPECT_TRUE(VerifyOpLogChecksum(out.entries[0]));
}

TEST(OpLogBatchRecordCodecTest, RoundTripsMultiEntryBatch) {
    auto in = MakeBatch(
        3, {MakeEntry(10), MakeEntry(11, OpType::REMOVE, "dead-key", "")});
    auto encoded = EncodeOpLogBatchRecord(in);

    OpLogBatchRecord out;
    std::string reason;
    ASSERT_TRUE(DecodeOpLogBatchRecord(encoded, &out, &reason));
    EXPECT_EQ(in.batch_id, out.batch_id);
    EXPECT_EQ(in.first_seq, out.first_seq);
    EXPECT_EQ(in.last_seq, out.last_seq);
    ASSERT_EQ(2u, out.entries.size());
    EXPECT_EQ(in.entries[1].op_type, out.entries[1].op_type);
    EXPECT_EQ(in.entries[1].object_key, out.entries[1].object_key);
    EXPECT_EQ(in.entries[1].payload, out.entries[1].payload);
}

TEST(OpLogBatchRecordCodecTest, RejectsCorruptedChecksum) {
    auto root =
        ParseJson(EncodeOpLogBatchRecord(MakeBatch(3, {MakeEntry(10)})));
    ASSERT_TRUE(root.isArray());
    ASSERT_EQ(6u, root.size());
    root[5] = root[5].asUInt() + 1;

    OpLogBatchRecord out;
    std::string reason;
    EXPECT_FALSE(DecodeOpLogBatchRecord(WriteJson(root), &out, &reason));
    EXPECT_FALSE(reason.empty());
}

TEST(OpLogBatchRecordCodecTest, RejectsMalformedTypedFieldsWithoutThrowing) {
    OpLogBatchRecord out;
    std::string reason;
    EXPECT_NO_THROW(EXPECT_FALSE(
        DecodeOpLogBatchRecord(R"(["x","x","x","x",[],"x"])", &out, &reason)));
    EXPECT_FALSE(reason.empty());
}

TEST(OpLogBatchRecordCodecTest, RejectsWrongElementCount) {
    OpLogBatchRecord out;
    std::string reason;
    EXPECT_FALSE(DecodeOpLogBatchRecord(R"([1,3,10,10,[]])", &out, &reason));
    EXPECT_FALSE(reason.empty());
}

TEST(OpLogBatchRecordCodecTest, RejectsCorruptedSequenceContinuity) {
    auto in = MakeBatch(3, {MakeEntry(10), MakeEntry(11)});
    auto root = ParseJson(EncodeOpLogBatchRecord(in));
    ASSERT_TRUE(root.isArray());
    root[3] = Json::UInt64(12);

    OpLogBatchRecord out;
    std::string reason;
    EXPECT_FALSE(DecodeOpLogBatchRecord(WriteJson(root), &out, &reason));
    EXPECT_FALSE(reason.empty());
}

TEST(OpLogBatchRecordCodecTest, RejectsUnsupportedSchemaVersion) {
    auto root =
        ParseJson(EncodeOpLogBatchRecord(MakeBatch(3, {MakeEntry(10)})));
    ASSERT_TRUE(root.isArray());
    root[0] = kOpLogBatchRecordSchemaVersion + 1;

    OpLogBatchRecord out;
    std::string reason;
    EXPECT_FALSE(DecodeOpLogBatchRecord(WriteJson(root), &out, &reason));
    EXPECT_FALSE(reason.empty());
}

TEST(OpLogBatchRecordCodecTest, RejectsInvalidBase64Payload) {
    auto root =
        ParseJson(EncodeOpLogBatchRecord(MakeBatch(3, {MakeEntry(10)})));
    ASSERT_TRUE(root.isArray());
    root[4][0][3] = "%%%";
    Json::Value checksum_payload(Json::arrayValue);
    for (Json::ArrayIndex i = 0; i < 5; ++i) {
        checksum_payload.append(root[i]);
    }
    const auto serialized = WriteJson(checksum_payload);
    root[5] =
        static_cast<Json::UInt>(XXH32(serialized.data(), serialized.size(), 0));

    OpLogBatchRecord out;
    std::string reason;
    EXPECT_FALSE(DecodeOpLogBatchRecord(WriteJson(root), &out, &reason));
    EXPECT_NE(std::string::npos, reason.find("base64"));
}

}  // namespace mooncake::test
