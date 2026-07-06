#include "ha/oplog/oplog_batch_codec.h"
#include "ha/oplog/oplog_batch_types.h"

#include <gtest/gtest.h>
#include <xxhash.h>

#include <string>
#include <vector>

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
    auto encoded = EncodeOpLogBatchRecord(MakeBatch(3, {MakeEntry(10)}));
    auto pos = encoded.find("\"checksum\"");
    ASSERT_NE(std::string::npos, pos);
    pos = encoded.find_first_of("0123456789", pos);
    ASSERT_NE(std::string::npos, pos);
    encoded[pos] = encoded[pos] == '0' ? '1' : '0';

    OpLogBatchRecord out;
    std::string reason;
    EXPECT_FALSE(DecodeOpLogBatchRecord(encoded, &out, &reason));
    EXPECT_FALSE(reason.empty());
}

TEST(OpLogBatchRecordCodecTest, RejectsMalformedTypedFieldsWithoutThrowing) {
    OpLogBatchRecord out;
    std::string reason;
    EXPECT_NO_THROW(EXPECT_FALSE(DecodeOpLogBatchRecord(
        R"({"schema_version":"x","batch_id":"x","first_seq":"x","last_seq":"x","entries":[],"checksum":"x"})",
        &out, &reason)));
    EXPECT_FALSE(reason.empty());
}

TEST(OpLogBatchRecordCodecTest, RejectsMissingRequiredFieldsBeforeChecksum) {
    auto encoded = EncodeOpLogBatchRecord(MakeBatch(3, {MakeEntry(10)}));
    auto pos = encoded.find("\"batch_id\":3,");
    ASSERT_NE(std::string::npos, pos);
    encoded.erase(pos, std::string("\"batch_id\":3,").size());

    OpLogBatchRecord out;
    std::string reason;
    EXPECT_FALSE(DecodeOpLogBatchRecord(encoded, &out, &reason));
    EXPECT_NE(std::string::npos, reason.find("missing"));
}

TEST(OpLogBatchRecordCodecTest, RejectsCorruptedSequenceContinuity) {
    auto in = MakeBatch(3, {MakeEntry(10), MakeEntry(11)});
    auto encoded = EncodeOpLogBatchRecord(in);
    auto pos = encoded.find("\"last_seq\":11");
    ASSERT_NE(std::string::npos, pos);
    encoded.replace(pos, std::string("\"last_seq\":11").size(),
                    "\"last_seq\":12");

    OpLogBatchRecord out;
    std::string reason;
    EXPECT_FALSE(DecodeOpLogBatchRecord(encoded, &out, &reason));
    EXPECT_FALSE(reason.empty());
}

TEST(OpLogBatchRecordCodecTest, RejectsUnsupportedSchemaVersion) {
    auto encoded = EncodeOpLogBatchRecord(MakeBatch(3, {MakeEntry(10)}));
    auto pos = encoded.find("\"schema_version\":1");
    ASSERT_NE(std::string::npos, pos);
    encoded.replace(pos, std::string("\"schema_version\":1").size(),
                    "\"schema_version\":2");

    OpLogBatchRecord out;
    std::string reason;
    EXPECT_FALSE(DecodeOpLogBatchRecord(encoded, &out, &reason));
    EXPECT_FALSE(reason.empty());
}

}  // namespace mooncake::test
