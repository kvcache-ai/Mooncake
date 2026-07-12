#include "ha/oplog/oplog_batch_standby_reader.h"

#include <gtest/gtest.h>
#include <xxhash.h>

#include <map>
#include <string_view>
#include <string>
#include <vector>

#include "ha/kv/ha_kv_backend.h"
#include "ha/oplog/oplog_applier.h"
#include "ha/oplog/oplog_batch_codec.h"
#include "mock_metadata_store.h"

namespace mooncake::test {
namespace {

class FakeHaKvBackend : public HaKvBackend {
   public:
    ErrorCode Get(std::string_view key, std::string& value) override {
        auto it = values_.find(std::string(key));
        if (it == values_.end()) {
            return ErrorCode::ETCD_KEY_NOT_EXIST;
        }
        value = it->second;
        return ErrorCode::OK;
    }
    ErrorCode Put(std::string_view key, std::string_view value) override {
        values_[std::string(key)] = std::string(value);
        return ErrorCode::OK;
    }
    ErrorCode Range(std::string_view begin_key, std::string_view end_key,
                    size_t limit, std::vector<KvPair>& kvs) override {
        kvs.clear();
        ++range_calls_;
        for (const auto& [key, value] : values_) {
            if (key >= begin_key && key < end_key) {
                kvs.push_back({.key = key, .value = value});
                if (limit != 0 && kvs.size() >= limit) break;
            }
        }
        return ErrorCode::OK;
    }
    bool SupportsTxn() const override { return true; }
    ErrorCode Txn(const KvTxn&) override { return ErrorCode::OK; }

    int range_calls() const { return range_calls_; }

   private:
    std::map<std::string, std::string> values_;
    int range_calls_{0};
};

OpLogEntry MakeEntry(uint64_t sequence_id) {
    OpLogEntry entry;
    entry.sequence_id = sequence_id;
    entry.op_type = OpType::REMOVE;
    entry.tenant_id = "tenant";
    entry.object_key = "key" + std::to_string(sequence_id);
    entry.checksum = static_cast<uint32_t>(
        XXH32(entry.payload.data(), entry.payload.size(), 0));
    entry.prefix_hash = static_cast<uint32_t>(
        XXH32(entry.object_key.data(), entry.object_key.size(), 0));
    return entry;
}

OpLogBatchRecord MakeBatch(uint64_t batch_id, uint64_t first_seq,
                           uint64_t last_seq) {
    OpLogBatchRecord batch;
    batch.batch_id = batch_id;
    batch.first_seq = first_seq;
    batch.last_seq = last_seq;
    for (uint64_t seq = first_seq; seq <= last_seq; ++seq) {
        batch.entries.push_back(MakeEntry(seq));
    }
    return batch;
}

}  // namespace

TEST(OpLogBatchStandbyReaderTest, LegacyPathIsUsedWhenDurablePrefixIsAbsent) {
    FakeHaKvBackend backend;
    MockMetadataStore metadata_store;
    OpLogApplier applier(&metadata_store, "clusterA");
    OpLogBatchStandbyReader reader("clusterA", backend, applier);

    auto result = reader.PollOnce();

    ASSERT_EQ(ErrorCode::OK, result.error);
    EXPECT_TRUE(result.used_legacy_path);
    EXPECT_EQ(0u, result.applied_entries);
    EXPECT_EQ(1u, applier.GetExpectedSequenceId());
}

TEST(OpLogBatchStandbyReaderTest, RangeReadsBatchesWhenDurablePrefixAdvances) {
    FakeHaKvBackend backend;
    ASSERT_EQ(ErrorCode::OK,
              backend.Put(BuildDurablePrefixKey("clusterA"),
                          EncodeDurablePrefix({.batch_id = 1, .last_seq = 2})));
    ASSERT_EQ(ErrorCode::OK,
              backend.Put(BuildBatchRecordKey("clusterA", 1),
                          EncodeOpLogBatchRecord(MakeBatch(1, 1, 2))));
    MockMetadataStore metadata_store;
    OpLogApplier applier(&metadata_store, "clusterA");
    OpLogBatchStandbyReader reader("clusterA", backend, applier);

    auto result = reader.PollOnce();

    ASSERT_EQ(ErrorCode::OK, result.error);
    EXPECT_FALSE(result.used_legacy_path);
    EXPECT_EQ(1, backend.range_calls());
    EXPECT_EQ(2u, result.applied_entries);
    EXPECT_EQ(3u, applier.GetExpectedSequenceId());
}

TEST(OpLogBatchStandbyReaderTest, DoesNothingWhenDurablePrefixDoesNotAdvance) {
    FakeHaKvBackend backend;
    ASSERT_EQ(ErrorCode::OK,
              backend.Put(BuildDurablePrefixKey("clusterA"),
                          EncodeDurablePrefix({.batch_id = 1, .last_seq = 1})));
    ASSERT_EQ(ErrorCode::OK,
              backend.Put(BuildBatchRecordKey("clusterA", 1),
                          EncodeOpLogBatchRecord(MakeBatch(1, 1, 1))));
    MockMetadataStore metadata_store;
    OpLogApplier applier(&metadata_store, "clusterA");
    OpLogBatchStandbyReader reader("clusterA", backend, applier);

    ASSERT_EQ(ErrorCode::OK, reader.PollOnce().error);
    int range_calls_after_first_poll = backend.range_calls();
    auto second = reader.PollOnce();

    ASSERT_EQ(ErrorCode::OK, second.error);
    EXPECT_FALSE(second.used_legacy_path);
    EXPECT_EQ(0u, second.applied_entries);
    EXPECT_EQ(range_calls_after_first_poll, backend.range_calls());
    EXPECT_EQ(2u, applier.GetExpectedSequenceId());
}

TEST(OpLogBatchStandbyReaderTest, StopsApplyingAtDurablePrefixLastSeq) {
    FakeHaKvBackend backend;
    ASSERT_EQ(ErrorCode::OK,
              backend.Put(BuildDurablePrefixKey("clusterA"),
                          EncodeDurablePrefix({.batch_id = 1, .last_seq = 2})));
    ASSERT_EQ(ErrorCode::OK,
              backend.Put(BuildBatchRecordKey("clusterA", 1),
                          EncodeOpLogBatchRecord(MakeBatch(1, 1, 3))));
    MockMetadataStore metadata_store;
    OpLogApplier applier(&metadata_store, "clusterA");
    OpLogBatchStandbyReader reader("clusterA", backend, applier);

    auto result = reader.PollOnce();

    ASSERT_EQ(ErrorCode::OK, result.error);
    EXPECT_EQ(2u, result.applied_entries);
    EXPECT_EQ(3u, applier.GetExpectedSequenceId());
}

TEST(OpLogBatchStandbyReaderTest, AppliesExpandedEntriesInSequenceOrder) {
    FakeHaKvBackend backend;
    ASSERT_EQ(ErrorCode::OK,
              backend.Put(BuildDurablePrefixKey("clusterA"),
                          EncodeDurablePrefix({.batch_id = 2, .last_seq = 3})));
    ASSERT_EQ(ErrorCode::OK,
              backend.Put(BuildBatchRecordKey("clusterA", 1),
                          EncodeOpLogBatchRecord(MakeBatch(1, 1, 1))));
    ASSERT_EQ(ErrorCode::OK,
              backend.Put(BuildBatchRecordKey("clusterA", 2),
                          EncodeOpLogBatchRecord(MakeBatch(2, 2, 3))));
    MockMetadataStore metadata_store;
    OpLogApplier applier(&metadata_store, "clusterA");
    OpLogBatchStandbyReader reader("clusterA", backend, applier);

    auto result = reader.PollOnce();

    ASSERT_EQ(ErrorCode::OK, result.error);
    EXPECT_EQ(3u, result.applied_entries);
    EXPECT_EQ(4u, applier.GetExpectedSequenceId());
}

TEST(OpLogBatchStandbyReaderTest, AcceptsFirstBatchAtLegacyLatestPlusOne) {
    FakeHaKvBackend backend;
    ASSERT_EQ(ErrorCode::OK,
              backend.Put(BuildDurablePrefixKey("clusterA"),
                          EncodeDurablePrefix({.batch_id = 1, .last_seq = 4})));
    ASSERT_EQ(ErrorCode::OK,
              backend.Put(BuildBatchRecordKey("clusterA", 1),
                          EncodeOpLogBatchRecord(MakeBatch(1, 3, 4))));
    MockMetadataStore metadata_store;
    OpLogApplier applier(&metadata_store, "clusterA");
    applier.Recover(2);
    OpLogBatchStandbyReader reader("clusterA", backend, applier);

    auto result = reader.PollOnce();

    ASSERT_EQ(ErrorCode::OK, result.error);
    EXPECT_EQ(2u, result.applied_entries);
    EXPECT_EQ(5u, applier.GetExpectedSequenceId());
}

TEST(OpLogBatchStandbyReaderTest,
     WaitsWhenLegacyHasNotReachedFirstBatchSequence) {
    FakeHaKvBackend backend;
    ASSERT_EQ(ErrorCode::OK,
              backend.Put(BuildDurablePrefixKey("clusterA"),
                          EncodeDurablePrefix({.batch_id = 1, .last_seq = 4})));
    ASSERT_EQ(ErrorCode::OK,
              backend.Put(BuildBatchRecordKey("clusterA", 1),
                          EncodeOpLogBatchRecord(MakeBatch(1, 4, 4))));
    MockMetadataStore metadata_store;
    OpLogApplier applier(&metadata_store, "clusterA");
    applier.Recover(2);
    OpLogBatchStandbyReader reader("clusterA", backend, applier);

    auto result = reader.PollOnce();

    EXPECT_EQ(ErrorCode::OK, result.error);
    EXPECT_TRUE(result.waiting_for_legacy_catch_up);
    EXPECT_EQ(0u, result.applied_entries);
    EXPECT_EQ(3u, applier.GetExpectedSequenceId());
}

TEST(OpLogBatchStandbyReaderTest, FailsWhenLaterBatchHasSequenceGap) {
    FakeHaKvBackend backend;
    ASSERT_EQ(ErrorCode::OK,
              backend.Put(BuildDurablePrefixKey("clusterA"),
                          EncodeDurablePrefix({.batch_id = 2, .last_seq = 4})));
    ASSERT_EQ(ErrorCode::OK,
              backend.Put(BuildBatchRecordKey("clusterA", 1),
                          EncodeOpLogBatchRecord(MakeBatch(1, 1, 2))));
    ASSERT_EQ(ErrorCode::OK,
              backend.Put(BuildBatchRecordKey("clusterA", 2),
                          EncodeOpLogBatchRecord(MakeBatch(2, 4, 4))));
    MockMetadataStore metadata_store;
    OpLogApplier applier(&metadata_store, "clusterA");
    OpLogBatchStandbyReader reader("clusterA", backend, applier);

    auto result = reader.PollOnce();

    EXPECT_NE(ErrorCode::OK, result.error);
    EXPECT_FALSE(result.waiting_for_legacy_catch_up);
    EXPECT_EQ(2u, result.applied_entries);
    EXPECT_EQ(3u, applier.GetExpectedSequenceId());
}

TEST(OpLogBatchStandbyReaderTest, MissingBatchMarksReaderUnhealthy) {
    FakeHaKvBackend backend;
    ASSERT_EQ(ErrorCode::OK,
              backend.Put(BuildDurablePrefixKey("clusterA"),
                          EncodeDurablePrefix({.batch_id = 2, .last_seq = 2})));
    MockMetadataStore metadata_store;
    OpLogApplier applier(&metadata_store, "clusterA");
    OpLogBatchStandbyReader reader("clusterA", backend, applier);

    auto result = reader.PollOnce();

    EXPECT_NE(ErrorCode::OK, result.error);
    EXPECT_EQ(0u, result.applied_entries);
    EXPECT_EQ(1u, applier.GetExpectedSequenceId());
}

TEST(OpLogBatchStandbyReaderTest,
     LaterBatchWithContiguousEntriesStillRequiresPreviousBatch) {
    FakeHaKvBackend backend;
    ASSERT_EQ(ErrorCode::OK,
              backend.Put(BuildDurablePrefixKey("clusterA"),
                          EncodeDurablePrefix({.batch_id = 2, .last_seq = 2})));
    ASSERT_EQ(ErrorCode::OK,
              backend.Put(BuildBatchRecordKey("clusterA", 2),
                          EncodeOpLogBatchRecord(MakeBatch(2, 1, 2))));
    MockMetadataStore metadata_store;
    OpLogApplier applier(&metadata_store, "clusterA");
    OpLogBatchStandbyReader reader("clusterA", backend, applier);

    auto result = reader.PollOnce();

    EXPECT_NE(ErrorCode::OK, result.error);
    EXPECT_EQ(0u, result.applied_entries);
    EXPECT_EQ(1u, applier.GetExpectedSequenceId());
}

TEST(OpLogBatchStandbyReaderTest, ChecksumFailureMarksReaderUnhealthy) {
    FakeHaKvBackend backend;
    ASSERT_EQ(ErrorCode::OK,
              backend.Put(BuildDurablePrefixKey("clusterA"),
                          EncodeDurablePrefix({.batch_id = 1, .last_seq = 1})));
    std::string encoded = EncodeOpLogBatchRecord(MakeBatch(1, 1, 1));
    encoded.back() = static_cast<char>(encoded.back() ^ 0x1);
    ASSERT_EQ(ErrorCode::OK,
              backend.Put(BuildBatchRecordKey("clusterA", 1), encoded));
    MockMetadataStore metadata_store;
    OpLogApplier applier(&metadata_store, "clusterA");
    OpLogBatchStandbyReader reader("clusterA", backend, applier);

    auto result = reader.PollOnce();

    EXPECT_NE(ErrorCode::OK, result.error);
    EXPECT_EQ(0u, result.applied_entries);
    EXPECT_EQ(1u, applier.GetExpectedSequenceId());
}

TEST(OpLogBatchStandbyReaderTest, BatchRecordModeDoesNotAdvanceAcrossGap) {
    FakeHaKvBackend backend;
    ASSERT_EQ(ErrorCode::OK,
              backend.Put(BuildDurablePrefixKey("clusterA"),
                          EncodeDurablePrefix({.batch_id = 1, .last_seq = 2})));
    ASSERT_EQ(ErrorCode::OK,
              backend.Put(BuildBatchRecordKey("clusterA", 1),
                          EncodeOpLogBatchRecord(MakeBatch(1, 2, 2))));
    MockMetadataStore metadata_store;
    OpLogApplier applier(&metadata_store, "clusterA");
    OpLogBatchStandbyReader reader("clusterA", backend, applier);

    auto failed = reader.PollOnce();
    EXPECT_EQ(ErrorCode::OK, failed.error);
    EXPECT_TRUE(failed.waiting_for_legacy_catch_up);
    EXPECT_EQ(1u, applier.GetExpectedSequenceId());

    ASSERT_EQ(ErrorCode::OK,
              backend.Put(BuildBatchRecordKey("clusterA", 1),
                          EncodeOpLogBatchRecord(MakeBatch(1, 1, 2))));
    auto repaired = reader.PollOnce();

    ASSERT_EQ(ErrorCode::OK, repaired.error);
    EXPECT_EQ(2u, repaired.applied_entries);
    EXPECT_EQ(3u, applier.GetExpectedSequenceId());
}

TEST(OpLogBatchStandbyReaderTest, AlreadyAppliedEntriesAreSkipped) {
    FakeHaKvBackend backend;
    ASSERT_EQ(ErrorCode::OK,
              backend.Put(BuildDurablePrefixKey("clusterA"),
                          EncodeDurablePrefix({.batch_id = 1, .last_seq = 3})));
    ASSERT_EQ(ErrorCode::OK,
              backend.Put(BuildBatchRecordKey("clusterA", 1),
                          EncodeOpLogBatchRecord(MakeBatch(1, 1, 3))));
    MockMetadataStore metadata_store;
    OpLogApplier applier(&metadata_store, "clusterA");
    applier.Recover(2);
    OpLogBatchStandbyReader reader("clusterA", backend, applier);

    auto result = reader.PollOnce();

    ASSERT_EQ(ErrorCode::OK, result.error);
    EXPECT_EQ(1u, result.applied_entries);
    EXPECT_EQ(4u, applier.GetExpectedSequenceId());
}

}  // namespace mooncake::test
