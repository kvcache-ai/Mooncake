#include "ha/oplog/oplog_batch_standby_reader.h"

#include <gtest/gtest.h>
#include <xxhash.h>

#include <map>
#include <functional>
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
    ErrorCode DeleteRange(std::string_view, std::string_view) override {
        return ErrorCode::INVALID_PARAMS;
    }

    ErrorCode Get(std::string_view key, std::string& value) override {
        if (key.rfind("/oplog/clusterA/batches/", 0) == 0) {
            ++batch_get_calls_;
        }
        if (next_get_error_ != ErrorCode::OK) {
            auto error = next_get_error_;
            next_get_error_ = ErrorCode::OK;
            return error;
        }
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
        ++range_calls_;
        return RangeFrom(values_, begin_key, end_key, limit, kvs);
    }
    ErrorCode RangeSerializable(std::string_view begin_key,
                                std::string_view end_key, size_t limit,
                                std::vector<KvPair>& kvs) {
        ++serializable_range_calls_;
        if (before_serializable_range) {
            auto callback = std::move(before_serializable_range);
            callback();
        }
        return RangeFrom(local_view_enabled_ ? local_values_ : values_,
                         begin_key, end_key, limit, kvs);
    }
    bool SupportsTxn() const override { return true; }
    ErrorCode Txn(const KvTxn&) override { return ErrorCode::OK; }

    int range_calls() const { return range_calls_; }
    int serializable_range_calls() const { return serializable_range_calls_; }
    int batch_get_calls() const { return batch_get_calls_; }
    std::function<void()> before_range;
    std::function<void()> before_serializable_range;
    void Erase(std::string_view key) { values_.erase(std::string(key)); }
    void FailNextGet(ErrorCode error) { next_get_error_ = error; }
    void FailNextRange(ErrorCode error) { next_range_error_ = error; }
    void PublishLocal(std::string_view key) {
        local_view_enabled_ = true;
        const auto it = values_.find(std::string(key));
        if (it != values_.end()) local_values_[it->first] = it->second;
    }

   private:
    ErrorCode RangeFrom(const std::map<std::string, std::string>& values,
                        std::string_view begin_key, std::string_view end_key,
                        size_t limit, std::vector<KvPair>& kvs) {
        if (before_range) {
            auto callback = std::move(before_range);
            callback();
        }
        if (next_range_error_ != ErrorCode::OK) {
            auto error = next_range_error_;
            next_range_error_ = ErrorCode::OK;
            return error;
        }
        kvs.clear();
        for (const auto& [key, value] : values) {
            if (key >= begin_key && key < end_key) {
                kvs.push_back({.key = key, .value = value});
                if (limit != 0 && kvs.size() >= limit) break;
            }
        }
        return ErrorCode::OK;
    }
    std::map<std::string, std::string> values_;
    std::map<std::string, std::string> local_values_;
    bool local_view_enabled_{false};
    int range_calls_{0};
    int serializable_range_calls_{0};
    int batch_get_calls_{0};
    ErrorCode next_get_error_{ErrorCode::OK};
    ErrorCode next_range_error_{ErrorCode::OK};
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

TEST(OpLogBatchStandbyReaderTest, CompactionFloorBoundariesAndDeletion) {
    FakeHaKvBackend backend;
    const std::string floor_key = "/oplog/clusterA/snapshot/compaction_floor";
    backend.Put(BuildDurablePrefixKey("clusterA"),
                EncodeDurablePrefix({.batch_id = 2, .last_seq = 2}));
    backend.Put(BuildBatchRecordKey("clusterA", 2),
                EncodeOpLogBatchRecord(MakeBatch(2, 2, 2)));
    MockMetadataStore metadata;
    OpLogApplier applier(&metadata, "clusterA");
    applier.Recover(2);
    OpLogBatchStandbyReader reader("clusterA", backend, applier);
    ASSERT_EQ(ErrorCode::OK,
              reader.SetBaselineCursor({.batch_id = 2, .last_seq = 2}));
    EXPECT_EQ(ErrorCode::OK, reader.PollOnce().error);
    for (const auto* value : {"1", "2", "3", "18446744073709551615"}) {
        backend.Put(floor_key, value);
        if (std::string(value) == "2") {
            backend.Erase(BuildBatchRecordKey("clusterA", 2));
        }
        const auto poll = reader.PollOnce();
        if (std::string(value) == "1" || std::string(value) == "2") {
            EXPECT_EQ(OpLogBatchStandbyPollDisposition::OK, poll.disposition);
        } else {
            EXPECT_EQ(OpLogBatchStandbyPollDisposition::REBOOTSTRAP_REQUIRED,
                      poll.disposition);
            EXPECT_EQ(0u, poll.applied_entries);
        }
        EXPECT_EQ(3u, applier.GetExpectedSequenceId());
    }
    backend.Erase(floor_key);
    backend.Put(BuildBatchRecordKey("clusterA", 2),
                EncodeOpLogBatchRecord(MakeBatch(2, 2, 2)));
    EXPECT_EQ(ErrorCode::OK, reader.PollOnce().error);
}

TEST(OpLogBatchStandbyReaderTest, InvalidFloorFailsClosed) {
    for (const auto* value :
         {"", "-1", "+1", " 1", "1 ", "1x", "18446744073709551616"}) {
        SCOPED_TRACE(value);
        FakeHaKvBackend backend;
        backend.Put("/oplog/clusterA/snapshot/compaction_floor", value);
        MockMetadataStore metadata;
        OpLogApplier applier(&metadata, "clusterA");
        OpLogBatchStandbyReader reader("clusterA", backend, applier);
        EXPECT_EQ(OpLogBatchStandbyPollDisposition::FATAL,
                  reader.PollOnce().disposition);
    }
}

TEST(OpLogBatchStandbyReaderTest, FloorAdvancingDuringPollRequiresRebootstrap) {
    FakeHaKvBackend backend;
    backend.Put(BuildDurablePrefixKey("clusterA"),
                EncodeDurablePrefix({.batch_id = 3, .last_seq = 3}));
    backend.Put(BuildBatchRecordKey("clusterA", 3),
                EncodeOpLogBatchRecord(MakeBatch(3, 3, 3)));
    backend.before_range = [&] {
        backend.Put("/oplog/clusterA/snapshot/compaction_floor", "2");
    };
    MockMetadataStore metadata;
    OpLogApplier applier(&metadata, "clusterA");
    applier.Recover(1);
    OpLogBatchStandbyReader reader("clusterA", backend, applier);
    ASSERT_EQ(ErrorCode::OK,
              reader.SetBaselineCursor({.batch_id = 1, .last_seq = 1}));
    const auto result = reader.PollOnce();
    EXPECT_EQ(OpLogBatchStandbyPollDisposition::REBOOTSTRAP_REQUIRED,
              result.disposition);
    EXPECT_EQ(2u, result.compaction_floor);
    EXPECT_EQ(0u, result.applied_entries);
    EXPECT_EQ(2u, applier.GetExpectedSequenceId());
}

TEST(OpLogBatchStandbyReaderTest, FloorDoesNotHideUnknownSuffixGap) {
    FakeHaKvBackend backend;
    backend.Put("/oplog/clusterA/snapshot/compaction_floor", "1");
    backend.Put(BuildDurablePrefixKey("clusterA"),
                EncodeDurablePrefix({.batch_id = 3, .last_seq = 3}));
    backend.Put(BuildBatchRecordKey("clusterA", 3),
                EncodeOpLogBatchRecord(MakeBatch(3, 3, 3)));
    MockMetadataStore metadata;
    OpLogApplier applier(&metadata, "clusterA");
    applier.Recover(1);
    OpLogBatchStandbyReader reader("clusterA", backend, applier);
    ASSERT_EQ(ErrorCode::OK,
              reader.SetBaselineCursor({.batch_id = 1, .last_seq = 1}));
    EXPECT_EQ(OpLogBatchStandbyPollDisposition::FATAL,
              reader.PollOnce().disposition);
    EXPECT_EQ(2u, applier.GetExpectedSequenceId());
}

TEST(OpLogBatchStandbyReaderTest, MissingPrefixWaitsWithoutLegacyFallback) {
    FakeHaKvBackend backend;
    MockMetadataStore metadata_store;
    OpLogApplier applier(&metadata_store, "clusterA");
    OpLogBatchStandbyReader reader("clusterA", backend, applier);

    auto result = reader.PollOnce();

    ASSERT_EQ(ErrorCode::OK, result.error);
    EXPECT_FALSE(result.durable_prefix_present);
    EXPECT_EQ(0u, result.applied_entries);
    EXPECT_EQ(1u, applier.GetExpectedSequenceId());
}

TEST(OpLogBatchStandbyReaderTest, ResumesAfterSnapshotBatchCursor) {
    FakeHaKvBackend backend;
    ASSERT_EQ(ErrorCode::OK,
              backend.Put(BuildDurablePrefixKey("clusterA"),
                          EncodeDurablePrefix({.batch_id = 2, .last_seq = 2})));
    ASSERT_EQ(ErrorCode::OK,
              backend.Put(BuildBatchRecordKey("clusterA", 2),
                          EncodeOpLogBatchRecord(MakeBatch(2, 2, 2))));

    MockMetadataStore metadata_store;
    OpLogApplier applier(&metadata_store, "clusterA");
    applier.Recover(1);
    OpLogBatchStandbyReader reader("clusterA", backend, applier);
    ASSERT_EQ(ErrorCode::OK,
              reader.SetBaselineCursor({.batch_id = 1, .last_seq = 1}));

    const auto result = reader.PollOnce();
    EXPECT_EQ(ErrorCode::OK, result.error);
    EXPECT_EQ(1u, result.applied_entries);
    EXPECT_EQ(3u, applier.GetExpectedSequenceId());
}

TEST(OpLogBatchStandbyReaderTest,
     UsesSerializableRangeAndWaitsForLocalFollowerToCatchUp) {
    FakeHaKvBackend backend;
    const auto first = MakeBatch(1, 1, 1);
    const auto second = MakeBatch(2, 2, 2);
    ASSERT_EQ(ErrorCode::OK,
              backend.Put(BuildDurablePrefixKey("clusterA"),
                          EncodeDurablePrefix({.batch_id = 2, .last_seq = 2})));
    ASSERT_EQ(ErrorCode::OK,
              backend.Put(BuildBatchRecordKey("clusterA", 1),
                          EncodeOpLogBatchRecord(first)));
    ASSERT_EQ(ErrorCode::OK,
              backend.Put(BuildBatchRecordKey("clusterA", 2),
                          EncodeOpLogBatchRecord(second)));
    backend.PublishLocal(BuildBatchRecordKey("clusterA", 1));

    MockMetadataStore metadata;
    OpLogApplier applier(&metadata, "clusterA");
    OpLogBatchStandbyReader reader("clusterA", backend, applier);

    const auto stale = reader.PollOnce(8);
    EXPECT_EQ(OpLogBatchStandbyPollDisposition::OK, stale.disposition);
    EXPECT_EQ(ErrorCode::OK, stale.error);
    EXPECT_EQ(1u, stale.applied_batches);
    EXPECT_FALSE(stale.has_more_pages);
    EXPECT_EQ(2u, applier.GetExpectedSequenceId());
    ASSERT_TRUE(reader.GetLastAppliedDurablePrefix().has_value());
    EXPECT_EQ(1u, reader.GetLastAppliedDurablePrefix()->batch_id);
    EXPECT_EQ(1u, reader.GetLastAppliedDurablePrefix()->last_seq);
    EXPECT_EQ(1, backend.serializable_range_calls());
    EXPECT_EQ(0, backend.range_calls());
    EXPECT_EQ(0, backend.batch_get_calls());

    backend.PublishLocal(BuildBatchRecordKey("clusterA", 2));
    const auto caught_up = reader.PollOnce(8);
    EXPECT_EQ(OpLogBatchStandbyPollDisposition::OK, caught_up.disposition);
    EXPECT_EQ(ErrorCode::OK, caught_up.error);
    EXPECT_EQ(1u, caught_up.applied_batches);
    EXPECT_EQ(3u, applier.GetExpectedSequenceId());
    EXPECT_EQ(2, backend.serializable_range_calls());
    EXPECT_EQ(0, backend.range_calls());
    EXPECT_EQ(0, backend.batch_get_calls());
    const auto steady = reader.PollOnce(8);
    EXPECT_EQ(ErrorCode::OK, steady.error);
    EXPECT_EQ(2, backend.serializable_range_calls());
    EXPECT_EQ(0, backend.batch_get_calls());
}

TEST(OpLogBatchStandbyReaderTest,
     AdvancesSnapshotCursorAcrossCappedSerializablePages) {
    FakeHaKvBackend backend;
    ASSERT_EQ(ErrorCode::OK,
              backend.Put(BuildDurablePrefixKey("clusterA"),
                          EncodeDurablePrefix({.batch_id = 2, .last_seq = 2})));
    ASSERT_EQ(ErrorCode::OK,
              backend.Put(BuildBatchRecordKey("clusterA", 1),
                          EncodeOpLogBatchRecord(MakeBatch(1, 1, 1))));
    ASSERT_EQ(ErrorCode::OK,
              backend.Put(BuildBatchRecordKey("clusterA", 2),
                          EncodeOpLogBatchRecord(MakeBatch(2, 2, 2))));

    MockMetadataStore metadata;
    OpLogApplier applier(&metadata, "clusterA");
    OpLogBatchStandbyReader reader("clusterA", backend, applier);

    const auto first_page = reader.PollOnce(1);
    ASSERT_EQ(ErrorCode::OK, first_page.error);
    EXPECT_EQ(1u, first_page.applied_batches);
    EXPECT_TRUE(first_page.has_more_pages);
    ASSERT_TRUE(reader.GetLastAppliedDurablePrefix().has_value());
    EXPECT_EQ(1u, reader.GetLastAppliedDurablePrefix()->batch_id);
    EXPECT_EQ(1u, reader.GetLastAppliedDurablePrefix()->last_seq);

    const auto second_page = reader.PollOnce(1);
    ASSERT_EQ(ErrorCode::OK, second_page.error);
    EXPECT_EQ(1u, second_page.applied_batches);
    EXPECT_FALSE(second_page.has_more_pages);
    ASSERT_TRUE(reader.GetLastAppliedDurablePrefix().has_value());
    EXPECT_EQ(2u, reader.GetLastAppliedDurablePrefix()->batch_id);
    EXPECT_EQ(2u, reader.GetLastAppliedDurablePrefix()->last_seq);
}

TEST(OpLogBatchStandbyReaderTest,
     RejectsBatchBeyondObservedDurablePrefixOnCappedPage) {
    FakeHaKvBackend backend;
    ASSERT_EQ(ErrorCode::OK,
              backend.Put(BuildDurablePrefixKey("clusterA"),
                          EncodeDurablePrefix({.batch_id = 2, .last_seq = 2})));
    ASSERT_EQ(ErrorCode::OK,
              backend.Put(BuildBatchRecordKey("clusterA", 1),
                          EncodeOpLogBatchRecord(MakeBatch(1, 1, 3))));
    ASSERT_EQ(ErrorCode::OK,
              backend.Put(BuildBatchRecordKey("clusterA", 2),
                          EncodeOpLogBatchRecord(MakeBatch(2, 4, 4))));

    MockMetadataStore metadata;
    OpLogApplier applier(&metadata, "clusterA");
    OpLogBatchStandbyReader reader("clusterA", backend, applier);

    const auto result = reader.PollOnce(1);
    EXPECT_EQ(OpLogBatchStandbyPollDisposition::FATAL, result.disposition);
    EXPECT_EQ(ErrorCode::INCOMPLETE_OPLOG_CATCH_UP, result.error);
    EXPECT_FALSE(reader.GetLastAppliedDurablePrefix().has_value());
}

TEST(OpLogBatchStandbyReaderTest, MissingPrefixAfterObservationFailsClosed) {
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

    backend.Erase(BuildDurablePrefixKey("clusterA"));
    auto result = reader.PollOnce();

    EXPECT_NE(ErrorCode::OK, result.error);
    EXPECT_EQ(OpLogBatchStandbyPollDisposition::FATAL, result.disposition);
}

TEST(OpLogBatchStandbyReaderTest, PrefixTransportErrorIsRetryable) {
    FakeHaKvBackend backend;
    MockMetadataStore metadata_store;
    OpLogApplier applier(&metadata_store, "clusterA");
    OpLogBatchStandbyReader reader("clusterA", backend, applier);

    backend.FailNextGet(ErrorCode::ETCD_OPERATION_ERROR);
    auto result = reader.PollOnce();

    EXPECT_EQ(ErrorCode::ETCD_OPERATION_ERROR, result.error);
    EXPECT_EQ(OpLogBatchStandbyPollDisposition::RETRYABLE, result.disposition);
}

TEST(OpLogBatchStandbyReaderTest, RangeTransportErrorIsRetryable) {
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
    backend.FailNextRange(ErrorCode::ETCD_OPERATION_ERROR);

    auto result = reader.PollOnce();

    EXPECT_EQ(ErrorCode::ETCD_OPERATION_ERROR, result.error);
    EXPECT_EQ(OpLogBatchStandbyPollDisposition::RETRYABLE, result.disposition);
}

TEST(OpLogBatchStandbyReaderTest, WaitsWhenSerializableRangeHasNotCaughtUp) {
    FakeHaKvBackend backend;
    ASSERT_EQ(ErrorCode::OK,
              backend.Put(BuildDurablePrefixKey("clusterA"),
                          EncodeDurablePrefix({.batch_id = 1, .last_seq = 1})));
    MockMetadataStore metadata_store;
    OpLogApplier applier(&metadata_store, "clusterA");
    OpLogBatchStandbyReader reader("clusterA", backend, applier);

    auto result = reader.PollOnce();

    EXPECT_EQ(OpLogBatchStandbyPollDisposition::OK, result.disposition);
    EXPECT_EQ(ErrorCode::OK, result.error);
    EXPECT_EQ(0u, result.applied_batches);
    EXPECT_EQ(1u, applier.GetExpectedSequenceId());
}

TEST(OpLogBatchStandbyReaderTest, DurablePrefixRegressionFailsClosed) {
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

    ASSERT_EQ(ErrorCode::OK,
              backend.Put(BuildDurablePrefixKey("clusterA"),
                          EncodeDurablePrefix({.batch_id = 0, .last_seq = 0})));
    EXPECT_NE(ErrorCode::OK, reader.PollOnce().error);
}

TEST(OpLogBatchStandbyReaderTest, BatchZeroMustHaveZeroSequence) {
    FakeHaKvBackend backend;
    ASSERT_EQ(
        ErrorCode::OK,
        backend.Put(BuildDurablePrefixKey("clusterA"),
                    EncodeDurablePrefix({.batch_id = 0, .last_seq = 42})));
    MockMetadataStore metadata_store;
    OpLogApplier applier(&metadata_store, "clusterA");
    OpLogBatchStandbyReader reader("clusterA", backend, applier);

    auto result = reader.PollOnce();

    EXPECT_EQ(ErrorCode::INCOMPLETE_OPLOG_CATCH_UP, result.error);
    EXPECT_EQ(OpLogBatchStandbyPollDisposition::FATAL, result.disposition);
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
    EXPECT_EQ(1, backend.serializable_range_calls());
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
    int range_calls_after_first_poll = backend.serializable_range_calls();
    auto second = reader.PollOnce();

    ASSERT_EQ(ErrorCode::OK, second.error);
    EXPECT_EQ(0u, second.applied_entries);
    EXPECT_EQ(range_calls_after_first_poll,
              backend.serializable_range_calls());
    EXPECT_EQ(2u, applier.GetExpectedSequenceId());
}

TEST(OpLogBatchStandbyReaderTest, FullPageContinuesOnNextPoll) {
    FakeHaKvBackend backend;
    ASSERT_EQ(ErrorCode::OK,
              backend.Put(BuildDurablePrefixKey("clusterA"),
                          EncodeDurablePrefix({.batch_id = 3, .last_seq = 3})));
    for (uint64_t id = 1; id <= 3; ++id) {
        ASSERT_EQ(ErrorCode::OK,
                  backend.Put(BuildBatchRecordKey("clusterA", id),
                              EncodeOpLogBatchRecord(MakeBatch(id, id, id))));
    }
    MockMetadataStore metadata_store;
    OpLogApplier applier(&metadata_store, "clusterA");
    OpLogBatchStandbyReader reader("clusterA", backend, applier);

    auto first = reader.PollOnce(/*max_batches=*/2);
    ASSERT_EQ(ErrorCode::OK, first.error);
    EXPECT_EQ(2u, first.applied_entries);
    EXPECT_EQ(2u, first.applied_batches);
    EXPECT_TRUE(first.has_more_pages);
    EXPECT_EQ(0, backend.batch_get_calls());
    EXPECT_EQ(3u, applier.GetExpectedSequenceId());
    auto first_page_prefix = reader.GetLastAppliedDurablePrefix();
    ASSERT_TRUE(first_page_prefix.has_value());
    EXPECT_EQ(2u, first_page_prefix->batch_id);
    EXPECT_EQ(2u, first_page_prefix->last_seq);

    auto second = reader.PollOnce(/*max_batches=*/2);
    ASSERT_EQ(ErrorCode::OK, second.error);
    EXPECT_EQ(1u, second.applied_entries);
    EXPECT_EQ(1u, second.applied_batches);
    EXPECT_FALSE(second.has_more_pages);
    EXPECT_EQ(0, backend.batch_get_calls());
    EXPECT_EQ(4u, applier.GetExpectedSequenceId());
    auto applied_prefix = reader.GetLastAppliedDurablePrefix();
    ASSERT_TRUE(applied_prefix.has_value());
    EXPECT_EQ(3u, applied_prefix->batch_id);
    EXPECT_EQ(3u, applied_prefix->last_seq);
}

TEST(OpLogBatchStandbyReaderTest,
     DoesNotAdvancePastPrefixObservedBeforeConcurrentAppend) {
    FakeHaKvBackend backend;
    ASSERT_EQ(ErrorCode::OK,
              backend.Put(BuildDurablePrefixKey("clusterA"),
                          EncodeDurablePrefix({.batch_id = 1, .last_seq = 1})));
    ASSERT_EQ(ErrorCode::OK,
              backend.Put(BuildBatchRecordKey("clusterA", 1),
                          EncodeOpLogBatchRecord(MakeBatch(1, 1, 1))));
    backend.before_range = [&] {
        backend.Put(BuildBatchRecordKey("clusterA", 2),
                    EncodeOpLogBatchRecord(MakeBatch(2, 2, 2)));
        backend.Put(BuildDurablePrefixKey("clusterA"),
                    EncodeDurablePrefix({.batch_id = 2, .last_seq = 2}));
    };

    MockMetadataStore metadata_store;
    OpLogApplier applier(&metadata_store, "clusterA");
    OpLogBatchStandbyReader reader("clusterA", backend, applier);

    const auto first = reader.PollOnce(/*max_batches=*/2);
    ASSERT_EQ(ErrorCode::OK, first.error);
    EXPECT_EQ(1u, first.applied_entries);
    EXPECT_EQ(1u, first.applied_batches);
    EXPECT_EQ(2u, applier.GetExpectedSequenceId());

    const auto second = reader.PollOnce(/*max_batches=*/2);
    ASSERT_EQ(ErrorCode::OK, second.error);
    EXPECT_EQ(1u, second.applied_entries);
    EXPECT_EQ(1u, second.applied_batches);
    EXPECT_EQ(3u, applier.GetExpectedSequenceId());
    EXPECT_EQ(0, backend.batch_get_calls());
}

TEST(OpLogBatchStandbyReaderTest, ReportsValidZeroBoundaryAsComplete) {
    FakeHaKvBackend backend;
    ASSERT_EQ(ErrorCode::OK,
              backend.Put(BuildDurablePrefixKey("clusterA"),
                          EncodeDurablePrefix({.batch_id = 0, .last_seq = 0})));
    MockMetadataStore metadata_store;
    OpLogApplier applier(&metadata_store, "clusterA");
    OpLogBatchStandbyReader reader("clusterA", backend, applier);

    ASSERT_EQ(ErrorCode::OK, reader.PollOnce().error);
    auto applied_prefix = reader.GetLastAppliedDurablePrefix();
    ASSERT_TRUE(applied_prefix.has_value());
    EXPECT_EQ(0u, applied_prefix->batch_id);
    EXPECT_EQ(0u, applied_prefix->last_seq);
}

TEST(OpLogBatchStandbyReaderTest, RejectsPrefixPointingIntoBatch) {
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

    EXPECT_NE(ErrorCode::OK, result.error);
    EXPECT_EQ(0u, result.applied_entries);
    EXPECT_EQ(1u, applier.GetExpectedSequenceId());
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
    auto applied_prefix = reader.GetLastAppliedDurablePrefix();
    ASSERT_TRUE(applied_prefix.has_value());
    EXPECT_EQ(2u, applied_prefix->batch_id);
    EXPECT_EQ(3u, applied_prefix->last_seq);
}

TEST(OpLogBatchStandbyReaderTest, AcceptsFirstBatchAfterSnapshotBaseline) {
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
    EXPECT_EQ(2u, result.applied_entries);
    EXPECT_EQ(1u, result.applied_batches);
    EXPECT_EQ(3u, applier.GetExpectedSequenceId());
    auto applied_prefix = reader.GetLastAppliedDurablePrefix();
    ASSERT_TRUE(applied_prefix.has_value());
    EXPECT_EQ(1u, applied_prefix->batch_id);
    EXPECT_EQ(2u, applied_prefix->last_seq);
}

TEST(OpLogBatchStandbyReaderTest, MissingBatchMarksReaderUnhealthy) {
    FakeHaKvBackend backend;
    ASSERT_EQ(ErrorCode::OK,
              backend.Put(BuildDurablePrefixKey("clusterA"),
                          EncodeDurablePrefix({.batch_id = 2, .last_seq = 2})));
    ASSERT_EQ(ErrorCode::OK,
              backend.Put(BuildBatchRecordKey("clusterA", 2),
                          EncodeOpLogBatchRecord(MakeBatch(2, 2, 2))));
    MockMetadataStore metadata_store;
    OpLogApplier applier(&metadata_store, "clusterA");
    OpLogBatchStandbyReader reader("clusterA", backend, applier);

    auto result = reader.PollOnce();

    EXPECT_EQ(ErrorCode::INCOMPLETE_OPLOG_CATCH_UP, result.error);
    EXPECT_EQ(OpLogBatchStandbyPollDisposition::FATAL, result.disposition);
    EXPECT_EQ(0u, result.applied_entries);
    EXPECT_EQ(1u, applier.GetExpectedSequenceId());
}

TEST(OpLogBatchStandbyReaderTest, StartsFromEarliestExistingBatch) {
    FakeHaKvBackend backend;
    ASSERT_EQ(
        ErrorCode::OK,
        backend.Put(BuildDurablePrefixKey("clusterA"),
                    EncodeDurablePrefix({.batch_id = 42, .last_seq = 44})));
    ASSERT_EQ(ErrorCode::OK,
              backend.Put(BuildBatchRecordKey("clusterA", 42),
                          EncodeOpLogBatchRecord(MakeBatch(42, 43, 44))));
    MockMetadataStore metadata_store;
    OpLogApplier applier(&metadata_store, "clusterA");
    applier.Recover(42);
    OpLogBatchStandbyReader reader("clusterA", backend, applier);

    auto result = reader.PollOnce();

    EXPECT_EQ(ErrorCode::OK, result.error);
    EXPECT_EQ(2u, result.applied_entries);
    EXPECT_EQ(45u, applier.GetExpectedSequenceId());
}

TEST(OpLogBatchStandbyReaderTest, RejectsSequenceOverlapAcrossBatches) {
    FakeHaKvBackend backend;
    ASSERT_EQ(ErrorCode::OK,
              backend.Put(BuildDurablePrefixKey("clusterA"),
                          EncodeDurablePrefix({.batch_id = 2, .last_seq = 3})));
    ASSERT_EQ(ErrorCode::OK,
              backend.Put(BuildBatchRecordKey("clusterA", 1),
                          EncodeOpLogBatchRecord(MakeBatch(1, 1, 2))));
    ASSERT_EQ(ErrorCode::OK,
              backend.Put(BuildBatchRecordKey("clusterA", 2),
                          EncodeOpLogBatchRecord(MakeBatch(2, 2, 3))));
    MockMetadataStore metadata_store;
    OpLogApplier applier(&metadata_store, "clusterA");
    OpLogBatchStandbyReader reader("clusterA", backend, applier);

    auto result = reader.PollOnce();

    EXPECT_NE(ErrorCode::OK, result.error);
    EXPECT_EQ(2u, result.applied_entries);
    EXPECT_EQ(3u, applier.GetExpectedSequenceId());
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

TEST(OpLogBatchStandbyReaderTest, RejectsFutureFirstSequence) {
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

    auto result = reader.PollOnce();
    EXPECT_EQ(ErrorCode::INCOMPLETE_OPLOG_CATCH_UP, result.error);
    EXPECT_EQ(OpLogBatchStandbyPollDisposition::FATAL, result.disposition);
    EXPECT_EQ(0u, result.applied_entries);
    EXPECT_EQ(1u, applier.GetExpectedSequenceId());
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
