#include "ha/kv/ha_kv_backend.h"

#include <gtest/gtest.h>

#include <atomic>
#include <memory>
#include <string>
#include <unistd.h>
#include <vector>

#include <gflags/gflags.h>

#include "ha/kv/ha_kv_backend_factory.h"
#include "ha/oplog/mock_metadata_store.h"
#include "ha/oplog/oplog_applier.h"
#include "ha/oplog/oplog_batch_standby_reader.h"
#include "ha/oplog/oplog_batch_storage.h"
#include "ha/oplog/oplog_batch_types.h"
#include "ha/oplog/oplog_types.h"
#include "metadata_store.h"

DECLARE_string(redis_endpoint);

namespace mooncake::test {
namespace {

std::string MakeClusterId() {
    static std::atomic<int> sequence{0};
    return "redisha" + std::to_string(::getpid()) + "n" +
           std::to_string(sequence.fetch_add(1));
}

OpLogEntry MakePut(uint64_t seq, const std::string& key) {
    MetadataPayload payload;
    payload.client_id = {1, 2};
    payload.size = 64;
    const auto packed = struct_pack::serialize(payload);
    OpLogEntry entry;
    entry.sequence_id = seq;
    entry.timestamp_ms = 1234567890;
    entry.op_type = OpType::PUT_END;
    entry.tenant_id = "tenant";
    entry.object_key = key;
    entry.payload.assign(packed.begin(), packed.end());
    entry.checksum = ComputeOpLogChecksum(entry.payload);
    return entry;
}

OpLogBatchRecord MakeBatch(uint64_t batch_id, uint64_t seq,
                           const std::string& key) {
    OpLogBatchRecord batch;
    batch.batch_id = batch_id;
    batch.first_seq = seq;
    batch.last_seq = seq;
    batch.entries.push_back(MakePut(seq, key));
    return batch;
}

// The Lua script has already returned success. The next Txn reports the
// connection failure RedisHaKvBackend returns when the reply is lost.
class HideCommittedTxnReply : public HaKvBackend {
   public:
    explicit HideCommittedTxnReply(HaKvBackend& inner) : inner_(inner) {}

    void HideNextSuccess() { hide_next_success_ = true; }

    ErrorCode Get(std::string_view key, std::string& value) override {
        return inner_.Get(key, value);
    }
    ErrorCode Put(std::string_view key, std::string_view value) override {
        return inner_.Put(key, value);
    }
    ErrorCode Range(std::string_view begin_key, std::string_view end_key,
                    size_t limit, std::vector<KvPair>& kvs) override {
        return inner_.Range(begin_key, end_key, limit, kvs);
    }
    ErrorCode DeleteRange(std::string_view begin_key,
                          std::string_view end_key) override {
        return inner_.DeleteRange(begin_key, end_key);
    }
    bool SupportsTxn() const override { return inner_.SupportsTxn(); }
    ErrorCode Txn(const KvTxn& txn) override {
        const ErrorCode err = inner_.Txn(txn);
        if (hide_next_success_ && err == ErrorCode::OK) {
            hide_next_success_ = false;
            return ErrorCode::ETCD_OPERATION_ERROR;
        }
        return err;
    }

   private:
    HaKvBackend& inner_;
    bool hide_next_success_{false};
};

class RedisOpLogRecoveryTest : public ::testing::Test {
   protected:
    void SetUp() override {
        if (FLAGS_redis_endpoint.empty()) {
            GTEST_SKIP() << "Redis endpoint is not configured";
        }
        cluster_id_ = MakeClusterId();
        auto backend = CreateHaKvBackend(ha::HABackendSpec{
            .type = ha::HABackendType::REDIS,
            .connstring = FLAGS_redis_endpoint,
            .cluster_namespace = cluster_id_,
        });
        ASSERT_TRUE(backend.has_value()) << toString(backend.error());
        backend_ = std::move(backend.value());
        metadata_ = std::make_unique<MockMetadataStore>();
        applier_ = std::make_unique<OpLogApplier>(metadata_.get(), cluster_id_);
        reader_ = std::make_unique<OpLogBatchStandbyReader>(
            cluster_id_, *backend_, *applier_);
    }

    void TearDown() override {
        if (!backend_) {
            return;
        }
        const std::string begin = "/oplog/" + cluster_id_ + "/\x01";
        const std::string end = "/oplog/" + cluster_id_ + "/~";
        EXPECT_EQ(ErrorCode::OK, backend_->DeleteRange(begin, end));
    }

    ErrorCode Write(const OpLogBatchRecord& batch, DurablePrefix& prefix,
                    ViewVersionId view) {
        const ErrorCode err =
            storage().WriteBatchAndAdvancePrefix(batch, prefix, view);
        if (err == ErrorCode::OK) {
            prefix = {.batch_id = batch.batch_id, .last_seq = batch.last_seq};
        }
        return err;
    }

    OpLogBatchStorage storage() {
        return OpLogBatchStorage(cluster_id_, *backend_);
    }

    std::string cluster_id_;
    std::shared_ptr<HaKvBackend> backend_;
    std::unique_ptr<MockMetadataStore> metadata_;
    std::unique_ptr<OpLogApplier> applier_;
    std::unique_ptr<OpLogBatchStandbyReader> reader_;
};

TEST_F(RedisOpLogRecoveryTest, StandbyAppliesPrimaryBatchesFromRedis) {
    OpLogBatchStorage primary(cluster_id_, *backend_);
    DurablePrefix prefix;
    ASSERT_EQ(ErrorCode::OK, primary.InitDurablePrefix(prefix));
    ASSERT_EQ(ErrorCode::OK, primary.ClaimProducerView(3));
    ASSERT_EQ(ErrorCode::OK,
              Write(MakeBatch(1, 1, "alpha"), prefix, /*view=*/3));

    const auto first = reader_->PollOnce();
    EXPECT_EQ(OpLogBatchStandbyPollDisposition::OK, first.disposition);
    EXPECT_EQ(1u, first.applied_entries);
    auto stored = metadata_->GetMetadata("tenant", "alpha");
    ASSERT_TRUE(stored.has_value());
    EXPECT_EQ(64u, stored->size);

    ASSERT_EQ(ErrorCode::OK,
              Write(MakeBatch(2, 2, "beta"), prefix, /*view=*/3));
    const auto second = reader_->PollOnce();
    EXPECT_EQ(OpLogBatchStandbyPollDisposition::OK, second.disposition);
    EXPECT_EQ(1u, second.applied_entries);
    EXPECT_TRUE(metadata_->GetMetadata("tenant", "beta").has_value());
}

TEST_F(RedisOpLogRecoveryTest, PromotedLeaderContinuesTheSameOpLog) {
    OpLogBatchStorage primary(cluster_id_, *backend_);
    DurablePrefix prefix;
    ASSERT_EQ(ErrorCode::OK, primary.InitDurablePrefix(prefix));
    ASSERT_EQ(ErrorCode::OK, primary.ClaimProducerView(3));
    ASSERT_EQ(ErrorCode::OK,
              Write(MakeBatch(1, 1, "before"), prefix, /*view=*/3));
    ASSERT_EQ(OpLogBatchStandbyPollDisposition::OK,
              reader_->PollOnce().disposition);

    ASSERT_EQ(ErrorCode::OK, primary.ClaimProducerView(4));
    EXPECT_EQ(ErrorCode::ETCD_TRANSACTION_FAIL,
              primary.WriteBatchAndAdvancePrefix(MakeBatch(2, 2, "stale"),
                                                 prefix, /*view=*/3));
    ASSERT_EQ(ErrorCode::OK,
              Write(MakeBatch(2, 2, "after"), prefix, /*view=*/4));

    const auto caught_up = reader_->PollOnce();
    EXPECT_EQ(OpLogBatchStandbyPollDisposition::OK, caught_up.disposition);
    EXPECT_EQ(1u, caught_up.applied_entries);
    EXPECT_TRUE(metadata_->GetMetadata("tenant", "after").has_value());
    DurablePrefix durable;
    ASSERT_EQ(ErrorCode::OK, primary.ReadDurablePrefix(durable));
    EXPECT_EQ(2u, durable.batch_id);
    EXPECT_EQ(2u, durable.last_seq);
}

TEST_F(RedisOpLogRecoveryTest, LostReplyAfterCommitIsReconciled) {
    HideCommittedTxnReply hidden(*backend_);
    OpLogBatchStorage primary(cluster_id_, hidden);
    DurablePrefix prefix;
    ASSERT_EQ(ErrorCode::OK, primary.InitDurablePrefix(prefix));
    ASSERT_EQ(ErrorCode::OK, primary.ClaimProducerView(1));
    hidden.HideNextSuccess();
    ASSERT_EQ(ErrorCode::OK,
              Write(MakeBatch(1, 1, "acked"), prefix, /*view=*/1));

    std::vector<OpLogBatchRecord> batches;
    ASSERT_EQ(ErrorCode::OK, primary.ReadBatchesAfter(0, 10, batches));
    ASSERT_EQ(1u, batches.size());
    EXPECT_EQ(1u, batches[0].batch_id);

    DurablePrefix stale{.batch_id = 0, .last_seq = 0};
    EXPECT_EQ(ErrorCode::OK, primary.WriteBatchAndAdvancePrefix(
                                 MakeBatch(1, 1, "acked"), stale, /*view=*/1));
    ASSERT_EQ(ErrorCode::OK, primary.ReadBatchesAfter(0, 10, batches));
    EXPECT_EQ(1u, batches.size());
}

TEST_F(RedisOpLogRecoveryTest, EmptyRedisDoesNotContinueACaughtUpStandby) {
    OpLogBatchStorage primary(cluster_id_, *backend_);
    DurablePrefix prefix;
    ASSERT_EQ(ErrorCode::OK, primary.InitDurablePrefix(prefix));
    ASSERT_EQ(ErrorCode::OK, primary.ClaimProducerView(1));
    ASSERT_EQ(ErrorCode::OK,
              Write(MakeBatch(1, 1, "gone"), prefix, /*view=*/1));
    ASSERT_EQ(OpLogBatchStandbyPollDisposition::OK,
              reader_->PollOnce().disposition);

    const std::string begin = "/oplog/" + cluster_id_ + "/\x01";
    const std::string end = "/oplog/" + cluster_id_ + "/~";
    ASSERT_EQ(ErrorCode::OK, backend_->DeleteRange(begin, end));

    const auto lost = reader_->PollOnce();
    EXPECT_EQ(OpLogBatchStandbyPollDisposition::FATAL, lost.disposition);
    EXPECT_EQ(ErrorCode::INCOMPLETE_OPLOG_CATCH_UP, lost.error);

    OpLogBatchStorage restarted(cluster_id_, *backend_);
    DurablePrefix fresh;
    ASSERT_EQ(ErrorCode::OK, restarted.InitDurablePrefix(fresh));
    EXPECT_EQ(0u, fresh.batch_id);
    EXPECT_EQ(0u, fresh.last_seq);
}

TEST_F(RedisOpLogRecoveryTest, MissingPrefixWithBatchesIsNotAFreshLog) {
    OpLogBatchStorage primary(cluster_id_, *backend_);
    DurablePrefix prefix;
    ASSERT_EQ(ErrorCode::OK, primary.InitDurablePrefix(prefix));
    ASSERT_EQ(ErrorCode::OK, primary.ClaimProducerView(1));
    ASSERT_EQ(ErrorCode::OK,
              Write(MakeBatch(1, 1, "orphan"), prefix, /*view=*/1));
    ASSERT_EQ(OpLogBatchStandbyPollDisposition::OK,
              reader_->PollOnce().disposition);

    const std::string durable = BuildDurablePrefixKey(cluster_id_);
    ASSERT_EQ(ErrorCode::OK,
              backend_->DeleteRange(durable, durable + std::string(1, '\0')));

    DurablePrefix ignored;
    EXPECT_EQ(ErrorCode::INTERNAL_ERROR, primary.InitDurablePrefix(ignored));
    const auto lost = reader_->PollOnce();
    EXPECT_EQ(OpLogBatchStandbyPollDisposition::FATAL, lost.disposition);
    EXPECT_EQ(ErrorCode::INCOMPLETE_OPLOG_CATCH_UP, lost.error);
}

}  // namespace
}  // namespace mooncake::test
