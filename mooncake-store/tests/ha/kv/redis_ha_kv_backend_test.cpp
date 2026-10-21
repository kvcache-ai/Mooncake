#include "ha/kv/redis_ha_kv_backend.h"

#include <gtest/gtest.h>

#include <atomic>
#include <cstdlib>
#include <memory>
#include <string>
#include <unistd.h>
#include <vector>

#include "ha/kv/ha_kv_backend_factory.h"
#include "ha/oplog/oplog_batch_storage.h"
#include "ha/oplog/oplog_batch_types.h"
#include "ha/oplog/oplog_types.h"

#ifdef STORE_USE_REDIS
#include <gflags/gflags.h>

#include "ha/common/redis/redis_test_utils.h"

DEFINE_string(redis_endpoint, "",
              "Redis endpoint for OpLog KV tests, e.g. 127.0.0.1:6379");
#endif

namespace mooncake::test {
namespace {

OpLogEntry MakeEntry(uint64_t seq) {
    OpLogEntry entry;
    entry.sequence_id = seq;
    entry.timestamp_ms = 1234567890;
    entry.op_type = OpType::PUT_END;
    entry.tenant_id = "tenant";
    entry.object_key = "key" + std::to_string(seq);
    entry.payload = "value" + std::to_string(seq);
    entry.checksum = ComputeOpLogChecksum(entry.payload);
    return entry;
}

OpLogBatchRecord MakeBatch(uint64_t batch_id, uint64_t first_seq,
                           size_t count) {
    OpLogBatchRecord batch;
    batch.batch_id = batch_id;
    batch.first_seq = first_seq;
    batch.last_seq = first_seq + count - 1;
    for (size_t i = 0; i < count; ++i) {
        batch.entries.push_back(MakeEntry(first_seq + i));
    }
    return batch;
}

}  // namespace

TEST(RedisHaKvBackendTest, CreateRevisionCompareIsRejected) {
    RedisHaKvBackend backend;
    KvTxn txn;
    txn.compares.push_back({.key = "/oplog/clusterA/durable_prefix",
                            .kind = KvCompareKind::kCreateRevisionEquals,
                            .expected_value = "",
                            .expected_revision = 1});
    EXPECT_EQ(ErrorCode::INVALID_PARAMS, backend.Txn(txn));
}

#ifdef STORE_USE_REDIS

namespace {

std::string MakeClusterId() {
    static std::atomic<int> sequence{0};
    return "redisop" + std::to_string(::getpid()) + "n" +
           std::to_string(sequence.fetch_add(1));
}

class RedisOpLogBackendTest : public ::testing::Test {
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
    }

    void TearDown() override {
        if (!backend_) {
            return;
        }
        const std::string begin = "/oplog/" + cluster_id_ + "/\x01";
        const std::string end = "/oplog/" + cluster_id_ + "/~";
        EXPECT_EQ(ErrorCode::OK, backend_->DeleteRange(begin, end));
    }

    std::string cluster_id_;
    std::shared_ptr<HaKvBackend> backend_;
};

}  // namespace

TEST_F(RedisOpLogBackendTest, CompareAndSwapAndBinaryValue) {
    const std::string key = "/oplog/" + cluster_id_ + "/durable_prefix";
    const std::string binary_value("a\0b", 3);

    KvTxn create;
    create.compares.push_back({.key = key,
                               .kind = KvCompareKind::kKeyNotExists,
                               .expected_value = ""});
    create.puts.push_back({.key = key, .value = binary_value});
    ASSERT_EQ(ErrorCode::OK, backend_->Txn(create));

    KvTxn conflict;
    conflict.compares.push_back({.key = key,
                                 .kind = KvCompareKind::kKeyNotExists,
                                 .expected_value = ""});
    conflict.puts.push_back({.key = key, .value = "other"});
    EXPECT_EQ(ErrorCode::ETCD_TRANSACTION_FAIL, backend_->Txn(conflict));

    KvTxn mismatch;
    mismatch.compares.push_back({.key = key,
                                 .kind = KvCompareKind::kValueEquals,
                                 .expected_value = "nope"});
    mismatch.puts.push_back({.key = key, .value = "next"});
    EXPECT_EQ(ErrorCode::ETCD_TRANSACTION_FAIL, backend_->Txn(mismatch));

    std::string value;
    ASSERT_EQ(ErrorCode::OK, backend_->Get(key, value));
    EXPECT_EQ(binary_value, value);

    const std::string redis_key =
        "mooncake-store/{" + cluster_id_ + "}/oplog/durable_prefix";
    auto context = testing::ConnectRedisForTest(FLAGS_redis_endpoint);
    ASSERT_TRUE(context.has_value());
    auto* raw = static_cast<redisReply*>(redisCommand(
        context.value().get(), "GET %b", redis_key.data(), redis_key.size()));
    testing::RedisReplyPtr reply(raw);
    ASSERT_NE(reply, nullptr);
    ASSERT_EQ(REDIS_REPLY_STRING, reply->type);
    EXPECT_EQ(binary_value, std::string(reply->str, reply->len));
}

TEST_F(RedisOpLogBackendTest, RangeIsOrderedAndPaginated) {
    const std::string prefix = "/oplog/" + cluster_id_ + "/batches/";
    const std::string key1 = prefix + "00000000000000000001";
    const std::string key2 = prefix + "00000000000000000002";
    const std::string key3 = prefix + "00000000000000000003";
    ASSERT_EQ(ErrorCode::OK, backend_->Put(key1, "a"));
    ASSERT_EQ(ErrorCode::OK, backend_->Put(key2, "b"));
    ASSERT_EQ(ErrorCode::OK, backend_->Put(key3, "c"));

    const std::string end = "/oplog/" + cluster_id_ + "/batches0";
    std::vector<KvPair> page;
    ASSERT_EQ(ErrorCode::OK, backend_->Range(key1, end, 1, page));
    ASSERT_EQ(1u, page.size());
    EXPECT_EQ(key1, page[0].key);
    EXPECT_EQ("a", page[0].value);

    const std::string next = page[0].key + '\0';
    ASSERT_EQ(ErrorCode::OK, backend_->Range(next, end, 10, page));
    ASSERT_EQ(2u, page.size());
    EXPECT_EQ(key2, page[0].key);
    EXPECT_EQ(key3, page[1].key);
}

TEST_F(RedisOpLogBackendTest, DeleteRangeRemovesCoveredBatches) {
    const std::string prefix = "/oplog/" + cluster_id_ + "/batches/";
    const std::string key1 = prefix + "00000000000000000001";
    const std::string key2 = prefix + "00000000000000000002";
    ASSERT_EQ(ErrorCode::OK, backend_->Put(key1, "a"));
    ASSERT_EQ(ErrorCode::OK, backend_->Put(key2, "b"));
    ASSERT_EQ(ErrorCode::OK, backend_->DeleteRange(key1, key2));

    std::string value;
    EXPECT_EQ(ErrorCode::ETCD_KEY_NOT_EXIST, backend_->Get(key1, value));
    ASSERT_EQ(ErrorCode::OK, backend_->Get(key2, value));
    EXPECT_EQ("b", value);
}

TEST_F(RedisOpLogBackendTest, StorageWritesAndReadsContiguousBatches) {
    OpLogBatchStorage storage(cluster_id_, *backend_);
    DurablePrefix prefix;
    ASSERT_EQ(ErrorCode::OK, storage.InitDurablePrefix(prefix));
    ASSERT_EQ(ErrorCode::OK, storage.ClaimProducerView(7));

    const auto first = MakeBatch(1, 1, 1);
    ASSERT_EQ(ErrorCode::OK,
              storage.WriteBatchAndAdvancePrefix(first, prefix, 7));
    prefix = {.batch_id = first.batch_id, .last_seq = first.last_seq};
    const auto second = MakeBatch(2, 2, 2);
    EXPECT_EQ(ErrorCode::ETCD_TRANSACTION_FAIL,
              storage.WriteBatchAndAdvancePrefix(second, prefix, 8));

    std::vector<OpLogBatchRecord> batches;
    ASSERT_EQ(ErrorCode::OK, storage.ReadBatchesAfter(0, 10, batches));
    ASSERT_EQ(1u, batches.size());
    EXPECT_EQ(1u, batches[0].batch_id);

    ASSERT_EQ(ErrorCode::OK, storage.ClaimProducerView(8));
    ASSERT_EQ(ErrorCode::OK,
              storage.WriteBatchAndAdvancePrefix(second, prefix, 8));
    ASSERT_EQ(ErrorCode::OK, storage.ReadBatchesAfter(0, 10, batches));
    ASSERT_EQ(2u, batches.size());
    EXPECT_EQ(2u, batches[1].batch_id);
    EXPECT_EQ(3u, batches[1].last_seq);
}

#endif

}  // namespace mooncake::test

#ifdef STORE_USE_REDIS
int main(int argc, char** argv) {
    gflags::ParseCommandLineFlags(&argc, &argv, true);
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
#endif
