#ifdef STORE_USE_REDIS

#include <gtest/gtest.h>

#include <chrono>
#include <cstdlib>
#include <string>
#include <thread>

#include <unistd.h>
#include <hiredis/hiredis.h>

#include "ha/oplog/oplog_store_factory.h"
#include "ha/oplog/redis_oplog_store.h"
#include "p2p_master_service.h"
#include "redis_util.h"
#include "../../redis_test_utils.h"

namespace mooncake {
namespace {

TEST(RedisOpLogStoreStandaloneTest, InvalidEndpointReturnsError) {
    RedisOpLogStore store("invalid_endpoint_test", "127.0.0.1:notaport",
                          /*enable_write=*/true, /*poll_interval_ms=*/10);
    EXPECT_EQ(ErrorCode::INTERNAL_ERROR, store.Init());

    auto factory_store = OpLogStoreFactory::Create(
        OpLogStoreType::REDIS, "invalid_endpoint_test", OpLogStoreRole::WRITER,
        "127.0.0.1:notaport",
        /*poll_interval_ms=*/10);
    EXPECT_EQ(nullptr, factory_store);
}

TEST(RedisOpLogStoreStandaloneTest, EndpointRequiresExplicitPort) {
    for (const std::string& endpoint : {"", "127.0.0.1", "[::1]", "::1"}) {
        RedisOpLogStore store("invalid_endpoint_test", endpoint,
                              /*enable_write=*/true,
                              /*poll_interval_ms=*/10);
        EXPECT_EQ(ErrorCode::INTERNAL_ERROR, store.Init())
            << "endpoint=" << endpoint;
    }
}

TEST(RedisOpLogStoreStandaloneTest, OnlyAddReplicaIsBestEffort) {
    EXPECT_TRUE(IsBestEffortP2POpLog(OpType_ADD_REPLICA));
    EXPECT_FALSE(IsBestEffortP2POpLog(OpType_REMOVE_REPLICA));
    EXPECT_FALSE(IsBestEffortP2POpLog(OpType_MOUNT_SEGMENT));
    EXPECT_FALSE(IsBestEffortP2POpLog(OpType_UNMOUNT_SEGMENT));
    EXPECT_FALSE(IsBestEffortP2POpLog(OpType_REGISTER_CLIENT));
    EXPECT_FALSE(IsBestEffortP2POpLog(OpType_UNREGISTER_CLIENT));
}

class RedisOpLogStoreTest : public ::testing::Test {
   protected:
    void SetUp() override {
        const char* endpoint = std::getenv("MOONCAKE_REDIS_ENDPOINT");
        const char* username = std::getenv("MOONCAKE_REDIS_USERNAME");
        const char* password = std::getenv("MOONCAKE_REDIS_PASSWORD");
        redis_endpoint_ = endpoint ? endpoint : "127.0.0.1:6379";
        redis_username_ = username ? username : "";
        redis_password_ = password ? password : "";
        cluster_id_ = "redis_oplog_test_" + std::to_string(::getpid()) + "_" +
                      std::to_string(test_counter_++);
        if (!RedisAvailable()) {
            GTEST_SKIP() << "Redis is not available at " << redis_endpoint_;
        }
        CleanupKeys();
    }

    void TearDown() override { CleanupKeys(); }

    bool RedisAvailable() const {
        auto [host, port] = ParseEndpoint();
        redisContext* ctx = redisConnect(host.c_str(), port);
        if (!ctx || ctx->err) {
            if (ctx) redisFree(ctx);
            return false;
        }
        bool ok = testing::AuthenticateRedisContext(ctx, redis_username_,
                                                    redis_password_);
        redisFree(ctx);
        if (!ok) {
            return false;
        }
        return true;
    }

    std::pair<std::string, int> ParseEndpoint() const {
        std::string host = "127.0.0.1";
        int port = 6379;
        auto colon_pos = redis_endpoint_.rfind(':');
        if (colon_pos != std::string::npos) {
            host = redis_endpoint_.substr(0, colon_pos);
            port = std::stoi(redis_endpoint_.substr(colon_pos + 1));
        } else if (!redis_endpoint_.empty()) {
            host = redis_endpoint_;
        }
        return {host, port};
    }

    void CleanupKeys() const {
        auto [host, port] = ParseEndpoint();
        redisContext* ctx = redisConnect(host.c_str(), port);
        if (!testing::AuthenticateRedisContext(ctx, redis_username_,
                                               redis_password_)) {
            if (ctx) redisFree(ctx);
            return;
        }
        std::string cursor = "0";
        const std::string pattern = "mooncake:{" + cluster_id_ + "}:oplog*";
        do {
            RedisReplyPtr scan((redisReply*)redisCommand(
                ctx, "SCAN %b MATCH %b COUNT 100", cursor.data(), cursor.size(),
                pattern.data(), pattern.size()));
            if (!scan || scan->type != REDIS_REPLY_ARRAY ||
                scan->elements != 2 || !scan->element[0] ||
                scan->element[0]->type != REDIS_REPLY_STRING ||
                !scan->element[1] ||
                scan->element[1]->type != REDIS_REPLY_ARRAY) {
                break;
            }
            cursor.assign(scan->element[0]->str, scan->element[0]->len);
            redisReply* keys = scan->element[1];
            for (size_t i = 0; i < keys->elements; ++i) {
                redisReply* key = keys->element[i];
                if (!key || key->type != REDIS_REPLY_STRING) continue;
                RedisReplyPtr del((redisReply*)redisCommand(
                    ctx, "DEL %b", key->str, key->len));
            }
        } while (cursor != "0");
        redisFree(ctx);
    }

    std::unique_ptr<RedisOpLogStore> CreateWriter() const {
        auto store = std::make_unique<RedisOpLogStore>(
            cluster_id_, redis_endpoint_, /*enable_write=*/true,
            /*poll_interval_ms=*/10, redis_password_, redis_username_);
        EXPECT_EQ(ErrorCode::OK, store->Init());
        return store;
    }

    std::unique_ptr<RedisOpLogStore> CreateReader() const {
        auto store = std::make_unique<RedisOpLogStore>(
            cluster_id_, redis_endpoint_, /*enable_write=*/false,
            /*poll_interval_ms=*/10, redis_password_, redis_username_);
        EXPECT_EQ(ErrorCode::OK, store->Init());
        return store;
    }

    OpLogEntry MakeEntry(uint64_t sequence_id,
                         const std::string& key = "redis_key") const {
        OpLogEntry entry;
        entry.sequence_id = sequence_id;
        entry.op_type = OpType::PUT_END;
        entry.object_key = key;
        entry.payload = "payload_" + std::to_string(sequence_id);
        entry.timestamp_ms = sequence_id * 10;
        return entry;
    }

    std::string redis_endpoint_;
    std::string redis_username_;
    std::string redis_password_;
    std::string cluster_id_;
    static inline int test_counter_ = 0;
};

TEST_F(RedisOpLogStoreTest, WriteReadAndLatestSequence) {
    auto writer = CreateWriter();
    ASSERT_EQ(ErrorCode::OK, writer->WriteOpLog(MakeEntry(1), true));
    ASSERT_EQ(ErrorCode::OK, writer->WriteOpLog(MakeEntry(2), true));

    auto reader = CreateReader();
    OpLogEntry entry;
    ASSERT_EQ(ErrorCode::OK, reader->ReadOpLog(2, entry));
    EXPECT_EQ(2u, entry.sequence_id);
    EXPECT_EQ("payload_2", entry.payload);

    uint64_t latest = 0;
    ASSERT_EQ(ErrorCode::OK, reader->GetLatestSequenceId(latest));
    EXPECT_EQ(2u, latest);
}

TEST_F(RedisOpLogStoreTest, ReadSinceReturnsOrderedEntries) {
    auto writer = CreateWriter();
    ASSERT_EQ(ErrorCode::OK, writer->WriteOpLog(MakeEntry(1), true));
    ASSERT_EQ(ErrorCode::OK, writer->WriteOpLog(MakeEntry(2), true));
    ASSERT_EQ(ErrorCode::OK, writer->WriteOpLog(MakeEntry(3), true));

    std::vector<OpLogEntry> entries;
    ASSERT_EQ(ErrorCode::OK, writer->ReadOpLogSince(1, 10, entries));
    ASSERT_EQ(2u, entries.size());
    EXPECT_EQ(2u, entries[0].sequence_id);
    EXPECT_EQ(3u, entries[1].sequence_id);
}

TEST_F(RedisOpLogStoreTest, ReadSinceKeepsUint64Ordering) {
    auto writer = CreateWriter();
    constexpr uint64_t kBase = 9007199254740993ULL;
    ASSERT_EQ(ErrorCode::OK, writer->UpdateLatestSequenceId(kBase - 1));
    ASSERT_EQ(ErrorCode::OK, writer->WriteOpLog(MakeEntry(kBase), true));
    ASSERT_EQ(ErrorCode::OK, writer->WriteOpLog(MakeEntry(kBase + 1), true));
    ASSERT_EQ(ErrorCode::OK, writer->WriteOpLog(MakeEntry(kBase + 2), true));

    std::vector<OpLogEntry> entries;
    ASSERT_EQ(ErrorCode::OK, writer->ReadOpLogSince(kBase, 10, entries));
    ASSERT_EQ(2u, entries.size());
    EXPECT_EQ(kBase + 1, entries[0].sequence_id);
    EXPECT_EQ(kBase + 2, entries[1].sequence_id);

    uint64_t latest = 0;
    ASSERT_EQ(ErrorCode::OK, writer->GetLatestSequenceId(latest));
    EXPECT_EQ(kBase + 2, latest);

    uint64_t max_sequence_id = 0;
    ASSERT_EQ(ErrorCode::OK, writer->GetMaxSequenceId(max_sequence_id));
    EXPECT_EQ(kBase + 2, max_sequence_id);
}

TEST_F(RedisOpLogStoreTest, AsyncWriteEventuallyAdvancesLatest) {
    auto writer = CreateWriter();
    ASSERT_EQ(ErrorCode::OK, writer->WriteOpLog(MakeEntry(1), false));
    ASSERT_EQ(ErrorCode::OK, writer->WriteOpLog(MakeEntry(2), false));

    uint64_t latest = 0;
    for (int i = 0; i < 100; ++i) {
        ASSERT_EQ(ErrorCode::OK, writer->GetLatestSequenceId(latest));
        if (latest == 2) {
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    EXPECT_EQ(2u, latest);

    std::vector<OpLogEntry> entries;
    ASSERT_EQ(ErrorCode::OK, writer->ReadOpLogSince(0, 10, entries));
    ASSERT_EQ(2u, entries.size());
    EXPECT_EQ(1u, entries[0].sequence_id);
    EXPECT_EQ(2u, entries[1].sequence_id);
}

TEST_F(RedisOpLogStoreTest, ReadSinceHonorsCommittedLatest) {
    auto writer = CreateWriter();
    ASSERT_EQ(ErrorCode::OK, writer->WriteOpLog(MakeEntry(1), true));
    ASSERT_EQ(ErrorCode::OK, writer->WriteOpLog(MakeEntry(2), true));
    ASSERT_EQ(ErrorCode::OK, writer->UpdateLatestSequenceId(1));

    std::vector<OpLogEntry> entries;
    ASSERT_EQ(ErrorCode::OK, writer->ReadOpLogSince(0, 10, entries));
    ASSERT_EQ(1u, entries.size());
    EXPECT_EQ(1u, entries[0].sequence_id);
}

TEST_F(RedisOpLogStoreTest, AsyncWriteDoesNotAdvanceLatestPastGap) {
    auto writer = CreateWriter();
    ASSERT_EQ(ErrorCode::OK, writer->WriteOpLog(MakeEntry(2), false));

    uint64_t latest = 0;
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    ASSERT_EQ(ErrorCode::OK, writer->GetLatestSequenceId(latest));
    EXPECT_EQ(0u, latest);

    ASSERT_EQ(ErrorCode::OK, writer->WriteOpLog(MakeEntry(1), true));
    ASSERT_EQ(ErrorCode::OK, writer->GetLatestSequenceId(latest));
    EXPECT_EQ(2u, latest);
}

TEST_F(RedisOpLogStoreTest, BypassOverflowCreatesReadableGap) {
    auto writer = std::make_unique<RedisOpLogStore>(
        cluster_id_, redis_endpoint_, /*enable_write=*/true,
        /*poll_interval_ms=*/10, redis_password_, redis_username_,
        /*db_index=*/0, /*async_queue_max_entries=*/1,
        OpLogAsyncQueueOverflowMode::BYPASS,
        /*best_effort_max_retries=*/3);
    ASSERT_EQ(ErrorCode::OK, writer->Init());

    // seq=2 is persisted but retained in-flight because seq=1 is missing,
    // making the one-entry queue deterministically full.
    ASSERT_EQ(ErrorCode::OK, writer->WriteOpLog(MakeEntry(2), false));
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    ASSERT_EQ(ErrorCode::OK, writer->WriteOpLog(MakeEntry(1), false));

    uint64_t latest = 0;
    for (int i = 0; i < 100 && latest != 2; ++i) {
        ASSERT_EQ(ErrorCode::OK, writer->GetLatestSequenceId(latest));
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    ASSERT_EQ(2u, latest);

    std::vector<OpLogEntry> entries;
    ASSERT_EQ(ErrorCode::OK, writer->ReadOpLogSince(0, 10, entries));
    ASSERT_EQ(1u, entries.size());
    EXPECT_EQ(2u, entries.front().sequence_id);

    OpLogReadProgress progress;
    ASSERT_EQ(ErrorCode::OK,
              writer->ReadOpLogSinceWithProgress(0, 10, entries, progress));
    ASSERT_EQ(1u, entries.size());
    EXPECT_EQ(2u, progress.last_scanned_sequence_id);
}

TEST_F(RedisOpLogStoreTest, SparseReadLimitsScannedSequenceRange) {
    auto writer = CreateWriter();
    ASSERT_EQ(ErrorCode::OK, writer->UpdateLatestSequenceId(2500));
    ASSERT_EQ(ErrorCode::OK, writer->WriteOpLog(MakeEntry(2500), false));

    OpLogEntry stored;
    for (int i = 0; i < 100; ++i) {
        if (writer->ReadOpLog(2500, stored) == ErrorCode::OK) {
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    ASSERT_EQ(2500u, stored.sequence_id);

    std::vector<OpLogEntry> entries;
    OpLogReadProgress progress;
    ASSERT_EQ(ErrorCode::OK,
              writer->ReadOpLogSinceWithProgress(0, 1000, entries, progress));
    EXPECT_TRUE(entries.empty());
    EXPECT_EQ(1000u, progress.last_scanned_sequence_id);

    ASSERT_EQ(ErrorCode::OK,
              writer->ReadOpLogSinceWithProgress(
                  progress.last_scanned_sequence_id, 1000, entries, progress));
    EXPECT_TRUE(entries.empty());
    EXPECT_EQ(2000u, progress.last_scanned_sequence_id);

    ASSERT_EQ(ErrorCode::OK,
              writer->ReadOpLogSinceWithProgress(
                  progress.last_scanned_sequence_id, 1000, entries, progress));
    ASSERT_EQ(1u, entries.size());
    EXPECT_EQ(2500u, entries.front().sequence_id);
    EXPECT_EQ(2500u, progress.last_scanned_sequence_id);
}

TEST_F(RedisOpLogStoreTest, FactoryRejectsInvalidAsyncConfiguration) {
    EXPECT_EQ(nullptr,
              OpLogStoreFactory::Create(OpLogStoreType::REDIS, cluster_id_,
                                        OpLogStoreRole::WRITER, redis_endpoint_,
                                        10, redis_password_, redis_username_, 0,
                                        0, "reject", 3));
    EXPECT_EQ(nullptr,
              OpLogStoreFactory::Create(OpLogStoreType::REDIS, cluster_id_,
                                        OpLogStoreRole::WRITER, redis_endpoint_,
                                        10, redis_password_, redis_username_, 0,
                                        10, "invalid", 3));
}

TEST_F(RedisOpLogStoreTest, RewriteExistingSequenceOverwritesEntry) {
    auto writer = CreateWriter();
    auto entry = MakeEntry(1);
    ASSERT_EQ(ErrorCode::OK, writer->WriteOpLog(entry, true));
    EXPECT_EQ(ErrorCode::OK, writer->WriteOpLog(entry, true));

    auto replacement = entry;
    replacement.payload = "different_payload";
    EXPECT_EQ(ErrorCode::OK, writer->WriteOpLog(replacement, true));

    OpLogEntry stored;
    ASSERT_EQ(ErrorCode::OK, writer->ReadOpLog(1, stored));
    EXPECT_EQ(replacement.payload, stored.payload);
}

TEST_F(RedisOpLogStoreTest, CleanupRemovesOldEntries) {
    auto writer = CreateWriter();
    for (uint64_t i = 1; i <= 5; ++i) {
        ASSERT_EQ(ErrorCode::OK, writer->WriteOpLog(MakeEntry(i), true));
    }
    ASSERT_EQ(ErrorCode::OK, writer->CleanupOpLogBefore(4));

    OpLogEntry entry;
    EXPECT_EQ(ErrorCode::OPLOG_ENTRY_NOT_FOUND, writer->ReadOpLog(3, entry));
    ASSERT_EQ(ErrorCode::OK, writer->ReadOpLog(4, entry));

    std::vector<OpLogEntry> entries;
    ASSERT_EQ(ErrorCode::OK, writer->ReadOpLogSince(0, 10, entries));
    ASSERT_EQ(2u, entries.size());
    EXPECT_EQ(4u, entries[0].sequence_id);
    EXPECT_EQ(5u, entries[1].sequence_id);

    uint64_t max_sequence_id = 0;
    ASSERT_EQ(ErrorCode::OK, writer->GetMaxSequenceId(max_sequence_id));
    EXPECT_EQ(5u, max_sequence_id);
}

TEST_F(RedisOpLogStoreTest, SnapshotSequenceRoundTrip) {
    auto writer = CreateWriter();
    ASSERT_EQ(ErrorCode::OK, writer->RecordSnapshotSequenceId("snap-a", 42));

    uint64_t sequence_id = 0;
    ASSERT_EQ(ErrorCode::OK,
              writer->GetSnapshotSequenceId("snap-a", sequence_id));
    EXPECT_EQ(42u, sequence_id);
    EXPECT_EQ(ErrorCode::OPLOG_ENTRY_NOT_FOUND,
              writer->GetSnapshotSequenceId("missing", sequence_id));
}

TEST_F(RedisOpLogStoreTest, FactoryCreatesRedisStore) {
    auto store = OpLogStoreFactory::Create(
        OpLogStoreType::REDIS, cluster_id_, OpLogStoreRole::WRITER,
        redis_endpoint_, /*poll_interval_ms=*/10, redis_password_,
        redis_username_);
    ASSERT_NE(store, nullptr);
    ASSERT_EQ(ErrorCode::OK, store->WriteOpLog(MakeEntry(1), true));
}

TEST_F(RedisOpLogStoreTest, RejectsInvalidDatabaseIndex) {
    RedisOpLogStore store(cluster_id_, redis_endpoint_,
                          /*enable_write=*/true,
                          /*poll_interval_ms=*/10, redis_password_,
                          redis_username_, /*db_index=*/-1);
    EXPECT_EQ(ErrorCode::INTERNAL_ERROR, store.Init());
}

TEST_F(RedisOpLogStoreTest, MasterServiceUsesRedisEndpointForRedisOpLog) {
    MasterServiceConfig config;
    config.enable_oplog = true;
    config.oplog_store_type = "redis";
    config.cluster_id = cluster_id_;
    config.redis_endpoint = redis_endpoint_;
    config.redis_username = redis_username_;
    config.redis_password = redis_password_;
    config.oplog_data_dir = "127.0.0.1:notaport";

    EXPECT_NO_THROW({ P2PMasterService service(config); });
}

}  // namespace
}  // namespace mooncake

#endif  // STORE_USE_REDIS
