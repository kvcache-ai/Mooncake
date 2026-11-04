#ifdef STORE_USE_REDIS

#include <gtest/gtest.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdlib>
#include <vector>
#include <string>
#include <thread>
#include <future>

#include <unistd.h>
#include <hiredis/hiredis.h>
#include <xxhash.h>

#include "p2p/ha/oplog/oplog_manager.h"
#include "p2p/ha/oplog/oplog_store_factory.h"
#include "p2p/ha/oplog/redis_oplog_store.h"
#include "p2p/ha/oplog/oplog_replicator.h"
#include "p2p/ha/oplog/p2p_oplog_applier.h"
#include "p2p/ha/oplog/p2p_oplog_types.h"
#include "p2p/ha/oplog/p2p_standby_metadata_store.h"
#include "p2p/ha/ha_metric_manager.h"
#include "p2p/ha/oplog/p2p_standby_snapshot_service.h"
#include "p2p/master/p2p_master_service.h"
#include "p2p/ha/redis_util.h"
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
    for (const char* endpoint : {"", "127.0.0.1", "[::1]", "::1"}) {
        RedisOpLogStore store("invalid_endpoint_test", endpoint,
                              /*enable_write=*/true,
                              /*poll_interval_ms=*/10);
        EXPECT_EQ(ErrorCode::INTERNAL_ERROR, store.Init())
            << "endpoint=" << endpoint;
    }
}

TEST(RedisOpLogStoreStandaloneTest, OnlyPublishRouteIsBestEffort) {
    EXPECT_TRUE(IsBestEffortP2POpLog(OpType_PUBLISH_ROUTE));
    EXPECT_FALSE(IsBestEffortP2POpLog(OpType_WITHDRAW_ROUTE));
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

    std::vector<std::string> ScanKeys(const std::string& pattern) const {
        std::vector<std::string> keys;
        auto [host, port] = ParseEndpoint();
        redisContext* ctx = redisConnect(host.c_str(), port);
        if (!testing::AuthenticateRedisContext(ctx, redis_username_,
                                               redis_password_)) {
            if (ctx) redisFree(ctx);
            return keys;
        }
        std::string cursor = "0";
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
            redisReply* scanned_keys = scan->element[1];
            for (size_t i = 0; i < scanned_keys->elements; ++i) {
                redisReply* key = scanned_keys->element[i];
                if (!key || key->type != REDIS_REPLY_STRING) continue;
                keys.emplace_back(key->str, key->len);
            }
        } while (cursor != "0");
        redisFree(ctx);
        return keys;
    }

    void CleanupKeys() const {
        auto keys = ScanKeys("mooncake:" + cluster_id_ + ":oplog*");
        auto [host, port] = ParseEndpoint();
        redisContext* ctx = redisConnect(host.c_str(), port);
        if (!testing::AuthenticateRedisContext(ctx, redis_username_,
                                               redis_password_)) {
            if (ctx) redisFree(ctx);
            return;
        }
        for (const auto& key : keys) {
            RedisReplyPtr del((redisReply*)redisCommand(
                ctx, "DEL %b", key.data(), key.size()));
        }
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
                         const std::string& key = "redis_key",
                         OpType op_type = OpType::PUT_END) const {
        OpLogEntry entry;
        entry.sequence_id = sequence_id;
        entry.op_type = op_type;
        entry.object_key = key;
        entry.payload = "payload_" + std::to_string(sequence_id);
        entry.timestamp_ms = sequence_id * 10;
        return entry;
    }

    OpLogEntry MakeWithdrawEntry(uint64_t sequence_id,
                                 const std::string& key = "redis_key") const {
        WithdrawRoutePayload payload;
        payload.object_key = key;
        payload.client_id = {1, 2};
        payload.segment_id = {3, 4};
        OpLogEntry entry = MakeEntry(sequence_id, key, OpType_WITHDRAW_ROUTE);
        entry.payload = SerializeP2PPayload(payload);
        entry.checksum = static_cast<uint32_t>(
            XXH32(entry.payload.data(), entry.payload.size(), 0));
        entry.prefix_hash =
            key.empty()
                ? 0
                : static_cast<uint32_t>(XXH32(key.data(), key.size(), 0));
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

TEST_F(RedisOpLogStoreTest, OpLogKeysDoNotUseRedisHashTags) {
    auto writer = CreateWriter();
    ASSERT_EQ(ErrorCode::OK, writer->WriteOpLog(MakeEntry(1), true));
    ASSERT_EQ(ErrorCode::OK, writer->RecordSnapshotSequenceId("snap-a", 1));

    const std::string key_prefix = "mooncake:" + cluster_id_ + ":oplog";
    auto keys = ScanKeys(key_prefix + "*");
    EXPECT_NE(std::find(keys.begin(), keys.end(), key_prefix + ":latest"),
              keys.end());
    EXPECT_NE(std::find(keys.begin(), keys.end(), key_prefix + ":trimmed"),
              keys.end());
    EXPECT_NE(std::find(keys.begin(), keys.end(),
                        key_prefix + ":entry:00000000000000000001"),
              keys.end());
    EXPECT_NE(
        std::find(keys.begin(), keys.end(), key_prefix + ":snapshot:snap-a"),
        keys.end());
    for (const auto& key : keys) {
        EXPECT_EQ(std::string::npos, key.find('{')) << key;
        EXPECT_EQ(std::string::npos, key.find('}')) << key;
    }

    auto old_hash_tag_keys = ScanKeys("mooncake:{" + cluster_id_ + "}:oplog*");
    EXPECT_TRUE(old_hash_tag_keys.empty());
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

TEST_F(RedisOpLogStoreTest, StopWritingAbortsSyncWaitAndRejectsNewWrites) {
    std::shared_ptr<RedisOpLogStore> writer(CreateWriter());
    OpLogManager manager;
    manager.SetOpLogStore(writer);
    auto pending = std::async(std::launch::async, [&] {
        // seq=2 cannot commit while seq=1 is missing.
        return manager.PersistEntry(MakeEntry(2), true);
    });
    OpLogEntry stored;
    for (int i = 0; i < 200; ++i) {
        if (writer->ReadOpLog(2, stored) == ErrorCode::OK) break;
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    ASSERT_EQ(2u, stored.sequence_id);
    ASSERT_EQ(1, HAMetricManager::instance().get_oplog_async_queue_size());
    EXPECT_EQ(pending.wait_for(std::chrono::milliseconds(0)),
              std::future_status::timeout);

    manager.RequestStopWriting();
    ASSERT_EQ(pending.wait_for(std::chrono::seconds(1)),
              std::future_status::ready);
    EXPECT_EQ(pending.get(), ErrorCode::INTERNAL_ERROR);
    EXPECT_EQ(manager.PersistEntry(MakeEntry(1), true),
              ErrorCode::INTERNAL_ERROR);
    EXPECT_EQ(manager.PersistEntry(MakeEntry(1), false),
              ErrorCode::INTERNAL_ERROR);

    // Concurrent blocking stops and destruction must still join after
    // RequestStopWriting().
    auto stop = std::async(std::launch::async, [&] { manager.StopWriting(); });
    manager.StopWriting();
    stop.get();
    uint64_t latest = 99;
    ASSERT_EQ(writer->GetLatestSequenceId(latest), ErrorCode::OK);
    EXPECT_EQ(0u, latest);
    EXPECT_EQ(writer->ReadOpLog(1, stored), ErrorCode::OPLOG_ENTRY_NOT_FOUND);
    EXPECT_EQ(0, HAMetricManager::instance().get_oplog_async_queue_size());
}

TEST_F(RedisOpLogStoreTest, StopRacesWithSyncWaitAndIdleWorkers) {
    for (int i = 0; i < 50; ++i) {
        SCOPED_TRACE(i);
        auto writer = CreateWriter();
        // A missing seq=1 keeps seq=2 uncommitted, even if it reaches Redis.
        auto pending = std::async(std::launch::async, [&] {
            return writer->WriteOpLog(MakeEntry(2), true);
        });
        std::this_thread::yield();
        writer->RequestStopWriting();
        EXPECT_EQ(pending.wait_for(std::chrono::seconds(1)),
                  std::future_status::ready);
        writer->StopWriting();
        EXPECT_EQ(pending.get(), ErrorCode::INTERNAL_ERROR);
    }
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
    const auto remove = MakeWithdrawEntry(2);
    ASSERT_EQ(ErrorCode::OK, writer->WriteOpLog(remove, false));
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
    ASSERT_EQ(2u, entries.size());
    EXPECT_EQ(1u, entries[0].sequence_id);
    EXPECT_EQ(OpType_NOOP, entries[0].op_type);
    EXPECT_EQ(2u, entries[1].sequence_id);

    OpLogReadProgress progress;
    ASSERT_EQ(ErrorCode::OK,
              writer->ReadOpLogSinceWithProgress(0, 10, entries, progress));
    ASSERT_EQ(2u, entries.size());
    EXPECT_EQ(OpType_NOOP, entries[0].op_type);
    EXPECT_EQ(2u, progress.last_scanned_sequence_id);
    EXPECT_TRUE(
        FindMissingSequenceIds(0, entries, progress.last_scanned_sequence_id)
            .empty());

    auto reader = CreateReader();
    P2PStandbyMetadataStore metadata_store;
    const P2PRouteLocation location{UUID{1, 2}, UUID{3, 4}};
    ASSERT_TRUE(metadata_store.PublishRoute("redis_key", location,
                                            /*object_size=*/1024,
                                            /*sequence_id=*/0));
    P2POpLogApplier applier(&metadata_store, cluster_id_);
    auto notifier = reader->CreateChangeNotifier(cluster_id_);
    OpLogReplicator replicator(notifier.get(), &applier);
    const auto noops_before =
        HAMetricManager::instance().get_oplog_applied_noop_entries_total();
    ASSERT_TRUE(replicator.StartFromSequenceId(0));
    for (int i = 0; i < 100 && replicator.GetLastProcessedSequenceId() < 2;
         ++i) {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    replicator.Stop();
    EXPECT_EQ(2u, replicator.GetLastProcessedSequenceId());
    EXPECT_TRUE(applier.IsHealthy());
    EXPECT_EQ(3u, applier.GetExpectedSequenceId());
    EXPECT_FALSE(metadata_store.RouteExists("redis_key"));
    EXPECT_EQ(
        noops_before + 1,
        HAMetricManager::instance().get_oplog_applied_noop_entries_total());
}

TEST_F(RedisOpLogStoreTest, DroppedSequenceNeedsLaterPersistedEntry) {
    auto writer = std::make_unique<RedisOpLogStore>(
        cluster_id_, redis_endpoint_, /*enable_write=*/true,
        /*poll_interval_ms=*/10, redis_password_, redis_username_,
        /*db_index=*/0, /*async_queue_max_entries=*/1,
        OpLogAsyncQueueOverflowMode::BYPASS,
        /*best_effort_max_retries=*/3);
    ASSERT_EQ(ErrorCode::OK, writer->Init());

    ASSERT_EQ(ErrorCode::OK, writer->WriteOpLog(MakeEntry(3), false));
    OpLogEntry stored;
    for (int i = 0; i < 100; ++i) {
        if (writer->ReadOpLog(3, stored) == ErrorCode::OK) {
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    ASSERT_EQ(3u, stored.sequence_id);

    ASSERT_EQ(ErrorCode::OK,
              writer->WriteOpLog(
                  MakeEntry(1, "best_effort", OpType_PUBLISH_ROUTE), false));

    uint64_t latest = 0;
    for (int i = 0; i < 20; ++i) {
        ASSERT_EQ(ErrorCode::OK, writer->GetLatestSequenceId(latest));
        EXPECT_EQ(0u, latest);
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }

    ASSERT_EQ(ErrorCode::OK, writer->WriteOpLog(MakeEntry(2), true));
    ASSERT_EQ(ErrorCode::OK, writer->GetLatestSequenceId(latest));
    EXPECT_EQ(3u, latest);

    std::vector<OpLogEntry> entries;
    OpLogReadProgress progress;
    ASSERT_EQ(ErrorCode::OK,
              writer->ReadOpLogSinceWithProgress(0, 10, entries, progress));
    ASSERT_EQ(3u, entries.size());
    EXPECT_EQ(1u, entries[0].sequence_id);
    EXPECT_EQ(OpType_NOOP, entries[0].op_type);
    EXPECT_EQ(2u, entries[1].sequence_id);
    EXPECT_EQ(3u, entries[2].sequence_id);
    EXPECT_TRUE(
        FindMissingSequenceIds(0, entries, progress.last_scanned_sequence_id)
            .empty());
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
    ASSERT_EQ(1000u, entries.size());
    EXPECT_EQ(1u, entries.front().sequence_id);
    EXPECT_EQ(1000u, entries.back().sequence_id);
    EXPECT_EQ(OpType_NOOP, entries.front().op_type);
    EXPECT_EQ(1000u, progress.last_scanned_sequence_id);

    ASSERT_EQ(ErrorCode::OK,
              writer->ReadOpLogSinceWithProgress(
                  progress.last_scanned_sequence_id, 1000, entries, progress));
    ASSERT_EQ(1000u, entries.size());
    EXPECT_EQ(1001u, entries.front().sequence_id);
    EXPECT_EQ(2000u, entries.back().sequence_id);
    EXPECT_EQ(OpType_NOOP, entries.back().op_type);
    EXPECT_EQ(2000u, progress.last_scanned_sequence_id);

    ASSERT_EQ(ErrorCode::OK,
              writer->ReadOpLogSinceWithProgress(
                  progress.last_scanned_sequence_id, 1000, entries, progress));
    ASSERT_EQ(500u, entries.size());
    EXPECT_EQ(2001u, entries.front().sequence_id);
    EXPECT_EQ(OpType_NOOP, entries.front().op_type);
    EXPECT_EQ(2500u, entries.back().sequence_id);
    EXPECT_EQ(2500u, progress.last_scanned_sequence_id);
}

TEST_F(RedisOpLogStoreTest, ReplaysLargeSparseRangeAcrossPollBatches) {
    constexpr uint64_t kNoopCount = 1500;
    constexpr uint64_t kLastSequence = kNoopCount + 1;
    auto writer = CreateWriter();
    ASSERT_EQ(ErrorCode::OK, writer->UpdateLatestSequenceId(kLastSequence));
    ASSERT_EQ(ErrorCode::OK,
              writer->WriteOpLog(MakeWithdrawEntry(kLastSequence), false));

    OpLogEntry stored;
    for (int i = 0; i < 100; ++i) {
        if (writer->ReadOpLog(kLastSequence, stored) == ErrorCode::OK) {
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    ASSERT_EQ(kLastSequence, stored.sequence_id);

    auto reader = CreateReader();
    P2PStandbyMetadataStore metadata_store;
    const P2PRouteLocation location{UUID{1, 2}, UUID{3, 4}};
    ASSERT_TRUE(metadata_store.PublishRoute("redis_key", location,
                                            /*object_size=*/1024,
                                            /*sequence_id=*/0));
    P2POpLogApplier applier(&metadata_store, cluster_id_);
    auto notifier = reader->CreateChangeNotifier(cluster_id_);
    OpLogReplicator replicator(notifier.get(), &applier);
    const auto noops_before =
        HAMetricManager::instance().get_oplog_applied_noop_entries_total();
    ASSERT_TRUE(replicator.StartFromSequenceId(0));
    for (int i = 0;
         i < 500 && replicator.GetLastProcessedSequenceId() < kLastSequence;
         ++i) {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    replicator.Stop();

    EXPECT_EQ(kLastSequence, replicator.GetLastProcessedSequenceId());
    EXPECT_TRUE(applier.IsHealthy());
    EXPECT_EQ(kLastSequence + 1, applier.GetExpectedSequenceId());
    EXPECT_FALSE(metadata_store.RouteExists("redis_key"));
    EXPECT_EQ(0, HAMetricManager::instance().get_oplog_pending_entries());
    EXPECT_EQ(
        noops_before + kNoopCount,
        HAMetricManager::instance().get_oplog_applied_noop_entries_total());
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
    EXPECT_EQ(ErrorCode::OPLOG_TRIMMED, writer->ReadOpLogSince(0, 10, entries));
    uint64_t trimmed_sequence_id = 0;
    ASSERT_EQ(ErrorCode::OK, writer->GetTrimmedSequenceId(trimmed_sequence_id));
    EXPECT_EQ(3u, trimmed_sequence_id);
    ASSERT_EQ(ErrorCode::OK, writer->ReadOpLogSince(3, 10, entries));
    ASSERT_EQ(2u, entries.size());
    EXPECT_EQ(4u, entries[0].sequence_id);
    EXPECT_EQ(5u, entries[1].sequence_id);

    uint64_t max_sequence_id = 0;
    ASSERT_EQ(ErrorCode::OK, writer->GetMaxSequenceId(max_sequence_id));
    EXPECT_EQ(5u, max_sequence_id);
}

TEST_F(RedisOpLogStoreTest, MasterRegistryDiscoversAliveInstances) {
    RedisMasterRegistry registry(cluster_id_, redis_endpoint_, redis_username_,
                                 redis_password_, 0);
    RedisMasterRegistryEntry standby{"instance-a",
                                     "127.0.0.1:51051",
                                     "127.0.0.1:53051",
                                     "standby",
                                     true,
                                     42};
    RedisMasterRegistryEntry primary{"instance-b",
                                     "127.0.0.1:51052",
                                     "127.0.0.1:53052",
                                     "primary",
                                     false,
                                     43};
    ASSERT_EQ(registry.Refresh(standby), ErrorCode::OK);
    ASSERT_EQ(registry.Refresh(primary), ErrorCode::OK);

    std::vector<RedisMasterRegistryEntry> masters;
    ASSERT_EQ(registry.DiscoverAlive(std::chrono::seconds(10), masters),
              ErrorCode::OK);
    ASSERT_EQ(masters.size(), 2);
    auto standby_it = std::find_if(
        masters.begin(), masters.end(),
        [](const auto& master) { return master.instance_id == "instance-a"; });
    ASSERT_NE(standby_it, masters.end());
    EXPECT_EQ(standby_it->master_endpoint, standby.master_endpoint);
    EXPECT_EQ(standby_it->snapshot_endpoint, standby.snapshot_endpoint);
    EXPECT_EQ(standby_it->role, "standby");
    EXPECT_TRUE(standby_it->snapshot_ready);
    EXPECT_EQ(standby_it->applied_sequence_id, 42);

    EXPECT_EQ(registry.Remove(standby.instance_id), ErrorCode::OK);
    EXPECT_EQ(registry.Remove(primary.instance_id), ErrorCode::OK);
}

TEST_F(RedisOpLogStoreTest, MasterRegistryHeartbeatUpdatesAndUnregisters) {
    auto registry = std::make_unique<RedisMasterRegistry>(
        cluster_id_, redis_endpoint_, redis_username_, redis_password_, 0);
    RedisMasterRegistryHeartbeat heartbeat(
        std::move(registry),
        RedisMasterRegistryEntry{"heartbeat-instance", "127.0.0.1:51051",
                                 "127.0.0.1:53051", "starting", false},
        std::chrono::seconds(10));
    ASSERT_EQ(heartbeat.Start(), ErrorCode::OK);
    std::atomic<bool> snapshot_ready{false};
    heartbeat.SetAppliedSequenceProvider([] { return 42; });
    heartbeat.SetSnapshotReadyProvider(
        [&snapshot_ready] { return snapshot_ready.load(); });
    snapshot_ready.store(true);
    heartbeat.UpdateRole("standby", false);

    RedisMasterRegistry reader(cluster_id_, redis_endpoint_, redis_username_,
                               redis_password_, 0);
    std::vector<RedisMasterRegistryEntry> masters;
    const auto deadline =
        std::chrono::steady_clock::now() + std::chrono::seconds(2);
    do {
        ASSERT_EQ(reader.DiscoverAlive(std::chrono::seconds(10), masters),
                  ErrorCode::OK);
        if (masters.size() == 1 && masters.front().role == "standby" &&
            masters.front().snapshot_ready &&
            masters.front().applied_sequence_id == 42) {
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
    } while (std::chrono::steady_clock::now() < deadline);
    ASSERT_EQ(masters.size(), 1);
    EXPECT_EQ(masters.front().instance_id, "heartbeat-instance");
    EXPECT_EQ(masters.front().role, "standby");
    EXPECT_TRUE(masters.front().snapshot_ready);
    EXPECT_EQ(masters.front().applied_sequence_id, 42);

    heartbeat.Stop();
    ASSERT_EQ(reader.DiscoverAlive(std::chrono::seconds(10), masters),
              ErrorCode::OK);
    EXPECT_TRUE(masters.empty());
}

TEST_F(RedisOpLogStoreTest, ClearingProvidersWaitsForInflightCallback) {
    auto registry = std::make_unique<RedisMasterRegistry>(
        cluster_id_, redis_endpoint_, redis_username_, redis_password_, 0);
    RedisMasterRegistryHeartbeat heartbeat(
        std::move(registry),
        RedisMasterRegistryEntry{"provider-instance", "127.0.0.1:51051",
                                 "127.0.0.1:53051", "starting", false},
        std::chrono::seconds(10));
    ASSERT_EQ(heartbeat.Start(), ErrorCode::OK);

    std::atomic<bool> provider_entered{false};
    std::atomic<bool> release_provider{false};
    std::atomic<bool> providers_cleared{false};
    heartbeat.SetStandbyProviders(
        [&] {
            provider_entered.store(true, std::memory_order_release);
            while (!release_provider.load(std::memory_order_acquire)) {
                std::this_thread::yield();
            }
            return uint64_t{42};
        },
        [] { return true; });
    heartbeat.UpdateRole("standby", false);

    const auto deadline =
        std::chrono::steady_clock::now() + std::chrono::seconds(2);
    while (!provider_entered.load(std::memory_order_acquire) &&
           std::chrono::steady_clock::now() < deadline) {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    ASSERT_TRUE(provider_entered.load(std::memory_order_acquire));

    std::thread clear_thread([&] {
        heartbeat.ClearStandbyProviders();
        providers_cleared.store(true, std::memory_order_release);
    });
    std::this_thread::sleep_for(std::chrono::milliseconds(20));
    EXPECT_FALSE(providers_cleared.load(std::memory_order_acquire));

    release_provider.store(true, std::memory_order_release);
    clear_thread.join();
    EXPECT_TRUE(providers_cleared.load(std::memory_order_acquire));
    heartbeat.Stop();
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
    P2PMasterConfig config;
    config.oplog.enabled = true;
    config.oplog.store_type = "redis";
    config.cluster_id = cluster_id_;
    config.redis.endpoint = redis_endpoint_;
    config.redis.username = redis_username_;
    config.redis.password = redis_password_;
    config.oplog.data_dir = "127.0.0.1:notaport";

    EXPECT_NO_THROW({ P2PMasterService service(config); });
}

}  // namespace
}  // namespace mooncake

#endif  // STORE_USE_REDIS
