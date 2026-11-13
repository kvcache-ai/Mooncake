#ifdef STORE_USE_REDIS

#include <gtest/gtest.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdarg>
#include <cstdlib>
#include <cstring>
#include <map>
#include <mutex>
#include <string_view>
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
#include "p2p/ha/oplog/p2p_hot_standby_service.h"
#include "p2p/ha/oplog/p2p_standby_metadata_store.h"
#include "p2p/ha/ha_metric_manager.h"
#include "p2p/ha/ha_status.h"
#include "p2p/ha/oplog/p2p_standby_snapshot_service.h"
#include "p2p/master/p2p_master_service.h"
#include "p2p/ha/redis_util.h"
#include "../../redis_test_utils.h"

namespace mooncake {
namespace {

struct DelayedLatestReply {
    std::promise<void> entered;
    std::promise<void> release;
    std::future<void> released{release.get_future()};
};
std::atomic<DelayedLatestReply*> delayed_latest_reply{nullptr};
std::atomic<bool> fail_one_latest_write{false};
std::atomic<size_t> max_pipelined_sets{0};
std::atomic<bool> fail_one_pipelined_reply{false};
std::atomic<bool> fail_one_pipelined_status{false};
std::atomic<bool> trace_pipeline_sets{false};
std::atomic<bool> hold_initial_pipeline{false};
std::atomic<bool> hold_failed_retry{false};
std::atomic<bool> failed_retry_blocked{false};
std::mutex pipeline_trace_mutex;
std::map<std::string, size_t> pipeline_set_attempts;
std::vector<std::string> failed_pipeline_keys;
std::string failed_retry_key;
thread_local size_t appended_sets = 0;
thread_local std::vector<std::string> appended_set_keys;
thread_local std::vector<std::string> pipeline_set_keys;
thread_local size_t pipeline_replies_total = 0;
thread_local size_t pipeline_replies_remaining = 0;

void ResetPipelineTrace() {
    std::lock_guard<std::mutex> lock(pipeline_trace_mutex);
    pipeline_set_attempts.clear();
    failed_pipeline_keys.clear();
    failed_retry_key.clear();
    failed_retry_blocked.store(false);
    hold_initial_pipeline.store(true);
    hold_failed_retry.store(true);
    trace_pipeline_sets.store(true);
}

void StopPipelineTrace() {
    hold_initial_pipeline.store(false);
    hold_failed_retry.store(false);
    trace_pipeline_sets.store(false);
    fail_one_pipelined_reply.store(false);
    fail_one_pipelined_status.store(false);
}

void WaitForPipelineRelease(const std::atomic<bool>& hold) {
    const auto deadline =
        std::chrono::steady_clock::now() + std::chrono::seconds(10);
    while (hold.load() && std::chrono::steady_clock::now() < deadline) {
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
}

extern "C" void* __wrap_redisCommand(redisContext* ctx, const char* format,
                                     ...) {
    va_list args;
    va_start(args, format);
    va_list copy;
    va_copy(copy, args);
    DelayedLatestReply* delay = nullptr;
    if (std::strcmp(format, "SET %b %b") == 0) {
        const auto* key = va_arg(copy, const char*);
        const auto length = va_arg(copy, size_t);
        if (std::string_view(key, length).ends_with(":latest")) {
            if (fail_one_latest_write.exchange(false)) {
                va_end(copy);
                va_end(args);
                return nullptr;
            }
            delay = delayed_latest_reply.exchange(nullptr);
        }
    }
    va_end(copy);
    void* return_value = redisvCommand(ctx, format, args);
    va_end(args);
    if (delay) {
        // The worker still holds its queue lock until this reply is returned.
        delay->entered.set_value();
        delay->released.wait();
    }
    return return_value;
}

extern "C" int __wrap_redisAppendCommand(redisContext* ctx, const char* format,
                                         ...) {
    va_list args;
    va_start(args, format);
    std::string key;
    const bool tracing = trace_pipeline_sets.load();
    if (tracing && std::strcmp(format, "SET %b %b") == 0) {
        va_list copy;
        va_copy(copy, args);
        const auto* data = va_arg(copy, const char*);
        const auto length = va_arg(copy, size_t);
        key.assign(data, length);
        va_end(copy);
        // Queue the full workload before allowing workers to finish batches.
        WaitForPipelineRelease(hold_initial_pipeline);
        bool block_retry = false;
        {
            std::lock_guard<std::mutex> lock(pipeline_trace_mutex);
            block_retry = key == failed_retry_key &&
                          pipeline_set_attempts.count(key) != 0;
        }
        if (block_retry && hold_failed_retry.load()) {
            failed_retry_blocked.store(true);
            WaitForPipelineRelease(hold_failed_retry);
        }
    }
    const int result = redisvAppendCommand(ctx, format, args);
    va_end(args);
    if (result == REDIS_OK && std::strcmp(format, "SET %b %b") == 0) {
        const size_t count = ++appended_sets;
        size_t previous = max_pipelined_sets.load();
        while (count > previous &&
               !max_pipelined_sets.compare_exchange_weak(previous, count)) {
        }
        if (tracing) {
            appended_set_keys.push_back(key);
            std::lock_guard<std::mutex> lock(pipeline_trace_mutex);
            ++pipeline_set_attempts[key];
        }
    }
    return result;
}

extern "C" int __real_redisGetReply(redisContext* ctx, void** reply);
extern "C" int __wrap_redisGetReply(redisContext* ctx, void** reply) {
    if (appended_sets > 0) {
        pipeline_replies_total = appended_sets;
        pipeline_replies_remaining = appended_sets;
        appended_sets = 0;
        if (trace_pipeline_sets.load()) {
            pipeline_set_keys = std::move(appended_set_keys);
            appended_set_keys.clear();
        }
    }
    const int result = __real_redisGetReply(ctx, reply);
    if (result == REDIS_OK && pipeline_replies_remaining > 0) {
        const size_t reply_number =
            pipeline_replies_total - pipeline_replies_remaining + 1;
        --pipeline_replies_remaining;
        if (pipeline_replies_total > 1 && reply_number == 2 &&
            fail_one_pipelined_reply.exchange(false)) {
            if (trace_pipeline_sets.load()) {
                std::lock_guard<std::mutex> lock(pipeline_trace_mutex);
                failed_pipeline_keys = pipeline_set_keys;
                failed_retry_key = pipeline_set_keys.front();
            }
            freeReplyObject(*reply);
            *reply = nullptr;
            ctx->err = REDIS_ERR_IO;
            return REDIS_ERR;
        }
        if (pipeline_replies_total > 1 && reply_number == 2 &&
            fail_one_pipelined_status.exchange(false)) {
            if (trace_pipeline_sets.load()) {
                std::lock_guard<std::mutex> lock(pipeline_trace_mutex);
                failed_pipeline_keys = pipeline_set_keys;
                failed_retry_key = pipeline_set_keys[1];
            }
            // The SET reached Redis, but the worker cannot accept this reply.
            auto* status = static_cast<redisReply*>(*reply);
            if (status->type == REDIS_REPLY_STATUS && status->len >= 2) {
                std::memcpy(status->str, "NO", 2);
            }
        }
    }
    return result;
}

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

    void CheckPipelineFaultRecovery(bool reply_read_failure) {
        auto writer = CreateWriter();
        ResetPipelineTrace();
        // Declared after writer: release hooks before its destructor joins.
        struct TraceCleanup {
            ~TraceCleanup() { StopPipelineTrace(); }
        } cleanup;
        max_pipelined_sets.store(0);
        fail_one_pipelined_reply.store(reply_read_failure);
        fail_one_pipelined_status.store(!reply_read_failure);
        auto& metrics = HAMetricManager::instance();
        const auto failures_before =
            metrics.get_oplog_redis_pipeline_batch_failures_total();
        const auto retries_before =
            metrics.get_oplog_redis_pipeline_batch_retries_total();

        // With the initial appends held, four workers cannot drain these
        // entries one at a time; at least one batch must contain multiple SETs.
        for (uint64_t seq = 1; seq <= 40; ++seq) {
            ASSERT_EQ(ErrorCode::OK, writer->WriteOpLog(MakeEntry(seq), false));
        }
        hold_initial_pipeline.store(false);
        const auto deadline =
            std::chrono::steady_clock::now() + std::chrono::seconds(3);
        while (!failed_retry_blocked.load() &&
               std::chrono::steady_clock::now() < deadline) {
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
        ASSERT_TRUE(failed_retry_blocked.load());
        std::string retry_key;
        {
            std::lock_guard<std::mutex> lock(pipeline_trace_mutex);
            retry_key = failed_retry_key;
        }
        ASSERT_FALSE(retry_key.empty());
        const auto failed_sequence =
            std::stoull(retry_key.substr(retry_key.rfind(':') + 1));
        uint64_t latest = 0;
        ASSERT_EQ(ErrorCode::OK, writer->GetLatestSequenceId(latest));
        EXPECT_LT(latest, failed_sequence);
        ASSERT_EQ(ErrorCode::OK, writer->GetLocalCommittedSequenceId(latest));
        EXPECT_LT(latest, failed_sequence);

        hold_failed_retry.store(false);
        ASSERT_TRUE(writer->DrainWriting(5000).drained);
        // Join before inspecting attempts or disabling tracing.
        writer->StopWriting();
        EXPECT_GT(max_pipelined_sets.load(), 1u);
        EXPECT_FALSE(fail_one_pipelined_reply.load());
        EXPECT_FALSE(fail_one_pipelined_status.load());
        EXPECT_GT(metrics.get_oplog_redis_pipeline_batch_failures_total(),
                  failures_before);
        EXPECT_GT(metrics.get_oplog_redis_pipeline_batch_retries_total(),
                  retries_before);

        std::vector<std::string> failed_keys;
        std::map<std::string, size_t> attempts;
        {
            std::lock_guard<std::mutex> lock(pipeline_trace_mutex);
            failed_keys = failed_pipeline_keys;
            attempts = pipeline_set_attempts;
        }
        ASSERT_GT(failed_keys.size(), 1u);
        for (const auto& key : failed_keys) {
            EXPECT_EQ(reply_read_failure || key == retry_key ? 2u : 1u,
                      attempts[key])
                << key;
        }

        auto reader = CreateReader();
        ASSERT_EQ(ErrorCode::OK, reader->GetLatestSequenceId(latest));
        EXPECT_EQ(40u, latest);
        for (uint64_t seq = 1; seq <= 40; ++seq) {
            OpLogEntry entry;
            ASSERT_EQ(ErrorCode::OK, reader->ReadOpLog(seq, entry)) << seq;
            EXPECT_EQ("payload_" + std::to_string(seq), entry.payload);
        }
    }

    std::string redis_endpoint_;
    std::string redis_username_;
    std::string redis_password_;
    std::string cluster_id_;
    static inline int test_counter_ = 0;
};

TEST_F(RedisOpLogStoreTest, WriteReadAndLatestSequence) {
    HAMetricManager::instance().set_oplog_last_successful_write_timestamp_ms(0);
    HAMetricManager::instance().set_oplog_consecutive_write_failures(7);

    auto writer = CreateWriter();
    ASSERT_EQ(ErrorCode::OK, writer->WriteOpLog(MakeEntry(1), true));
    ASSERT_EQ(ErrorCode::OK, writer->WriteOpLog(MakeEntry(2), true));
    EXPECT_GT(HAMetricManager::instance()
                  .get_oplog_last_successful_write_timestamp_ms(),
              0);
    EXPECT_EQ(
        0, HAMetricManager::instance().get_oplog_consecutive_write_failures());

    auto reader = CreateReader();
    OpLogEntry entry;
    ASSERT_EQ(ErrorCode::OK, reader->ReadOpLog(2, entry));
    EXPECT_EQ(2u, entry.sequence_id);
    EXPECT_EQ("payload_2", entry.payload);

    uint64_t latest = 0;
    ASSERT_EQ(ErrorCode::OK, reader->GetLatestSequenceId(latest));
    EXPECT_EQ(2u, latest);
}

TEST_F(RedisOpLogStoreTest, LatestReadsPreserveLocalAppliedMetric) {
    struct StatusResetGuard {
        ~StatusResetGuard() { HAStatusManager::instance().Reset(); }
    } reset_guard;
    auto& status = HAStatusManager::instance();
    auto& metrics = HAMetricManager::instance();
    status.Reset();
    status.Configure(true, cluster_id_, "local");
    status.SetRoleState("primary", "RUNNING");

    std::shared_ptr<RedisOpLogStore> writer(CreateWriter());
    ASSERT_EQ(ErrorCode::OK, writer->WriteOpLog(MakeEntry(1), true));
    ASSERT_EQ(ErrorCode::OK, writer->WriteOpLog(MakeEntry(2), true));
    OpLogManager manager;
    manager.SetOpLogStore(writer);
    ASSERT_EQ(manager.SetInitialSequenceId(2), ErrorCode::OK);
    // Leave an allocated entry unpersisted so the two watermarks differ
    // deterministically, without racing an asynchronous Redis writer.
    const auto pending =
        manager.AllocateEntry(OpType::PUT_END, "pending", "payload");
    ASSERT_EQ(pending.sequence_id, 3u);
    const auto persisted = manager.ReadLatestOpLogSequenceId();
    ASSERT_TRUE(persisted.has_value());
    ASSERT_EQ(*persisted, 2u);
    status.SetOpLogProgress(manager.GetLastSequenceId(), *persisted);
    EXPECT_EQ(status.Snapshot().applied_sequence_id, 3u);
    EXPECT_EQ(status.Snapshot().latest_sequence_id, 2u);
    EXPECT_EQ(metrics.get_oplog_applied_sequence_id(), 3);

    uint64_t latest = 0;
    ASSERT_EQ(ErrorCode::OK, writer->GetLatestSequenceId(latest));
    EXPECT_EQ(2u, latest);
    EXPECT_EQ(metrics.get_oplog_last_sequence_id(), 2);
    EXPECT_EQ(metrics.get_oplog_applied_sequence_id(), 3);
    auto reader = CreateReader();
    ASSERT_EQ(reader->GetLatestSequenceId(latest), ErrorCode::OK);
    EXPECT_EQ(latest, 2u);
    EXPECT_EQ(metrics.get_oplog_applied_sequence_id(), 3);

    ASSERT_EQ(manager.PersistEntry(pending, /*sync=*/true), ErrorCode::OK);
    ASSERT_EQ(writer->GetLatestSequenceId(latest), ErrorCode::OK);
    EXPECT_EQ(latest, 3u);
    EXPECT_EQ(metrics.get_oplog_last_sequence_id(), 3);
    EXPECT_EQ(metrics.get_oplog_applied_sequence_id(), 3);
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

TEST_F(RedisOpLogStoreTest, DrainWritingWaitsForCommits) {
    std::shared_ptr<RedisOpLogStore> writer(CreateWriter());
    OpLogManager manager;
    manager.SetOpLogStore(writer);

    ASSERT_EQ(ErrorCode::OK, writer->WriteOpLog(MakeEntry(1), false));
    ASSERT_EQ(ErrorCode::OK, writer->WriteOpLog(MakeEntry(2), false));

    const auto drain = manager.DrainWriting(3000);
    EXPECT_TRUE(drain.drained);
    EXPECT_EQ(2u, drain.committed_sequence_id);
    EXPECT_EQ(2u, drain.max_known_sequence_id);
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

TEST_F(RedisOpLogStoreTest, NonblockingStopDoesNotWaitForLatestReply) {
    auto writer = CreateWriter();
    DelayedLatestReply delay;
    auto entered = delay.entered.get_future();
    std::promise<void> cleanup_started;
    auto cleanup_entered = cleanup_started.get_future();
    std::future<ErrorCode> pending;
    std::future<void> stop;
    std::future<void> cleanup;
    {
        // Release before joining workers or destroying futures, even on ASSERT.
        struct ReleaseReply {
            DelayedLatestReply& delay;
            RedisOpLogStore& writer;
            ~ReleaseReply() {
                delayed_latest_reply.store(nullptr);
                delay.release.set_value();
                writer.StopWriting();
            }
        } release{delay, *writer};
        delayed_latest_reply.store(&delay);
        pending = std::async(std::launch::async, [&] {
            return writer->WriteOpLog(MakeEntry(1), true);
        });
        ASSERT_EQ(entered.wait_for(std::chrono::seconds(2)),
                  std::future_status::ready);
        stop = std::async(std::launch::async,
                          [&] { writer->RequestStopWriting(); });
        EXPECT_EQ(stop.wait_for(std::chrono::milliseconds(200)),
                  std::future_status::ready);
        cleanup = std::async(std::launch::async, [&] {
            cleanup_started.set_value();
            writer->StopWriting();
        });
        ASSERT_EQ(cleanup_entered.wait_for(std::chrono::seconds(2)),
                  std::future_status::ready);
        EXPECT_EQ(cleanup.wait_for(std::chrono::milliseconds(100)),
                  std::future_status::timeout);
    }
    stop.get();
    EXPECT_EQ(cleanup.wait_for(std::chrono::seconds(2)),
              std::future_status::ready);
    cleanup.get();
    // The latest command already in flight may succeed despite cancellation.
    const auto result = pending.get();
    EXPECT_TRUE(result == ErrorCode::OK || result == ErrorCode::INTERNAL_ERROR);
    EXPECT_EQ(writer->WriteOpLog(MakeEntry(2), true),
              ErrorCode::INTERNAL_ERROR);
    EXPECT_EQ(writer->WriteOpLog(MakeEntry(2), false),
              ErrorCode::INTERNAL_ERROR);
    EXPECT_EQ(HAMetricManager::instance().get_oplog_async_queue_size(), 0);
}

TEST_F(RedisOpLogStoreTest, DrainWritingReturnsAfterLeaseLossStopRequest) {
    auto writer = CreateWriter();
    // seq=2 cannot commit while seq=1 is missing, so the writer keeps it
    // inflight and a drain cannot complete normally.
    auto pending = std::async(std::launch::async, [&] {
        return writer->WriteOpLog(MakeEntry(2), true);
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

    auto draining = std::async(std::launch::async,
                               [&] { return writer->DrainWriting(5000); });
    EXPECT_EQ(draining.wait_for(std::chrono::milliseconds(200)),
              std::future_status::timeout);

    // A lost lease cancels the writer while the active stepdown drain is in
    // progress. The drain must return promptly instead of waiting for a
    // timeout.
    writer->RequestStopWriting();
    ASSERT_EQ(draining.wait_for(std::chrono::seconds(1)),
              std::future_status::ready);
    EXPECT_FALSE(draining.get().drained);

    EXPECT_EQ(pending.wait_for(std::chrono::seconds(1)),
              std::future_status::ready);
    EXPECT_EQ(pending.get(), ErrorCode::INTERNAL_ERROR);
    writer->StopWriting();
}

TEST_F(RedisOpLogStoreTest,
       CancelledWriterDoesNotReportPendingWritesAsDrained) {
    auto writer = CreateWriter();
    // A missing seq=1 keeps seq=2 uncommitted without relying on Redis timing.
    ASSERT_EQ(writer->WriteOpLog(MakeEntry(2), false), ErrorCode::OK);
    const auto start = std::chrono::steady_clock::now();
    const auto timed_out = writer->DrainWriting(50);
    EXPECT_FALSE(timed_out.drained);
    EXPECT_EQ(timed_out.inflight_count, 1u);
    EXPECT_LT(std::chrono::steady_clock::now() - start,
              std::chrono::seconds(1));

    // Cancellation can precede entry to DrainWriting, not only interrupt it.
    writer->RequestStopWriting();
    const auto cancelled = writer->DrainWriting(5000);
    EXPECT_FALSE(cancelled.drained);
    EXPECT_EQ(cancelled.inflight_count, 1u);
    EXPECT_EQ(cancelled.committed_sequence_id, 0u);
    EXPECT_EQ(cancelled.max_known_sequence_id, 2u);
    writer->StopWriting();
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

TEST_F(RedisOpLogStoreTest, StopWritingWhileEntriesAreBeingPersisted) {
    auto writer = CreateWriter();
    const auto first = MakeEntry(1);
    ASSERT_EQ(writer->WriteOpLog(first, true), ErrorCode::OK);
    std::promise<void> producer_ready;
    auto ready = producer_ready.get_future();
    std::future<ErrorCode> producer;
    {
        // Stop on every exit before the producer future is joined/destroyed.
        struct StopWriter {
            RedisOpLogStore& writer;
            ~StopWriter() { writer.StopWriting(); }
        } cleanup{*writer};
        producer = std::async(std::launch::async, [&] {
            const auto deadline =
                std::chrono::steady_clock::now() + std::chrono::seconds(10);
            // Missing seq=2 keeps the committed prefix at 1 while writes run.
            for (uint64_t seq = 3; std::chrono::steady_clock::now() < deadline;
                 ++seq) {
                const auto result = writer->WriteOpLog(MakeEntry(seq), false);
                if (result != ErrorCode::OK) return result;
                if (seq == 102) producer_ready.set_value();
                // Stay below queue capacity until stop rejects a write.
                std::this_thread::sleep_for(std::chrono::milliseconds(1));
            }
            // Reaching the safety deadline must fail the rejection assertion.
            return ErrorCode::OK;
        });
        ASSERT_EQ(ready.wait_for(std::chrono::seconds(2)),
                  std::future_status::ready);
        OpLogEntry persisted;
        for (int i = 0; i < 200; ++i) {
            if (writer->ReadOpLog(3, persisted) == ErrorCode::OK) break;
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
        ASSERT_EQ(persisted.sequence_id, 3u);
        EXPECT_EQ(producer.wait_for(std::chrono::milliseconds(0)),
                  std::future_status::timeout);
        writer->RequestStopWriting();
        ASSERT_EQ(producer.wait_for(std::chrono::seconds(2)),
                  std::future_status::ready);
        EXPECT_EQ(producer.get(), ErrorCode::INTERNAL_ERROR);
    }
    const auto pattern = "mooncake:" + cluster_id_ + ":oplog:entry:*";
    const auto count = ScanKeys(pattern).size();
    EXPECT_GT(count, 1u);
    std::this_thread::sleep_for(std::chrono::milliseconds(200));
    EXPECT_EQ(ScanKeys(pattern).size(), count);
    uint64_t latest = 0;
    ASSERT_EQ(writer->GetLatestSequenceId(latest), ErrorCode::OK);
    EXPECT_EQ(latest, 1u);
    OpLogEntry stored;
    ASSERT_EQ(writer->ReadOpLog(1, stored), ErrorCode::OK);
    EXPECT_EQ(stored.payload, first.payload);
    EXPECT_EQ(writer->WriteOpLog(MakeEntry(2), true),
              ErrorCode::INTERNAL_ERROR);
    EXPECT_EQ(writer->WriteOpLog(MakeEntry(2), false),
              ErrorCode::INTERNAL_ERROR);
    EXPECT_EQ(HAMetricManager::instance().get_oplog_async_queue_size(), 0);
}

TEST_F(RedisOpLogStoreTest, PipelineReplyReadFailureRetriesWholeBatch) {
    CheckPipelineFaultRecovery(/*reply_read_failure=*/true);
}

TEST_F(RedisOpLogStoreTest, PipelineNonOkReplyRetriesOnlyFailedEntry) {
    CheckPipelineFaultRecovery(/*reply_read_failure=*/false);
}

TEST_F(RedisOpLogStoreTest, LatestWriteFailureRetriesWithoutNewEntry) {
    auto writer = CreateWriter();
    fail_one_latest_write.store(true);
    ASSERT_EQ(ErrorCode::OK, writer->WriteOpLog(MakeEntry(1), true));
    EXPECT_FALSE(fail_one_latest_write.load());

    uint64_t latest = 0;
    ASSERT_EQ(ErrorCode::OK, writer->GetLatestSequenceId(latest));
    EXPECT_EQ(1u, latest);
}

TEST_F(RedisOpLogStoreTest, RejectsInvalidWriteBatchSize) {
    for (size_t batch_size : {size_t{0}, kMaxRedisOpLogWriteBatchSize + 1}) {
        auto writer = OpLogStoreFactory::Create(
            OpLogStoreType::REDIS, cluster_id_, OpLogStoreRole::WRITER,
            redis_endpoint_, /*poll_interval_ms=*/10, redis_password_,
            redis_username_, /*db_index=*/0,
            /*async_queue_max_entries=*/100000, /*overflow_mode=*/"reject",
            /*best_effort_max_retries=*/3, batch_size);
        EXPECT_EQ(nullptr, writer) << "batch_size=" << batch_size;
    }
}

TEST_F(RedisOpLogStoreTest, ConfiguredBatchSizeAboveDefaultIsUsed) {
    max_pipelined_sets.store(0);
    RedisOpLogStore store(cluster_id_, redis_endpoint_, /*enable_write=*/true,
                          /*poll_interval_ms=*/10, redis_password_,
                          redis_username_, /*db_index=*/0,
                          /*async_queue_max_entries=*/100000,
                          OpLogAsyncQueueOverflowMode::REJECT,
                          /*best_effort_max_retries=*/3,
                          /*async_write_batch_size=*/128);
    ASSERT_EQ(ErrorCode::OK, store.Init());

    for (uint64_t seq = 2; seq <= 1000; ++seq) {
        ASSERT_EQ(ErrorCode::OK, store.WriteOpLog(MakeEntry(seq), false));
    }
    ASSERT_EQ(ErrorCode::OK, store.WriteOpLog(MakeEntry(1), true));

    auto reader = CreateReader();
    uint64_t latest = 0;
    const auto deadline =
        std::chrono::steady_clock::now() + std::chrono::seconds(3);
    do {
        ASSERT_EQ(ErrorCode::OK, reader->GetLatestSequenceId(latest));
        if (latest == 1000u) break;
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    } while (std::chrono::steady_clock::now() < deadline);
    EXPECT_EQ(1000u, latest);
    EXPECT_GT(max_pipelined_sets.load(), 64u);
    EXPECT_LE(max_pipelined_sets.load(), 128u);
}

class RedisOpLogDoctorTest : public RedisOpLogStoreTest {
   protected:
    void SetUp() override {
        RedisOpLogStoreTest::SetUp();
        if (IsSkipped()) return;
        auto [host, port] = ParseEndpoint();
        admin_.reset(redisConnect(host.c_str(), port));
        ASSERT_TRUE(testing::AuthenticateRedisContext(
            admin_.get(), redis_username_, redis_password_));
        user_ = cluster_id_ + "_doctor";
        ASSERT_TRUE(AllowEntries("*[3-9]"));
        status_.Reset();
        status_.Configure(true, cluster_id_, "test-primary", 1);
        status_.SetLeader("test-primary", 1);
        status_.SetMasterView("test-primary", 1);
        status_.SetRoleState("primary", "RUNNING");
        writer_ = std::make_unique<RedisOpLogStore>(
            cluster_id_, redis_endpoint_, true, 10, "doctor-test", user_);
        ASSERT_EQ(writer_->Init(), ErrorCode::OK);
    }

    void TearDown() override {
        writer_.reset();
        if (admin_ && !user_.empty()) {
            RedisReplyPtr reply((redisReply*)redisCommand(
                admin_.get(), "ACL DELUSER %s", user_.c_str()));
        }
        status_.Reset();
        RedisOpLogStoreTest::TearDown();
    }

    bool AllowEntries(const char* pattern) {
        RedisReplyPtr reply((redisReply*)redisCommand(
            admin_.get(),
            "ACL SETUSER %s reset on >doctor-test +@all "
            "~mooncake:%s:oplog:latest "
            "~mooncake:%s:oplog:trimmed "
            "~mooncake:%s:oplog:entry:%s",
            user_.c_str(), cluster_id_.c_str(), cluster_id_.c_str(),
            cluster_id_.c_str(), pattern));
        return reply && reply->type == REDIS_REPLY_STATUS;
    }

    template <typename Predicate>
    bool WaitUntil(Predicate predicate) {
        const auto deadline =
            std::chrono::steady_clock::now() + std::chrono::seconds(3);
        while (!predicate() && std::chrono::steady_clock::now() < deadline) {
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
        return predicate();
    }

    bool HasFailureCount(size_t count) {
        const auto report = status_.SerializeDoctorJson();
        return count == 0
                   ? report.find("OPLOG_WRITE_FAILED") == std::string::npos
                   : report.find("unrecovered_writes=" +
                                 std::to_string(count)) != std::string::npos;
    }

    bool HasLatestStall() {
        return status_.SerializeDoctorJson().find("OPLOG_LATEST_STALLED") !=
               std::string::npos;
    }

    HAStatusManager& status_{HAStatusManager::instance()};
    std::unique_ptr<redisContext, decltype(&redisFree)> admin_{nullptr,
                                                               &redisFree};
    std::string user_;
    std::unique_ptr<RedisOpLogStore> writer_;
};

TEST_F(RedisOpLogDoctorTest, CommitFailureReportsReasonAndClearsOnRecovery) {
    // Entries can persist, but committing the latest watermark is denied.
    RedisReplyPtr reply((redisReply*)redisCommand(
        admin_.get(), "ACL SETUSER %s resetkeys ~mooncake:%s:oplog:entry:*",
        user_.c_str(), cluster_id_.c_str()));
    ASSERT_TRUE(reply && reply->type == REDIS_REPLY_STATUS);
    ASSERT_EQ(writer_->WriteOpLog(MakeEntry(1), false), ErrorCode::OK);
    ASSERT_TRUE(WaitUntil([&] {
        return status_.SerializeDoctorJson().find("OPLOG_COMMIT_FAILED") !=
               std::string::npos;
    }));
    const auto report = status_.GetDoctorReport();
    const auto failure = std::find_if(
        report.issues.begin(), report.issues.end(),
        [](const auto& issue) { return issue.code == "OPLOG_COMMIT_FAILED"; });
    ASSERT_NE(failure, report.issues.end());
    EXPECT_NE(failure->message.find("target=1"), std::string::npos);
    EXPECT_NE(failure->message.find("NOPERM"), std::string::npos);

    ASSERT_TRUE(AllowEntries("*"));
    ASSERT_TRUE(WaitUntil([&] {
        uint64_t committed = 0;
        return writer_->GetLocalCommittedSequenceId(committed) ==
                   ErrorCode::OK &&
               committed == 1;
    }));
    EXPECT_EQ(status_.SerializeDoctorJson().find("OPLOG_COMMIT_FAILED"),
              std::string::npos);
}

TEST_F(RedisOpLogDoctorTest, LatestStallSurvivesRetriesAndNewEntries) {
    // The fixture's ACL keeps seq=1 failing, so new work cannot advance latest.
    ASSERT_EQ(writer_->WriteOpLog(MakeEntry(1), false), ErrorCode::OK);
    ASSERT_TRUE(WaitUntil([&] { return HasLatestStall(); }));
    const auto retries =
        HAMetricManager::instance().get_oplog_write_retries_total();
    ASSERT_EQ(writer_->WriteOpLog(MakeEntry(3), false), ErrorCode::OK);
    OpLogEntry entry;
    ASSERT_TRUE(WaitUntil([&] {
        return writer_->ReadOpLog(3, entry) == ErrorCode::OK &&
               HAMetricManager::instance().get_oplog_write_retries_total() >
                   retries;
    }));
    EXPECT_TRUE(HasFailureCount(1));
    EXPECT_TRUE(HasLatestStall());
    uint64_t latest = 0;
    ASSERT_EQ(writer_->GetLocalCommittedSequenceId(latest), ErrorCode::OK);
    EXPECT_EQ(latest, 0u);

    ASSERT_TRUE(AllowEntries("*"));
    ASSERT_TRUE(WaitUntil([&] {
        return writer_->GetLocalCommittedSequenceId(latest) == ErrorCode::OK &&
               latest == 1;
    }));
    EXPECT_FALSE(HasLatestStall());  // Progress resets the timer, not new work.
    ASSERT_TRUE(WaitUntil([&] { return HasLatestStall(); }));
    EXPECT_TRUE(HasFailureCount(0));  // Seq=3 still waits for missing seq=2.
    ASSERT_EQ(writer_->WriteOpLog(MakeEntry(2), true), ErrorCode::OK);
    EXPECT_FALSE(HasLatestStall());
    std::this_thread::sleep_for(std::chrono::milliseconds(1100));
    EXPECT_FALSE(HasLatestStall());  // No timer after all pending work drains.
}

TEST_F(RedisOpLogDoctorTest, CommittedRewritesDoNotStartLatestStallTimer) {
    ASSERT_EQ(writer_->UpdateLatestSequenceId(1), ErrorCode::OK);
    ASSERT_EQ(writer_->WriteOpLog(MakeEntry(1), false), ErrorCode::OK);
    ASSERT_TRUE(WaitUntil([&] { return HasFailureCount(1); }));
    std::this_thread::sleep_for(std::chrono::milliseconds(1100));
    EXPECT_FALSE(HasLatestStall());  // Even a failed rewrite needs no advance.

    ASSERT_EQ(writer_->WriteOpLog(MakeEntry(2), false), ErrorCode::OK);
    EXPECT_FALSE(HasLatestStall());
    ASSERT_TRUE(WaitUntil([&] { return HasLatestStall(); }));
    ASSERT_TRUE(AllowEntries("*"));
    ASSERT_TRUE(writer_->DrainWriting(3000).drained);
    EXPECT_FALSE(HasLatestStall());
    // Preserve the old test's coverage of successful committed rewrites too.
    ASSERT_EQ(writer_->WriteOpLog(MakeEntry(2), true), ErrorCode::OK);
    std::this_thread::sleep_for(std::chrono::milliseconds(1100));
    EXPECT_FALSE(HasLatestStall());
}

TEST_F(RedisOpLogDoctorTest, LatestStallIsReadableWhileRedisReplyIsBlocked) {
    ASSERT_TRUE(AllowEntries("*"));
    DelayedLatestReply delay;
    auto entered = delay.entered.get_future();
    std::future<std::string> diagnosis;
    std::future<void> stop;
    {
        // Release before joining workers or destroying futures, even on ASSERT.
        struct ReleaseReply {
            DelayedLatestReply& delay;
            RedisOpLogStore& writer;
            ~ReleaseReply() {
                delayed_latest_reply.store(nullptr);
                delay.release.set_value();
                writer.StopWriting();
            }
        } cleanup{delay, *writer_};
        delayed_latest_reply.store(&delay);
        ASSERT_EQ(writer_->WriteOpLog(MakeEntry(1), false), ErrorCode::OK);
        ASSERT_EQ(entered.wait_for(std::chrono::seconds(2)),
                  std::future_status::ready);
        std::this_thread::sleep_for(std::chrono::milliseconds(1100));
        diagnosis = std::async(std::launch::async,
                               [&] { return status_.SerializeDoctorJson(); });
        EXPECT_EQ(diagnosis.wait_for(std::chrono::milliseconds(200)),
                  std::future_status::ready);
        stop = std::async(std::launch::async,
                          [&] { writer_->RequestStopWriting(); });
        EXPECT_EQ(stop.wait_for(std::chrono::milliseconds(200)),
                  std::future_status::ready);
    }
    stop.get();
    EXPECT_NE(diagnosis.get().find("OPLOG_LATEST_STALLED"), std::string::npos);
    EXPECT_FALSE(HasLatestStall());
}

TEST_F(RedisOpLogDoctorTest, TracksFailuresUntilEachEntryRecovers) {
    const auto retries =
        HAMetricManager::instance().get_oplog_write_retries_total();
    ASSERT_EQ(writer_->WriteOpLog(MakeEntry(1), false), ErrorCode::OK);
    ASSERT_EQ(writer_->WriteOpLog(MakeEntry(2), false), ErrorCode::OK);
    ASSERT_TRUE(WaitUntil([&] {
        return HAMetricManager::instance().get_oplog_write_retries_total() >=
               retries + 6;
    }));
    EXPECT_TRUE(HasFailureCount(2));  // Repeated retries do not add failures.
    ASSERT_EQ(writer_->WriteOpLog(MakeEntry(3), false), ErrorCode::OK);
    OpLogEntry entry;
    ASSERT_TRUE(WaitUntil(
        [&] { return writer_->ReadOpLog(3, entry) == ErrorCode::OK; }));
    EXPECT_TRUE(
        HasFailureCount(2));  // Another worker's success cannot clear it.

    ASSERT_TRUE(AllowEntries("*[2-9]"));
    ASSERT_TRUE(WaitUntil([&] { return HasFailureCount(1); }));
    EXPECT_NE(status_.SerializeDoctorJson().find("degraded"),
              std::string::npos);
    // Leave seq=5 waiting for missing seq=4, so recovery need not drain the
    // queue.
    ASSERT_EQ(writer_->WriteOpLog(MakeEntry(5), false), ErrorCode::OK);
    ASSERT_TRUE(AllowEntries("*"));
    ASSERT_TRUE(WaitUntil([&] { return HasFailureCount(0); }));
    EXPECT_GT(HAMetricManager::instance().get_oplog_async_queue_size(), 0);
    EXPECT_NE(status_.SerializeDoctorJson().find("\"ok\""), std::string::npos);
}

TEST_F(RedisOpLogDoctorTest, ClearsDroppedAndStoppedFailuresIndependently) {
    ASSERT_EQ(writer_->WriteOpLog(MakeEntry(1, "required"), false),
              ErrorCode::OK);
    ASSERT_EQ(writer_->WriteOpLog(
                  MakeEntry(2, "optional", OpType_PUBLISH_ROUTE), false),
              ErrorCode::OK);
    ASSERT_TRUE(WaitUntil([&] { return HasFailureCount(2); }));
    ASSERT_TRUE(WaitUntil([&] {
        return HAMetricManager::instance().get_oplog_async_queue_size() == 1;
    }));
    EXPECT_TRUE(HasFailureCount(1));
    writer_->StopWriting();
    EXPECT_TRUE(HasFailureCount(0));
}

TEST_F(RedisOpLogDoctorTest, DroppingLastFailedEntryClearsIssue) {
    ASSERT_EQ(writer_->WriteOpLog(
                  MakeEntry(1, "optional", OpType_PUBLISH_ROUTE), false),
              ErrorCode::OK);
    ASSERT_TRUE(WaitUntil([&] { return HasFailureCount(1); }));
    ASSERT_TRUE(WaitUntil([&] {
        return HAMetricManager::instance().get_oplog_async_queue_size() == 0;
    }));
    EXPECT_TRUE(HasFailureCount(0));
    uint64_t latest = 0;
    ASSERT_EQ(writer_->GetLocalCommittedSequenceId(latest), ErrorCode::OK);
    EXPECT_EQ(latest, 0u);
    std::this_thread::sleep_for(std::chrono::milliseconds(1100));
    EXPECT_FALSE(HasLatestStall());  // A dropped-only tail is idle.
    ASSERT_TRUE(AllowEntries("*"));
    ASSERT_EQ(writer_->WriteOpLog(MakeEntry(2), true), ErrorCode::OK);
    ASSERT_EQ(writer_->GetLocalCommittedSequenceId(latest), ErrorCode::OK);
    EXPECT_EQ(latest, 2u);  // Later progress can bridge the allowed hole.
    EXPECT_FALSE(HasLatestStall());
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

class RedisPromotionTest : public RedisOpLogStoreTest {
   protected:
    void SetUp() override {
        RedisOpLogStoreTest::SetUp();
        if (IsSkipped()) return;
        auto& status = HAStatusManager::instance();
        status.Reset();
        status.Configure(true, cluster_id_, "test-primary");
        status.SetRoleState("primary", "STARTING");
    }

    void TearDown() override {
        RedisOpLogStoreTest::TearDown();
        HAStatusManager::instance().Reset();
    }
};

class RedisPromotionSequenceTest
    : public RedisPromotionTest,
      public ::testing::WithParamInterface<uint64_t> {};

TEST_P(RedisPromotionSequenceTest, NewWritesPreserveUnappliedCommittedEntries) {
    {
        auto old_writer = CreateWriter();
        for (uint64_t sequence_id = 1; sequence_id <= 3; ++sequence_id) {
            ASSERT_EQ(old_writer->WriteOpLog(MakeEntry(sequence_id), true),
                      ErrorCode::OK);
        }
    }

    P2PMasterConfig config;
    config.oplog.enabled = true;
    config.oplog.store_type = "redis";
    config.cluster_id = cluster_id_;
    config.redis.endpoint = redis_endpoint_;
    config.redis.username = redis_username_;
    config.redis.password = redis_password_;
    config.oplog.data_dir = "127.0.0.1:notaport";

    P2PMasterService promoted(config);
    // Empty metadata isolates sequence initialization from metadata replay.
    ASSERT_EQ(promoted.RestoreFromStandbyMetadata(/*metadata=*/{}, GetParam()),
              ErrorCode::OK);
    ASSERT_NE(promoted.GetOpLogManager(), nullptr);
    EXPECT_EQ(promoted.GetOpLogManager()->GetLastSequenceId(), 3);
    const auto expect_diagnosis = [&] {
        const auto report = HAStatusManager::instance().GetDoctorReport();
        const auto issue = std::find_if(
            report.issues.begin(), report.issues.end(), [](const auto& item) {
                return item.code == "PROMOTION_CATCHUP_INCOMPLETE";
            });
        if (GetParam() < 3) {
            ASSERT_NE(issue, report.issues.end());
            EXPECT_NE(issue->message.find("oplog_applied_sequence_id=" +
                                          std::to_string(GetParam()) +
                                          ", oplog_latest_sequence_id=3"),
                      std::string::npos);
        } else {
            EXPECT_EQ(issue, report.issues.end());
        }
    };
    expect_diagnosis();

    auto sequence = promoted.GetOpLogManager()->AppendAndPersist(
        OpType::PUT_END, "new-primary", "new-payload", true);
    ASSERT_TRUE(sequence.has_value());
    EXPECT_EQ(sequence.value(), 4);

    auto reader = CreateReader();
    uint64_t latest = 0;
    ASSERT_EQ(reader->GetLatestSequenceId(latest), ErrorCode::OK);
    EXPECT_EQ(latest, 4);
    // New writes must not clear or rewrite the original promotion event.
    expect_diagnosis();
    for (uint64_t sequence_id = 1; sequence_id <= 3; ++sequence_id) {
        OpLogEntry entry;
        ASSERT_EQ(reader->ReadOpLog(sequence_id, entry), ErrorCode::OK);
        EXPECT_EQ(entry.object_key, MakeEntry(sequence_id).object_key);
        EXPECT_EQ(entry.payload, MakeEntry(sequence_id).payload);
    }
    OpLogEntry new_entry;
    ASSERT_EQ(reader->ReadOpLog(4, new_entry), ErrorCode::OK);
    EXPECT_EQ(new_entry.object_key, "new-primary");
    EXPECT_EQ(new_entry.payload, "new-payload");
}

INSTANTIATE_TEST_SUITE_P(RestoredProgress, RedisPromotionSequenceTest,
                         ::testing::Values(0, 1, 3));

TEST_F(RedisPromotionTest, BatchLimitPromotionPreservesTailAndReportsCatchup) {
    // Keep background delivery paused so the real Promote() loop does all
    // replay. The counting reader still executes the real Redis scans.
    class PausedNotifier : public OpLogChangeNotifier {
       public:
        ErrorCode Start(uint64_t, EntryCallback, ErrorCallback,
                        MaintenanceCallback = {}) override {
            return ErrorCode::OK;
        }
        void Stop() override {}
        bool IsHealthy() const override { return true; }
    };
    class CountingReader : public RedisOpLogStore {
       public:
        CountingReader(const P2PHotStandbyConfig& config,
                       std::atomic<size_t>& batch_reads)
            : RedisOpLogStore(config.cluster_id, config.redis_endpoint, false,
                              10, config.redis_password, config.redis_username),
              batch_reads_(batch_reads) {}
        std::unique_ptr<OpLogChangeNotifier> CreateChangeNotifier(
            const std::string&, size_t = 1000) override {
            return std::make_unique<PausedNotifier>();
        }
        ErrorCode ReadOpLogSinceWithProgress(
            uint64_t start, size_t limit, std::vector<OpLogEntry>& entries,
            OpLogReadProgress& progress) override {
            EXPECT_EQ(start, batch_reads_.fetch_add(1) * 1000);
            EXPECT_EQ(limit, 1000u);
            return RedisOpLogStore::ReadOpLogSinceWithProgress(
                start, limit, entries, progress);
        }

       private:
        std::atomic<size_t>& batch_reads_;
    };

    P2PHotStandbyConfig standby_config;
    standby_config.cluster_id = cluster_id_;
    standby_config.oplog_store_type = OpLogStoreType::REDIS;
    standby_config.redis_endpoint = redis_endpoint_;
    standby_config.redis_username = redis_username_;
    standby_config.redis_password = redis_password_;
    // Promotion's fixed scan limit must not use the background poll setting.
    standby_config.oplog_poll_batch_size = 5000;
    std::atomic<size_t> batch_reads{0};
    P2PHotStandbyService standby(standby_config, [&] {
        auto reader =
            std::make_unique<CountingReader>(standby_config, batch_reads);
        EXPECT_EQ(reader->Init(), ErrorCode::OK);
        return reader;
    });
    auto& status = HAStatusManager::instance();
    status.SetRoleState("standby", "WATCHING");
    ASSERT_EQ(standby.Start(), ErrorCode::OK);

    constexpr uint64_t kCaughtUp = 100 * 1000;
    constexpr uint64_t kLatest = kCaughtUp + 1;
    const UUID first_client{1, 1};
    const UUID tail_client{2, 2};
    const auto make_registration = [&](uint64_t sequence, UUID client) {
        auto entry = MakeEntry(sequence, "", OpType_REGISTER_CLIENT);
        entry.payload = SerializeP2PPayload(
            RegisterClientPayload{client, "127.0.0.1", 50051, {}});
        entry.checksum = static_cast<uint32_t>(
            XXH32(entry.payload.data(), entry.payload.size(), 0));
        return entry;
    };
    const auto first = make_registration(1, first_client);
    const auto tail = make_registration(kLatest, tail_client);
    {
        auto writer = CreateWriter();
        ASSERT_EQ(writer->WriteOpLog(first, true), ErrorCode::OK);
        // Sparse committed IDs exercise 100 real scan batches without
        // constructing 100,000 unrelated metadata records.
        ASSERT_EQ(writer->UpdateLatestSequenceId(kCaughtUp), ErrorCode::OK);
        ASSERT_EQ(writer->WriteOpLog(tail, true), ErrorCode::OK);
    }
    ASSERT_EQ(standby.GetLatestAppliedSequenceId(), 0u);
    EXPECT_EQ(batch_reads.load(), 0u);
    EXPECT_EQ(status.SerializeDoctorJson().find("PROMOTION_CATCHUP_INCOMPLETE"),
              std::string::npos);

    ASSERT_EQ(standby.Promote(), ErrorCode::OK);
    EXPECT_EQ(standby.GetState(), StandbyState::PROMOTED);
    EXPECT_EQ(batch_reads.load(), 100u);
    const auto applied = standby.GetLatestAppliedSequenceId();
    ASSERT_EQ(applied, kCaughtUp);
    const auto metadata = standby.ExportMetadata();
    ASSERT_EQ(metadata.clients.size(), 1u);
    EXPECT_EQ(metadata.clients.count(first_client), 1u);
    EXPECT_EQ(metadata.clients.count(tail_client), 0u);

    P2PMasterConfig master_config;
    master_config.oplog.enabled = true;
    master_config.oplog.store_type = "redis";
    master_config.cluster_id = cluster_id_;
    master_config.redis.endpoint = redis_endpoint_;
    master_config.redis.username = redis_username_;
    master_config.redis.password = redis_password_;
    status.SetRoleState("primary", "STARTING");
    P2PMasterService promoted(master_config);
    ASSERT_EQ(promoted.RestoreFromStandbyMetadata(metadata, applied),
              ErrorCode::OK);
    status.SetLeader("test-primary", 1);
    status.SetRoleState("primary", "RUNNING");
    const auto expect_diagnosis = [&] {
        const auto report = status.GetDoctorReport();
        EXPECT_EQ(report.health, "degraded");
        const auto issue = std::find_if(
            report.issues.begin(), report.issues.end(), [](const auto& item) {
                return item.code == "PROMOTION_CATCHUP_INCOMPLETE";
            });
        ASSERT_NE(issue, report.issues.end());
        EXPECT_NE(issue->message.find("oplog_applied_sequence_id=100000, "
                                      "oplog_latest_sequence_id=100001"),
                  std::string::npos);
    };
    expect_diagnosis();

    P2PRegisterClientRequest request;
    request.client_id = UUID{3, 3};
    request.ip_address = "127.0.0.1";
    request.rpc_port = 50051;
    ASSERT_TRUE(promoted.RegisterClient(request).has_value());
    ASSERT_NE(promoted.GetOpLogManager(), nullptr);
    EXPECT_EQ(promoted.GetOpLogManager()->GetLastSequenceId(), kLatest + 1);
    ASSERT_TRUE(promoted.GetOpLogManager()->DrainWriting(5000).drained);
    auto reader = CreateReader();
    uint64_t latest = 0;
    ASSERT_EQ(reader->GetLatestSequenceId(latest), ErrorCode::OK);
    EXPECT_EQ(latest, kLatest + 1);
    for (const auto& original :
         {first, tail, make_registration(kLatest + 1, request.client_id)}) {
        OpLogEntry stored;
        ASSERT_EQ(reader->ReadOpLog(original.sequence_id, stored),
                  ErrorCode::OK);
        EXPECT_EQ(stored.op_type, original.op_type);
        EXPECT_EQ(stored.payload, original.payload);
    }
    expect_diagnosis();
}

TEST_F(RedisOpLogStoreTest, PromotionWriterStartSkipsSnapshotCoveredGap) {
    std::shared_ptr<RedisOpLogStore> writer(CreateWriter());
    ASSERT_EQ(ErrorCode::OK, writer->UpdateLatestSequenceId(100));

    OpLogManager manager;
    manager.SetOpLogStore(writer);
    ASSERT_EQ(manager.SetInitialSequenceId(101), ErrorCode::OK);
    ASSERT_EQ(manager.GetLastSequenceId(), 101u);

    {
        auto reader = CreateReader();
        uint64_t latest = 0;
        ASSERT_EQ(reader->GetLatestSequenceId(latest), ErrorCode::OK);
        EXPECT_EQ(latest, 100u);
    }

    const auto sequence = manager.AppendAndPersist(
        OpType::PUT_END, "promoted-key", "promoted-payload", /*sync=*/true);
    ASSERT_TRUE(sequence.has_value());
    EXPECT_EQ(sequence.value(), 102u);

    auto reader = CreateReader();
    uint64_t latest = 0;
    ASSERT_EQ(reader->GetLatestSequenceId(latest), ErrorCode::OK);
    EXPECT_EQ(latest, 102u);

    std::vector<OpLogEntry> entries;
    ASSERT_EQ(reader->ReadOpLogSince(100, 10, entries), ErrorCode::OK);
    ASSERT_EQ(entries.size(), 2u);
    EXPECT_EQ(entries[0].sequence_id, 101u);
    EXPECT_EQ(entries[0].op_type, OpType_NOOP);
    EXPECT_EQ(entries[1].sequence_id, 102u);
    EXPECT_EQ(entries[1].object_key, "promoted-key");
    EXPECT_EQ(entries[1].payload, "promoted-payload");
}

TEST_F(RedisOpLogStoreTest, PromotionRejectsUnreadableOrChangedLatest) {
    {
        auto old_writer = CreateWriter();
        ASSERT_EQ(old_writer->UpdateLatestSequenceId(200), ErrorCode::OK);
    }

    P2PMasterConfig config;
    config.oplog.enabled = true;
    config.oplog.store_type = "redis";
    config.cluster_id = cluster_id_;
    config.redis.endpoint = redis_endpoint_;
    config.redis.username = redis_username_;
    config.redis.password = redis_password_;
    config.oplog.data_dir = "127.0.0.1:notaport";

    P2PMasterService promoted(config);
    auto [host, port] = ParseEndpoint();
    std::unique_ptr<redisContext, decltype(&redisFree)> admin(
        redisConnect(host.c_str(), port), &redisFree);
    ASSERT_TRUE(testing::AuthenticateRedisContext(admin.get(), redis_username_,
                                                  redis_password_));
    const auto key = "mooncake:" + cluster_id_ + ":oplog:latest";

    RedisReplyPtr corrupt(
        (redisReply*)redisCommand(admin.get(), "SET %s invalid", key.c_str()));
    ASSERT_TRUE(corrupt);
    EXPECT_EQ(promoted.RestoreFromStandbyMetadata(/*metadata=*/{}, 0),
              ErrorCode::INTERNAL_ERROR);
    EXPECT_EQ(promoted.GetOpLogManager()->GetLastSequenceId(), 0);

    RedisReplyPtr changed(
        (redisReply*)redisCommand(admin.get(), "SET %s 300", key.c_str()));
    ASSERT_TRUE(changed);
    EXPECT_EQ(promoted.RestoreFromStandbyMetadata(/*metadata=*/{}, 100),
              ErrorCode::INTERNAL_ERROR);
    EXPECT_EQ(promoted.GetOpLogManager()->GetLastSequenceId(), 0);

    RedisReplyPtr repair(
        (redisReply*)redisCommand(admin.get(), "SET %s 200", key.c_str()));
    ASSERT_TRUE(repair);
    ASSERT_EQ(promoted.RestoreFromStandbyMetadata(/*metadata=*/{}, 100),
              ErrorCode::OK);
    auto sequence = promoted.GetOpLogManager()->AppendAndPersist(
        OpType::PUT_END, "new-key", "new-payload", true);
    ASSERT_TRUE(sequence.has_value());
    EXPECT_EQ(sequence.value(), 201);

    auto reader = CreateReader();
    uint64_t latest = 0;
    ASSERT_EQ(reader->GetLatestSequenceId(latest), ErrorCode::OK);
    EXPECT_EQ(latest, 201);
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
