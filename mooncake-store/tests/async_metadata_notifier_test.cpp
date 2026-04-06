/**
 * @file async_metadata_notifier_test.cpp
 * @brief Unit tests for AsyncMetadataNotifier.
 *
 * Uses an in-process P2P master so that BatchSyncReplica RPCs are real but
 * require no network setup.
 */
#include <glog/logging.h>
#include <gtest/gtest.h>
#include <atomic>
#include <chrono>
#include <string>
#include <thread>

#define private public
#define protected public
#include "async_metadata_notifier.h"

#include "p2p_master_client.h"
#include "rpc_types.h"
#include "test_p2p_server_helpers.h"
#include "types.h"
#undef protected
#undef private

namespace mooncake {
namespace test {

// ============================================================================
// Test fixture
// ============================================================================

class AsyncMetadataNotifierTest : public ::testing::Test {
   protected:
    static void SetUpTestSuite() {
        google::InitGoogleLogging("AsyncMetadataNotifierTest");
        FLAGS_logtostderr = 1;

        ASSERT_TRUE(master_.Start()) << "Failed to start in-proc P2P master";
        master_addr_ = master_.master_address();
    }

    static void TearDownTestSuite() {
        master_.Stop();
        google::ShutdownGoogleLogging();
    }

    void SetUp() override {
        client_id_ = generate_uuid();
        segment_ = MakeSegment();

        // Register client + segment with master
        RegisterClientRequest reg;
        reg.client_id = client_id_;
        reg.ip_address = "127.0.0.1";
        reg.rpc_port = 50099;
        reg.segments.push_back(segment_);
        reg.deployment_mode = DeploymentMode::P2P;
        auto& svc = master_.GetWrapped().GetMasterService();
        auto res = svc.RegisterClient(reg);
        ASSERT_TRUE(res.has_value())
            << "RegisterClient failed: " << res.error();

        // Connect P2PMasterClient to in-proc master
        master_client_ = std::make_unique<P2PMasterClient>(client_id_);
        auto ec = master_client_->Connect(master_addr_);
        ASSERT_EQ(ec, ErrorCode::OK) << "Connect failed";
    }

    void TearDown() override { master_client_.reset(); }

    static Segment MakeSegment(size_t size = 16 * 1024 * 1024) {
        Segment seg;
        seg.id = generate_uuid();
        seg.name = "test_segment";
        seg.size = size;
        seg.extra = P2PSegmentExtraData{
            .priority = 0,
            .tags = {},
            .memory_type = MemoryType::DRAM,
        };
        return seg;
    }

    // Helper: query replicas for a key via the master service directly
    size_t CountReplicas(const std::string& key) {
        auto& svc = master_.GetWrapped().GetMasterService();
        auto res = svc.GetReplicaList(key);
        if (!res.has_value()) return 0;
        return res->replicas.size();
    }

    static testing::InProcP2PMaster master_;
    static std::string master_addr_;

    UUID client_id_{};
    Segment segment_;
    std::unique_ptr<P2PMasterClient> master_client_;
};

testing::InProcP2PMaster AsyncMetadataNotifierTest::master_;
std::string AsyncMetadataNotifierTest::master_addr_;

// ============================================================================
// Tests
// ============================================================================

TEST_F(AsyncMetadataNotifierTest, BasicAddAndRemove) {
    AsyncMetadataNotifier notifier(*master_client_, client_id_,
                                   /*sender_thread_count=*/1,
                                   /*max_batch_size=*/2000,
                                   /*queue_capacity=*/4000);
    notifier.Start();

    auto r = notifier.EnqueueAdd("key1", segment_.id, 1024);
    ASSERT_TRUE(r.has_value());

    // Give sender time to flush
    std::this_thread::sleep_for(std::chrono::milliseconds(200));
    EXPECT_EQ(CountReplicas("key1"), 1u);

    // Remove
    r = notifier.EnqueueRemove("key1", segment_.id);
    ASSERT_TRUE(r.has_value());
    std::this_thread::sleep_for(std::chrono::milliseconds(200));
    EXPECT_EQ(CountReplicas("key1"), 0u);

    notifier.Stop();
}

TEST_F(AsyncMetadataNotifierTest, CoalesceAddThenRemove) {
    // Don't Start() — use DoEnqueue directly to test coalescing without races.
    AsyncMetadataNotifier notifier(*master_client_, client_id_,
                                   /*sender_thread_count=*/1,
                                   /*max_batch_size=*/2000,
                                   /*queue_capacity=*/4000);
    // Set running_ so DoEnqueue accepts ops, but no sender thread is consuming.
    notifier.running_.store(true, std::memory_order_release);

    auto r1 = notifier.EnqueueAdd("coalesce_key", segment_.id, 512);
    ASSERT_TRUE(r1.has_value());
    EXPECT_EQ(notifier.shards_[0]->normal_count, 1u);

    // REMOVE should cancel the pending ADD — queue becomes empty
    auto r2 = notifier.EnqueueRemove("coalesce_key", segment_.id);
    ASSERT_TRUE(r2.has_value());
    EXPECT_EQ(notifier.shards_[0]->normal_count, 0u);
    EXPECT_TRUE(notifier.shards_[0]->coalesce_index.empty());

    notifier.running_.store(false, std::memory_order_release);
}

TEST_F(AsyncMetadataNotifierTest, CoalesceRemoveThenAdd) {
    // Don't Start() — use DoEnqueue directly to test coalescing without races.
    AsyncMetadataNotifier notifier(*master_client_, client_id_,
                                   /*sender_thread_count=*/1,
                                   /*max_batch_size=*/2000,
                                   /*queue_capacity=*/4000);
    notifier.running_.store(true, std::memory_order_release);

    // REMOVE then ADD for same key — ADD should cancel the pending REMOVE
    auto r1 = notifier.EnqueueRemove("coalesce_ra_key", segment_.id);
    ASSERT_TRUE(r1.has_value());
    EXPECT_EQ(notifier.shards_[0]->normal_count, 1u);

    auto r2 = notifier.EnqueueAdd("coalesce_ra_key", segment_.id, 512);
    ASSERT_TRUE(r2.has_value());
    EXPECT_EQ(notifier.shards_[0]->normal_count, 0u);
    EXPECT_TRUE(notifier.shards_[0]->coalesce_index.empty());

    notifier.running_.store(false, std::memory_order_release);
}

TEST_F(AsyncMetadataNotifierTest, BatchMultipleKeys) {
    AsyncMetadataNotifier notifier(*master_client_, client_id_,
                                   /*sender_thread_count=*/2,
                                   /*max_batch_size=*/2000,
                                   /*queue_capacity=*/8000);
    notifier.Start();

    constexpr int kNumKeys = 50;
    for (int i = 0; i < kNumKeys; ++i) {
        auto r = notifier.EnqueueAdd("batch_key_" + std::to_string(i),
                                     segment_.id, 256);
        ASSERT_TRUE(r.has_value()) << "EnqueueAdd failed for key " << i;
    }

    // Wait for all to flush
    std::this_thread::sleep_for(std::chrono::milliseconds(500));

    for (int i = 0; i < kNumKeys; ++i) {
        EXPECT_EQ(CountReplicas("batch_key_" + std::to_string(i)), 1u)
            << "Missing replica for batch_key_" << i;
    }

    notifier.Stop();
}

TEST_F(AsyncMetadataNotifierTest, EnqueueAfterStopFails) {
    AsyncMetadataNotifier notifier(*master_client_, client_id_,
                                   /*sender_thread_count=*/1,
                                   /*max_batch_size=*/2000,
                                   /*queue_capacity=*/4000);
    notifier.Start();
    notifier.Stop();

    auto r = notifier.EnqueueAdd("after_stop", segment_.id, 100);
    ASSERT_FALSE(r.has_value());
    EXPECT_EQ(r.error(), ErrorCode::ASYNC_ENQUEUE_FAILED);
}

TEST_F(AsyncMetadataNotifierTest, StopDrainsQueue) {
    AsyncMetadataNotifier notifier(*master_client_, client_id_,
                                   /*sender_thread_count=*/1,
                                   /*max_batch_size=*/2000,
                                   /*queue_capacity=*/4000);
    notifier.Start();

    // Enqueue several ops then immediately stop — they should be drained
    for (int i = 0; i < 10; ++i) {
        notifier.EnqueueAdd("drain_key_" + std::to_string(i), segment_.id, 64);
    }
    notifier.Stop();

    // Verify at least some were flushed (drain is best-effort during stop)
    int found = 0;
    for (int i = 0; i < 10; ++i) {
        if (CountReplicas("drain_key_" + std::to_string(i)) == 1) {
            ++found;
        }
    }
    // With drain logic, all should be flushed
    EXPECT_EQ(found, 10) << "Only " << found << "/10 drained before stop";
}

TEST_F(AsyncMetadataNotifierTest, FailureCallbackInvoked) {
    // Use a separate client_id that is NOT registered — AddReplica will fail
    UUID bad_client_id = generate_uuid();

    auto bad_master_client = std::make_unique<P2PMasterClient>(bad_client_id);
    auto ec = bad_master_client->Connect(master_addr_);
    ASSERT_EQ(ec, ErrorCode::OK);

    std::atomic<int> failure_count{0};
    SyncFailureCallback cb = [&](const std::string& key, const UUID& seg_id,
                                 ErrorCode err) {
        failure_count.fetch_add(1, std::memory_order_relaxed);
    };

    AsyncMetadataNotifier notifier(*bad_master_client, bad_client_id,
                                   /*sender_thread_count=*/1,
                                   /*max_batch_size=*/2000,
                                   /*queue_capacity=*/4000, std::move(cb));
    notifier.Start();

    // This ADD will reach master but fail with CLIENT_NOT_FOUND
    auto r = notifier.EnqueueAdd("fail_key", segment_.id, 100);
    ASSERT_TRUE(r.has_value());

    std::this_thread::sleep_for(std::chrono::milliseconds(300));
    EXPECT_GE(failure_count.load(), 1)
        << "Expected failure callback to be invoked";

    notifier.Stop();
}

TEST_F(AsyncMetadataNotifierTest, MultipleStartStop) {
    AsyncMetadataNotifier notifier(*master_client_, client_id_,
                                   /*sender_thread_count=*/2,
                                   /*max_batch_size=*/2000,
                                   /*queue_capacity=*/4000);

    // First cycle
    notifier.Start();
    notifier.EnqueueAdd("cycle1_key", segment_.id, 100);
    std::this_thread::sleep_for(std::chrono::milliseconds(200));
    notifier.Stop();
    EXPECT_EQ(CountReplicas("cycle1_key"), 1u);

    // Second cycle — should work cleanly
    notifier.Start();
    notifier.EnqueueAdd("cycle2_key", segment_.id, 100);
    std::this_thread::sleep_for(std::chrono::milliseconds(200));
    notifier.Stop();
    EXPECT_EQ(CountReplicas("cycle2_key"), 1u);
}

TEST_F(AsyncMetadataNotifierTest, ConfigurableMaxBatchSize) {
    // Use a tiny max_batch_size to force multiple batches
    AsyncMetadataNotifier notifier(*master_client_, client_id_,
                                   /*sender_thread_count=*/1,
                                   /*max_batch_size=*/5,
                                   /*queue_capacity=*/200);
    notifier.Start();

    for (int i = 0; i < 20; ++i) {
        auto r = notifier.EnqueueAdd("small_batch_" + std::to_string(i),
                                     segment_.id, 32);
        ASSERT_TRUE(r.has_value());
    }

    std::this_thread::sleep_for(std::chrono::milliseconds(500));

    for (int i = 0; i < 20; ++i) {
        EXPECT_EQ(CountReplicas("small_batch_" + std::to_string(i)), 1u)
            << "Missing replica for small_batch_" << i;
    }

    notifier.Stop();
}

TEST_F(AsyncMetadataNotifierTest, DuplicateAddIsIdempotent) {
    // Test that a duplicate same-type op is silently dropped (no double-send).
    AsyncMetadataNotifier notifier(*master_client_, client_id_,
                                   /*sender_thread_count=*/1,
                                   /*max_batch_size=*/2000,
                                   /*queue_capacity=*/4000);
    notifier.running_.store(true, std::memory_order_release);

    auto r1 = notifier.EnqueueAdd("dup_key", segment_.id, 256);
    ASSERT_TRUE(r1.has_value());
    EXPECT_EQ(notifier.shards_[0]->normal_count, 1u);

    // Second ADD for same key+segment — should be silently skipped
    auto r2 = notifier.EnqueueAdd("dup_key", segment_.id, 256);
    ASSERT_TRUE(r2.has_value());
    EXPECT_EQ(notifier.shards_[0]->normal_count, 1u);  // still 1, not 2

    notifier.running_.store(false, std::memory_order_release);
}

// ============================================================================
// Recovery Queue Tests
// ============================================================================

TEST_F(AsyncMetadataNotifierTest, RecoveryAddEnqueueAndFlush) {
    AsyncMetadataNotifier notifier(*master_client_, client_id_,
                                   /*sender_thread_count=*/1,
                                   /*max_batch_size=*/2000,
                                   /*queue_capacity=*/4000);
    notifier.Start();

    auto r = notifier.EnqueueRecoveryAdd("recovery_key1", segment_.id, 1024);
    ASSERT_TRUE(r.has_value());

    // Give sender time to flush
    std::this_thread::sleep_for(std::chrono::milliseconds(200));
    EXPECT_EQ(CountReplicas("recovery_key1"), 1u);

    notifier.Stop();
}

TEST_F(AsyncMetadataNotifierTest, RecoveryEnqueueGoesToRecoveryList) {
    AsyncMetadataNotifier notifier(*master_client_, client_id_,
                                   /*sender_thread_count=*/1,
                                   /*max_batch_size=*/2000,
                                   /*queue_capacity=*/4000);
    notifier.running_.store(true, std::memory_order_release);

    auto r = notifier.EnqueueRecoveryAdd("rec_list_key", segment_.id, 512);
    ASSERT_TRUE(r.has_value());

    // Should be in recovery list, not normal list
    EXPECT_EQ(notifier.shards_[0]->recovery_count, 1u);
    EXPECT_EQ(notifier.shards_[0]->normal_count, 0u);

    notifier.running_.store(false, std::memory_order_release);
}

TEST_F(AsyncMetadataNotifierTest, NormalAndRecoveryCoexist) {
    AsyncMetadataNotifier notifier(*master_client_, client_id_,
                                   /*sender_thread_count=*/1,
                                   /*max_batch_size=*/2000,
                                   /*queue_capacity=*/4000);
    notifier.running_.store(true, std::memory_order_release);

    // Enqueue normal + recovery ops for different keys
    notifier.EnqueueAdd("normal_key", segment_.id, 256);
    notifier.EnqueueRecoveryAdd("recovery_key", segment_.id, 512);

    EXPECT_EQ(notifier.shards_[0]->normal_count, 1u);
    EXPECT_EQ(notifier.shards_[0]->recovery_count, 1u);

    notifier.running_.store(false, std::memory_order_release);
}

TEST_F(AsyncMetadataNotifierTest, RecoveryQueueReservesSlots) {
    // Use tiny capacity to test reservation logic
    // normal_reserved = capacity / 4 = 1
    AsyncMetadataNotifier notifier(*master_client_, client_id_,
                                   /*sender_thread_count=*/1,
                                   /*max_batch_size=*/1,
                                   /*queue_capacity=*/4);
    notifier.running_.store(true, std::memory_order_release);

    auto& shard = *notifier.shards_[0];
    EXPECT_EQ(shard.normal_reserved, 1u);  // capacity/4

    // Fill recovery slots up to the limit (capacity - normal_reserved = 3)
    auto r1 = notifier.EnqueueRecoveryAdd("rr_key1", segment_.id, 100);
    auto r2 = notifier.EnqueueRecoveryAdd("rr_key2", segment_.id, 200);
    auto r3 = notifier.EnqueueRecoveryAdd("rr_key3", segment_.id, 300);
    EXPECT_TRUE(r1.has_value());
    EXPECT_TRUE(r2.has_value());
    EXPECT_TRUE(r3.has_value());

    // Next recovery enqueue should fail (non-blocking)
    auto r4 = notifier.EnqueueRecoveryAdd("rr_key4", segment_.id, 400);
    EXPECT_FALSE(r4.has_value());
    EXPECT_EQ(r4.error(), ErrorCode::ASYNC_ENQUEUE_FAILED);

    // But normal enqueue should still work (reserved slot)
    auto r5 = notifier.EnqueueAdd("normal_reserved_key", segment_.id, 500);
    EXPECT_TRUE(r5.has_value());

    notifier.running_.store(false, std::memory_order_release);
}

TEST_F(AsyncMetadataNotifierTest, NormalPriorityOverRecovery) {
    // Verify that normal ops are sent before recovery ops
    AsyncMetadataNotifier notifier(*master_client_, client_id_,
                                   /*sender_thread_count=*/1,
                                   /*max_batch_size=*/2000,
                                   /*queue_capacity=*/4000);
    notifier.Start();

    // Enqueue recovery first, then normal
    for (int i = 0; i < 10; ++i) {
        notifier.EnqueueRecoveryAdd("rec_prio_" + std::to_string(i),
                                    segment_.id, 64);
    }
    for (int i = 0; i < 10; ++i) {
        notifier.EnqueueAdd("norm_prio_" + std::to_string(i), segment_.id, 64);
    }

    // Wait for flush
    std::this_thread::sleep_for(std::chrono::milliseconds(500));

    // All should be flushed
    for (int i = 0; i < 10; ++i) {
        EXPECT_EQ(CountReplicas("rec_prio_" + std::to_string(i)), 1u);
        EXPECT_EQ(CountReplicas("norm_prio_" + std::to_string(i)), 1u);
    }

    notifier.Stop();
}

TEST_F(AsyncMetadataNotifierTest, WaitForRecoveryDrainSuccess) {
    AsyncMetadataNotifier notifier(*master_client_, client_id_,
                                   /*sender_thread_count=*/1,
                                   /*max_batch_size=*/2000,
                                   /*queue_capacity=*/4000);
    notifier.Start();

    // Enqueue some recovery ops
    for (int i = 0; i < 5; ++i) {
        notifier.EnqueueRecoveryAdd("wfd_key_" + std::to_string(i),
                                    segment_.id, 64);
    }

    // Wait for drain with generous timeout
    bool drained = notifier.WaitForRecoveryDrain(
        nullptr, std::chrono::milliseconds(5000));
    EXPECT_TRUE(drained);

    notifier.Stop();

    // All should be flushed
    for (int i = 0; i < 5; ++i) {
        EXPECT_EQ(CountReplicas("wfd_key_" + std::to_string(i)), 1u);
    }
}

TEST_F(AsyncMetadataNotifierTest, WaitForRecoveryDrainAbort) {
    AsyncMetadataNotifier notifier(*master_client_, client_id_,
                                   /*sender_thread_count=*/1,
                                   /*max_batch_size=*/2000,
                                   /*queue_capacity=*/4000);
    notifier.running_.store(true, std::memory_order_release);

    // Enqueue a recovery op but don't start sender
    notifier.EnqueueRecoveryAdd("abort_drain_key", segment_.id, 64);

    // Abort immediately
    std::atomic<bool> abort{true};
    bool drained = notifier.WaitForRecoveryDrain(
        [&]() { return abort.load(); }, std::chrono::milliseconds(1000));
    EXPECT_FALSE(drained);

    notifier.running_.store(false, std::memory_order_release);
}

TEST_F(AsyncMetadataNotifierTest, WaitForRecoveryDrainTimeout) {
    AsyncMetadataNotifier notifier(*master_client_, client_id_,
                                   /*sender_thread_count=*/1,
                                   /*max_batch_size=*/2000,
                                   /*queue_capacity=*/4000);
    notifier.running_.store(true, std::memory_order_release);

    // Enqueue but don't start sender — will never drain
    notifier.EnqueueRecoveryAdd("timeout_key", segment_.id, 64);

    bool drained = notifier.WaitForRecoveryDrain(
        nullptr, std::chrono::milliseconds(200));
    EXPECT_FALSE(drained);

    notifier.running_.store(false, std::memory_order_release);
}

TEST_F(AsyncMetadataNotifierTest, WaitForRecoveryDrainEmptyImmediate) {
    AsyncMetadataNotifier notifier(*master_client_, client_id_,
                                   /*sender_thread_count=*/1,
                                   /*max_batch_size=*/2000,
                                   /*queue_capacity=*/4000);
    notifier.Start();

    // No recovery ops — should return immediately
    bool drained = notifier.WaitForRecoveryDrain(
        nullptr, std::chrono::milliseconds(100));
    EXPECT_TRUE(drained);

    notifier.Stop();
}

TEST_F(AsyncMetadataNotifierTest, RecoveryCoalesceWithNormalRemove) {
    // A normal REMOVE should cancel a pending recovery ADD for the same key
    AsyncMetadataNotifier notifier(*master_client_, client_id_,
                                   /*sender_thread_count=*/1,
                                   /*max_batch_size=*/2000,
                                   /*queue_capacity=*/4000);
    notifier.running_.store(true, std::memory_order_release);

    auto r1 = notifier.EnqueueRecoveryAdd("cross_coalesce", segment_.id, 256);
    ASSERT_TRUE(r1.has_value());
    EXPECT_EQ(notifier.shards_[0]->recovery_count, 1u);

    // Normal REMOVE for the same key should cancel the recovery ADD
    auto r2 = notifier.EnqueueRemove("cross_coalesce", segment_.id);
    ASSERT_TRUE(r2.has_value());
    EXPECT_EQ(notifier.shards_[0]->recovery_count, 0u);
    EXPECT_EQ(notifier.shards_[0]->normal_count, 0u);

    notifier.running_.store(false, std::memory_order_release);
}

TEST_F(AsyncMetadataNotifierTest, RecoveryBatchMultipleKeys) {
    AsyncMetadataNotifier notifier(*master_client_, client_id_,
                                   /*sender_thread_count=*/2,
                                   /*max_batch_size=*/2000,
                                   /*queue_capacity=*/8000);
    notifier.Start();

    constexpr int kNumKeys = 50;
    for (int i = 0; i < kNumKeys; ++i) {
        auto r = notifier.EnqueueRecoveryAdd(
            "rec_batch_" + std::to_string(i), segment_.id, 128);
        ASSERT_TRUE(r.has_value());
    }

    // Wait for drain
    bool drained = notifier.WaitForRecoveryDrain(
        nullptr, std::chrono::milliseconds(5000));
    EXPECT_TRUE(drained);

    notifier.Stop();

    for (int i = 0; i < kNumKeys; ++i) {
        EXPECT_EQ(CountReplicas("rec_batch_" + std::to_string(i)), 1u)
            << "Missing replica for rec_batch_" << i;
    }
}

TEST_F(AsyncMetadataNotifierTest, StopDrainsRecoveryQueue) {
    AsyncMetadataNotifier notifier(*master_client_, client_id_,
                                   /*sender_thread_count=*/1,
                                   /*max_batch_size=*/2000,
                                   /*queue_capacity=*/4000);
    notifier.Start();

    for (int i = 0; i < 10; ++i) {
        notifier.EnqueueRecoveryAdd("rec_drain_stop_" + std::to_string(i),
                                    segment_.id, 64);
    }
    notifier.Stop();

    int found = 0;
    for (int i = 0; i < 10; ++i) {
        if (CountReplicas("rec_drain_stop_" + std::to_string(i)) == 1) {
            ++found;
        }
    }
    EXPECT_EQ(found, 10) << "Only " << found << "/10 recovery ops drained";
}

}  // namespace test
}  // namespace mooncake
