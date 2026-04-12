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

    // Enqueue all ops before Stop() — sender drains until queue is empty before
    // exiting, so all ops are guaranteed to be sent.
    for (int i = 0; i < 10; ++i) {
        notifier.EnqueueAdd("drain_key_" + std::to_string(i), segment_.id, 64);
    }
    notifier.Stop();

    for (int i = 0; i < 10; ++i) {
        EXPECT_EQ(CountReplicas("drain_key_" + std::to_string(i)), 1u)
            << "drain_key_" << i << " was not sent before Stop()";
    }
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

    // MaxRetryCount=3 with backoffs 100ms+200ms; add margin for RPC overhead.
    std::this_thread::sleep_for(std::chrono::milliseconds(1000));
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
    // Duplicate same-type ops are silently dropped (no double-send).
    AsyncMetadataNotifier notifier(*master_client_, client_id_,
                                   /*sender_thread_count=*/1,
                                   /*max_batch_size=*/2000,
                                   /*queue_capacity=*/4000);
    notifier.running_.store(true, std::memory_order_release);

    auto r1 = notifier.EnqueueAdd("dup_key", segment_.id, 256);
    ASSERT_TRUE(r1.has_value());
    EXPECT_EQ(notifier.shards_[0]->normal_count, 1u);

    // Second ADD for same key+segment — silently skipped
    auto r2 = notifier.EnqueueAdd("dup_key", segment_.id, 256);
    ASSERT_TRUE(r2.has_value());
    EXPECT_EQ(notifier.shards_[0]->normal_count, 1u);  // still 1, not 2

    // Second REMOVE for same key+segment — also silently skipped
    auto r3 = notifier.EnqueueRemove("dup_key", segment_.id);
    ASSERT_TRUE(r3.has_value());
    // ADD cancelled by REMOVE → count drops to 0
    EXPECT_EQ(notifier.shards_[0]->normal_count, 0u);
    auto r4 = notifier.EnqueueRemove("dup_key", segment_.id);
    ASSERT_TRUE(r4.has_value());
    EXPECT_EQ(notifier.shards_[0]->normal_count, 0u);  // still 0, not 1

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
    uint64_t queue_capacity = 4;
    uint64_t normal_reserved = queue_capacity / 4;
    AsyncMetadataNotifier notifier(*master_client_, client_id_,
                                   /*sender_thread_count=*/1,
                                   /*max_batch_size=*/1, queue_capacity);
    notifier.running_.store(true, std::memory_order_release);

    auto& shard = *notifier.shards_[0];
    EXPECT_EQ(shard.normal_reserved, normal_reserved);

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
    // Verify that CollectBatch drains normal ops before recovery ops.
    // Use max_batch_size=5 and no sender thread so we can call CollectBatch
    // directly and observe which list is dequeued first.
    AsyncMetadataNotifier notifier(*master_client_, client_id_,
                                   /*sender_thread_count=*/1,
                                   /*max_batch_size=*/5,
                                   /*queue_capacity=*/4000);
    notifier.running_.store(true, std::memory_order_release);

    // Enqueue recovery ops first — they should be sent LAST
    for (int i = 0; i < 5; ++i) {
        notifier.EnqueueRecoveryAdd("rec_prio_" + std::to_string(i),
                                    segment_.id, 64);
    }
    // Enqueue normal ops second — they should be sent FIRST
    for (int i = 0; i < 5; ++i) {
        notifier.EnqueueAdd("norm_prio_" + std::to_string(i), segment_.id, 64);
    }

    ASSERT_EQ(notifier.shards_[0]->normal_count, 5u);
    ASSERT_EQ(notifier.shards_[0]->recovery_count, 5u);

    // CollectBatch with max_batch_size=5 must return only normal ops
    size_t n =
        notifier.CollectBatch(*notifier.shards_[0], notifier.batch_buffers_[0]);
    ASSERT_EQ(n, 5u);

    // Normal queue must now be empty; recovery queue must still hold all 5 ops
    EXPECT_EQ(notifier.shards_[0]->normal_count, 0u);
    EXPECT_EQ(notifier.shards_[0]->recovery_count, 5u);

    // Every collected op must belong to the normal list (key prefix
    // "norm_prio_")
    for (size_t i = 0; i < n; ++i) {
        EXPECT_NE(notifier.batch_buffers_[0][i].key.find("norm_prio_"),
                  std::string::npos)
            << "Expected normal op at batch index " << i
            << " but got key: " << notifier.batch_buffers_[0][i].key;
    }

    notifier.running_.store(false, std::memory_order_release);
}

TEST_F(AsyncMetadataNotifierTest, WaitForRecoveryDrainSuccess) {
    // Use a small batch size so the sender needs multiple cycles to drain all
    // ops, making it necessary for WaitForRecoveryDrain to actually block.
    AsyncMetadataNotifier notifier(*master_client_, client_id_,
                                   /*sender_thread_count=*/1,
                                   /*max_batch_size=*/3,
                                   /*queue_capacity=*/4000);
    notifier.Start();

    constexpr int kNumKeys = 15;
    for (int i = 0; i < kNumKeys; ++i) {
        notifier.EnqueueRecoveryAdd("wfd_key_" + std::to_string(i), segment_.id,
                                    64);
    }

    bool drained =
        notifier.WaitForRecoveryDrain(nullptr, std::chrono::milliseconds(5000));
    EXPECT_TRUE(drained);

    // Verify BEFORE Stop(): Stop() also drains the queue, which would mask a
    // buggy WaitForRecoveryDrain that returned before all ops were actually
    // sent.
    for (int i = 0; i < kNumKeys; ++i) {
        EXPECT_EQ(CountReplicas("wfd_key_" + std::to_string(i)), 1u);
    }

    notifier.Stop();
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

    bool drained =
        notifier.WaitForRecoveryDrain(nullptr, std::chrono::milliseconds(200));
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
    bool drained =
        notifier.WaitForRecoveryDrain(nullptr, std::chrono::milliseconds(100));
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
        auto r = notifier.EnqueueRecoveryAdd("rec_batch_" + std::to_string(i),
                                             segment_.id, 128);
        ASSERT_TRUE(r.has_value());
    }

    // Wait for drain
    bool drained =
        notifier.WaitForRecoveryDrain(nullptr, std::chrono::milliseconds(5000));
    EXPECT_TRUE(drained);

    for (int i = 0; i < kNumKeys; ++i) {
        EXPECT_EQ(CountReplicas("rec_batch_" + std::to_string(i)), 1u)
            << "Missing replica for rec_batch_" << i;
    }

    notifier.Stop();
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

    for (int i = 0; i < 10; ++i) {
        EXPECT_EQ(CountReplicas("rec_drain_stop_" + std::to_string(i)), 1u)
            << "rec_drain_stop_" << i << " was not sent before Stop()";
    }
}

TEST_F(AsyncMetadataNotifierTest, StopDropsPendingOps) {
    // Compare Stop(drop_pending=true) vs Stop(drop_pending=false):
    //   true  — sender exits after current batch; queued ops are dropped.
    //   false — sender drains the full queue before exiting.
    //
    // max_batch_size=1 limits throughput so the sender cannot process all
    // kNumKeys ops in the time it takes the enqueue loop + Stop() to run.
    constexpr int kNumKeys = 200;

    // --- Part 1: Stop(drop_pending=true) ---
    {
        AsyncMetadataNotifier notifier(*master_client_, client_id_,
                                       /*sender_thread_count=*/1,
                                       /*max_batch_size=*/1,
                                       /*queue_capacity=*/4000);
        notifier.Start();
        for (int i = 0; i < kNumKeys; ++i)
            notifier.EnqueueAdd("drop_" + std::to_string(i), segment_.id, 64);
        notifier.Stop(/*drop_pending=*/true);

        int sent = 0;
        for (int i = 0; i < kNumKeys; ++i)
            sent += (CountReplicas("drop_" + std::to_string(i)) == 1u) ? 1 : 0;
        // With max_batch_size=1 the sender processes one RPC at a time; 200 ops
        // cannot all be sent in the time the enqueue loop + Stop() take to run.
        EXPECT_LT(sent, kNumKeys)
            << "Stop(drop_pending=true) should leave some ops unsent";
    }

    // --- Part 2: Stop(drop_pending=false) ---
    {
        AsyncMetadataNotifier notifier(*master_client_, client_id_,
                                       /*sender_thread_count=*/1,
                                       /*max_batch_size=*/1,
                                       /*queue_capacity=*/4000);
        notifier.Start();
        for (int i = 0; i < kNumKeys; ++i)
            notifier.EnqueueAdd("drain_" + std::to_string(i), segment_.id, 64);
        notifier.Stop(/*drop_pending=*/false);

        for (int i = 0; i < kNumKeys; ++i)
            EXPECT_EQ(CountReplicas("drain_" + std::to_string(i)), 1u)
                << "Stop(drop_pending=false) should drain all ops";
    }
}

}  // namespace test
}  // namespace mooncake
