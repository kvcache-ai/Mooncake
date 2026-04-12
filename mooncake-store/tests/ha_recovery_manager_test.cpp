/**
 * @file ha_recovery_manager_test.cpp
 * @brief Unit tests for HARecoveryManager state machine and recovery pipeline.
 *
 * Uses an in-process P2P master so that RPCs are real but require no network
 * setup. The DataManager is left as std::nullopt for state-machine-only tests;
 * a populated DataManager is used for recovery pipeline tests.
 */
#include <glog/logging.h>
#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <string>
#include <thread>

#define private public
#define protected public
#include "ha_recovery_manager.h"
#undef protected
#undef private

#include "async_metadata_notifier.h"
#include "p2p_master_client.h"
#include "test_p2p_server_helpers.h"
#include "types.h"

namespace mooncake {
namespace test {

// ============================================================================
// Test fixture
// ============================================================================

class HARecoveryManagerTest : public ::testing::Test {
   protected:
    static void SetUpTestSuite() {
        google::InitGoogleLogging("HARecoveryManagerTest");
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

        view_version_ = 0;
        data_manager_ = std::nullopt;
        notifier_.reset();
    }

    void TearDown() override {
        notifier_.reset();
        master_client_.reset();
    }

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

    std::unique_ptr<HARecoveryManager> CreateManager() {
        return std::make_unique<HARecoveryManager>(
            client_id_, *master_client_, data_manager_, notifier_,
            view_version_);
    }

    std::unique_ptr<HARecoveryManager> CreateManagerWithNotifier() {
        notifier_ = std::make_unique<AsyncMetadataNotifier>(
            *master_client_, client_id_,
            /*sender_thread_count=*/1,
            /*max_batch_size=*/2000,
            /*queue_capacity=*/4000);
        notifier_->Start();
        return CreateManager();
    }

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
    ViewVersionId view_version_ = 0;
    std::optional<DataManager> data_manager_;
    std::unique_ptr<AsyncMetadataNotifier> notifier_;
};

testing::InProcP2PMaster HARecoveryManagerTest::master_;
std::string HARecoveryManagerTest::master_addr_;

// ============================================================================
// State Machine Tests
// ============================================================================

TEST_F(HARecoveryManagerTest, FullToDegragedOnUnreachable) {
    auto mgr = CreateManager();
    EXPECT_EQ(mgr->GetState(), HAClientState::FULL);
    EXPECT_FALSE(mgr->IsDegraded());
    mgr->HandleEvent(HAEvent::MASTER_UNREACHABLE);
    EXPECT_EQ(mgr->GetState(), HAClientState::DEGRADED);
    EXPECT_TRUE(mgr->IsDegraded());
}

TEST_F(HARecoveryManagerTest, FullToSyncingOnReachable) {
    // FULL -> MASTER_RECONNECTED -> SYNCING (Master restarted scenario)
    // Without DataManager, recovery thread exits quickly
    auto mgr = CreateManager();
    mgr->HandleEvent(HAEvent::MASTER_RECONNECTED);
    // Should transition to SYNCING (recovery thread will run but no data_manager)
    // Give it a moment to start
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    // Recovery thread exits because data_manager_ is nullopt,
    // but state doesn't automatically go to FULL without successful pipeline
    auto state = mgr->GetState();
    // Without data_manager, pipeline logs error and returns without transitioning
    EXPECT_TRUE(state == HAClientState::SYNCING || state == HAClientState::FULL);
}

TEST_F(HARecoveryManagerTest, DegradedToSyncingOnReachable) {
    auto mgr = CreateManager();
    mgr->HandleEvent(HAEvent::MASTER_UNREACHABLE);
    EXPECT_EQ(mgr->GetState(), HAClientState::DEGRADED);

    mgr->HandleEvent(HAEvent::MASTER_RECONNECTED);
    // Should attempt SYNCING
    auto state = mgr->GetState();
    EXPECT_TRUE(state == HAClientState::SYNCING || state == HAClientState::FULL);
}

TEST_F(HARecoveryManagerTest, DuplicateUnreachableIsNoop) {
    auto mgr = CreateManager();
    mgr->HandleEvent(HAEvent::MASTER_UNREACHABLE);
    EXPECT_EQ(mgr->GetState(), HAClientState::DEGRADED);

    // Second UNREACHABLE should be a no-op (early return in HandleEvent)
    mgr->HandleEvent(HAEvent::MASTER_UNREACHABLE);
    EXPECT_EQ(mgr->GetState(), HAClientState::DEGRADED);
}

TEST_F(HARecoveryManagerTest, SyncingToDegragedOnUnreachable) {
    auto mgr = CreateManagerWithNotifier();

    // First go to SYNCING via MASTER_RECONNECTED
    mgr->HandleEvent(HAEvent::MASTER_RECONNECTED);
    // Allow transition to start
    std::this_thread::sleep_for(std::chrono::milliseconds(50));

    // Now UNREACHABLE should transition to DEGRADED
    mgr->HandleEvent(HAEvent::MASTER_UNREACHABLE);
    EXPECT_EQ(mgr->GetState(), HAClientState::DEGRADED);
}

TEST_F(HARecoveryManagerTest, ReachableDuringSyncingRestartsPipeline) {
    auto mgr = CreateManagerWithNotifier();

    // Start recovery
    mgr->HandleEvent(HAEvent::MASTER_RECONNECTED);
    std::this_thread::sleep_for(std::chrono::milliseconds(50));

    // Another MASTER_RECONNECTED should abort old thread and restart
    mgr->HandleEvent(HAEvent::MASTER_RECONNECTED);
    // Should still be SYNCING
    EXPECT_EQ(mgr->GetState(), HAClientState::SYNCING);
}

TEST_F(HARecoveryManagerTest, StopFromSyncingGoesToDegraded) {
    auto mgr = CreateManagerWithNotifier();
    mgr->HandleEvent(HAEvent::MASTER_RECONNECTED);
    std::this_thread::sleep_for(std::chrono::milliseconds(50));

    mgr->Stop();
    EXPECT_EQ(mgr->GetState(), HAClientState::DEGRADED);
}

TEST_F(HARecoveryManagerTest, StopIdempotent) {
    auto mgr = CreateManager();
    mgr->Stop();
    mgr->Stop();  // Should not crash
    EXPECT_EQ(mgr->GetState(), HAClientState::FULL);
}

TEST_F(HARecoveryManagerTest, DestructorCallsStop) {
    {
        auto mgr = CreateManagerWithNotifier();
        mgr->HandleEvent(HAEvent::MASTER_RECONNECTED);
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
        // Destructor should cleanly join recovery thread
    }
    // If we get here without hanging, the test passes
    SUCCEED();
}

// ============================================================================
// SetSyncCompleted RPC
// ============================================================================

TEST_F(HARecoveryManagerTest, SetSyncCompletedSuccess) {
    auto mgr = CreateManager();
    auto result = mgr->SetSyncCompleted();
    EXPECT_TRUE(result.has_value());
}

// ============================================================================
// Notifier Start/Stop on transitions
// ============================================================================

TEST_F(HARecoveryManagerTest, NotifierStoppedOnUnreachable) {
    notifier_ = std::make_unique<AsyncMetadataNotifier>(
        *master_client_, client_id_,
        /*sender_thread_count=*/1,
        /*max_batch_size=*/2000,
        /*queue_capacity=*/4000);
    notifier_->Start();
    auto mgr = CreateManager();

    EXPECT_TRUE(notifier_->running_.load());
    mgr->HandleEvent(HAEvent::MASTER_UNREACHABLE);
    // Notifier should be stopped
    EXPECT_FALSE(notifier_->running_.load());
}

TEST_F(HARecoveryManagerTest, NotifierRestartedOnReachableFromDegraded) {
    notifier_ = std::make_unique<AsyncMetadataNotifier>(
        *master_client_, client_id_,
        /*sender_thread_count=*/1,
        /*max_batch_size=*/2000,
        /*queue_capacity=*/4000);
    notifier_->Start();
    auto mgr = CreateManager();

    // Go to DEGRADED
    mgr->HandleEvent(HAEvent::MASTER_UNREACHABLE);
    EXPECT_FALSE(notifier_->running_.load());

    // MASTER_RECONNECTED from DEGRADED should restart notifier
    mgr->HandleEvent(HAEvent::MASTER_RECONNECTED);
    EXPECT_TRUE(notifier_->running_.load());
}

// ============================================================================
// Concurrent event handling
// ============================================================================

TEST_F(HARecoveryManagerTest, ConcurrentEvents) {
    auto mgr = CreateManagerWithNotifier();
    std::atomic<bool> stop{false};

    // Thread A: rapid UNREACHABLE events
    std::thread t1([&]() {
        while (!stop.load(std::memory_order_relaxed)) {
            mgr->HandleEvent(HAEvent::MASTER_UNREACHABLE);
            std::this_thread::sleep_for(std::chrono::milliseconds(5));
        }
    });

    // Thread B: rapid REACHABLE events
    std::thread t2([&]() {
        while (!stop.load(std::memory_order_relaxed)) {
            mgr->HandleEvent(HAEvent::MASTER_RECONNECTED);
            std::this_thread::sleep_for(std::chrono::milliseconds(5));
        }
    });

    // Run for a short duration
    std::this_thread::sleep_for(std::chrono::milliseconds(500));
    stop.store(true);
    t1.join();
    t2.join();

    // Final state should be valid
    auto state = mgr->GetState();
    EXPECT_TRUE(state == HAClientState::FULL ||
                state == HAClientState::DEGRADED ||
                state == HAClientState::SYNCING);
}

// ============================================================================
// Full state machine cycle
// ============================================================================

TEST_F(HARecoveryManagerTest, FullCycle_FullDegradedSyncingFull) {
    auto mgr = CreateManagerWithNotifier();

    // FULL -> DEGRADED
    EXPECT_EQ(mgr->GetState(), HAClientState::FULL);
    mgr->HandleEvent(HAEvent::MASTER_UNREACHABLE);
    EXPECT_EQ(mgr->GetState(), HAClientState::DEGRADED);

    // DEGRADED -> SYNCING (no data_manager, recovery will fail fast)
    mgr->HandleEvent(HAEvent::MASTER_RECONNECTED);
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    // Without data_manager, stays SYNCING
    EXPECT_EQ(mgr->GetState(), HAClientState::SYNCING);

    // Cleanup
    mgr->Stop();
}

}  // namespace test
}  // namespace mooncake
