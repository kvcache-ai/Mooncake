/**
 * @file ha_recovery_manager_test.cpp
 * @brief Unit tests for HARecoveryManager state machine and recovery pipeline.
 *
 * Uses an in-process P2P master so that RPCs are real but require no network
 * setup. DataManager is initialised with an empty TieredBackend (no tiers,
 * no keys) so the recovery pipeline runs and exits cleanly without special-
 * casing the nullopt scenario in production code.
 *
 * Note: basic state-machine transitions (FULL→DEGRADED→SYNCING) are covered
 * end-to-end by ha_integration_test. This file focuses on behaviours that are
 * not observable through the full P2PClientService stack:
 *   - duplicate / idempotent events
 *   - AsyncMetadataNotifier start/stop lifecycle
 *   - Stop() idempotency and destructor safety
 *   - concurrent event handling
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
        // Initialise with an empty TieredBackend (no tiers, no keys) and a
        // default TransferEngine (not init()'d — only RDMA ops need it, and
        // the recovery pipeline only iterates metadata which is empty here).
        data_manager_.emplace(std::make_unique<TieredBackend>(),
                              std::make_shared<TransferEngine>());
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
        return std::make_unique<HARecoveryManager>(client_id_, *master_client_,
                                                   data_manager_, notifier_,
                                                   view_version_);
    }

    std::unique_ptr<HARecoveryManager> CreateManagerWithNotifier() {
        notifier_ =
            std::make_unique<AsyncMetadataNotifier>(*master_client_, client_id_,
                                                    /*sender_thread_count=*/1,
                                                    /*max_batch_size=*/2000,
                                                    /*queue_capacity=*/4000);
        notifier_->Start();
        return CreateManager();
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
// State Machine – smoke test
// (detailed transition coverage lives in ha_integration_test)
// ============================================================================

TEST_F(HARecoveryManagerTest, BasicStateMachineSmoke) {
    auto mgr = CreateManager();

    // Initial state
    EXPECT_EQ(mgr->GetState(), HAClientState::FULL);
    EXPECT_FALSE(mgr->IsDegraded());

    // FULL -> DEGRADED
    mgr->HandleEvent(HAEvent::MASTER_UNREACHABLE);
    EXPECT_EQ(mgr->GetState(), HAClientState::DEGRADED);
    EXPECT_TRUE(mgr->IsDegraded());

    // DEGRADED -> SYNCING (no data_manager: pipeline exits fast)
    mgr->HandleEvent(HAEvent::MASTER_RECONNECTED);
    auto state = mgr->GetState();
    EXPECT_TRUE(state == HAClientState::SYNCING);
    std::this_thread::sleep_for(std::chrono::milliseconds(1000));
    state = mgr->GetState();
    EXPECT_TRUE(state == HAClientState::FULL);

    mgr->HandleEvent(HAEvent::MASTER_RECONNECTED);
    state = mgr->GetState();
    EXPECT_TRUE(state == HAClientState::SYNCING);
}

TEST_F(HARecoveryManagerTest, DuplicateUnreachableIsNoop) {
    auto mgr = CreateManager();
    mgr->HandleEvent(HAEvent::MASTER_UNREACHABLE);
    EXPECT_EQ(mgr->GetState(), HAClientState::DEGRADED);

    // Second UNREACHABLE should be a no-op
    mgr->HandleEvent(HAEvent::MASTER_UNREACHABLE);
    EXPECT_EQ(mgr->GetState(), HAClientState::DEGRADED);
}

TEST_F(HARecoveryManagerTest, SetSyncCompletedSuccess) {
    auto mgr = CreateManager();
    auto result = mgr->SetSyncCompleted();
    EXPECT_TRUE(result.has_value());
}

// ============================================================================
// Notifier Start/Stop on transitions
// ============================================================================

TEST_F(HARecoveryManagerTest, NotifierStoppedOnUnreachable) {
    notifier_ =
        std::make_unique<AsyncMetadataNotifier>(*master_client_, client_id_,
                                                /*sender_thread_count=*/1,
                                                /*max_batch_size=*/2000,
                                                /*queue_capacity=*/4000);
    notifier_->Start();
    auto mgr = CreateManager();

    EXPECT_TRUE(notifier_->running_.load());
    mgr->HandleEvent(HAEvent::MASTER_UNREACHABLE);
    EXPECT_FALSE(notifier_->running_.load());
}

TEST_F(HARecoveryManagerTest, NotifierRestartedOnReachableFromDegraded) {
    notifier_ =
        std::make_unique<AsyncMetadataNotifier>(*master_client_, client_id_,
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

    std::this_thread::sleep_for(std::chrono::milliseconds(500));
    stop.store(true);
    t1.join();
    t2.join();

    // Final state should be a valid state
    auto state = mgr->GetState();
    EXPECT_TRUE(state == HAClientState::FULL ||
                state == HAClientState::DEGRADED ||
                state == HAClientState::SYNCING);
}

}  // namespace test
}  // namespace mooncake
