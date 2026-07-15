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
#include <json/json.h>

#include <atomic>
#include <chrono>
#include <memory>
#include <optional>
#include <string>
#include <thread>
#include <vector>

#define private public
#define protected public
#include "ha_recovery_manager.h"
#undef protected
#undef private

#include "async_metadata_notifier.h"
#include "p2p_master_client.h"
#include "tiered_cache/tiered_backend.h"
#include "test_p2p_server_helpers.h"
#include "types.h"
#include "utils/common.h"

namespace mooncake {
namespace test {

static bool ParseJsonString(const std::string& json_str, Json::Value& value) {
    Json::CharReaderBuilder builder;
    std::unique_ptr<Json::CharReader> reader(builder.newCharReader());
    std::string errs;
    return reader->parse(json_str.data(), json_str.data() + json_str.size(),
                         &value, &errs);
}

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

    std::optional<UUID> InitLocalDataManagerAndPut(const std::string& key,
                                                   const std::string& value) {
        std::string json_config_str = R"({
            "tiers": [
                {
                    "type": "DRAM",
                    "capacity": 67108864,
                    "priority": 10,
                    "tags": ["fast", "local"],
                    "allocator_type": "OFFSET"
                }
            ]
        })";
        Json::Value config;
        if (!ParseJsonString(json_config_str, config)) {
            ADD_FAILURE() << "Failed to parse test tier config";
            return std::nullopt;
        }

        auto tiered_backend = std::make_unique<TieredBackend>();
        auto init_result = InitTieredBackendForTest(*tiered_backend, config);
        if (!init_result.has_value()) {
            ADD_FAILURE() << "InitTieredBackendForTest failed: "
                          << init_result.error();
            return std::nullopt;
        }

        auto transfer_engine = std::make_shared<TransferEngine>(false);
        LocalTransferConfig transfer_config;
        transfer_config.mode = LocalTransferMode::MEMCPY;

        data_manager_.reset();
        data_manager_.emplace(std::move(tiered_backend), transfer_engine,
                              /*metadata_shard_count=*/1024, transfer_config);

        std::vector<char> buffer(value.begin(), value.end());
        std::vector<Slice> slices = {
            Slice{buffer.data(), static_cast<uint64_t>(buffer.size())}};
        auto put_result = data_manager_->Put(key, slices);
        if (!put_result.has_value()) {
            ADD_FAILURE() << "DataManager Put failed: " << put_result.error();
            return std::nullopt;
        }
        auto wait_result = put_result.value()->Wait();
        if (!wait_result.has_value()) {
            ADD_FAILURE() << "DataManager Put wait failed: "
                          << wait_result.error();
            return std::nullopt;
        }

        auto tier_ids = data_manager_->GetReplicaTierIds(key);
        if (tier_ids.size() != 1) {
            ADD_FAILURE() << "Expected 1 local replica tier, got "
                          << tier_ids.size();
            return std::nullopt;
        }
        return tier_ids[0];
    }

    void MountLocalTierOnMaster(const UUID& tier_id) {
        Segment local_segment = MakeSegment();
        local_segment.id = tier_id;
        local_segment.name = "local_recovery_tier";
        auto& svc = master_.GetWrapped().GetMasterService();
        auto result = svc.MountSegment(local_segment, client_id_);
        ASSERT_TRUE(result.has_value())
            << "MountSegment failed: " << result.error();
    }

    void WaitUntilFull(HARecoveryManager& mgr) {
        for (int i = 0; i < 100; ++i) {
            if (mgr.GetState() == HAClientState::FULL) {
                return;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(50));
        }
        FAIL() << "HARecoveryManager did not reach FULL state";
    }

    static testing::InProcP2PMaster master_;
    static std::string master_addr_;

    UUID client_id_{};
    Segment segment_;
    std::unique_ptr<P2PMasterClient> master_client_;
    std::atomic<ViewVersionId> view_version_{0};
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
    mgr->SetReadyForRecovery();

    // Initial state
    EXPECT_EQ(mgr->GetState(), HAClientState::FULL);
    EXPECT_FALSE(mgr->IsDegraded());

    // FULL -> DEGRADED
    mgr->HandleEvent(HAEvent::MASTER_UNREACHABLE);
    EXPECT_EQ(mgr->GetState(), HAClientState::DEGRADED);
    EXPECT_TRUE(mgr->IsDegraded());

    // DEGRADED -> SYNCING -> FULL (no data_manager: pipeline completes
    // instantly, so SYNCING may be unobservable by the time we check).
    mgr->HandleEvent(HAEvent::MASTER_RECONNECTED);
    auto state = mgr->GetState();
    EXPECT_TRUE(state == HAClientState::SYNCING || state == HAClientState::FULL)
        << "Expected SYNCING or FULL, got " << static_cast<int>(state);

    // Wait for recovery to settle at FULL.
    std::this_thread::sleep_for(std::chrono::milliseconds(1000));
    EXPECT_EQ(mgr->GetState(), HAClientState::FULL);

    // FULL + MASTER_RECONNECTED -> SYNCING -> FULL again (same race).
    mgr->HandleEvent(HAEvent::MASTER_RECONNECTED);
    state = mgr->GetState();
    EXPECT_TRUE(state == HAClientState::SYNCING || state == HAClientState::FULL)
        << "Expected SYNCING or FULL, got " << static_cast<int>(state);
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
    mgr->SetReadyForRecovery();

    // Go to DEGRADED
    mgr->HandleEvent(HAEvent::MASTER_UNREACHABLE);
    EXPECT_FALSE(notifier_->running_.load());

    // MASTER_RECONNECTED from DEGRADED should restart notifier
    mgr->HandleEvent(HAEvent::MASTER_RECONNECTED);
    EXPECT_TRUE(notifier_->running_.load());
}

// ============================================================================
// LOCAL_ONLY (proactive unregister) state
// ============================================================================

TEST_F(HARecoveryManagerTest, EnterLocalOnlyForcesStateAndStopsNotifier) {
    notifier_ =
        std::make_unique<AsyncMetadataNotifier>(*master_client_, client_id_,
                                                /*sender_thread_count=*/1,
                                                /*max_batch_size=*/2000,
                                                /*queue_capacity=*/4000);
    notifier_->Start();
    auto mgr = CreateManager();
    mgr->SetReadyForRecovery();

    EXPECT_EQ(mgr->GetState(), HAClientState::FULL);
    EXPECT_FALSE(mgr->IsLocalService());
    EXPECT_TRUE(notifier_->running_.load());

    mgr->EnterLocalOnly();

    EXPECT_EQ(mgr->GetState(), HAClientState::LOCAL_ONLY);
    EXPECT_TRUE(mgr->IsLocalService());
    // LOCAL_ONLY is distinct from DEGRADED.
    EXPECT_FALSE(mgr->IsDegraded());
    // Notifier stopped: no more metadata pushed to master.
    EXPECT_FALSE(notifier_->running_.load());
}

TEST_F(HARecoveryManagerTest, IsLocalServiceTrueForDegradedAndLocalOnly) {
    auto mgr = CreateManager();
    EXPECT_FALSE(mgr->IsLocalService());  // FULL

    mgr->HandleEvent(HAEvent::MASTER_UNREACHABLE);
    EXPECT_TRUE(mgr->IsLocalService());  // DEGRADED

    mgr->EnterLocalOnly();
    EXPECT_TRUE(mgr->IsLocalService());  // LOCAL_ONLY
}

TEST_F(HARecoveryManagerTest,
       ReconnectFromLocalOnlyRestartsNotifierAndRecovers) {
    notifier_ =
        std::make_unique<AsyncMetadataNotifier>(*master_client_, client_id_,
                                                /*sender_thread_count=*/1,
                                                /*max_batch_size=*/2000,
                                                /*queue_capacity=*/4000);
    notifier_->Start();
    auto mgr = CreateManager();
    mgr->SetReadyForRecovery();

    mgr->EnterLocalOnly();
    EXPECT_EQ(mgr->GetState(), HAClientState::LOCAL_ONLY);
    EXPECT_FALSE(notifier_->running_.load());

    // Re-registration path: MASTER_RECONNECTED from LOCAL_ONLY restarts the
    // notifier and drives SYNCING -> FULL (empty data_manager: completes fast).
    mgr->HandleEvent(HAEvent::MASTER_RECONNECTED);
    EXPECT_TRUE(notifier_->running_.load());
    std::this_thread::sleep_for(std::chrono::milliseconds(1000));
    EXPECT_EQ(mgr->GetState(), HAClientState::FULL);
}

TEST_F(HARecoveryManagerTest, ReconnectResyncsLocalReplicaToP2PMaster) {
    const std::string key = "recovery-key-" + std::to_string(client_id_.first) +
                            "-" + std::to_string(client_id_.second);
    const std::string value = "recovery-value";
    auto tier_id = InitLocalDataManagerAndPut(key, value);
    ASSERT_TRUE(tier_id.has_value());
    MountLocalTierOnMaster(tier_id.value());

    auto mgr = CreateManagerWithNotifier();
    mgr->SetReadyForRecovery();

    mgr->HandleEvent(HAEvent::MASTER_UNREACHABLE);
    EXPECT_EQ(mgr->GetState(), HAClientState::DEGRADED);
    EXPECT_FALSE(notifier_->running_.load());

    mgr->HandleEvent(HAEvent::MASTER_RECONNECTED);
    EXPECT_TRUE(notifier_->running_.load());
    WaitUntilFull(*mgr);

    auto& svc = master_.GetWrapped().GetMasterService();
    auto result = svc.GetReplicaList(key);
    ASSERT_TRUE(result.has_value())
        << "GetReplicaList failed: " << result.error();
    ASSERT_EQ(result.value().replicas.size(), 1);
    const auto& replica = result.value().replicas[0];
    ASSERT_TRUE(replica.is_p2p_proxy_replica());
    const auto& desc = replica.get_p2p_proxy_descriptor();
    EXPECT_EQ(desc.client_id, client_id_);
    EXPECT_EQ(desc.segment_id, tier_id.value());
}

// ============================================================================
// Concurrent event handling
// ============================================================================

TEST_F(HARecoveryManagerTest, ConcurrentEvents) {
    auto mgr = CreateManagerWithNotifier();
    mgr->SetReadyForRecovery();
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
