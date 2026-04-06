/**
 * @file ha_integration_test.cpp
 * @brief Integration tests for HA recovery: master restart, degraded mode,
 *        and full recovery pipeline through P2PClientService.
 *
 * Launches an in-process P2P master and one P2PClientService, exercises
 * degraded mode behavior and master-restart recovery flow.
 */
#include <glog/logging.h>
#include <gtest/gtest.h>

#include <chrono>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#define private public
#define protected public
#include "p2p_client_service.h"
#include "p2p_master_service.h"
#include "master_service.h"
#undef protected
#undef private

#include "p2p_client_meta.h"
#include "test_p2p_server_helpers.h"
#include "types.h"

namespace mooncake {
namespace testing {

// ============================================================================
// Test fixture
// ============================================================================

class HAIntegrationTest : public ::testing::Test {
   protected:
    static std::shared_ptr<P2PClientService> CreateP2PClient(
        const std::string& host_name, uint32_t rpc_port = 0) {
        if (rpc_port == 0) rpc_port = getFreeTcpPort();

        auto config = ClientConfigBuilder::build_p2p_real_client(
            host_name, "P2PHANDSHAKE", "tcp", std::nullopt, master_address_,
            R"({"tiers": [{"type": "DRAM", "capacity": 67108864, "priority": 100}]})",
            /*local_buffer_size=*/0, nullptr, "", rpc_port);

        auto client = std::make_shared<P2PClientService>(
            config.local_ip, config.te_port, config.metadata_connstring,
            config.labels);

        auto err = client->Init(config);
        EXPECT_EQ(err, ErrorCode::OK)
            << "Init failed: " << static_cast<int>(err);

        return client;
    }

    static void SetUpTestSuite() {
        google::InitGoogleLogging("HAIntegrationTest");
        FLAGS_logtostderr = 1;

        ASSERT_TRUE(master_.Start()) << "Failed to start P2P master";
        master_address_ = master_.master_address();
        LOG(INFO) << "P2P master started at " << master_address_;

        client_ = CreateP2PClient("localhost:18901");
        ASSERT_NE(client_, nullptr);

        // Wait for heartbeat to stabilize
        std::this_thread::sleep_for(std::chrono::milliseconds(500));
    }

    static void TearDownTestSuite() {
        if (client_) {
            client_->Stop();
            client_->Destroy();
            client_.reset();
        }
        master_.Stop();
        google::ShutdownGoogleLogging();
    }

    static InProcP2PMaster master_;
    static std::string master_address_;
    static std::shared_ptr<P2PClientService> client_;
};

InProcP2PMaster HAIntegrationTest::master_;
std::string HAIntegrationTest::master_address_;
std::shared_ptr<P2PClientService> HAIntegrationTest::client_ = nullptr;

// ============================================================================
// Baseline: normal operations work before any HA events
// ============================================================================

TEST_F(HAIntegrationTest, BaselinePutAndGet) {
    const std::string key = "ha_baseline";
    const std::string data = "baseline_data";

    std::vector<Slice> slices;
    slices.emplace_back(Slice{const_cast<char*>(data.data()), data.size()});
    auto put = client_->Put(key, slices, WriteRouteRequestConfig{});
    ASSERT_TRUE(put.has_value())
        << "Put failed: " << static_cast<int>(put.error());

    std::vector<char> buf(data.size(), 0);
    auto get = client_->Get(key, {(void*)buf.data()}, {buf.size()});
    ASSERT_TRUE(get.has_value())
        << "Get failed: " << static_cast<int>(get.error());
    EXPECT_EQ(std::string(buf.data(), buf.size()), data);
}

// ============================================================================
// HARecoveryManager state via P2PClientService
// ============================================================================

TEST_F(HAIntegrationTest, InitialStateIsFull) {
    ASSERT_NE(client_->ha_manager_, nullptr);
    EXPECT_EQ(client_->ha_manager_->GetState(), HAClientState::FULL);
}

// ============================================================================
// Degraded mode: MASTER_UNREACHABLE -> local-only operations
// ============================================================================

TEST_F(HAIntegrationTest, DegradedModePutLocal) {
    // Put some data first in FULL mode
    const std::string key = "ha_degraded_put";
    const std::string data = "degraded_data";

    std::vector<Slice> slices;
    slices.emplace_back(Slice{const_cast<char*>(data.data()), data.size()});
    auto put = client_->Put(key, slices, WriteRouteRequestConfig{});
    ASSERT_TRUE(put.has_value());

    // Simulate MASTER_UNREACHABLE
    client_->ha_manager_->HandleEvent(HAEvent::MASTER_UNREACHABLE);
    EXPECT_TRUE(client_->ha_manager_->IsDegraded());

    // PutViaRoute should fall back to PutLocal in degraded mode
    const std::string key2 = "ha_degraded_put2";
    const std::string data2 = "degraded_data2";
    std::vector<Slice> slices2;
    slices2.emplace_back(Slice{const_cast<char*>(data2.data()), data2.size()});
    auto put2 = client_->Put(key2, slices2, WriteRouteRequestConfig{});
    ASSERT_TRUE(put2.has_value())
        << "Degraded PutLocal failed: " << static_cast<int>(put2.error());

    // Local Get should still work for locally stored data
    std::vector<char> buf(data2.size(), 0);
    auto get = client_->Get(key2, {(void*)buf.data()}, {buf.size()});
    ASSERT_TRUE(get.has_value())
        << "Degraded local Get failed: " << static_cast<int>(get.error());
    EXPECT_EQ(std::string(buf.data(), buf.size()), data2);

    // Restore state for subsequent tests
    client_->ha_manager_->HandleEvent(HAEvent::MASTER_REACHABLE);
    std::this_thread::sleep_for(std::chrono::milliseconds(500));
}

TEST_F(HAIntegrationTest, DegradedModeIsExistLocalOnly) {
    // Put a key in FULL mode
    const std::string key = "ha_degraded_exist";
    const std::string data = "exist_data";
    std::vector<Slice> slices;
    slices.emplace_back(Slice{const_cast<char*>(data.data()), data.size()});
    auto put = client_->Put(key, slices, WriteRouteRequestConfig{});
    ASSERT_TRUE(put.has_value());

    // Go degraded
    client_->ha_manager_->HandleEvent(HAEvent::MASTER_UNREACHABLE);
    EXPECT_TRUE(client_->ha_manager_->IsDegraded());

    // IsExist for locally stored key should still work (local hit)
    auto exist = client_->IsExist(key);
    ASSERT_TRUE(exist.has_value());
    EXPECT_TRUE(exist.value());

    // IsExist for unknown key should return false (no Master fallback)
    auto not_exist = client_->IsExist("ha_nonexistent_key_xyz");
    ASSERT_TRUE(not_exist.has_value());
    EXPECT_FALSE(not_exist.value());

    // Restore
    client_->ha_manager_->HandleEvent(HAEvent::MASTER_REACHABLE);
    std::this_thread::sleep_for(std::chrono::milliseconds(500));
}

TEST_F(HAIntegrationTest, DegradedModeQueryFails) {
    client_->ha_manager_->HandleEvent(HAEvent::MASTER_UNREACHABLE);
    EXPECT_TRUE(client_->ha_manager_->IsDegraded());

    // Query requires Master — should fail with INACCESSIBLE_MASTER
    auto query = client_->Query("any_key");
    EXPECT_FALSE(query.has_value());
    EXPECT_EQ(query.error(), ErrorCode::INACCESSIBLE_MASTER);

    // Restore
    client_->ha_manager_->HandleEvent(HAEvent::MASTER_REACHABLE);
    std::this_thread::sleep_for(std::chrono::milliseconds(500));
}

TEST_F(HAIntegrationTest, DegradedModeGetRemoteKeyFails) {
    client_->ha_manager_->HandleEvent(HAEvent::MASTER_UNREACHABLE);
    EXPECT_TRUE(client_->ha_manager_->IsDegraded());

    // Get for a key that doesn't exist locally should fail
    std::vector<char> buf(100, 0);
    auto get = client_->Get("ha_remote_only_key", {(void*)buf.data()},
                            {buf.size()});
    EXPECT_FALSE(get.has_value());
    EXPECT_EQ(get.error(), ErrorCode::INACCESSIBLE_MASTER);

    // Restore
    client_->ha_manager_->HandleEvent(HAEvent::MASTER_REACHABLE);
    std::this_thread::sleep_for(std::chrono::milliseconds(500));
}

// ============================================================================
// Recovery: DEGRADED -> MASTER_REACHABLE -> SYNCING -> FULL
// ============================================================================

TEST_F(HAIntegrationTest, RecoverFromDegraded) {
    // Write some data first
    const std::string key = "ha_recovery_key";
    const std::string data = "recovery_data";
    std::vector<Slice> slices;
    slices.emplace_back(Slice{const_cast<char*>(data.data()), data.size()});
    auto put = client_->Put(key, slices, WriteRouteRequestConfig{});
    ASSERT_TRUE(put.has_value());

    // Go degraded
    client_->ha_manager_->HandleEvent(HAEvent::MASTER_UNREACHABLE);
    EXPECT_EQ(client_->ha_manager_->GetState(), HAClientState::DEGRADED);

    // Come back
    client_->ha_manager_->HandleEvent(HAEvent::MASTER_REACHABLE);

    // Wait for recovery pipeline to complete
    auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
    while (client_->ha_manager_->GetState() != HAClientState::FULL &&
           std::chrono::steady_clock::now() < deadline) {
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
    EXPECT_EQ(client_->ha_manager_->GetState(), HAClientState::FULL);

    // After recovery, normal operations should work
    auto exist = client_->IsExist(key);
    ASSERT_TRUE(exist.has_value());
    EXPECT_TRUE(exist.value());
}

// ============================================================================
// State transitions are consistent under rapid events
// ============================================================================

TEST_F(HAIntegrationTest, RapidDegradedRecoveredCycle) {
    for (int i = 0; i < 5; ++i) {
        client_->ha_manager_->HandleEvent(HAEvent::MASTER_UNREACHABLE);
        EXPECT_TRUE(client_->ha_manager_->IsDegraded());

        client_->ha_manager_->HandleEvent(HAEvent::MASTER_REACHABLE);
        // Should be SYNCING or FULL
        auto state = client_->ha_manager_->GetState();
        EXPECT_TRUE(state == HAClientState::SYNCING ||
                    state == HAClientState::FULL);

        // Wait briefly for recovery
        std::this_thread::sleep_for(std::chrono::milliseconds(200));
    }

    // Final recovery wait
    auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
    while (client_->ha_manager_->GetState() != HAClientState::FULL &&
           std::chrono::steady_clock::now() < deadline) {
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
    EXPECT_EQ(client_->ha_manager_->GetState(), HAClientState::FULL);
}

// ============================================================================
// SetSyncCompleted via RPC round-trip
// ============================================================================

TEST_F(HAIntegrationTest, SetSyncCompletedRPC) {
    ASSERT_NE(client_->ha_manager_, nullptr);
    auto result = client_->ha_manager_->SetSyncCompleted();
    EXPECT_TRUE(result.has_value());
}

// ============================================================================
// OnClientRegistered sets is_syncing on Master side
// ============================================================================

TEST_F(HAIntegrationTest, ClientRegistrationSetsSyncing) {
    auto& svc = master_.GetWrapped().GetMasterService();
    auto client_meta = svc.GetClientManager().GetClient(client_->GetClientID());
    ASSERT_NE(client_meta, nullptr);

    auto p2p_meta = std::dynamic_pointer_cast<P2PClientMeta>(client_meta);
    ASSERT_NE(p2p_meta, nullptr);
    // After Init() calls SetSyncCompleted(), is_syncing should be false
    EXPECT_FALSE(p2p_meta->IsSyncing());
}

}  // namespace testing
}  // namespace mooncake
