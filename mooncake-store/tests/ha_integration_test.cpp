/**
 * @file ha_integration_test.cpp
 * @brief Integration tests for HA recovery with two P2PClientService instances.
 *
 * Group A: Client-side HA logic (long TTL master, stopped heartbeat,
 *          manual HandleEvent for deterministic state control).
 * Group B: End-to-end failure scenarios (master restart, client disconnect).
 *
 * Two clients (client1_, client2_) connect to the same InProcP2PMaster.
 * Heartbeat threads are stopped after Init(); tests manually control HA
 * state via HandleEvent() and manual heartbeat RPCs.
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
    // Create a P2PClient and connect to the given master address.
    // async_sender_thread_count > 0 enables the async notifier for recovery
    // metadata sync.
    static std::shared_ptr<P2PClientService> CreateP2PClient(
        const std::string& host_name, const std::string& master_addr,
        uint32_t rpc_port = 0, size_t async_sender_thread_count = 1) {
        if (rpc_port == 0) rpc_port = getFreeTcpPort();

        auto config = ClientConfigBuilder::build_p2p_real_client(
            host_name, "P2PHANDSHAKE", "tcp", std::nullopt, master_addr,
            R"({"tiers": [{"type": "DRAM", "capacity": 67108864, "priority": 100}]})",
            /*local_buffer_size=*/0, nullptr, "", rpc_port,
            /*rpc_thread_num=*/2, /*lock_shard_count=*/1024,
            /*route_cache_max_memory_bytes=*/300 * 1024 * 1024,
            /*route_cache_ttl_ms=*/5 * 60 * 1000,
            /*local_transfer_mode=*/"memcpy",
            /*local_memcpy_async_worker_num=*/32,
            /*labels=*/{}, async_sender_thread_count);

        auto client = std::make_shared<P2PClientService>(
            config.local_ip, config.te_port, config.metadata_connstring,
            config.labels);

        auto err = client->Init(config);
        EXPECT_EQ(err, ErrorCode::OK)
            << "Init failed: " << static_cast<int>(err);

        return client;
    }

    // ---- Helpers ----

    static tl::expected<void, ErrorCode> PutData(
        std::shared_ptr<P2PClientService>& client, const std::string& key,
        const std::string& data, const WriteRouteRequestConfig& config = {}) {
        std::vector<Slice> slices;
        slices.emplace_back(Slice{const_cast<char*>(data.data()), data.size()});
        return client->Put(key, slices, config);
    }

    static tl::expected<std::string, ErrorCode> GetData(
        std::shared_ptr<P2PClientService>& client, const std::string& key,
        size_t buf_size) {
        std::vector<char> buf(buf_size, 0);
        auto result = client->Get(key, {(void*)buf.data()}, {buf.size()});
        if (!result.has_value()) {
            return tl::unexpected(result.error());
        }
        size_t actual_size = static_cast<size_t>(result.value());
        return std::string(buf.data(), actual_size);
    }

    // For cross-client reads immediately after a Put on another client, the
    // async BatchSyncReplica notification may not have reached master yet.
    // Retry on OBJECT_NOT_FOUND until master learns about the replica.
    static tl::expected<std::string, ErrorCode> GetDataWithRetry(
        std::shared_ptr<P2PClientService>& client, const std::string& key,
        size_t buf_size,
        std::chrono::milliseconds timeout = std::chrono::milliseconds(1000)) {
        auto deadline = std::chrono::steady_clock::now() + timeout;
        tl::expected<std::string, ErrorCode> result =
            tl::unexpected(ErrorCode::OBJECT_NOT_FOUND);
        do {
            result = GetData(client, key, buf_size);
            if (result.has_value() ||
                result.error() != ErrorCode::OBJECT_NOT_FOUND) {
                return result;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(50));
        } while (std::chrono::steady_clock::now() < deadline);
        return result;
    }

    static void ForceDegraded(std::shared_ptr<P2PClientService>& client) {
        ASSERT_NE(client->ha_manager_, nullptr);
        client->ha_manager_->HandleEvent(HAEvent::MASTER_UNREACHABLE);
        ASSERT_TRUE(client->ha_manager_->IsDegraded())
            << "Expected DEGRADED state after MASTER_UNREACHABLE";
    }

    static void ForceRecover(std::shared_ptr<P2PClientService>& client) {
        ASSERT_NE(client->ha_manager_, nullptr);
        client->ha_manager_->HandleEvent(HAEvent::MASTER_RECONNECTED);

        auto deadline =
            std::chrono::steady_clock::now() + std::chrono::seconds(10);
        while (client->ha_manager_->GetState() != HAClientState::FULL &&
               std::chrono::steady_clock::now() < deadline) {
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }

        // If recovery failed (still SYNCING), try re-registering and retry.
        // This can happen if master restarted and lost client registration.
        if (client->ha_manager_->GetState() != HAClientState::FULL) {
            auto reg = client->RegisterClient();
            if (reg.has_value()) {
                client->ha_manager_->HandleEvent(HAEvent::MASTER_RECONNECTED);
                deadline =
                    std::chrono::steady_clock::now() + std::chrono::seconds(10);
                while (client->ha_manager_->GetState() != HAClientState::FULL &&
                       std::chrono::steady_clock::now() < deadline) {
                    std::this_thread::sleep_for(std::chrono::milliseconds(100));
                }
            }
        }
        ASSERT_EQ(client->ha_manager_->GetState(), HAClientState::FULL)
            << "Client did not recover to FULL within 10s";
    }

    static void SendManualHeartbeat(std::shared_ptr<P2PClientService>& client) {
        HeartbeatRequest req;
        req.client_id = client->GetClientID();
        auto result = client->GetMasterClient().Heartbeat(req);
        ASSERT_TRUE(result.has_value()) << "Manual heartbeat failed";
    }

    // ---- Suite setup / teardown ----

    static void SetUpTestSuite() {
        google::InitGoogleLogging("HAIntegrationTest");
        FLAGS_logtostderr = 1;

        // Start master with long TTL so it won't mark clients as
        // DISCONNECTION when heartbeat is stopped.
        InProcMasterConfigBuilder builder;
        builder.set_client_live_ttl_sec(3600);
        builder.set_client_crashed_ttl_sec(7200);
        auto master_config = builder.build();

        ASSERT_TRUE(master_.Start(master_config))
            << "Failed to start P2P master";
        master_address_ = master_.master_address();
        LOG(INFO) << "P2P master started at " << master_address_;

        client1_ = CreateP2PClient("localhost:18901", master_address_);
        ASSERT_NE(client1_, nullptr);
        client2_ = CreateP2PClient("localhost:18902", master_address_);
        ASSERT_NE(client2_, nullptr);

        // Stop heartbeat threads to prevent race conditions.
        client1_->StopHeartbeat();
        client2_->StopHeartbeat();

        // Brief wait to let any in-flight heartbeat RPC complete.
        std::this_thread::sleep_for(std::chrono::milliseconds(200));
    }

    static void TearDownTestSuite() {
        if (client1_) {
            client1_->Stop();
            client1_->Destroy();
            client1_.reset();
        }
        if (client2_) {
            client2_->Stop();
            client2_->Destroy();
            client2_.reset();
        }
        master_.Stop();
        google::ShutdownGoogleLogging();
    }

    // Per-test setup: ensure both clients are in FULL state.
    void SetUp() override {
        for (auto* client : {&client1_, &client2_}) {
            if ((*client)->ha_manager_ &&
                (*client)->ha_manager_->GetState() != HAClientState::FULL) {
                ForceRecover(*client);
            }
        }
    }

    static InProcP2PMaster master_;
    static std::string master_address_;
    static std::shared_ptr<P2PClientService> client1_;
    static std::shared_ptr<P2PClientService> client2_;
};

InProcP2PMaster HAIntegrationTest::master_;
std::string HAIntegrationTest::master_address_;
std::shared_ptr<P2PClientService> HAIntegrationTest::client1_ = nullptr;
std::shared_ptr<P2PClientService> HAIntegrationTest::client2_ = nullptr;

// ============================================================================
// Group A: Client-side HA logic tests
// (long TTL master, stopped heartbeat, manual HandleEvent)
// ============================================================================

// A1: Baseline — put from client1 routed to client2, then read back.
TEST_F(HAIntegrationTest, RemotePutAndGet) {
    // Put with allow_local=false: master routes to client2
    WriteRouteRequestConfig config;
    config.allow_local = false;

    auto put = PutData(client1_, "a1_client1_remote_baseline", "hello", config);
    ASSERT_TRUE(put.has_value())
        << "Remote put failed: " << static_cast<int>(put.error());

    // client1 Get: local miss → queries master → reads from client2
    auto get = GetData(client1_, "a1_client1_remote_baseline", 5);
    ASSERT_TRUE(get.has_value())
        << "Remote get failed: " << static_cast<int>(get.error());
    EXPECT_EQ(get.value(), "hello");

    // Verify data physically resides on client2
    auto exist = client2_->IsExist("a1_client1_remote_baseline");
    ASSERT_TRUE(exist.has_value());
    EXPECT_TRUE(exist.value());
}

// A2: Degraded mode — local Put/Get/IsExist all work; remote-only keys
//     and nonexistent keys return false for IsExist.
TEST_F(HAIntegrationTest, DegradedModeLocalOps) {
    // Write locally on client1 (default config: prefer_local=true, master
    // now sorts candidates by priority so local segment wins).
    auto put_a = PutData(client1_, "a2_client1_local", "local_data");
    ASSERT_TRUE(put_a.has_value());

    // Write locally on client2 (client1's cache never populated)
    auto put_b = PutData(client2_, "a2_client2_remote_only", "remote_data");
    ASSERT_TRUE(put_b.has_value());

    ForceDegraded(client1_);

    // Degraded Put falls back to PutLocal
    auto put_c = PutData(client1_, "a2_client1_degraded_put", "degraded_data");
    ASSERT_TRUE(put_c.has_value())
        << "Degraded PutLocal failed: " << static_cast<int>(put_c.error());

    // Local Get works for pre-existing and degraded-mode keys
    auto get_a = GetData(client1_, "a2_client1_local", 10);
    ASSERT_TRUE(get_a.has_value());
    EXPECT_EQ(get_a.value(), "local_data");

    auto get_c = GetData(client1_, "a2_client1_degraded_put", 13);
    ASSERT_TRUE(get_c.has_value());
    EXPECT_EQ(get_c.value(), "degraded_data");

    // IsExist: local key → true
    auto exist_local = client1_->IsExist("a2_client1_local");
    ASSERT_TRUE(exist_local.has_value());
    EXPECT_TRUE(exist_local.value());

    // IsExist: key only on client2 → false (local miss, degraded skip)
    auto exist_remote = client1_->IsExist("a2_client2_remote_only");
    ASSERT_TRUE(exist_remote.has_value());
    EXPECT_FALSE(exist_remote.value());

    // IsExist: nonexistent key → false
    auto exist_none = client1_->IsExist("a2_nonexistent_key");
    ASSERT_TRUE(exist_none.has_value());
    EXPECT_FALSE(exist_none.value());

    ForceRecover(client1_);
}

// A3: Degraded mode — remote Get returns OBJECT_NOT_FOUND; Query fails with
//     INACCESSIBLE_MASTER.
TEST_F(HAIntegrationTest, DegradedModeRemoteOpsFail) {
    // Write on client2 — client1's route cache not populated
    auto put = PutData(client2_, "a3_client2_key", "remote_val");
    ASSERT_TRUE(put.has_value());

    // Verify data is accessible before degradation (client1 → master →
    // client2). Use retry: async BatchSyncReplica may not have reached master
    // yet.
    auto get_before = GetDataWithRetry(client1_, "a3_client2_key", 10);
    ASSERT_TRUE(get_before.has_value()) << "Pre-degradation get failed: "
                                        << static_cast<int>(get_before.error());
    EXPECT_EQ(get_before.value(), "remote_val");

    ForceDegraded(client1_);

    // Get: local miss → degraded check → OBJECT_NOT_FOUND (master unreachable)
    std::vector<char> buf(100, 0);
    auto get =
        client1_->Get("a3_client2_key", {(void*)buf.data()}, {buf.size()});
    EXPECT_FALSE(get.has_value());
    EXPECT_EQ(get.error(), ErrorCode::OBJECT_NOT_FOUND);

    // Query: requires master → fails with INACCESSIBLE_MASTER
    auto query = client1_->Query("a3_client1_query_key");
    EXPECT_FALSE(query.has_value());
    EXPECT_EQ(query.error(), ErrorCode::INACCESSIBLE_MASTER);

    ForceRecover(client1_);
}

// A4: Recovery — after degraded, remote data on client2 becomes accessible.
TEST_F(HAIntegrationTest, RecoverFromDegradedRemoteGet) {
    auto put = PutData(client2_, "a4_client2_key", "recoverable");
    ASSERT_TRUE(put.has_value());

    // Verify data is accessible before degradation (client1 → master →
    // client2). Use retry: async BatchSyncReplica may not have reached master
    // yet.
    auto get_before = GetDataWithRetry(client1_, "a4_client2_key", 11);
    ASSERT_TRUE(get_before.has_value()) << "Pre-degradation get failed: "
                                        << static_cast<int>(get_before.error());
    EXPECT_EQ(get_before.value(), "recoverable");

    ForceDegraded(client1_);

    // Verify Get fails during degradation (local miss → OBJECT_NOT_FOUND)
    std::vector<char> buf(11, 0);
    auto get_fail =
        client1_->Get("a4_client2_key", {(void*)buf.data()}, {buf.size()});
    EXPECT_FALSE(get_fail.has_value());
    EXPECT_EQ(get_fail.error(), ErrorCode::OBJECT_NOT_FOUND);

    ForceRecover(client1_);

    // After recovery, Get succeeds via master routing to client2
    auto get_ok = GetData(client1_, "a4_client2_key", 11);
    ASSERT_TRUE(get_ok.has_value())
        << "Post-recovery get failed: " << static_cast<int>(get_ok.error());
    EXPECT_EQ(get_ok.value(), "recoverable");
}

// A5: Degraded writes are synced to master after recovery,
//     allowing the other client to read them.
TEST_F(HAIntegrationTest, RecoverDegradedWritesSyncToMaster) {
    ForceDegraded(client1_);

    // Write during degraded mode → PutLocal with metadata skip.
    // The add_replica_callback detects degraded mode and skips notifier
    // enqueue, so the local write succeeds without master involvement.
    auto put = PutData(client1_, "a5_client1_degraded_write", "synced_data");
    ASSERT_TRUE(put.has_value())
        << "Degraded PutLocal failed: " << static_cast<int>(put.error());

    // Recovery: notifier syncs all local metadata to master
    ForceRecover(client1_);

    // client2 reads data written by client1 during degradation.
    // master now has the route (synced by recovery pipeline).
    auto get = GetData(client2_, "a5_client1_degraded_write", 11);
    ASSERT_TRUE(get.has_value())
        << "Cross-client get after recovery sync failed: "
        << static_cast<int>(get.error());
    EXPECT_EQ(get.value(), "synced_data");
}

// A6: Eviction (Delete) during degraded mode succeeds locally without
//     failing on master notification.
TEST_F(HAIntegrationTest, DegradedModeEvictionSkipsMasterSync) {
    // Write a key locally on client1 in FULL mode
    auto put = PutData(client1_, "a6_client1_evict_key", "evict_data");
    ASSERT_TRUE(put.has_value());

    // Verify key exists before degradation
    auto exist_before = client1_->IsExist("a6_client1_evict_key");
    ASSERT_TRUE(exist_before.has_value());
    EXPECT_TRUE(exist_before.value());

    ForceDegraded(client1_);

    // Simulate eviction: delete the key via data_manager while degraded.
    // Without the degraded skip fix in remove_replica_callback, this would
    // fail with ASYNC_ENQUEUE_FAILED because the notifier is stopped.
    auto del = client1_->data_manager_->Delete("a6_client1_evict_key");
    ASSERT_TRUE(del.has_value())
        << "Degraded Delete failed: " << static_cast<int>(del.error());

    // Verify key is gone locally
    auto exist_after = client1_->IsExist("a6_client1_evict_key");
    ASSERT_TRUE(exist_after.has_value());
    EXPECT_FALSE(exist_after.value());

    // Recover and verify master-side consistency:
    // After recovery, master should NOT have a stale route for the deleted key.
    ForceRecover(client1_);

    // client2 tries to read the deleted key — should fail (key doesn't exist)
    std::vector<char> buf(100, 0);
    auto get = client2_->Get("a6_client1_evict_key", {(void*)buf.data()},
                             {buf.size()});
    EXPECT_FALSE(get.has_value());
}

// A7: Rapid degraded/recovery cycles are consistent.
TEST_F(HAIntegrationTest, RapidDegradedRecoveryCycle) {
    for (int i = 0; i < 5; ++i) {
        client1_->ha_manager_->HandleEvent(HAEvent::MASTER_UNREACHABLE);
        EXPECT_TRUE(client1_->ha_manager_->IsDegraded());

        client1_->ha_manager_->HandleEvent(HAEvent::MASTER_RECONNECTED);
        auto state = client1_->ha_manager_->GetState();
        EXPECT_TRUE(state == HAClientState::SYNCING ||
                    state == HAClientState::FULL);

        // Wait for recovery pipeline to complete
        auto deadline =
            std::chrono::steady_clock::now() + std::chrono::seconds(10);
        while (client1_->ha_manager_->GetState() != HAClientState::FULL &&
               std::chrono::steady_clock::now() < deadline) {
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }
    }
    EXPECT_EQ(client1_->ha_manager_->GetState(), HAClientState::FULL);
}

// A7: Master-side state verification.
TEST_F(HAIntegrationTest, MasterSideState) {
    // SetSyncCompleted RPC round-trip
    ASSERT_NE(client1_->ha_manager_, nullptr);
    auto result = client1_->ha_manager_->SetSyncCompleted();
    EXPECT_TRUE(result.has_value());

    // Both clients should have is_syncing=false on master side
    auto& svc = master_.GetWrapped().GetMasterService();
    for (auto* client : {&client1_, &client2_}) {
        auto client_meta =
            svc.GetClientManager().GetClient((*client)->GetClientID());
        ASSERT_NE(client_meta, nullptr);

        auto p2p_meta = std::dynamic_pointer_cast<P2PClientMeta>(client_meta);
        ASSERT_NE(p2p_meta, nullptr);
        EXPECT_FALSE(p2p_meta->IsSyncing());
    }
}

// ============================================================================
// Group B: End-to-end failure scenario tests
// ============================================================================

// B1: Master crashes and restarts — both clients recover.
TEST_F(HAIntegrationTest, MasterRestartRecovery) {
    // Write data locally on client1 before master crash
    auto put = PutData(client1_, "b1_client1_before_crash", "persistent");
    ASSERT_TRUE(put.has_value());

    int port = master_.rpc_port();

    // Stop master (simulates crash)
    master_.Stop();

    // Both clients detect master down → DEGRADED
    ForceDegraded(client1_);
    ForceDegraded(client2_);

    // Restart master on the same port
    InProcMasterConfigBuilder builder;
    builder.set_rpc_port(port);
    builder.set_client_live_ttl_sec(3600);
    builder.set_client_crashed_ttl_sec(7200);
    ASSERT_TRUE(master_.Start(builder.build())) << "Failed to restart master";

    // Reconnect RPC clients to the restarted master (old connections are
    // stale). Retry with backoff since the restarted RPC server may not
    // be fully accepting connections immediately.
    // Each client has its own connection pool, so both need to clear stale
    // connections independently.
    for (int attempt = 0; attempt < 10; ++attempt) {
        std::this_thread::sleep_for(std::chrono::milliseconds(200));
        auto err = client1_->GetMasterClient().Connect(master_address_);
        if (err == ErrorCode::OK) break;
        if (attempt == 9) FAIL() << "Reconnect client1 failed after retries";
    }
    for (int attempt = 0; attempt < 10; ++attempt) {
        std::this_thread::sleep_for(std::chrono::milliseconds(200));
        auto err = client2_->GetMasterClient().Connect(master_address_);
        if (err == ErrorCode::OK) break;
        if (attempt == 9) FAIL() << "Reconnect client2 failed after retries";
    }

    // Re-register clients with the restarted master.
    auto reg1 = client1_->RegisterClient();
    ASSERT_TRUE(reg1.has_value())
        << "Re-register client1 failed: " << static_cast<int>(reg1.error());
    auto reg2 = client2_->RegisterClient();
    ASSERT_TRUE(reg2.has_value())
        << "Re-register client2 failed: " << static_cast<int>(reg2.error());

    // Recovery: DEGRADED → SYNCING → FULL
    ForceRecover(client1_);
    ForceRecover(client2_);

    // After recovery, verify the system is functional:
    // new Put + Get should work through the restarted master.
    auto put2 = PutData(client1_, "b1_client1_after_restart", "works");
    ASSERT_TRUE(put2.has_value())
        << "Post-restart put failed: " << static_cast<int>(put2.error());
    auto get = GetData(client1_, "b1_client1_after_restart", 5);
    ASSERT_TRUE(get.has_value())
        << "Post-restart get failed: " << static_cast<int>(get.error());
    EXPECT_EQ(get.value(), "works");
}

// B2: Single client disconnects (network failure), master marks it
//     DISCONNECTION, then client recovers via heartbeat.
//     Uses independent short-TTL master to avoid affecting other tests.
TEST_F(HAIntegrationTest, ClientDisconnectAndRecover) {
    // Start an independent master with short TTL
    InProcP2PMaster short_ttl_master;
    InProcMasterConfigBuilder builder;
    builder.set_client_live_ttl_sec(2);
    builder.set_client_crashed_ttl_sec(20);
    ASSERT_TRUE(short_ttl_master.Start(builder.build()));

    std::string short_master_addr = short_ttl_master.master_address();

    // Create two temporary clients on the short-TTL master
    auto tmp1 = CreateP2PClient("localhost:19001", short_master_addr);
    ASSERT_NE(tmp1, nullptr);
    auto tmp2 = CreateP2PClient("localhost:19002", short_master_addr);
    ASSERT_NE(tmp2, nullptr);

    // Verify both are HEALTH initially
    {
        QueryClientStatusRequest req;
        req.client_id = tmp1->GetClientID();
        auto res =
            short_ttl_master.GetWrapped().GetMasterService().QueryClientStatus(
                req);
        ASSERT_TRUE(res.has_value());
        ASSERT_EQ(res.value().status, ClientStatus::HEALTH);
    }

    // Simulate client1 network failure: stop its heartbeat
    tmp1->StopHeartbeat();

    // Wait for master to mark tmp1 as DISCONNECTION (TTL=2s)
    std::this_thread::sleep_for(std::chrono::seconds(3));

    // Verify master side: tmp1 is DISCONNECTION
    {
        QueryClientStatusRequest req;
        req.client_id = tmp1->GetClientID();
        auto res =
            short_ttl_master.GetWrapped().GetMasterService().QueryClientStatus(
                req);
        ASSERT_TRUE(res.has_value());
        EXPECT_EQ(res.value().status, ClientStatus::DISCONNECTION)
            << "Master should have marked disconnected client";
    }

    // Verify tmp2 is still HEALTH
    {
        QueryClientStatusRequest req;
        req.client_id = tmp2->GetClientID();
        auto res =
            short_ttl_master.GetWrapped().GetMasterService().QueryClientStatus(
                req);
        ASSERT_TRUE(res.has_value());
        EXPECT_EQ(res.value().status, ClientStatus::HEALTH);
    }

    // Recover: manually send heartbeat from tmp1
    {
        HeartbeatRequest req;
        req.client_id = tmp1->GetClientID();
        auto hb_res = tmp1->GetMasterClient().Heartbeat(req);
        ASSERT_TRUE(hb_res.has_value()) << "Recovery heartbeat failed";
        EXPECT_EQ(hb_res.value().status, ClientStatus::HEALTH)
            << "Client should recover to HEALTH after heartbeat";
    }

    // Verify master side: tmp1 is HEALTH again
    {
        QueryClientStatusRequest req;
        req.client_id = tmp1->GetClientID();
        auto res =
            short_ttl_master.GetWrapped().GetMasterService().QueryClientStatus(
                req);
        ASSERT_TRUE(res.has_value());
        EXPECT_EQ(res.value().status, ClientStatus::HEALTH)
            << "Client should be HEALTH after recovery heartbeat";
    }

    // Cleanup
    tmp1->Stop();
    tmp1->Destroy();
    tmp2->Stop();
    tmp2->Destroy();
    short_ttl_master.Stop();
}

}  // namespace testing
}  // namespace mooncake
