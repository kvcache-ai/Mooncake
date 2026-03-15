#include <gtest/gtest.h>
#include <glog/logging.h>
#include <atomic>
#include <chrono>
#include <thread>
#include <vector>
#define private public
#define protected public
#include "centralized_client_manager.h"
#undef private
#undef protected

namespace mooncake {

class CentralizedClientManagerTest : public ::testing::Test {
   protected:
    void SetUp() override {
        google::InitGoogleLogging("CentralizedClientManagerTest");
        FLAGS_logtostderr = 1;
    }

    void TearDown() override { google::ShutdownGoogleLogging(); }

    std::unique_ptr<CentralizedClientManager> CreateManager(
        int64_t disconnect_sec = 2, int64_t crash_sec = 5,
        BufferAllocatorType alloc_type = BufferAllocatorType::OFFSET,
        ViewVersionId view_version = 1) {
        return std::make_unique<CentralizedClientManager>(
            disconnect_sec, crash_sec, alloc_type, view_version);
    }

    RegisterClientRequest MakeCentralizedRegisterRequest(
        UUID client_id = {100, 200}, std::vector<Segment> segments = {}) {
        RegisterClientRequest req;
        req.client_id = client_id;
        req.deployment_mode = DeploymentMode::CENTRALIZATION;
        req.segments = std::move(segments);
        return req;
    }

    Segment MakeCentralizedSegment(UUID id, const std::string& name = "seg",
                                   size_t size = 1024 * 1024) {
        Segment seg;
        seg.id = id;
        seg.name = name;
        seg.size = size;
        seg.extra = CentralizedSegmentExtraData{
            .base = 0x100000000 + id.first * 0x100000000,
            .te_endpoint = "",
        };
        return seg;
    }
};

// ============================================================
// RegisterClient
// ============================================================

TEST_F(CentralizedClientManagerTest, RegisterClientSuccess) {
    auto mgr = CreateManager();
    mgr->Start();

    auto seg = MakeCentralizedSegment({1, 1}, "seg1", 16 * 1024 * 1024);
    auto req = MakeCentralizedRegisterRequest({100, 200}, {seg});

    auto res = mgr->RegisterClient(req);
    ASSERT_TRUE(res.has_value());
    EXPECT_EQ(res.value().view_version, 1);
}

TEST_F(CentralizedClientManagerTest, RegisterClientDuplicate) {
    auto mgr = CreateManager();
    mgr->Start();

    auto req = MakeCentralizedRegisterRequest();
    ASSERT_TRUE(mgr->RegisterClient(req).has_value());

    auto res = mgr->RegisterClient(req);
    EXPECT_FALSE(res.has_value());
    EXPECT_EQ(res.error(), ErrorCode::CLIENT_ALREADY_EXISTS);
}

TEST_F(CentralizedClientManagerTest, RegisterWrongDeploymentMode) {
    auto mgr = CreateManager();
    mgr->Start();

    RegisterClientRequest req;
    req.client_id = {100, 200};
    req.deployment_mode = DeploymentMode::P2P;  // Wrong mode!

    auto res = mgr->RegisterClient(req);
    EXPECT_FALSE(res.has_value());
}

TEST_F(CentralizedClientManagerTest, RegisterMultipleClients) {
    auto mgr = CreateManager();
    mgr->Start();

    for (int i = 0; i < 5; i++) {
        auto seg = MakeCentralizedSegment({static_cast<uint64_t>(i), 0},
                                          "seg_" + std::to_string(i),
                                          16 * 1024 * 1024);
        auto req = MakeCentralizedRegisterRequest({static_cast<uint64_t>(i), 0},
                                                  {seg});
        ASSERT_TRUE(mgr->RegisterClient(req).has_value());
    }

    auto clients = mgr->GetAllClients();
    EXPECT_EQ(clients.size(), 5);
}

TEST_F(CentralizedClientManagerTest, RegisterClientSameSegmentName) {
    auto mgr = CreateManager();
    mgr->Start();

    auto seg1 = MakeCentralizedSegment({1, 1}, "common_seg");
    auto req1 = MakeCentralizedRegisterRequest({100, 200}, {seg1});
    ASSERT_TRUE(mgr->RegisterClient(req1).has_value());

    auto seg2 = MakeCentralizedSegment({2, 2}, "common_seg");
    auto req2 = MakeCentralizedRegisterRequest({300, 400}, {seg2});
    ASSERT_TRUE(mgr->RegisterClient(req2).has_value());

    auto clients = mgr->GetAllClients();
    EXPECT_EQ(clients.size(), 2);
}

// ============================================================
// Heartbeat
// ============================================================

TEST_F(CentralizedClientManagerTest, HeartbeatSuccess) {
    auto mgr = CreateManager();
    mgr->Start();

    auto req = MakeCentralizedRegisterRequest();
    ASSERT_TRUE(mgr->RegisterClient(req).has_value());

    HeartbeatRequest hb_req;
    hb_req.client_id = {100, 200};

    auto res = mgr->Heartbeat(hb_req);
    ASSERT_TRUE(res.has_value());
    EXPECT_EQ(res.value().status, ClientStatus::HEALTH);
}

TEST_F(CentralizedClientManagerTest, HeartbeatUnregisteredClient) {
    auto mgr = CreateManager();
    mgr->Start();

    HeartbeatRequest hb_req;
    hb_req.client_id = {999, 999};

    auto res = mgr->Heartbeat(hb_req);
    ASSERT_TRUE(res.has_value());
    EXPECT_EQ(res.value().status, ClientStatus::UNDEFINED);
}

TEST_F(CentralizedClientManagerTest, HeartbeatRecoverFromDisconnection) {
    const int disconnect_sec = 1;
    auto mgr = CreateManager(disconnect_sec);
    mgr->Start();

    auto req = MakeCentralizedRegisterRequest();
    ASSERT_TRUE(mgr->RegisterClient(req).has_value());

    auto client = mgr->GetClient({100, 200});
    ASSERT_NE(client, nullptr);
    EXPECT_EQ(client->get_health_state().status, ClientStatus::HEALTH);

    // Wait for internal timeout
    std::this_thread::sleep_for(std::chrono::seconds(disconnect_sec + 1));

    // Background client_monitor_thread_ will transition status
    EXPECT_EQ(client->get_health_state().status, ClientStatus::DISCONNECTION);

    HeartbeatRequest hb_req;
    hb_req.client_id = {100, 200};
    auto res = mgr->Heartbeat(hb_req);
    ASSERT_TRUE(res.has_value());
    EXPECT_EQ(res.value().status, ClientStatus::HEALTH);
    EXPECT_EQ(client->get_health_state().status, ClientStatus::HEALTH);
}

TEST_F(CentralizedClientManagerTest, HeartbeatKeepCrashedStatus) {
    const int disconnect_sec = 1;
    const int crash_sec = 2;
    auto mgr = CreateManager(disconnect_sec, crash_sec);
    mgr->Start();

    auto req = MakeCentralizedRegisterRequest();
    ASSERT_TRUE(mgr->RegisterClient(req).has_value());

    // Wait for crash (crash_sec + safety margin)
    std::this_thread::sleep_for(std::chrono::seconds(crash_sec + 2));

    HeartbeatRequest hb_req;
    hb_req.client_id = {100, 200};
    auto res = mgr->Heartbeat(hb_req);
    ASSERT_TRUE(res.has_value());
    // Once crashed, ClientMonitorFunc removes it from client_metas_
    // So Heartbeat returns UNDEFINED
    EXPECT_EQ(res.value().status, ClientStatus::UNDEFINED);
}

TEST_F(CentralizedClientManagerTest, ClientMonitorStatusTransition) {
    std::atomic<int> removal_count{0};
    const int disconnect_sec = 1;
    const int crash_sec = 4;
    auto mgr = CreateManager(disconnect_sec, crash_sec);
    mgr->SetSegmentRemovalCallback(
        [&removal_count](const UUID& seg_id) { removal_count.fetch_add(1); });
    mgr->Start();

    auto seg = MakeCentralizedSegment({1, 1}, "seg1");
    auto req = MakeCentralizedRegisterRequest({100, 200}, {seg});
    ASSERT_TRUE(mgr->RegisterClient(req).has_value());

    // Initially 1 client
    EXPECT_EQ(mgr->GetAllClients().size(), 1);

    // After > disconnect_sec: DISCONNECTION (but still in client_metas_)
    std::this_thread::sleep_for(std::chrono::seconds(2));
    auto client = mgr->GetClient({100, 200});
    ASSERT_NE(client, nullptr);
    EXPECT_EQ(client->get_health_state().status, ClientStatus::DISCONNECTION);
    EXPECT_EQ(removal_count.load(), 0);

    // After > crash_sec: CRASHED and Removed
    std::this_thread::sleep_for(std::chrono::seconds(crash_sec));
    EXPECT_EQ(mgr->GetAllClients().size(), 0);

    // Verify removal callback was called
    EXPECT_EQ(removal_count.load(), 1);
}

// ============================================================
// Allocate
// ============================================================

TEST_F(CentralizedClientManagerTest, AllocateSuccess) {
    auto mgr = CreateManager();
    mgr->Start();

    // Register a client with a large segment
    auto seg = MakeCentralizedSegment({1, 1}, "seg1", 64 * 1024 * 1024);
    auto req = MakeCentralizedRegisterRequest({100, 200}, {seg});
    ASSERT_TRUE(mgr->RegisterClient(req).has_value());

    // Allocate a small slice
    auto res = mgr->Allocate(4096, 1, {"seg1"});
    ASSERT_TRUE(res.has_value());
    EXPECT_EQ(res.value().size(), 1);
}

TEST_F(CentralizedClientManagerTest, AllocateNoSpace) {
    auto mgr = CreateManager();
    mgr->Start();

    // Register with a very small segment (minimum 16MB for OFFSET allocator)
    auto seg = MakeCentralizedSegment({1, 1}, "seg1", 16 * 1024 * 1024);
    auto req = MakeCentralizedRegisterRequest({100, 200}, {seg});
    ASSERT_TRUE(mgr->RegisterClient(req).has_value());

    // Try to allocate more than available
    auto res = mgr->Allocate(64 * 1024 * 1024, 1, {"seg1"});
    EXPECT_FALSE(res.has_value());
}

TEST_F(CentralizedClientManagerTest, AllocateMultipleReplicas) {
    auto mgr = CreateManager();
    mgr->Start();

    // Register two clients with segments
    auto seg1 = MakeCentralizedSegment({1, 1}, "seg1", 64 * 1024 * 1024);
    auto req1 = MakeCentralizedRegisterRequest({100, 200}, {seg1});
    ASSERT_TRUE(mgr->RegisterClient(req1).has_value());

    auto seg2 = MakeCentralizedSegment({2, 2}, "seg2", 64 * 1024 * 1024);
    auto req2 = MakeCentralizedRegisterRequest({300, 400}, {seg2});
    ASSERT_TRUE(mgr->RegisterClient(req2).has_value());

    // Allocate with 2 replicas
    auto res = mgr->Allocate(4096, 2, {"seg1", "seg2"});
    ASSERT_TRUE(res.has_value());
    EXPECT_EQ(res.value().size(), 2);
}

TEST_F(CentralizedClientManagerTest, AllocateNonHealthClient) {
    const int disconnect_sec = 1;
    const int crash_sec = 5;
    auto mgr = CreateManager(disconnect_sec, crash_sec);
    mgr->Start();

    auto seg = MakeCentralizedSegment({1, 1}, "seg1", 64 * 1024 * 1024);
    auto req = MakeCentralizedRegisterRequest({100, 200}, {seg});
    ASSERT_TRUE(mgr->RegisterClient(req).has_value());

    // Initially Healthy
    ASSERT_TRUE(mgr->Allocate(4096, 1, {"seg1"}).has_value());

    // Wait for DISCONNECTED
    // sleep disconnect_sec + safety margin for ClientMonitorFunc (1s)
    std::this_thread::sleep_for(std::chrono::seconds(disconnect_sec + 1));

    auto client = mgr->GetClient({100, 200});
    ASSERT_NE(client, nullptr);
    EXPECT_EQ(client->get_health_state().status, ClientStatus::DISCONNECTION);

    // Allocate should fail when DISCONNECTED
    EXPECT_FALSE(mgr->Allocate(4096, 1, {"seg1"}).has_value());

    // Recover via Heartbeat
    HeartbeatRequest hb_req;
    hb_req.client_id = {100, 200};
    auto hb_res = mgr->Heartbeat(hb_req);
    ASSERT_TRUE(hb_res.has_value());
    EXPECT_EQ(hb_res.value().status, ClientStatus::HEALTH);

    // Allocate should succeed again
    EXPECT_TRUE(mgr->Allocate(4096, 1, {"seg1"}).has_value());

    // Wait for CRASHED
    std::this_thread::sleep_for(std::chrono::seconds(crash_sec + 1));
    EXPECT_FALSE(mgr->Allocate(4096, 1, {"seg1"}).has_value());
}

// ============================================================
// GetClient / QueryIp / QuerySegment
// ============================================================

TEST_F(CentralizedClientManagerTest, GetClientNonExistent) {
    auto mgr = CreateManager();
    mgr->Start();

    EXPECT_EQ(mgr->GetClient({999, 999}), nullptr);
}

TEST_F(CentralizedClientManagerTest, QuerySegmentsAfterRegister) {
    auto mgr = CreateManager();
    mgr->Start();

    auto seg = MakeCentralizedSegment({1, 1}, "my_seg", 16 * 1024 * 1024);
    auto req = MakeCentralizedRegisterRequest({100, 200}, {seg});
    ASSERT_TRUE(mgr->RegisterClient(req).has_value());

    auto res = mgr->QuerySegments("my_seg");
    ASSERT_TRUE(res.has_value());
    auto [used, capacity] = res.value();
    EXPECT_EQ(capacity, 16 * 1024 * 1024);
}

TEST_F(CentralizedClientManagerTest, QueryIpSuccess) {
    auto mgr = CreateManager();
    mgr->Start();

    UUID client_id = {100, 200};
    auto seg = MakeCentralizedSegment({1, 1}, "seg1");
    seg.extra = CentralizedSegmentExtraData{
        .base = 0x100000000,
        .te_endpoint = "127.0.0.1:1234",
    };

    RegisterClientRequest req =
        MakeCentralizedRegisterRequest(client_id, {seg});
    req.ip_address = "127.0.0.1";
    req.rpc_port = 1234;
    ASSERT_TRUE(mgr->RegisterClient(req).has_value());

    auto res = mgr->QueryIp(client_id);
    ASSERT_TRUE(res.has_value());
    ASSERT_FALSE(res.value().empty());
    EXPECT_EQ(res.value()[0], "127.0.0.1");
}

TEST_F(CentralizedClientManagerTest, QueryInterfacesNonHealth) {
    const int disconnect_sec = 1;
    const int crash_sec = 4;
    auto mgr = CreateManager(disconnect_sec, crash_sec);
    mgr->Start();

    auto seg = MakeCentralizedSegment({1, 1}, "seg1");
    auto req = MakeCentralizedRegisterRequest({100, 200}, {seg});
    ASSERT_TRUE(mgr->RegisterClient(req).has_value());

    // Wait for DISCONNECTED
    std::this_thread::sleep_for(std::chrono::seconds(disconnect_sec + 1));

    EXPECT_FALSE(mgr->QuerySegments("seg1").has_value());
    EXPECT_FALSE(mgr->QueryIp({100, 200}).has_value());

    // Recover via Heartbeat
    HeartbeatRequest hb_req;
    hb_req.client_id = {100, 200};
    ASSERT_TRUE(mgr->Heartbeat(hb_req).has_value());

    // Query should succeed after recovery
    EXPECT_TRUE(mgr->QuerySegments("seg1").has_value());
    EXPECT_TRUE(mgr->QueryIp({100, 200}).has_value());
}

TEST_F(CentralizedClientManagerTest, QuerySegmentAndGetClientSegments) {
    auto mgr = CreateManager();
    mgr->Start();

    // Client 1 with 2 segments
    UUID client_id1 = {100, 200};
    auto seg1_1 = MakeCentralizedSegment({1, 1}, "c1_seg1", 1024);
    auto seg1_2 = MakeCentralizedSegment({1, 2}, "c1_seg2", 2048);
    auto req1 = MakeCentralizedRegisterRequest(client_id1, {seg1_1, seg1_2});
    ASSERT_TRUE(mgr->RegisterClient(req1).has_value());

    // Client 2 with 1 segment
    UUID client_id2 = {300, 400};
    auto seg2_1 = MakeCentralizedSegment({2, 1}, "c2_seg1", 4096);
    auto req2 = MakeCentralizedRegisterRequest(client_id2, {seg2_1});
    ASSERT_TRUE(mgr->RegisterClient(req2).has_value());

    // Verify GetClientSegments for Client 1
    auto names_res1 = mgr->GetClientSegments(client_id1);
    ASSERT_TRUE(names_res1.has_value());
    EXPECT_EQ(names_res1.value().size(), 2);
    // Note: order might depend on implementation, but typically reflects
    // registration or map order
    std::set<std::string> expected_names1 = {"c1_seg1", "c1_seg2"};
    std::set<std::string> actual_names1(names_res1.value().begin(),
                                        names_res1.value().end());
    EXPECT_EQ(actual_names1, expected_names1);

    // Detailed verification of QuerySegment for Client 1, Segment 2
    auto seg_res = mgr->QuerySegment(client_id1, {1, 2});
    ASSERT_TRUE(seg_res.has_value());
    auto found_seg = seg_res.value();
    EXPECT_EQ(found_seg->id, (UUID{1, 2}));
    EXPECT_EQ(found_seg->name, "c1_seg2");
    EXPECT_EQ(found_seg->size, 2048);
    EXPECT_EQ(found_seg->GetCentralizedExtra().base,
              seg1_2.GetCentralizedExtra().base);
    EXPECT_EQ(found_seg->GetCentralizedExtra().te_endpoint,
              seg1_2.GetCentralizedExtra().te_endpoint);

    // Verify QuerySegment for non-existent segment or cross-client query
    EXPECT_FALSE(mgr->QuerySegment(client_id1, {2, 1}).has_value());
}

// ============================================================
// ForEachClient
// ============================================================

TEST_F(CentralizedClientManagerTest, ForEachClientOrdered) {
    auto mgr = CreateManager();
    mgr->Start();

    std::vector<UUID> expected_ids;
    for (int i = 0; i < 3; i++) {
        UUID id = {static_cast<uint64_t>(i), 0};
        expected_ids.push_back(id);
        auto seg = MakeCentralizedSegment(id, "seg_" + std::to_string(i));
        auto req = MakeCentralizedRegisterRequest(id, {seg});
        ASSERT_TRUE(mgr->RegisterClient(req).has_value());
    }

    std::vector<UUID> ids1, ids2;
    auto res1 =
        mgr->ForEachClient(ObjectIterateStrategy::ORDERED,
                           [&ids1](const std::shared_ptr<ClientMeta>& client)
                               -> tl::expected<bool, ErrorCode> {
                               ids1.push_back(client->get_client_id());
                               return false;
                           });
    auto res2 =
        mgr->ForEachClient(ObjectIterateStrategy::ORDERED,
                           [&ids2](const std::shared_ptr<ClientMeta>& client)
                               -> tl::expected<bool, ErrorCode> {
                               ids2.push_back(client->get_client_id());
                               return false;
                           });

    ASSERT_TRUE(res1.has_value());
    ASSERT_TRUE(res2.has_value());

    // Check count
    ASSERT_EQ(ids1.size(), expected_ids.size());
    EXPECT_EQ(ids1, ids2);

    // Verify all expected IDs are present
    std::set<UUID> expected_set(expected_ids.begin(), expected_ids.end());
    std::set<UUID> actual_set(ids1.begin(), ids1.end());
    EXPECT_EQ(actual_set, expected_set);
}

TEST_F(CentralizedClientManagerTest, ForEachClientRandomStrategies) {
    auto mgr = CreateManager();
    mgr->Start();

    std::set<UUID> expected_set;
    for (int i = 0; i < 5; i++) {
        UUID id = {static_cast<uint64_t>(i), 0};
        expected_set.insert(id);
        auto req = MakeCentralizedRegisterRequest(id);
        ASSERT_TRUE(mgr->RegisterClient(req).has_value());
    }

    std::set<UUID> actual_set;
    auto res = mgr->ForEachClient(
        ObjectIterateStrategy::RANDOM,
        [&actual_set](const std::shared_ptr<ClientMeta>& client)
            -> tl::expected<bool, ErrorCode> {
            actual_set.insert(client->get_client_id());
            return false;
        });
    EXPECT_TRUE(res.has_value());
    EXPECT_EQ(actual_set.size(), expected_set.size());
    EXPECT_EQ(actual_set, expected_set);
}

TEST_F(CentralizedClientManagerTest, ForEachClientHealthEffect) {
    const int disconnect_sec = 1;
    const int crash_sec = 2;
    auto mgr = CreateManager(disconnect_sec, crash_sec);
    // STOP the background monitor thread to manually control status transitions
    mgr->StopClientMonitor();

    // Register 4 clients
    for (int i = 1; i <= 4; ++i) {
        mgr->RegisterClient(
            MakeCentralizedRegisterRequest({static_cast<uint64_t>(i), 0}));
    }

    // Phase 1: All HEALTHY initially
    // Phase 2: Wait 1.1s. (1s < 1.1s < 2s)
    std::this_thread::sleep_for(std::chrono::milliseconds(1100));

    // Heartbeat Client 1, 2, 3. (Reset their timers)
    mgr->Heartbeat(HeartbeatRequest{.client_id = UUID{1, 0}, .tasks = {}});
    mgr->Heartbeat(HeartbeatRequest{.client_id = UUID{3, 0}, .tasks = {}});
    mgr->Heartbeat(HeartbeatRequest{.client_id = UUID{4, 0}, .tasks = {}});

    // Manually trigger monitor.
    // Client 1, 3, 4: HEALTHY
    // Client 2: transition to DISCONNECTION
    mgr->ClientMonitorFunc();

    // Phase 3: Wait another 1.1s
    std::this_thread::sleep_for(std::chrono::milliseconds(1100));

    // Refresh 1 & 4 again
    mgr->Heartbeat(HeartbeatRequest{.client_id = UUID{1, 0}, .tasks = {}});
    mgr->Heartbeat(HeartbeatRequest{.client_id = UUID{4, 0}, .tasks = {}});

    // Manually trigger monitor again.
    // Client 1, 4: HEALTHY (HB recently)
    // Client 2: transition to CRASHED
    // Client 3: transition to DISCONNECTION
    mgr->ClientMonitorFunc();

    int count = 0;
    std::set<UUID> found_ids;
    mgr->ForEachClient(
        ObjectIterateStrategy::ORDERED,
        [&count, &found_ids](const std::shared_ptr<ClientMeta>& client)
            -> tl::expected<bool, ErrorCode> {
            count++;
            found_ids.insert(client->get_client_id());
            return false;
        });

    // 3 clients should be visible (HEALTHY 1, 4 and DISCONNECTED 3)
    // Client 2 should be removed because it transitioned to CRASHED.
    EXPECT_EQ(count, 3);
    EXPECT_TRUE(found_ids.count({1, 0}));
    EXPECT_FALSE(found_ids.count({2, 0}));
    EXPECT_TRUE(found_ids.count({3, 0}));
    EXPECT_TRUE(found_ids.count({4, 0}));
}

TEST_F(CentralizedClientManagerTest, ForEachClientEarlyExit) {
    auto mgr = CreateManager();
    mgr->Start();

    for (int i = 0; i < 5; i++) {
        auto req =
            MakeCentralizedRegisterRequest({static_cast<uint64_t>(i), 0});
        ASSERT_TRUE(mgr->RegisterClient(req).has_value());
    }

    int count = 0;
    auto res =
        mgr->ForEachClient(ObjectIterateStrategy::ORDERED,
                           [&count](const std::shared_ptr<ClientMeta>& client)
                               -> tl::expected<bool, ErrorCode> {
                               count++;
                               return count == 2;  // stop after 2
                           });
    EXPECT_TRUE(res.has_value());
    EXPECT_EQ(count, 2);
}

// ============================================================
// Concurrency Tests
// ============================================================

TEST_F(CentralizedClientManagerTest, ConcurrentRegister) {
    auto mgr = CreateManager();
    mgr->Start();

    constexpr int kNumThreads = 16;
    std::vector<std::thread> threads;
    std::atomic<int> success_count{0};

    for (int i = 0; i < kNumThreads; i++) {
        threads.emplace_back([this, &mgr, &success_count, i]() {
            UUID client_id = {static_cast<uint64_t>(i + 1), 0};
            auto seg =
                MakeCentralizedSegment({static_cast<uint64_t>(i + 100), 0},
                                       "seg_" + std::to_string(i));
            auto req = MakeCentralizedRegisterRequest(client_id, {seg});
            auto res = mgr->RegisterClient(req);
            if (res.has_value()) {
                success_count.fetch_add(1);
            }
        });
    }

    for (auto& t : threads) {
        t.join();
    }

    EXPECT_EQ(success_count.load(), kNumThreads);

    // Verify all clients registered via ForEachClient
    std::set<UUID> registered_ids;
    auto res = mgr->ForEachClient(
        ObjectIterateStrategy::ORDERED,
        [&registered_ids](const std::shared_ptr<ClientMeta>& client)
            -> tl::expected<bool, ErrorCode> {
            registered_ids.insert(client->get_client_id());
            return false;  // continue
        });
    EXPECT_TRUE(res.has_value());
    EXPECT_EQ(registered_ids.size(), kNumThreads);
    for (int i = 0; i < kNumThreads; i++) {
        EXPECT_NE(registered_ids.find({static_cast<uint64_t>(i + 1), 0}),
                  registered_ids.end());
    }
}

TEST_F(CentralizedClientManagerTest, ConcurrentHeartbeat) {
    auto mgr = CreateManager();
    mgr->Start();

    constexpr int kNumClients = 8;
    // Register clients first
    for (int i = 0; i < kNumClients; i++) {
        UUID client_id = {static_cast<uint64_t>(i + 1), 0};
        auto seg = MakeCentralizedSegment({static_cast<uint64_t>(i + 100), 0},
                                          "seg_" + std::to_string(i));
        auto req = MakeCentralizedRegisterRequest(client_id, {seg});
        ASSERT_TRUE(mgr->RegisterClient(req).has_value());
    }

    std::vector<std::thread> threads;
    std::atomic<int> success_count{0};

    for (int i = 0; i < kNumClients; i++) {
        threads.emplace_back([&mgr, &success_count, i]() {
            UUID client_id = {static_cast<uint64_t>(i + 1), 0};
            HeartbeatRequest req;
            req.client_id = client_id;
            auto res = mgr->Heartbeat(req);
            if (res.has_value()) {
                success_count.fetch_add(1);
            }
        });
    }

    for (auto& t : threads) {
        t.join();
    }

    EXPECT_EQ(success_count.load(), kNumClients);
}

TEST_F(CentralizedClientManagerTest, ConcurrentRegisterSameClient) {
    auto mgr = CreateManager();
    mgr->Start();

    constexpr int kNumThreads = 8;
    UUID client_id = {42, 0};
    auto seg = MakeCentralizedSegment({1, 0}, "seg");

    std::vector<std::thread> threads;
    std::atomic<int> success_count{0};

    for (int i = 0; i < kNumThreads; i++) {
        threads.emplace_back([this, &mgr, &client_id, &seg, &success_count]() {
            auto req = MakeCentralizedRegisterRequest(client_id, {seg});
            auto res = mgr->RegisterClient(req);
            if (res.has_value()) {
                success_count.fetch_add(1);
            }
        });
    }

    for (auto& t : threads) {
        t.join();
    }

    // All should report success (RegisterClient is idempotent)
    // or exactly one succeeds depending on implementation
    EXPECT_GE(success_count.load(), 1);

    // Only one client should be registered
    int count = 0;
    auto res =
        mgr->ForEachClient(ObjectIterateStrategy::ORDERED,
                           [&count](const std::shared_ptr<ClientMeta>& client)
                               -> tl::expected<bool, ErrorCode> {
                               count++;
                               return false;
                           });
    EXPECT_TRUE(res.has_value());
    EXPECT_EQ(count, 1);
}

// ============================================================
// Mount and Offload Interfaces
// ============================================================

TEST_F(CentralizedClientManagerTest, MountAndOffloadInterfaces) {
    auto mgr = CreateManager();
    mgr->Start();

    UUID client_id = {100, 200};
    auto seg = MakeCentralizedSegment({1, 1}, "seg1");
    auto req = MakeCentralizedRegisterRequest(client_id, {seg});
    ASSERT_TRUE(mgr->RegisterClient(req).has_value());

    // Test MountLocalDiskSegment
    EXPECT_TRUE(mgr->MountLocalDiskSegment(client_id, true).has_value());

    // Test OffloadObjectHeartbeat
    auto offload_res = mgr->OffloadObjectHeartbeat(client_id, true);
    EXPECT_TRUE(offload_res.has_value());

    // Test PushOffloadingQueue
    EXPECT_TRUE(mgr->PushOffloadingQueue("my_key", 1024, "seg1").has_value());
    // Non-existent segment
    EXPECT_FALSE(
        mgr->PushOffloadingQueue("my_key", 1024, "no_seg").has_value());
}

}  // namespace mooncake
