#include <gtest/gtest.h>
#include <glog/logging.h>
#include <atomic>
#include <chrono>
#include <thread>
#include <vector>
#define private public
#define protected public
#include "p2p_client_manager.h"
#undef private
#undef protected
#include "p2p_client_meta.h"
#include <set>

namespace mooncake {

class P2PClientManagerTest : public ::testing::Test {
   protected:
    void SetUp() override {
        google::InitGoogleLogging("P2PClientManagerTest");
        FLAGS_logtostderr = 1;
    }

    void TearDown() override { google::ShutdownGoogleLogging(); }

    std::unique_ptr<P2PClientManager> CreateManager(
        int64_t disconnect_sec = 2, int64_t crash_sec = 5,
        ViewVersionId view_version = 1) {
        return std::make_unique<P2PClientManager>(disconnect_sec, crash_sec,
                                                  view_version);
    }

    RegisterClientRequest MakeP2PRegisterRequest(
        UUID client_id = {100, 200}, const std::string& ip = "10.0.0.1",
        uint16_t rpc_port = 50051, std::vector<Segment> segments = {}) {
        RegisterClientRequest req;
        req.client_id = client_id;
        req.deployment_mode = DeploymentMode::P2P;
        req.ip_address = ip;
        req.rpc_port = rpc_port;
        req.segments = std::move(segments);
        return req;
    }

    Segment MakeP2PSegment(UUID id, const std::string& name = "seg",
                           size_t size = 1024 * 1024, int priority = 1) {
        Segment seg;
        seg.id = id;
        seg.name = name;
        seg.size = size;
        seg.extra = P2PSegmentExtraData{
            .priority = priority,
            .tags = {},
            .memory_type = MemoryType::DRAM,
            .usage = 0,
        };
        return seg;
    }
};

// ============================================================
// RegisterClient
// ============================================================

TEST_F(P2PClientManagerTest, RegisterClientSuccess) {
    auto mgr = CreateManager();
    mgr->Start();

    auto seg = MakeP2PSegment({1, 1}, "seg1");
    auto req = MakeP2PRegisterRequest({100, 200}, "10.0.0.1", 50051, {seg});

    auto res = mgr->RegisterClient(req);
    ASSERT_TRUE(res.has_value());
    EXPECT_EQ(res.value().view_version, 1);
}

TEST_F(P2PClientManagerTest, RegisterClientDuplicate) {
    auto mgr = CreateManager();
    mgr->Start();

    auto req = MakeP2PRegisterRequest();
    ASSERT_TRUE(mgr->RegisterClient(req).has_value());

    // Second registration with same client_id should fail
    auto res = mgr->RegisterClient(req);
    EXPECT_FALSE(res.has_value());
    EXPECT_EQ(res.error(), ErrorCode::CLIENT_ALREADY_EXISTS);
}

TEST_F(P2PClientManagerTest, RegisterWrongDeploymentMode) {
    auto mgr = CreateManager();
    mgr->Start();

    RegisterClientRequest req;
    req.client_id = {100, 200};
    req.deployment_mode = DeploymentMode::CENTRALIZATION;  // Wrong mode!
    req.segments = {};

    auto res = mgr->RegisterClient(req);
    EXPECT_FALSE(res.has_value());
}

TEST_F(P2PClientManagerTest, RegisterComplexScenario) {
    auto mgr = CreateManager();
    mgr->Start();

    // 1. Register multiple clients, where some share common segment names
    auto common_seg_name = "shared_name";
    for (int i = 0; i < 5; i++) {
        std::string seg_name = (i < 3) ? ("seg_" + std::to_string(i))
                                       : std::string(common_seg_name);
        auto seg = MakeP2PSegment({static_cast<uint64_t>(i), 0}, seg_name);
        auto req =
            MakeP2PRegisterRequest({static_cast<uint64_t>(i), 0},
                                   "10.0.0." + std::to_string(i), 50051, {seg});
        ASSERT_TRUE(mgr->RegisterClient(req).has_value());
    }
    EXPECT_EQ(mgr->GetAllClients().size(), 5);

    // 2. Register a client with multiple segments
    UUID client_id3 = {999, 999};
    std::vector<Segment> segs;
    for (int i = 0; i < 5; i++) {
        segs.push_back(MakeP2PSegment({999, static_cast<uint64_t>(i)},
                                      "c3_seg_" + std::to_string(i)));
    }
    auto req3 = MakeP2PRegisterRequest(client_id3, "10.0.0.254", 50051, segs);
    ASSERT_TRUE(mgr->RegisterClient(req3).has_value());

    // Final verification
    EXPECT_EQ(mgr->GetAllClients().size(), 6);
    auto client3 = mgr->GetClient(client_id3);
    ASSERT_NE(client3, nullptr);
    auto segments_res = client3->GetSegments();
    ASSERT_TRUE(segments_res.has_value());
    EXPECT_EQ(segments_res.value().size(), 5);
}

// ============================================================
// Heartbeat
// ============================================================

TEST_F(P2PClientManagerTest, HeartbeatSuccess) {
    auto mgr = CreateManager();
    mgr->Start();

    auto req = MakeP2PRegisterRequest();
    ASSERT_TRUE(mgr->RegisterClient(req).has_value());

    HeartbeatRequest hb_req;
    hb_req.client_id = {100, 200};

    auto res = mgr->Heartbeat(hb_req);
    ASSERT_TRUE(res.has_value());
    EXPECT_EQ(res.value().status, ClientStatus::HEALTH);
}

TEST_F(P2PClientManagerTest, HeartbeatUnregisteredClient) {
    auto mgr = CreateManager();
    mgr->Start();

    HeartbeatRequest hb_req;
    hb_req.client_id = {999, 999};

    auto res = mgr->Heartbeat(hb_req);
    ASSERT_TRUE(res.has_value());
    EXPECT_EQ(res.value().status, ClientStatus::UNDEFINED);
}

TEST_F(P2PClientManagerTest, HeartbeatRecoverFromDisconnection) {
    const int disconnect_sec = 1;
    auto mgr = CreateManager(disconnect_sec);
    mgr->Start();

    UUID client_id = {100, 200};
    auto req = MakeP2PRegisterRequest(client_id);
    ASSERT_TRUE(mgr->RegisterClient(req).has_value());

    auto client = mgr->GetClient(client_id);
    ASSERT_NE(client, nullptr);
    EXPECT_EQ(client->get_health_state().status, ClientStatus::HEALTH);

    // Wait for internal timeout
    std::this_thread::sleep_for(std::chrono::seconds(disconnect_sec + 1));

    // Background client_monitor_thread_ will transition status
    EXPECT_EQ(client->get_health_state().status, ClientStatus::DISCONNECTION);

    HeartbeatRequest hb_req;
    hb_req.client_id = client_id;
    auto res = mgr->Heartbeat(hb_req);
    ASSERT_TRUE(res.has_value());
    EXPECT_EQ(res.value().status, ClientStatus::HEALTH);
    EXPECT_EQ(client->get_health_state().status, ClientStatus::HEALTH);
}

TEST_F(P2PClientManagerTest, HeartbeatKeepCrashedStatus) {
    const int disconnect_sec = 1;
    const int crash_sec = 2;
    auto mgr = CreateManager(disconnect_sec, crash_sec);
    mgr->Start();

    UUID client_id = {100, 200};
    auto req = MakeP2PRegisterRequest(client_id);
    ASSERT_TRUE(mgr->RegisterClient(req).has_value());

    // Wait for crash (crash_sec + safety margin)
    std::this_thread::sleep_for(std::chrono::seconds(crash_sec + 2));

    HeartbeatRequest hb_req;
    hb_req.client_id = client_id;
    auto res = mgr->Heartbeat(hb_req);
    ASSERT_TRUE(res.has_value());
    // Once crashed, ClientMonitorFunc removes it from client_metas_
    EXPECT_EQ(res.value().status, ClientStatus::UNDEFINED);
}

TEST_F(P2PClientManagerTest, ClientMonitorStatusTransition) {
    std::atomic<int> removal_count{0};
    const int disconnect_sec = 1;
    const int crash_sec = 4;
    auto mgr = CreateManager(disconnect_sec, crash_sec);
    mgr->SetSegmentRemovalCallback(
        [&removal_count](const UUID& seg_id) { removal_count.fetch_add(1); });
    mgr->Start();

    UUID client_id = {100, 200};
    auto seg = MakeP2PSegment({1, 1}, "seg1");
    auto req = MakeP2PRegisterRequest(client_id, "10.0.0.1", 50051, {seg});
    ASSERT_TRUE(mgr->RegisterClient(req).has_value());

    EXPECT_EQ(mgr->GetAllClients().size(), 1);

    // HEALTH -> DISCONNECTION
    std::this_thread::sleep_for(std::chrono::seconds(disconnect_sec + 2));
    auto client = mgr->GetClient(client_id);
    ASSERT_NE(client, nullptr);
    EXPECT_EQ(client->get_health_state().status, ClientStatus::DISCONNECTION);
    EXPECT_EQ(removal_count.load(), 0);

    // DISCONNECTION -> CRASHED -> REMOVED
    std::this_thread::sleep_for(std::chrono::seconds(crash_sec));
    EXPECT_EQ(mgr->GetAllClients().size(), 0);
    EXPECT_EQ(removal_count.load(), 1);
}

// ============================================================
// GetClient / GetAllClients
// ============================================================

TEST_F(P2PClientManagerTest, GetClientReturnsNull) {
    auto mgr = CreateManager();
    mgr->Start();

    auto client = mgr->GetClient({999, 999});
    EXPECT_EQ(client, nullptr);
}

TEST_F(P2PClientManagerTest, GetAllClientsEmpty) {
    auto mgr = CreateManager();
    mgr->Start();

    auto clients = mgr->GetAllClients();
    EXPECT_TRUE(clients.empty());
}

TEST_F(P2PClientManagerTest, GetAllClientsMultiple) {
    auto mgr = CreateManager();
    mgr->Start();

    for (int i = 0; i < 3; i++) {
        auto req = MakeP2PRegisterRequest({static_cast<uint64_t>(i), 0},
                                          "10.0.0." + std::to_string(i));
        ASSERT_TRUE(mgr->RegisterClient(req).has_value());
    }

    auto clients = mgr->GetAllClients();
    EXPECT_EQ(clients.size(), 3);
}

// ============================================================
// QueryIp / QuerySegments / QuerySegment / GetClientSegments
// ============================================================

TEST_F(P2PClientManagerTest, QueryIpAfterRegister) {
    auto mgr = CreateManager();
    mgr->Start();

    UUID client_id = {100, 200};
    auto seg = MakeP2PSegment({1, 1}, "seg1");
    auto req = MakeP2PRegisterRequest(client_id, "10.0.0.42", 50051, {seg});
    ASSERT_TRUE(mgr->RegisterClient(req).has_value());

    auto res = mgr->QueryIp(client_id);
    ASSERT_TRUE(res.has_value());
    ASSERT_FALSE(res.value().empty());
    EXPECT_EQ(res.value()[0], "10.0.0.42");
}

TEST_F(P2PClientManagerTest, QuerySegmentsAfterRegister) {
    auto mgr = CreateManager();
    mgr->Start();

    auto seg = MakeP2PSegment({1, 1}, "my_segment", 4096);
    auto req = MakeP2PRegisterRequest({100, 200}, "10.0.0.1", 50051, {seg});
    ASSERT_TRUE(mgr->RegisterClient(req).has_value());

    auto res = mgr->QuerySegments("my_segment");
    ASSERT_TRUE(res.has_value());
    auto [used, capacity] = res.value();
    EXPECT_EQ(capacity, 4096);
}

TEST_F(P2PClientManagerTest, QuerySegmentAndGetClientSegments) {
    auto mgr = CreateManager();
    mgr->Start();

    // Client 1 with 2 segments
    UUID client_id1 = {100, 200};
    auto seg1_1 = MakeP2PSegment({1, 1}, "c1_seg1", 1024, 10);
    auto seg1_2 = MakeP2PSegment({1, 2}, "c1_seg2", 2048, 20);
    auto req1 =
        MakeP2PRegisterRequest(client_id1, "10.0.0.1", 50051, {seg1_1, seg1_2});
    ASSERT_TRUE(mgr->RegisterClient(req1).has_value());

    // Client 2 with 1 segment
    UUID client_id2 = {300, 400};
    auto seg2_1 = MakeP2PSegment({2, 1}, "c2_seg1", 4096, 30);
    auto req2 = MakeP2PRegisterRequest(client_id2, "10.0.0.2", 50052, {seg2_1});
    ASSERT_TRUE(mgr->RegisterClient(req2).has_value());

    // Verify GetClientSegments for Client 1
    auto names_res1 = mgr->GetClientSegments(client_id1);
    ASSERT_TRUE(names_res1.has_value());
    EXPECT_EQ(names_res1.value().size(), 2);
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
    EXPECT_EQ(found_seg->GetP2PExtra().priority, 20);

    // Verify QuerySegment for non-existent segment or cross-client query
    EXPECT_FALSE(mgr->QuerySegment(client_id1, {2, 1}).has_value());
}

TEST_F(P2PClientManagerTest, QueryInterfacesNonHealth) {
    const int disconnect_sec = 1;
    const int crash_sec = 4;
    auto mgr = CreateManager(disconnect_sec, crash_sec);
    mgr->Start();

    auto seg = MakeP2PSegment({1, 1}, "seg1");
    auto req = MakeP2PRegisterRequest({100, 200}, "10.0.0.1", 50051, {seg});
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

// ============================================================
// ForEachClient
// ============================================================

TEST_F(P2PClientManagerTest, ForEachClientOrdered) {
    auto mgr = CreateManager();
    mgr->Start();

    std::vector<UUID> expected_ids;
    for (int i = 0; i < 3; i++) {
        UUID id = {static_cast<uint64_t>(i), 0};
        expected_ids.push_back(id);
        auto req = MakeP2PRegisterRequest(id, "10.0.0." + std::to_string(i));
        ASSERT_TRUE(mgr->RegisterClient(req).has_value());
    }

    std::vector<UUID> ids1, ids2;
    auto visitor1 = [&ids1](const std::shared_ptr<ClientMeta>& client)
        -> tl::expected<bool, ErrorCode> {
        ids1.push_back(client->get_client_id());
        return false;
    };
    auto visitor2 = [&ids2](const std::shared_ptr<ClientMeta>& client)
        -> tl::expected<bool, ErrorCode> {
        ids2.push_back(client->get_client_id());
        return false;
    };

    ASSERT_TRUE(mgr->ForEachClient(ObjectIterateStrategy::ORDERED, visitor1)
                    .has_value());
    ASSERT_TRUE(mgr->ForEachClient(ObjectIterateStrategy::ORDERED, visitor2)
                    .has_value());
    EXPECT_EQ(ids1, ids2);
    EXPECT_EQ(ids1.size(), 3);
}

TEST_F(P2PClientManagerTest, ForEachClientCapacityPriority) {
    auto mgr = CreateManager();
    mgr->Start();

    for (int i = 0; i < 3; i++) {
        auto seg = MakeP2PSegment({static_cast<uint64_t>(i), 0},
                                  "seg_" + std::to_string(i), 1024 * (i + 1));
        auto req =
            MakeP2PRegisterRequest({static_cast<uint64_t>(i), 0},
                                   "10.0.0." + std::to_string(i), 50051, {seg});
        ASSERT_TRUE(mgr->RegisterClient(req).has_value());
    }

    std::vector<UUID> ids;
    auto visitor = [&ids](const std::shared_ptr<ClientMeta>& client)
        -> tl::expected<bool, ErrorCode> {
        ids.push_back(client->get_client_id());
        return false;
    };

    ASSERT_TRUE(
        mgr->ForEachClient(ObjectIterateStrategy::CAPACITY_PRIORITY, visitor)
            .has_value());
    ASSERT_EQ(ids.size(), 3);
    EXPECT_EQ(ids[0], (UUID{2, 0}));
    EXPECT_EQ(ids[1], (UUID{1, 0}));
    EXPECT_EQ(ids[2], (UUID{0, 0}));

    // Test sorting change after usage update (simulated via SYNC_SEGMENT_META)
    ids.clear();
    HeartbeatRequest hb_req;
    hb_req.client_id = {2, 0};
    hb_req.tasks.push_back(
        {HeartbeatTaskType::SYNC_SEGMENT_META,
         SyncSegmentMetaParam{.tier_usages = {{UUID{2, 0}, 2500}}}});
    ASSERT_TRUE(mgr->Heartbeat(hb_req).has_value());
    // New capacity for {2,0} is 3072 - 2500 = 572.
    // Order should be: {1,0} (2048), {0,0} (1024), {2,0} (572)
    ASSERT_TRUE(
        mgr->ForEachClient(ObjectIterateStrategy::CAPACITY_PRIORITY, visitor)
            .has_value());
    ASSERT_EQ(ids.size(), 3);
    EXPECT_EQ(ids[0], (UUID{1, 0}));
    EXPECT_EQ(ids[1], (UUID{0, 0}));
    EXPECT_EQ(ids[2], (UUID{2, 0}));
}

TEST_F(P2PClientManagerTest, ForEachClientHealthEffect) {
    const int disconnect_sec = 1;
    const int crash_sec = 2;
    auto mgr = CreateManager(disconnect_sec, crash_sec);
    // STOP the background monitor thread to manually control status transitions
    mgr->StopClientMonitor();

    // Register 4 clients
    for (int i = 1; i <= 4; ++i) {
        mgr->RegisterClient(
            MakeP2PRegisterRequest({static_cast<uint64_t>(i), 0}));
    }

    // Phase 1: All HEALTHY initially
    // Phase 2: Wait 1.1s. (1s < 1.1s < 2s)
    std::this_thread::sleep_for(std::chrono::milliseconds(1100));

    // Heartbeat Client 1, 3, 4. (Reset their timers)
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
    // Client 2: transition to CRASHED (and removed)
    // Client 3: transition to DISCONNECTION (no HB in last 1.1s)
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

// ============================================================
// Concurrency Tests
// ============================================================

TEST_F(P2PClientManagerTest, ConcurrentRegister) {
    auto mgr = CreateManager();
    mgr->Start();

    constexpr int kNumThreads = 16;
    std::vector<std::thread> threads;
    std::atomic<int> success_count{0};

    for (int i = 0; i < kNumThreads; i++) {
        threads.emplace_back([this, &mgr, &success_count, i]() {
            UUID client_id = {static_cast<uint64_t>(i + 1), 0};
            auto seg = MakeP2PSegment({static_cast<uint64_t>(i + 100), 0},
                                      "seg_" + std::to_string(i));
            auto req = MakeP2PRegisterRequest(
                client_id, "10.0.0." + std::to_string(i + 1),
                static_cast<uint16_t>(50051 + i), {seg});
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
            return false;
        });
    EXPECT_TRUE(res.has_value());
    EXPECT_EQ(registered_ids.size(), kNumThreads);
    for (int i = 1; i <= kNumThreads; i++) {
        EXPECT_TRUE(registered_ids.count({static_cast<uint64_t>(i), 0}));
    }
}

TEST_F(P2PClientManagerTest, ConcurrentHeartbeat) {
    auto mgr = CreateManager();
    mgr->Start();

    constexpr int kNumClients = 8;
    for (int i = 0; i < kNumClients; i++) {
        UUID client_id = {static_cast<uint64_t>(i + 1), 0};
        auto seg = MakeP2PSegment({static_cast<uint64_t>(i + 100), 0},
                                  "seg_" + std::to_string(i));
        auto req =
            MakeP2PRegisterRequest(client_id, "10.0.0." + std::to_string(i + 1),
                                   static_cast<uint16_t>(50051 + i), {seg});
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

TEST_F(P2PClientManagerTest, ConcurrentRegisterSameClient) {
    auto mgr = CreateManager();
    mgr->Start();

    constexpr int kNumThreads = 8;
    UUID client_id = {42, 0};
    auto seg = MakeP2PSegment({1, 0}, "seg");

    std::vector<std::thread> threads;
    std::atomic<int> success_count{0};

    for (int i = 0; i < kNumThreads; i++) {
        threads.emplace_back([this, &mgr, &client_id, &seg, &success_count]() {
            auto req =
                MakeP2PRegisterRequest(client_id, "10.0.0.1", 50051, {seg});
            auto res = mgr->RegisterClient(req);
            if (res.has_value()) {
                success_count.fetch_add(1);
            }
        });
    }

    for (auto& t : threads) {
        t.join();
    }

    EXPECT_GE(success_count.load(), 1);

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

TEST_F(P2PClientManagerTest, HeartbeatSyncSegmentMeta) {
    auto mgr = CreateManager();
    mgr->Start();

    UUID client_id = {100, 200};
    auto seg = MakeP2PSegment({1, 1}, "seg1", 3 * 1024 * 1024);
    auto req = MakeP2PRegisterRequest(client_id, "10.0.0.1", 50051, {seg});
    ASSERT_TRUE(mgr->RegisterClient(req).has_value());

    HeartbeatRequest hb_req;
    hb_req.client_id = client_id;
    SyncSegmentMetaParam sync_param;
    sync_param.tier_usages.push_back({UUID{1, 1}, 1 * 1024 * 1024});
    hb_req.tasks.push_back({HeartbeatTaskType::SYNC_SEGMENT_META, sync_param});

    auto res = mgr->Heartbeat(hb_req);
    ASSERT_TRUE(res.has_value());
    EXPECT_EQ(res.value().task_results.size(), 1);
    EXPECT_EQ(res.value().task_results[0].error, ErrorCode::OK);

    // Verify usage update in P2PClientMeta
    auto p2p_meta =
        std::static_pointer_cast<P2PClientMeta>(mgr->GetClient(client_id));
    ASSERT_NE(p2p_meta, nullptr);
    auto seg_res = p2p_meta->QuerySegment({1, 1});
    ASSERT_TRUE(seg_res.has_value());
    EXPECT_EQ(seg_res.value()->GetP2PExtra().usage, 1 * 1024 * 1024);
}

TEST_F(P2PClientManagerTest, HeartbeatSyncSegmentMetaErrorPaths) {
    auto mgr = CreateManager();
    mgr->Start();

    UUID client_id = {100, 200};
    auto seg = MakeP2PSegment({1, 1}, "seg1", 1024);
    ASSERT_TRUE(mgr->RegisterClient(MakeP2PRegisterRequest(
                                        client_id, "10.0.0.1", 50051, {seg}))
                    .has_value());

    // 1. Unregistered Client (Heartbeat interface level)
    {
        HeartbeatRequest req;
        req.client_id = {999, 999};
        req.tasks.push_back(
            {HeartbeatTaskType::SYNC_SEGMENT_META, SyncSegmentMetaParam{}});
        auto res = mgr->Heartbeat(req);
        // Heartbeat should return success but status UNDEFINED
        ASSERT_TRUE(res.has_value());
        EXPECT_EQ(res.value().status, ClientStatus::UNDEFINED);
    }

    // 2. Mix valid and invalid Segment IDs
    {
        HeartbeatRequest req;
        req.client_id = client_id;
        SyncSegmentMetaParam param;
        param.tier_usages.push_back({UUID{1, 1}, 100});    // Valid
        param.tier_usages.push_back({UUID{88, 88}, 500});  // Invalid
        req.tasks.push_back({HeartbeatTaskType::SYNC_SEGMENT_META, param});

        auto res = mgr->Heartbeat(req);
        ASSERT_TRUE(res.has_value());

        auto& task_res = res.value().task_results[0];
        // failed sub-task should not affect the overall error code
        EXPECT_EQ(task_res.error, ErrorCode::OK);

        ASSERT_TRUE(
            std::holds_alternative<SyncSegmentMetaResult>(task_res.detail));
        auto sync_detail = std::get<SyncSegmentMetaResult>(task_res.detail);
        ASSERT_EQ(sync_detail.sub_results.size(), 2);

        EXPECT_EQ(sync_detail.sub_results[0].segment_id, (UUID{1, 1}));
        EXPECT_EQ(sync_detail.sub_results[0].error, ErrorCode::OK);

        EXPECT_EQ(sync_detail.sub_results[1].segment_id, (UUID{88, 88}));
        EXPECT_EQ(sync_detail.sub_results[1].error,
                  ErrorCode::SEGMENT_NOT_FOUND);

        auto p2p_meta =
            std::static_pointer_cast<P2PClientMeta>(mgr->GetClient(client_id));
        EXPECT_EQ(p2p_meta->QuerySegment({1, 1}).value()->GetP2PExtra().usage,
                  100);
    }

    // 3. Not supported task type (Force casting)
    {
        HeartbeatTask task;
        task.type_ = static_cast<HeartbeatTaskType>(999);
        auto res = mgr->ProcessTask(client_id, task);
        EXPECT_EQ(res.error, ErrorCode::NOT_IMPLEMENTED);
    }
}

}  // namespace mooncake
