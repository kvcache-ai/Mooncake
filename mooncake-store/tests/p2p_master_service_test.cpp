#include "p2p_master_service.h"

#include <glog/logging.h>
#include <gtest/gtest.h>

#include <memory>
#include <string>
#include <vector>

#include "master_config.h"
#include "p2p_rpc_types.h"
#include "rpc_types.h"
#include "types.h"

namespace mooncake::test {

class P2PMasterServiceTest : public ::testing::Test {
   protected:
    void SetUp() override {
        google::InitGoogleLogging("P2PMasterServiceTest");
        FLAGS_logtostderr = true;
    }

    void TearDown() override { google::ShutdownGoogleLogging(); }

    static constexpr size_t kDefaultSegmentSize = 1024 * 1024 * 16;  // 16MB

    /// Create a P2P segment with tags and priority
    Segment MakeP2PSegment(std::string name = "p2p_segment",
                           size_t size = kDefaultSegmentSize,
                           std::vector<std::string> tags = {}, int priority = 0,
                           MemoryType memory_type = MemoryType::DRAM) {
        Segment segment;
        segment.id = generate_uuid();
        segment.name = std::move(name);
        segment.size = size;
        segment.extra = P2PSegmentExtraData{
            .priority = priority,
            .tags = std::move(tags),
            .memory_type = memory_type,
        };
        return segment;
    }

    /// Create the service with given max_replicas config
    std::unique_ptr<P2PMasterService> CreateService(
        uint64_t max_replicas_per_key = 0) {
        auto config = MasterServiceConfig::builder()
                          .set_max_replicas_per_key(max_replicas_per_key)
                          .build();
        return std::make_unique<P2PMasterService>(config);
    }

    /// Register a client with given segments, returns client_id
    UUID RegisterP2PClient(P2PMasterService& service, const UUID& client_id,
                           const std::vector<Segment>& segments,
                           const std::string& ip = "127.0.0.1",
                           uint16_t port = 50051) {
        RegisterClientRequest req;
        req.client_id = client_id;
        req.ip_address = ip;
        req.rpc_port = port;
        req.segments = segments;
        auto res = service.RegisterClient(req);
        EXPECT_TRUE(res.has_value())
            << "Failed to register client: " << res.error();
        return req.client_id;
    }

    /// Helper to add a replica via AddReplica
    void AddReplicaHelper(P2PMasterService& service, const std::string& key,
                          size_t size, const UUID& client_id,
                          const UUID& segment_id) {
        AddReplicaRequest req;
        req.key = key;
        req.size = size;
        req.replica.client_id = client_id;
        req.replica.segment_id = segment_id;
        auto res = service.AddReplica(req);
        EXPECT_TRUE(res.has_value())
            << "Failed to add replica: " << res.error();
    }
};

// ============================================================
// RegisterClient Tests
// ============================================================

TEST_F(P2PMasterServiceTest, RegisterClientBasic) {
    auto service = CreateService();
    auto seg = MakeP2PSegment("seg1", kDefaultSegmentSize, {"gpu"}, 5);
    auto client_id = generate_uuid();
    RegisterP2PClient(*service, client_id, {seg}, "127.0.0.1", 50051);

    // Verify client exists by querying segments
    auto seg_res = service->QuerySegments(seg.name);
    EXPECT_TRUE(seg_res.has_value());
}

TEST_F(P2PMasterServiceTest, RegisterClientDuplicate) {
    auto service = CreateService();
    auto seg = MakeP2PSegment();
    auto client_id = generate_uuid();
    RegisterP2PClient(*service, client_id, {seg}, "127.0.0.1", 50051);

    // Try registering the same client_id again
    RegisterClientRequest req;
    req.client_id = client_id;
    req.segments = {MakeP2PSegment("seg2")};
    auto res = service->RegisterClient(req);
    EXPECT_FALSE(res.has_value());
    EXPECT_EQ(ErrorCode::CLIENT_ALREADY_EXISTS, res.error());
}

// ============================================================
// GetWriteRoute Tests
// ============================================================

TEST_F(P2PMasterServiceTest, GetWriteRouteBasic) {
    auto service = CreateService();
    auto seg = MakeP2PSegment("seg1", kDefaultSegmentSize, {}, 1);
    auto client_id = generate_uuid();
    RegisterP2PClient(*service, client_id, {seg}, "127.0.0.1", 50051);

    WriteRouteRequest req;
    req.key = "test_key";
    req.client_id = generate_uuid();  // different client requesting
    req.size = 1024;
    req.config.max_candidates = 1;
    req.config.strategy = ObjectIterateStrategy::CAPACITY_PRIORITY;

    auto res = service->GetWriteRoute(req);
    ASSERT_TRUE(res.has_value()) << "GetWriteRoute failed: " << res.error();
    EXPECT_EQ(1, res.value().candidates.size());
    EXPECT_EQ(client_id, res.value().candidates[0].replica.client_id);
    EXPECT_EQ(seg.id, res.value().candidates[0].replica.segment_id);
}

TEST_F(P2PMasterServiceTest, GetWriteRouteNoCapacity) {
    auto service = CreateService();
    auto seg = MakeP2PSegment("seg1", 1024);  // only 1024 bytes
    auto client_id = generate_uuid();
    RegisterP2PClient(*service, client_id, {seg}, "127.0.0.1", 50051);

    WriteRouteRequest req;
    req.key = "test_key";
    req.client_id = generate_uuid();
    req.size = 2048;  // larger than segment capacity
    req.config.max_candidates = 1;

    auto res = service->GetWriteRoute(req);
    EXPECT_FALSE(res.has_value());
    EXPECT_EQ(ErrorCode::SEGMENT_NOT_FOUND, res.error());
}

TEST_F(P2PMasterServiceTest, GetWriteRouteTagFilter) {
    auto service = CreateService();
    auto seg_gpu =
        MakeP2PSegment("seg_gpu", kDefaultSegmentSize, {"gpu", "fast"}, 1);
    auto seg_cpu = MakeP2PSegment("seg_cpu", kDefaultSegmentSize, {"cpu"}, 1);

    auto client1 = generate_uuid();
    auto client2 = generate_uuid();
    RegisterP2PClient(*service, client1, {seg_gpu}, "10.0.0.1", 50051);
    RegisterP2PClient(*service, client2, {seg_cpu}, "10.0.0.2", 50052);

    // Request with tag filter "gpu" — only seg_gpu matches
    WriteRouteRequest req;
    req.key = "test_key";
    req.client_id = generate_uuid();
    req.size = 1024;
    req.config.max_candidates = 10;
    req.config.tag_filters = {"gpu"};

    auto res = service->GetWriteRoute(req);
    ASSERT_TRUE(res.has_value());
    EXPECT_EQ(1, res.value().candidates.size());
    EXPECT_EQ(client1, res.value().candidates[0].replica.client_id);
}

TEST_F(P2PMasterServiceTest, GetWriteRoutePriorityFilter) {
    auto service = CreateService();
    auto seg_low = MakeP2PSegment("seg_low", kDefaultSegmentSize, {}, 1);
    auto seg_high = MakeP2PSegment("seg_high", kDefaultSegmentSize, {}, 10);

    auto client1 = generate_uuid();
    auto client2 = generate_uuid();
    RegisterP2PClient(*service, client1, {seg_low}, "10.0.0.1", 50051);
    RegisterP2PClient(*service, client2, {seg_high}, "10.0.0.2", 50052);

    // Request with priority_limit = 5 — only seg_high qualifies
    WriteRouteRequest req;
    req.key = "test_key";
    req.client_id = generate_uuid();
    req.size = 1024;
    req.config.max_candidates = 10;
    req.config.priority_limit = 5;

    auto res = service->GetWriteRoute(req);
    ASSERT_TRUE(res.has_value());
    EXPECT_EQ(1, res.value().candidates.size());
    EXPECT_EQ(client2, res.value().candidates[0].replica.client_id);
}

TEST_F(P2PMasterServiceTest, GetWriteRouteAllowLocal) {
    auto service = CreateService();
    auto seg = MakeP2PSegment("seg1", kDefaultSegmentSize, {}, 1);
    auto client_id = generate_uuid();
    RegisterP2PClient(*service, client_id, {seg}, "127.0.0.1", 50051);

    // Same client requesting, allow_local = false — should skip self
    WriteRouteRequest req;
    req.key = "test_key";
    req.client_id = client_id;
    req.size = 1024;
    req.config.max_candidates = 1;
    req.config.allow_local = false;

    auto res = service->GetWriteRoute(req);
    EXPECT_FALSE(res.has_value());  // only candidate is self, which is skipped

    // allow_local = true — should include self
    req.config.allow_local = true;
    auto res2 = service->GetWriteRoute(req);
    ASSERT_TRUE(res2.has_value());
    EXPECT_EQ(1, res2.value().candidates.size());
    EXPECT_EQ(client_id, res2.value().candidates[0].replica.client_id);
}

TEST_F(P2PMasterServiceTest, GetWriteRouteEarlyReturn) {
    auto service = CreateService();

    // Register 3 clients with segments
    for (int i = 0; i < 3; ++i) {
        auto seg = MakeP2PSegment("seg_" + std::to_string(i),
                                  kDefaultSegmentSize, {}, 1);
        auto client_id = generate_uuid();
        RegisterP2PClient(*service, client_id, {seg},
                          "10.0.0." + std::to_string(i + 1), 50051 + i);
    }

    WriteRouteRequest req;
    req.key = "test_key";
    req.client_id = generate_uuid();
    req.size = 1024;
    req.config.max_candidates = 2;
    req.config.early_return = true;

    auto res = service->GetWriteRoute(req);
    ASSERT_TRUE(res.has_value());
    // With early_return, should stop at max_candidates
    EXPECT_EQ(2, res.value().candidates.size());
}

TEST_F(P2PMasterServiceTest, GetWriteRouteMultipleSegments) {
    auto service = CreateService();
    auto seg1 = MakeP2PSegment("seg1", kDefaultSegmentSize, {"gpu"}, 5);
    auto seg2 = MakeP2PSegment("seg2", kDefaultSegmentSize, {"cpu"}, 3);
    auto seg3 = MakeP2PSegment("seg3", kDefaultSegmentSize, {"gpu"}, 5);
    auto seg4 = MakeP2PSegment("seg4", kDefaultSegmentSize, {"cpu"}, 3);
    auto client_id = generate_uuid();
    auto client_id2 = generate_uuid();
    auto client_id3 = generate_uuid();
    RegisterP2PClient(*service, client_id, {seg1}, "127.0.0.1", 50051);
    RegisterP2PClient(*service, client_id2, {seg2}, "127.0.0.2", 50051);
    RegisterP2PClient(*service, client_id3, {seg3, seg4}, "127.0.0.3", 50051);

    // No filters — should return both segments as candidates
    WriteRouteRequest req;
    req.key = "test_key";
    req.client_id = client_id;
    req.size = 1024;
    req.config.max_candidates = WriteRouteRequestConfig::RETURN_ALL_CANDIDATES;
    req.config.allow_local = false;
    req.config.early_return = false;

    auto res = service->GetWriteRoute(req);
    ASSERT_TRUE(res.has_value());
    EXPECT_EQ(3, res.value().candidates.size());
}

// ============================================================
// AddReplica Tests
// ============================================================

TEST_F(P2PMasterServiceTest, AddReplicaBasic) {
    auto service = CreateService();
    auto seg = MakeP2PSegment("seg1", kDefaultSegmentSize, {}, 1);
    auto client_id = generate_uuid();
    RegisterP2PClient(*service, client_id, {seg}, "127.0.0.1", 50051);

    // Add a replica
    AddReplicaRequest req;
    req.key = "key1";
    req.size = 1024;
    req.replica.client_id = client_id;
    req.replica.segment_id = seg.id;
    auto res = service->AddReplica(req);
    ASSERT_TRUE(res.has_value());

    // Verify it shows up in GetReplicaList
    auto get_res = service->GetReplicaList(req.key);
    ASSERT_TRUE(get_res.has_value());
    EXPECT_EQ(1, get_res.value().replicas.size());

    auto& desc = get_res.value().replicas[0];
    EXPECT_TRUE(desc.is_p2p_proxy_replica());
    EXPECT_EQ(client_id, desc.get_p2p_proxy_descriptor().client_id);
    EXPECT_EQ(seg.id, desc.get_p2p_proxy_descriptor().segment_id);
}

TEST_F(P2PMasterServiceTest, AddReplicaDuplicate) {
    auto service = CreateService();
    auto seg = MakeP2PSegment();
    auto client_id = generate_uuid();
    RegisterP2PClient(*service, client_id, {seg}, "127.0.0.1", 50051);

    AddReplicaRequest req;
    req.key = "key1";
    req.size = 1024;
    req.replica.client_id = client_id;
    req.replica.segment_id = seg.id;

    // First add
    auto res1 = service->AddReplica(req);
    ASSERT_TRUE(res1.has_value());

    // Duplicate add
    auto res2 = service->AddReplica(req);
    EXPECT_FALSE(res2.has_value());
    EXPECT_EQ(ErrorCode::REPLICA_ALREADY_EXISTS, res2.error());
}

TEST_F(P2PMasterServiceTest, AddReplicaMaxLimit) {
    auto service = CreateService(/* max_replicas_per_key= */ 2);
    auto seg1 = MakeP2PSegment("seg1");
    auto seg2 = MakeP2PSegment("seg2");
    auto seg3 = MakeP2PSegment("seg3");
    auto client1 = generate_uuid();
    auto client2 = generate_uuid();
    auto client3 = generate_uuid();
    RegisterP2PClient(*service, client1, {seg1}, "10.0.0.1", 50051);
    RegisterP2PClient(*service, client2, {seg2}, "10.0.0.2", 50052);
    RegisterP2PClient(*service, client3, {seg3}, "10.0.0.3", 50053);

    // Add first two replicas — should succeed
    AddReplicaHelper(*service, "key1", 1024, client1, seg1.id);
    AddReplicaHelper(*service, "key1", 1024, client2, seg2.id);

    // Third replica — should exceed limit
    AddReplicaRequest req;
    req.key = "key1";
    req.size = 1024;
    req.replica.client_id = client3;
    req.replica.segment_id = seg3.id;
    auto res = service->AddReplica(req);
    EXPECT_FALSE(res.has_value());
    EXPECT_EQ(ErrorCode::REPLICA_NUM_EXCEEDED, res.error());
}

TEST_F(P2PMasterServiceTest, AddReplicaClientNotFound) {
    auto service = CreateService();
    auto seg = MakeP2PSegment();

    AddReplicaRequest req;
    req.key = "key1";
    req.size = 1024;
    req.replica.client_id = generate_uuid();  // non-existent
    req.replica.segment_id = seg.id;
    auto res = service->AddReplica(req);
    EXPECT_FALSE(res.has_value());
    EXPECT_EQ(ErrorCode::CLIENT_NOT_FOUND, res.error());
}

TEST_F(P2PMasterServiceTest, AddReplicaSegmentNotFound) {
    auto service = CreateService();
    auto seg = MakeP2PSegment();
    auto client_id = generate_uuid();
    RegisterP2PClient(*service, client_id, {seg}, "127.0.0.1", 50051);

    AddReplicaRequest req;
    req.key = "key1";
    req.size = 1024;
    req.replica.client_id = client_id;
    req.replica.segment_id = generate_uuid();  // non-existent segment
    auto res = service->AddReplica(req);
    EXPECT_FALSE(res.has_value());
    EXPECT_EQ(ErrorCode::SEGMENT_NOT_FOUND, res.error());
}

// ============================================================
// RemoveReplica Tests
// ============================================================

TEST_F(P2PMasterServiceTest, RemoveReplicaBasic) {
    auto service = CreateService();
    auto seg = MakeP2PSegment();
    auto client_id = generate_uuid();
    RegisterP2PClient(*service, client_id, {seg}, "127.0.0.1", 50051);

    AddReplicaHelper(*service, "key1", 1024, client_id, seg.id);

    // Remove the replica
    RemoveReplicaRequest req;
    req.key = "key1";
    req.replica.client_id = client_id;
    req.replica.segment_id = seg.id;
    auto res = service->RemoveReplica(req);
    ASSERT_TRUE(res.has_value());

    // Verify key is gone (last replica removed → object removed)
    auto get_res = service->GetReplicaList("key1");
    EXPECT_FALSE(get_res.has_value());
    EXPECT_EQ(ErrorCode::OBJECT_NOT_FOUND, get_res.error());
}

TEST_F(P2PMasterServiceTest, RemoveReplicaPartial) {
    auto service = CreateService();
    auto seg1 = MakeP2PSegment("seg1");
    auto seg2 = MakeP2PSegment("seg2");
    auto client1 = generate_uuid();
    auto client2 = generate_uuid();
    RegisterP2PClient(*service, client1, {seg1}, "10.0.0.1", 50051);
    RegisterP2PClient(*service, client2, {seg2}, "10.0.0.2", 50052);

    AddReplicaHelper(*service, "key1", 1024, client1, seg1.id);
    AddReplicaHelper(*service, "key1", 1024, client2, seg2.id);

    // Remove one replica
    RemoveReplicaRequest req;
    req.key = "key1";
    req.replica.client_id = client1;
    req.replica.segment_id = seg1.id;
    auto res = service->RemoveReplica(req);
    ASSERT_TRUE(res.has_value());

    // Object still exists with one replica
    auto get_res = service->GetReplicaList("key1");
    ASSERT_TRUE(get_res.has_value());
    EXPECT_EQ(1, get_res.value().replicas.size());
}

TEST_F(P2PMasterServiceTest, RemoveReplicaNotFound) {
    auto service = CreateService();
    auto seg = MakeP2PSegment();
    auto client_id = generate_uuid();
    RegisterP2PClient(*service, client_id, {seg}, "127.0.0.1", 50051);

    AddReplicaHelper(*service, "key1", 1024, client_id, seg.id);

    // Try removing non-existent replica
    RemoveReplicaRequest req;
    req.key = "key1";
    req.replica.client_id = client_id;
    req.replica.segment_id = generate_uuid();  // wrong segment
    auto res = service->RemoveReplica(req);
    EXPECT_FALSE(res.has_value());
    EXPECT_EQ(ErrorCode::REPLICA_NOT_FOUND, res.error());
}

TEST_F(P2PMasterServiceTest, RemoveReplicaObjectNotFound) {
    auto service = CreateService();

    RemoveReplicaRequest req;
    req.key = "non_existent_key";
    req.replica.client_id = generate_uuid();
    req.replica.segment_id = generate_uuid();
    auto res = service->RemoveReplica(req);
    EXPECT_FALSE(res.has_value());
    EXPECT_EQ(ErrorCode::OBJECT_NOT_FOUND, res.error());
}

// ============================================================
// GetReplicaList + FilterReplicas Tests
// ============================================================

TEST_F(P2PMasterServiceTest, GetReplicaListBasic) {
    auto service = CreateService();
    auto seg = MakeP2PSegment("seg", kDefaultSegmentSize, {"fast"}, 5,
                              MemoryType::DRAM);
    auto client_id = generate_uuid();
    RegisterP2PClient(*service, client_id, {seg}, "127.0.0.1", 50051);
    AddReplicaHelper(*service, "key1", 1024, client_id, seg.id);

    auto res = service->GetReplicaList("key1");
    ASSERT_TRUE(res.has_value());
    EXPECT_EQ(1, res.value().replicas.size());
}

TEST_F(P2PMasterServiceTest, GetReplicaListNotFound) {
    auto service = CreateService();
    auto res = service->GetReplicaList("non_existent");
    EXPECT_FALSE(res.has_value());
    EXPECT_EQ(ErrorCode::OBJECT_NOT_FOUND, res.error());
}

TEST_F(P2PMasterServiceTest, FilterReplicasWithTagAndPriority) {
    auto service = CreateService();
    auto seg_a =
        MakeP2PSegment("seg_a", kDefaultSegmentSize, {"gpu", "fast"}, 10);
    auto seg_b = MakeP2PSegment("seg_b", kDefaultSegmentSize, {"cpu"}, 2);
    auto client1 = generate_uuid();
    auto client2 = generate_uuid();
    RegisterP2PClient(*service, client1, {seg_a}, "10.0.0.1", 50051);
    RegisterP2PClient(*service, client2, {seg_b}, "10.0.0.2", 50052);

    AddReplicaHelper(*service, "key1", 1024, client1, seg_a.id);
    AddReplicaHelper(*service, "key1", 1024, client2, seg_b.id);

    // Filter out replicas with tag "gpu" — seg_a is excluded, only seg_b
    // remains
    GetReplicaListRequestConfig config;
    config.max_candidates = 10;
    config.p2p_config = P2PGetReplicaListConfigExtra{
        .tag_filters = {"gpu"},
        .priority_limit = 0,
    };

    auto res = service->GetReplicaList("key1", config);
    ASSERT_TRUE(res.has_value());
    EXPECT_EQ(1, res.value().replicas.size());
    EXPECT_EQ(client2,
              res.value().replicas[0].get_p2p_proxy_descriptor().client_id);
}

TEST_F(P2PMasterServiceTest, FilterReplicasWithMaxCandidates) {
    auto service = CreateService();

    // Create 5 replicas across different clients
    std::vector<UUID> client_ids;
    for (int i = 0; i < 5; ++i) {
        auto seg = MakeP2PSegment("seg_" + std::to_string(i),
                                  kDefaultSegmentSize, {}, i + 1);
        auto cid = generate_uuid();
        RegisterP2PClient(*service, cid, {seg},
                          "10.0.0." + std::to_string(i + 1), 50051 + i);
        AddReplicaHelper(*service, "key1", 1024, cid, seg.id);
        client_ids.push_back(cid);
    }

    // Limit to 3 candidates — should return top 3 by priority
    GetReplicaListRequestConfig config;
    config.max_candidates = 3;
    config.p2p_config = P2PGetReplicaListConfigExtra{
        .tag_filters = {},
        .priority_limit = 0,
    };

    auto res = service->GetReplicaList("key1", config);
    ASSERT_TRUE(res.has_value());
    EXPECT_EQ(3, res.value().replicas.size());

    // The top 3 should have priorities 5, 4, 3 (descending)
    // Verify the first one has the highest priority
    EXPECT_EQ(client_ids[4],
              res.value().replicas[0].get_p2p_proxy_descriptor().client_id);
}

// ============================================================
// ExistKey / Remove / RemoveAll Tests
// ============================================================

TEST_F(P2PMasterServiceTest, ExistKeyAndRemove) {
    auto service = CreateService();
    auto seg = MakeP2PSegment();
    auto client_id = generate_uuid();
    RegisterP2PClient(*service, client_id, {seg}, "127.0.0.1", 50051);
    AddReplicaHelper(*service, "key1", 1024, client_id, seg.id);

    auto exist = service->ExistKey("key1");
    ASSERT_TRUE(exist.has_value());
    EXPECT_TRUE(exist.value());

    auto not_exist = service->ExistKey("non_existent");
    ASSERT_TRUE(not_exist.has_value());
    EXPECT_FALSE(not_exist.value());

    // Remove
    auto rm = service->Remove("key1");
    ASSERT_TRUE(rm.has_value());

    auto exist2 = service->ExistKey("key1");
    ASSERT_TRUE(exist2.has_value());
    EXPECT_FALSE(exist2.value());
}

TEST_F(P2PMasterServiceTest, RemoveAll) {
    auto service = CreateService();
    auto seg = MakeP2PSegment();
    auto client_id = generate_uuid();
    RegisterP2PClient(*service, client_id, {seg}, "127.0.0.1", 50051);

    for (int i = 0; i < 10; ++i) {
        AddReplicaHelper(*service, "key_" + std::to_string(i), 1024, client_id,
                         seg.id);
    }

    EXPECT_EQ(10, service->GetKeyCount());

    long removed = service->RemoveAll();
    EXPECT_EQ(10, removed);
    EXPECT_EQ(0, service->GetKeyCount());
}

// ============================================================
// MountSegment / UnmountSegment Tests
// ============================================================

TEST_F(P2PMasterServiceTest, MountUnmountSegment) {
    auto service = CreateService();
    auto seg = MakeP2PSegment();
    auto client_id = generate_uuid();
    RegisterP2PClient(*service, client_id, {}, "127.0.0.1", 50051);

    // Mount
    auto mount_res = service->MountSegment(seg, client_id);
    ASSERT_TRUE(mount_res.has_value());

    // Mount again — should succeed (idempotent)
    auto mount_res2 = service->MountSegment(seg, client_id);
    ASSERT_TRUE(mount_res2.has_value());

    // Unmount
    auto unmount_res = service->UnmountSegment(seg.id, client_id);
    ASSERT_TRUE(unmount_res.has_value());

    // Unmount again — should succeed (idempotent)
    auto unmount_res2 = service->UnmountSegment(seg.id, client_id);
    ASSERT_TRUE(unmount_res2.has_value());
}

// ============================================================
// Integration: Write Route → Add → Read → Remove cycle
// ============================================================

TEST_F(P2PMasterServiceTest, FullWriteReadCycle) {
    auto service = CreateService();
    auto seg = MakeP2PSegment("seg1", kDefaultSegmentSize, {"gpu"}, 5,
                              MemoryType::DRAM);
    auto writer_id = generate_uuid();
    auto reader_id = generate_uuid();
    RegisterP2PClient(*service, writer_id, {seg}, "10.0.0.1", 50051);
    RegisterP2PClient(*service, reader_id, {}, "10.0.0.2", 50052);

    // Step 1: Get write route
    WriteRouteRequest w_req;
    w_req.key = "data_001";
    w_req.client_id = reader_id;  // reader asks for write route
    w_req.size = 4096;
    w_req.config.max_candidates = 1;

    auto w_res = service->GetWriteRoute(w_req);
    ASSERT_TRUE(w_res.has_value());
    EXPECT_EQ(1, w_res.value().candidates.size());

    auto& candidate = w_res.value().candidates[0];
    EXPECT_EQ(writer_id, candidate.replica.client_id);

    // Step 2: Add replica (simulate write completion)
    AddReplicaRequest a_req;
    a_req.key = "data_001";
    a_req.size = 4096;
    a_req.replica = candidate.replica;
    auto a_res = service->AddReplica(a_req);
    ASSERT_TRUE(a_res.has_value());

    // Step 3: Read — GetReplicaList
    auto r_res = service->GetReplicaList("data_001");
    ASSERT_TRUE(r_res.has_value());
    EXPECT_EQ(1, r_res.value().replicas.size());

    // Step 4: Remove
    RemoveReplicaRequest rm_req;
    rm_req.key = "data_001";
    rm_req.replica = candidate.replica;
    auto rm_res = service->RemoveReplica(rm_req);
    ASSERT_TRUE(rm_res.has_value());

    // Verify gone
    auto r_res2 = service->GetReplicaList("data_001");
    EXPECT_FALSE(r_res2.has_value());
    EXPECT_EQ(ErrorCode::OBJECT_NOT_FOUND, r_res2.error());
}

}  // namespace mooncake::test
