#include <glog/logging.h>
#include <gtest/gtest.h>

#include <memory>
#include <string>
#include <vector>

#define private public
#define protected public
#include "p2p_master_service.h"
#undef protected
#undef private

#include "master_config.h"
#include "p2p_client_meta.h"
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
        uint64_t max_client_per_key = 0) {
        auto config = MasterServiceConfig::builder()
                          .set_max_client_per_key(max_client_per_key)
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
        req.deployment_mode = DeploymentMode::P2P;
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
        req.client_id = client_id;
        req.segment_id = segment_id;
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
    auto seg = MakeP2PSegment("seg1", kDefaultSegmentSize, {"memory"}, 5);
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
    req.deployment_mode = DeploymentMode::P2P;
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
    EXPECT_EQ(client_id, res.value().candidates[0].client_id);
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
    EXPECT_EQ(ErrorCode::NO_AVAILABLE_CANDIDATE, res.error());
}

TEST_F(P2PMasterServiceTest, GetWriteRouteTagFilter) {
    auto service = CreateService();
    auto seg_memory = MakeP2PSegment("seg_memory", kDefaultSegmentSize,
                                     {"memory", "fast"}, 1);
    auto seg_disk =
        MakeP2PSegment("seg_disk", kDefaultSegmentSize, {"disk"}, 1);

    auto client1 = generate_uuid();
    auto client2 = generate_uuid();
    RegisterP2PClient(*service, client1, {seg_memory}, "10.0.0.1", 50051);
    RegisterP2PClient(*service, client2, {seg_disk}, "10.0.0.2", 50052);

    // Request with tag filter "disk" — only seg_memory remains
    WriteRouteRequest req;
    req.key = "test_key";
    req.client_id = generate_uuid();
    req.size = 1024;
    req.config.max_candidates = 10;
    req.config.tag_filters = {"disk"};

    auto res = service->GetWriteRoute(req);
    ASSERT_TRUE(res.has_value());
    EXPECT_EQ(1, res.value().candidates.size());
    EXPECT_EQ(client1, res.value().candidates[0].client_id);
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
    EXPECT_EQ(client2, res.value().candidates[0].client_id);
}

TEST_F(P2PMasterServiceTest, GetWriteRouteForceRemoteExcludesLocal) {
    auto service = CreateService();
    auto seg = MakeP2PSegment("seg1", kDefaultSegmentSize, {}, 1);
    auto client_id = generate_uuid();
    RegisterP2PClient(*service, client_id, {seg}, "127.0.0.1", 50051);

    // Same client requesting, remote_weight = 1 (force remote) — should skip
    // self
    WriteRouteRequest req;
    req.key = "test_key";
    req.client_id = client_id;
    req.size = 1024;
    req.config.max_candidates = 1;
    req.config.remote_weight = 1.0;

    auto res = service->GetWriteRoute(req);
    EXPECT_FALSE(res.has_value());  // only candidate is self, which is skipped

    // remote_weight < 1 — should include self
    req.config.remote_weight = 0.5;
    auto res2 = service->GetWriteRoute(req);
    ASSERT_TRUE(res2.has_value());
    EXPECT_EQ(1, res2.value().candidates.size());
    EXPECT_EQ(client_id, res2.value().candidates[0].client_id);
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
    auto seg1 = MakeP2PSegment("seg1", kDefaultSegmentSize, {"memory"}, 5);
    auto seg2 = MakeP2PSegment("seg2", kDefaultSegmentSize, {"disk"}, 3);
    auto seg3 = MakeP2PSegment("seg3", kDefaultSegmentSize, {"memory"}, 5);
    auto seg4 = MakeP2PSegment("seg4", kDefaultSegmentSize, {"disk"}, 3);
    auto client_id = generate_uuid();
    auto client_id2 = generate_uuid();
    auto client_id3 = generate_uuid();
    RegisterP2PClient(*service, client_id, {seg1}, "127.0.0.1", 50051);
    RegisterP2PClient(*service, client_id2, {seg2}, "127.0.0.2", 50051);
    RegisterP2PClient(*service, client_id3, {seg3, seg4}, "127.0.0.3", 50051);

    WriteRouteRequest req;
    req.key = "test_key";
    req.client_id = client_id;
    req.size = 1024;
    req.config.max_candidates = WriteRouteRequestConfig::RETURN_ALL_CANDIDATES;
    req.config.remote_weight =
        1.0;  // force remote: exclude the requesting client
    req.config.early_return = false;

    auto res = service->GetWriteRoute(req);
    ASSERT_TRUE(res.has_value());
    EXPECT_EQ(2, res.value().candidates.size());
}

TEST_F(P2PMasterServiceTest, GetWriteRouteRejectsWhenOwnerClientLimitReached) {
    auto service = CreateService(/* max_client_per_key= */ 2);
    auto owner_seg1 = MakeP2PSegment("owner_seg1", kDefaultSegmentSize, {}, 1);
    auto owner_seg2 = MakeP2PSegment("owner_seg2", kDefaultSegmentSize, {}, 2);
    auto new_owner_seg = MakeP2PSegment("new_owner_seg", kDefaultSegmentSize);
    auto owner1 = generate_uuid();
    auto owner2 = generate_uuid();
    auto new_owner = generate_uuid();
    RegisterP2PClient(*service, owner1, {owner_seg1}, "10.0.0.1", 50051);
    RegisterP2PClient(*service, owner2, {owner_seg2}, "10.0.0.2", 50052);
    RegisterP2PClient(*service, new_owner, {new_owner_seg}, "10.0.0.3", 50053);

    AddReplicaHelper(*service, "key1", 1024, owner1, owner_seg1.id);
    AddReplicaHelper(*service, "key1", 1024, owner2, owner_seg2.id);

    WriteRouteRequest req;
    req.key = "key1";
    req.client_id = generate_uuid();
    req.size = 1024;
    req.config.max_candidates = 1;
    req.config.early_return = false;

    auto res = service->GetWriteRoute(req);
    EXPECT_FALSE(res.has_value());
    EXPECT_EQ(ErrorCode::REPLICA_NUM_EXCEEDED, res.error());
}

// A nearly-full local client is visited last under CAPACITY_PRIORITY, but a
// strong local preference (remote_weight close to 0) still lets it win after
// sorting. early_return is disabled so all candidates are collected.
TEST_F(P2PMasterServiceTest, GetWriteRouteLocalFirstBeatsCapacityOrdering) {
    auto service = CreateService();
    auto local_seg = MakeP2PSegment("local", 1000, {}, 1);
    local_seg.GetP2PExtra().usage = 900;  // free 100 -> free_ratio 0.1
    auto local_id = generate_uuid();
    RegisterP2PClient(*service, local_id, {local_seg}, "10.0.0.1", 50051);

    // Several near-empty, high-capacity remotes (visited first).
    for (int i = 0; i < 3; ++i) {
        auto seg = MakeP2PSegment("remote_" + std::to_string(i), 100000, {}, 1);
        RegisterP2PClient(*service, generate_uuid(), {seg},
                          "10.0.0." + std::to_string(i + 2), 50052 + i);
    }

    WriteRouteRequest req;
    req.key = "k";
    req.client_id = local_id;  // the requesting client is the local one
    req.size = 50;
    req.config.max_candidates = 1;
    req.config.early_return = false;  // collect all, then sort
    req.config.remote_weight = 0.02;  // strong local preference

    auto res = service->GetWriteRoute(req);
    ASSERT_TRUE(res.has_value()) << res.error();
    ASSERT_EQ(1u, res.value().candidates.size());
    EXPECT_EQ(local_id, res.value().candidates[0].client_id);
}

// With a weak local preference, a much-emptier remote out-scores the
// nearly-full local client after sorting.
TEST_F(P2PMasterServiceTest, GetWriteRouteWeightedRemoteCanWin) {
    auto service = CreateService();
    auto local_seg = MakeP2PSegment("local", 1000, {}, 1);
    local_seg.GetP2PExtra().usage = 900;  // free_ratio 0.1
    auto local_id = generate_uuid();
    RegisterP2PClient(*service, local_id, {local_seg}, "10.0.0.1", 50051);

    auto remote_seg =
        MakeP2PSegment("remote", 100000, {}, 1);  // free_ratio 1.0
    auto remote_id = generate_uuid();
    RegisterP2PClient(*service, remote_id, {remote_seg}, "10.0.0.2", 50052);

    WriteRouteRequest req;
    req.key = "k";
    req.client_id = local_id;
    req.size = 50;
    req.config.max_candidates = 1;
    req.config.early_return = false;  // collect all, then sort
    req.config.remote_weight = 0.4;   // weak local preference

    auto res = service->GetWriteRoute(req);
    ASSERT_TRUE(res.has_value()) << res.error();
    ASSERT_EQ(1u, res.value().candidates.size());
    EXPECT_EQ(remote_id, res.value().candidates[0].client_id);
}

// With early_return=true and max_candidates=1, ForEachClient stops after the
// first eligible candidate. Under CAPACITY_PRIORITY the high-capacity remote
// is visited first, so it is returned without visiting the local client.
TEST_F(P2PMasterServiceTest, GetWriteRouteEarlyReturnStopsAtFirstCandidate) {
    auto service = CreateService();
    auto local_seg = MakeP2PSegment("local", 1000, {}, 1);
    local_seg.GetP2PExtra().usage = 900;  // free_ratio 0.1
    auto local_id = generate_uuid();
    RegisterP2PClient(*service, local_id, {local_seg}, "10.0.0.1", 50051);

    auto remote_seg =
        MakeP2PSegment("remote", 100000, {}, 1);  // free_ratio 1.0
    auto remote_id = generate_uuid();
    RegisterP2PClient(*service, remote_id, {remote_seg}, "10.0.0.2", 50052);

    WriteRouteRequest req;
    req.key = "k";
    req.client_id = local_id;
    req.size = 50;
    req.config.max_candidates = 1;
    req.config.early_return = true;
    req.config.remote_weight = 0.4;

    auto res = service->GetWriteRoute(req);
    ASSERT_TRUE(res.has_value()) << res.error();
    ASSERT_EQ(1u, res.value().candidates.size());
    // CAPACITY_PRIORITY visits the 100 000-capacity remote first; early stop.
    EXPECT_EQ(remote_id, res.value().candidates[0].client_id);
}

// Problem 3: top_tier_only changes which client wins by scoring only the
// highest-priority tier's free ratio instead of summing all tiers.
TEST_F(P2PMasterServiceTest, GetWriteRouteTopTierCapacityAffectsScore) {
    auto service = CreateService();

    // Client A: small high-prio DRAM mostly free; large low-prio NVMe mostly
    // full.
    auto a_dram = MakeP2PSegment("a_dram", 1000, {}, 10);
    a_dram.GetP2PExtra().usage = 100;  // free 900 -> top-tier ratio 0.9
    auto a_nvme = MakeP2PSegment("a_nvme", 100000, {}, 0);
    a_nvme.GetP2PExtra().usage = 99000;  // free 1000
    auto a_id = generate_uuid();
    RegisterP2PClient(*service, a_id, {a_dram, a_nvme}, "10.0.0.1", 50051);

    // Client B: high-prio DRAM half free; large low-prio NVMe fully free.
    auto b_dram = MakeP2PSegment("b_dram", 1000, {}, 10);
    b_dram.GetP2PExtra().usage = 500;  // free 500 -> top-tier ratio 0.5
    auto b_nvme = MakeP2PSegment("b_nvme", 100000, {}, 0);
    b_nvme.GetP2PExtra().usage = 0;  // free 100000
    auto b_id = generate_uuid();
    RegisterP2PClient(*service, b_id, {b_dram, b_nvme}, "10.0.0.2", 50052);

    WriteRouteRequest req;
    req.key = "k";
    req.client_id = generate_uuid();  // non-local requester
    req.size = 50;
    req.config.max_candidates = WriteRouteRequestConfig::RETURN_ALL_CANDIDATES;
    req.config.early_return = false;
    req.config.remote_weight = 0.5;

    // All tiers: B is much emptier overall -> B wins.
    req.config.top_tier_only = false;
    auto res_all = service->GetWriteRoute(req);
    ASSERT_TRUE(res_all.has_value()) << res_all.error();
    EXPECT_EQ(b_id, res_all.value().candidates[0].client_id);

    // Top tier only: A's DRAM tier is emptier than B's -> A wins.
    req.config.top_tier_only = true;
    auto res_top = service->GetWriteRoute(req);
    ASSERT_TRUE(res_top.has_value()) << res_top.error();
    EXPECT_EQ(a_id, res_top.value().candidates[0].client_id);
}

// A client that already owns the key is excluded from write-route candidates so
// a write is never routed to create a duplicate replica on it.
TEST_F(P2PMasterServiceTest, GetWriteRouteExcludesExistingOwner) {
    auto service = CreateService();  // unlimited owners
    auto owner_seg = MakeP2PSegment("owner_seg", kDefaultSegmentSize, {}, 1);
    auto other_seg = MakeP2PSegment("other_seg", kDefaultSegmentSize, {}, 1);
    auto owner = generate_uuid();
    auto other = generate_uuid();
    RegisterP2PClient(*service, owner, {owner_seg}, "10.0.0.1", 50051);
    RegisterP2PClient(*service, other, {other_seg}, "10.0.0.2", 50052);

    // owner already holds "k".
    AddReplicaHelper(*service, "k", 1024, owner, owner_seg.id);

    WriteRouteRequest req;
    req.key = "k";
    req.client_id = generate_uuid();  // non-local, non-owner requester
    req.size = 1024;
    req.config.max_candidates = WriteRouteRequestConfig::RETURN_ALL_CANDIDATES;
    req.config.early_return = false;

    auto res = service->GetWriteRoute(req);
    ASSERT_TRUE(res.has_value()) << res.error();
    ASSERT_EQ(1u, res.value().candidates.size());
    EXPECT_EQ(other, res.value().candidates[0].client_id);
    for (const auto& c : res.value().candidates) {
        EXPECT_NE(owner, c.client_id);
    }
}

// Existing owners (including the requesting client itself) are excluded from
// write-route candidates. Self-overwrite is handled client-side via the local
// write path (remote_weight=0 bypasses the master entirely).
TEST_F(P2PMasterServiceTest, GetWriteRouteExcludesSelfOwner) {
    auto service = CreateService();
    auto seg = MakeP2PSegment("self_seg", kDefaultSegmentSize, {}, 1);
    auto self = generate_uuid();
    RegisterP2PClient(*service, self, {seg}, "10.0.0.1", 50051);

    // self already holds "k".
    AddReplicaHelper(*service, "k", 1024, self, seg.id);

    WriteRouteRequest req;
    req.key = "k";
    req.client_id = self;  // requester is the existing owner
    req.size = 1024;
    req.config.max_candidates = 1;

    // The only registered client is the existing owner -> no candidate.
    auto res = service->GetWriteRoute(req);
    EXPECT_FALSE(res.has_value());
    EXPECT_EQ(ErrorCode::NO_AVAILABLE_CANDIDATE, res.error());
}

// When the client limit is reached, GetWriteRoute rejects all requesters
// (including existing owners) — the key already has enough owner clients.
TEST_F(P2PMasterServiceTest, GetWriteRouteRejectsAllWhenOwnerLimitReached) {
    auto service = CreateService(/* max_client_per_key= */ 1);
    auto seg_a = MakeP2PSegment("seg_a", kDefaultSegmentSize, {}, 1);
    auto seg_b = MakeP2PSegment("seg_b", kDefaultSegmentSize, {}, 1);
    auto owner = generate_uuid();
    auto other = generate_uuid();
    RegisterP2PClient(*service, owner, {seg_a}, "10.0.0.1", 50051);
    RegisterP2PClient(*service, other, {seg_b}, "10.0.0.2", 50052);

    // owner already holds "k" -> 1 owner == limit.
    AddReplicaHelper(*service, "k", 1024, owner, seg_a.id);

    WriteRouteRequest req;
    req.key = "k";
    req.size = 1024;
    req.config.max_candidates = 1;

    // owner requests write route: rejected (limit reached).
    req.client_id = owner;
    auto res = service->GetWriteRoute(req);
    EXPECT_FALSE(res.has_value());
    EXPECT_EQ(ErrorCode::REPLICA_NUM_EXCEEDED, res.error());

    // other (non-owner) requests write route: also rejected.
    req.client_id = other;
    auto res2 = service->GetWriteRoute(req);
    EXPECT_FALSE(res2.has_value());
    EXPECT_EQ(ErrorCode::REPLICA_NUM_EXCEEDED, res2.error());
}

// w=0 fallback via master: single registered client is returned.
TEST_F(P2PMasterServiceTest, GetWriteRouteLocalOnlyFallback) {
    auto service = CreateService();
    auto seg = MakeP2PSegment("seg1", kDefaultSegmentSize, {}, 1);
    auto client_id = generate_uuid();
    RegisterP2PClient(*service, client_id, {seg}, "10.0.0.1", 50051);

    WriteRouteRequest req;
    req.key = "k";
    req.client_id = client_id;
    req.size = 1024;
    req.config.max_candidates = 1;
    req.config.remote_weight = 0.0;

    auto res = service->GetWriteRoute(req);
    ASSERT_TRUE(res.has_value()) << res.error();
    ASSERT_EQ(1u, res.value().candidates.size());
    EXPECT_EQ(client_id, res.value().candidates[0].client_id);
}

// w=0 fallback via master with no registered clients: NO_AVAILABLE_CANDIDATE.
TEST_F(P2PMasterServiceTest, GetWriteRouteLocalOnlyFallbackNoClient) {
    auto service = CreateService();

    WriteRouteRequest req;
    req.key = "k";
    req.client_id = generate_uuid();  // not registered
    req.size = 1024;
    req.config.max_candidates = 1;
    req.config.remote_weight = 0.0;

    auto res = service->GetWriteRoute(req);
    EXPECT_FALSE(res.has_value());
    EXPECT_EQ(ErrorCode::NO_AVAILABLE_CANDIDATE, res.error());
}

// Invalid config (waterline=0 + remote_weight=0) is rejected at entry.
TEST_F(P2PMasterServiceTest, GetWriteRouteInvalidConfig) {
    auto service = CreateService();
    auto seg = MakeP2PSegment("seg1", kDefaultSegmentSize, {}, 1);
    auto client_id = generate_uuid();
    RegisterP2PClient(*service, client_id, {seg}, "10.0.0.1", 50051);

    WriteRouteRequest req;
    req.key = "k";
    req.client_id = client_id;
    req.size = 1024;
    req.config.remote_weight = 0.0;
    req.config.local_write_waterline = 0.0;  // contradictory

    auto res = service->GetWriteRoute(req);
    EXPECT_FALSE(res.has_value());
    EXPECT_EQ(ErrorCode::INVALID_PARAMS, res.error());
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
    req.client_id = client_id;
    req.segment_id = seg.id;
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
    req.client_id = client_id;
    req.segment_id = seg.id;

    // First add
    auto res1 = service->AddReplica(req);
    ASSERT_TRUE(res1.has_value());

    // Duplicate add
    auto res2 = service->AddReplica(req);
    EXPECT_FALSE(res2.has_value());
    EXPECT_EQ(ErrorCode::REPLICA_ALREADY_EXISTS, res2.error());
}

TEST_F(P2PMasterServiceTest, AddReplicaMaxLimit) {
    auto service = CreateService(/* max_client_per_key= */ 2);
    auto seg1 = MakeP2PSegment("seg1");
    auto seg1_b = MakeP2PSegment("seg1_b");
    auto seg2 = MakeP2PSegment("seg2");
    auto seg3 = MakeP2PSegment("seg3");
    auto client1 = generate_uuid();
    auto client2 = generate_uuid();
    auto client3 = generate_uuid();
    RegisterP2PClient(*service, client1, {seg1, seg1_b}, "10.0.0.1", 50051);
    RegisterP2PClient(*service, client2, {seg2}, "10.0.0.2", 50052);
    RegisterP2PClient(*service, client3, {seg3}, "10.0.0.3", 50053);

    // Multiple replicas on one client are allowed.
    AddReplicaHelper(*service, "key1", 1024, client1, seg1.id);
    AddReplicaHelper(*service, "key1", 1024, client1, seg1_b.id);

    // The second owner client is allowed.
    AddReplicaHelper(*service, "key1", 1024, client2, seg2.id);

    // GetReplicaList aggregates per client: client1's two segment-replicas
    // collapse to one route, plus client2 -> 2 routes. (Both AddReplica calls
    // on client1 already succeeded above, confirming multiple replicas per
    // client are allowed.)
    auto get_res = service->GetReplicaList("key1");
    ASSERT_TRUE(get_res.has_value());
    EXPECT_EQ(2, get_res.value().replicas.size());

    // The third owner client should exceed the limit.
    AddReplicaRequest req;
    req.key = "key1";
    req.size = 1024;
    req.client_id = client3;
    req.segment_id = seg3.id;
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
    req.client_id = generate_uuid();  // non-existent
    req.segment_id = seg.id;
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
    req.client_id = client_id;
    req.segment_id = generate_uuid();  // non-existent segment
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
    req.client_id = client_id;
    req.segment_id = seg.id;
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
    req.client_id = client1;
    req.segment_id = seg1.id;
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
    req.client_id = client_id;
    req.segment_id = generate_uuid();  // wrong segment
    auto res = service->RemoveReplica(req);
    EXPECT_FALSE(res.has_value());
    EXPECT_EQ(ErrorCode::REPLICA_NOT_FOUND, res.error());
}

TEST_F(P2PMasterServiceTest, RemoveReplicaObjectNotFound) {
    auto service = CreateService();

    RemoveReplicaRequest req;
    req.key = "non_existent_key";
    req.client_id = generate_uuid();
    req.segment_id = generate_uuid();
    auto res = service->RemoveReplica(req);
    EXPECT_FALSE(res.has_value());
    EXPECT_EQ(ErrorCode::OBJECT_NOT_FOUND, res.error());
}

// ============================================================
// UnregisterClient Tests
// ============================================================

TEST_F(P2PMasterServiceTest, UnregisterClientRemovesReplicasAndSegments) {
    auto service = CreateService();
    auto seg = MakeP2PSegment("seg1");
    auto client_id = generate_uuid();
    RegisterP2PClient(*service, client_id, {seg}, "127.0.0.1", 50051);
    AddReplicaHelper(*service, "key1", 1024, client_id, seg.id);

    // Sanity: replica + segment + client present.
    ASSERT_TRUE(service->GetReplicaList("key1").has_value());
    ASSERT_TRUE(service->QuerySegments(seg.name).has_value());
    ASSERT_NE(service->GetClientManager().GetClient(client_id), nullptr);

    // Unregister cascades: segment unmount -> replica/object removal.
    auto res = service->UnregisterClient(
        UnregisterClientRequest{client_id, DeploymentMode::P2P});
    ASSERT_TRUE(res.has_value());

    EXPECT_EQ(service->GetClientManager().GetClient(client_id), nullptr);
    EXPECT_FALSE(service->QuerySegments(seg.name).has_value());
    auto get_res = service->GetReplicaList("key1");
    EXPECT_FALSE(get_res.has_value());
    EXPECT_EQ(ErrorCode::OBJECT_NOT_FOUND, get_res.error());
}

TEST_F(P2PMasterServiceTest, UnregisterClientPartialKeepsOtherOwner) {
    auto service = CreateService();
    auto seg1 = MakeP2PSegment("seg1");
    auto seg2 = MakeP2PSegment("seg2");
    auto client1 = generate_uuid();
    auto client2 = generate_uuid();
    RegisterP2PClient(*service, client1, {seg1}, "10.0.0.1", 50051);
    RegisterP2PClient(*service, client2, {seg2}, "10.0.0.2", 50052);
    AddReplicaHelper(*service, "key1", 1024, client1, seg1.id);
    AddReplicaHelper(*service, "key1", 1024, client2, seg2.id);

    // Unregister client1 -> only its replica is removed.
    ASSERT_TRUE(service
                    ->UnregisterClient(
                        UnregisterClientRequest{client1, DeploymentMode::P2P})
                    .has_value());

    EXPECT_EQ(service->GetClientManager().GetClient(client1), nullptr);
    EXPECT_NE(service->GetClientManager().GetClient(client2), nullptr);

    auto get_res = service->GetReplicaList("key1");
    ASSERT_TRUE(get_res.has_value());
    EXPECT_EQ(1, get_res.value().replicas.size());
    EXPECT_EQ(client2,
              get_res.value().replicas[0].get_p2p_proxy_descriptor().client_id);
}

TEST_F(P2PMasterServiceTest, UnregisterClientIdempotent) {
    auto service = CreateService();
    auto seg = MakeP2PSegment();
    auto client_id = generate_uuid();
    RegisterP2PClient(*service, client_id, {seg}, "127.0.0.1", 50051);

    ASSERT_TRUE(service
                    ->UnregisterClient(
                        UnregisterClientRequest{client_id, DeploymentMode::P2P})
                    .has_value());
    // Second call: client already gone -> still OK (idempotent).
    EXPECT_TRUE(service
                    ->UnregisterClient(
                        UnregisterClientRequest{client_id, DeploymentMode::P2P})
                    .has_value());
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
        MakeP2PSegment("seg_a", kDefaultSegmentSize, {"memory", "fast"}, 10);
    auto seg_b = MakeP2PSegment("seg_b", kDefaultSegmentSize, {"disk"}, 2);
    auto client1 = generate_uuid();
    auto client2 = generate_uuid();
    RegisterP2PClient(*service, client1, {seg_a}, "10.0.0.1", 50051);
    RegisterP2PClient(*service, client2, {seg_b}, "10.0.0.2", 50052);

    AddReplicaHelper(*service, "key1", 1024, client1, seg_a.id);
    AddReplicaHelper(*service, "key1", 1024, client2, seg_b.id);

    // Filter out replicas with tag "memory" — only seg_b remains
    GetReplicaListRequestConfig config;
    config.max_candidates = 10;
    config.p2p_config = P2PGetReplicaListConfigExtra{
        .tag_filters = {"memory"},
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

// A client holding the key on multiple segments (tiers) is aggregated into a
// single read route (representative = highest-priority segment).
TEST_F(P2PMasterServiceTest, GetReplicaListAggregatesPerClient) {
    auto service = CreateService();
    auto seg_hi = MakeP2PSegment("seg_hi", kDefaultSegmentSize, {}, 10);
    auto seg_lo = MakeP2PSegment("seg_lo", kDefaultSegmentSize, {}, 1);
    auto client_id = generate_uuid();
    RegisterP2PClient(*service, client_id, {seg_hi, seg_lo}, "10.0.0.1", 50051);
    AddReplicaHelper(*service, "key1", 1024, client_id, seg_hi.id);
    AddReplicaHelper(*service, "key1", 1024, client_id, seg_lo.id);

    auto res = service->GetReplicaList("key1");
    ASSERT_TRUE(res.has_value());
    // Two segment-replicas on the same client collapse to one route.
    EXPECT_EQ(1, res.value().replicas.size());
    EXPECT_EQ(client_id,
              res.value().replicas[0].get_p2p_proxy_descriptor().client_id);
    // Representative is the highest-priority segment.
    EXPECT_EQ(seg_hi.id,
              res.value().replicas[0].get_p2p_proxy_descriptor().segment_id);
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
    auto seg = MakeP2PSegment("seg1", kDefaultSegmentSize, {"memory"}, 5,
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
    EXPECT_EQ(writer_id, candidate.client_id);

    // Step 2: Add replica (simulate write completion). The route is
    // client-only; the client registers the concrete segment it actually wrote
    // (here seg.id).
    AddReplicaRequest a_req;
    a_req.key = "data_001";
    a_req.size = 4096;
    a_req.client_id = candidate.client_id;
    a_req.segment_id = seg.id;
    auto a_res = service->AddReplica(a_req);
    ASSERT_TRUE(a_res.has_value());

    // Step 3: Read — GetReplicaList
    auto r_res = service->GetReplicaList("data_001");
    ASSERT_TRUE(r_res.has_value());
    EXPECT_EQ(1, r_res.value().replicas.size());

    // Step 4: Remove
    RemoveReplicaRequest rm_req;
    rm_req.key = "data_001";
    rm_req.client_id = candidate.client_id;
    rm_req.segment_id = seg.id;
    auto rm_res = service->RemoveReplica(rm_req);
    ASSERT_TRUE(rm_res.has_value());

    // Verify gone
    auto r_res2 = service->GetReplicaList("data_001");
    EXPECT_FALSE(r_res2.has_value());
    EXPECT_EQ(ErrorCode::OBJECT_NOT_FOUND, r_res2.error());
}

// ============================================================
// SetSyncCompleted Tests
// ============================================================

TEST_F(P2PMasterServiceTest, SetSyncCompletedSuccess) {
    auto service = CreateService();
    auto seg = MakeP2PSegment();
    auto client_id = generate_uuid();
    RegisterP2PClient(*service, client_id, {seg}, "127.0.0.1", 50051);

    // After registration, OnClientRegistered sets is_syncing = true
    auto client = service->client_manager_->GetClient(client_id);
    ASSERT_NE(client, nullptr);
    auto p2p_client = std::dynamic_pointer_cast<P2PClientMeta>(client);
    ASSERT_NE(p2p_client, nullptr);
    EXPECT_TRUE(p2p_client->IsSyncing());

    // SetSyncCompleted should clear is_syncing
    auto result = service->SetSyncCompleted(client_id);
    ASSERT_TRUE(result.has_value());
    EXPECT_FALSE(p2p_client->IsSyncing());
}

TEST_F(P2PMasterServiceTest, SetSyncCompletedClientNotFound) {
    auto service = CreateService();
    auto result = service->SetSyncCompleted(generate_uuid());
    EXPECT_FALSE(result.has_value());
    EXPECT_EQ(result.error(), ErrorCode::CLIENT_NOT_FOUND);
}

TEST_F(P2PMasterServiceTest, SetSyncCompletedIdempotent) {
    auto service = CreateService();
    auto seg = MakeP2PSegment();
    auto client_id = generate_uuid();
    RegisterP2PClient(*service, client_id, {seg}, "127.0.0.1", 50051);

    // Call twice — should succeed both times
    EXPECT_TRUE(service->SetSyncCompleted(client_id).has_value());
    EXPECT_TRUE(service->SetSyncCompleted(client_id).has_value());

    auto client = service->client_manager_->GetClient(client_id);
    auto p2p_client = std::dynamic_pointer_cast<P2PClientMeta>(client);
    EXPECT_FALSE(p2p_client->IsSyncing());
}

}  // namespace mooncake::test
