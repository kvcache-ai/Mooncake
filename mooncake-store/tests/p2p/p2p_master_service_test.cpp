#include <glog/logging.h>
#include <gtest/gtest.h>

#include <algorithm>
#include <array>
#include <barrier>
#include <chrono>
#include <functional>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#define private public
#define protected public
#include "p2p/master/p2p_master_service.h"
#undef protected
#undef private

#include "p2p/master/p2p_client_meta.h"
#include "p2p/master/p2p_master_metric_manager.h"
#include "p2p/common/p2p_rpc_types.h"
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
    P2PSegment MakeP2PSegment(std::string name = "p2p_segment",
                              size_t size = kDefaultSegmentSize,
                              std::vector<std::string> tags = {},
                              int priority = 0,
                              MemoryType memory_type = MemoryType::DRAM) {
        P2PSegment segment;
        segment.id = generate_uuid();
        segment.name = std::move(name);
        segment.size = size;
        segment.priority = priority;
        segment.tags = std::move(tags);
        segment.memory_type = memory_type;
        return segment;
    }

    /// Create the service with given max_replicas config
    std::unique_ptr<P2PMasterService> CreateService(
        uint64_t max_client_per_key = 0) {
        P2PMasterConfig config;
        config.routes.max_clients_per_key = max_client_per_key;
        return std::make_unique<P2PMasterService>(config);
    }

    /// Register a client with given segments, returns client_id
    UUID RegisterP2PClient(P2PMasterService& service, const UUID& client_id,
                           const std::vector<P2PSegment>& segments,
                           const std::string& ip = "127.0.0.1",
                           uint16_t port = 50051) {
        P2PRegisterClientRequest req;
        req.client_id = client_id;
        req.ip_address = ip;
        req.rpc_port = port;
        req.segments = segments;
        auto res = service.RegisterClient(req);
        EXPECT_TRUE(res.has_value())
            << "Failed to register client: " << res.error();
        return req.client_id;
    }

    /// Helper to add a replica via PublishRoute
    void AddReplicaHelper(P2PMasterService& service, const std::string& key,
                          size_t size, const UUID& client_id,
                          const UUID& segment_id) {
        P2PPublishRouteRequest req;
        req.key = key;
        req.object_size = size;
        req.client_id = client_id;
        req.segment_id = segment_id;
        auto res = service.PublishRoute(req);
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

    // HA re-registering the same client_id is idempotent.
    P2PRegisterClientRequest req;
    req.client_id = client_id;
    req.segments = {MakeP2PSegment("seg2")};
    req.ip_address = "127.0.0.1";
    req.rpc_port = 50051;
    auto res = service->RegisterClient(req);
    ASSERT_TRUE(res.has_value()) << res.error();

    // Duplicate registration does not merge new segment metadata.
    auto seg_res = service->QuerySegments("seg2");
    EXPECT_FALSE(seg_res.has_value());
    EXPECT_EQ(ErrorCode::SEGMENT_NOT_FOUND, seg_res.error());
}

// ============================================================
// GetWriteRoute Tests
// ============================================================

TEST_F(P2PMasterServiceTest, GetWriteRouteBasic) {
    auto service = CreateService();
    auto seg = MakeP2PSegment("seg1", kDefaultSegmentSize, {}, 1);
    auto client_id = generate_uuid();
    RegisterP2PClient(*service, client_id, {seg}, "127.0.0.1", 50051);

    P2PGetWriteRouteRequest req;
    req.key = "test_key";
    req.client_id = generate_uuid();  // different client requesting
    req.object_size = 1024;
    req.config.max_candidates = 1;
    req.config.strategy = P2PClientSelectionStrategy::CAPACITY_PRIORITY;

    auto res = service->GetWriteRoute(req);
    ASSERT_TRUE(res.has_value()) << "GetWriteRoute failed: " << res.error();
    EXPECT_EQ(1, res.value().size());
    EXPECT_EQ(client_id, res.value()[0].client_id);
}

TEST_F(P2PMasterServiceTest, GetWriteRouteNoCapacity) {
    auto service = CreateService();
    auto seg = MakeP2PSegment("seg1", 1024);  // only 1024 bytes
    auto client_id = generate_uuid();
    RegisterP2PClient(*service, client_id, {seg}, "127.0.0.1", 50051);

    P2PGetWriteRouteRequest req;
    req.key = "test_key";
    req.client_id = generate_uuid();
    req.object_size = 2048;  // larger than segment capacity
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
    P2PGetWriteRouteRequest req;
    req.key = "test_key";
    req.client_id = generate_uuid();
    req.object_size = 1024;
    req.config.max_candidates = 10;
    req.config.tag_filters = {"disk"};

    auto res = service->GetWriteRoute(req);
    ASSERT_TRUE(res.has_value());
    EXPECT_EQ(1, res.value().size());
    EXPECT_EQ(client1, res.value()[0].client_id);
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
    P2PGetWriteRouteRequest req;
    req.key = "test_key";
    req.client_id = generate_uuid();
    req.object_size = 1024;
    req.config.max_candidates = 10;
    req.config.priority_limit = 5;

    auto res = service->GetWriteRoute(req);
    ASSERT_TRUE(res.has_value());
    EXPECT_EQ(1, res.value().size());
    EXPECT_EQ(client2, res.value()[0].client_id);
}

TEST_F(P2PMasterServiceTest, GetWriteRouteForceRemoteExcludesLocal) {
    auto service = CreateService();
    auto seg = MakeP2PSegment("seg1", kDefaultSegmentSize, {}, 1);
    auto client_id = generate_uuid();
    RegisterP2PClient(*service, client_id, {seg}, "127.0.0.1", 50051);

    // Same client requesting, remote_weight = 1 (force remote) — should skip
    // self
    P2PGetWriteRouteRequest req;
    req.key = "test_key";
    req.client_id = client_id;
    req.object_size = 1024;
    req.config.max_candidates = 1;
    req.config.remote_weight = 1.0;

    auto res = service->GetWriteRoute(req);
    EXPECT_FALSE(res.has_value());  // only candidate is self, which is skipped

    // remote_weight < 1 — should include self
    req.config.remote_weight = 0.5;
    auto res2 = service->GetWriteRoute(req);
    ASSERT_TRUE(res2.has_value());
    EXPECT_EQ(1, res2.value().size());
    EXPECT_EQ(client_id, res2.value()[0].client_id);
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

    P2PGetWriteRouteRequest req;
    req.key = "test_key";
    req.client_id = generate_uuid();
    req.object_size = 1024;
    req.config.max_candidates = 2;
    req.config.early_return = true;

    auto res = service->GetWriteRoute(req);
    ASSERT_TRUE(res.has_value());
    // With early_return, should stop at max_candidates
    EXPECT_EQ(2, res.value().size());
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

    P2PGetWriteRouteRequest req;
    req.key = "test_key";
    req.client_id = client_id;
    req.object_size = 1024;
    req.config.max_candidates = P2PWriteRouteConfig::RETURN_ALL_CANDIDATES;
    req.config.remote_weight =
        1.0;  // force remote: exclude the requesting client
    req.config.early_return = false;

    auto res = service->GetWriteRoute(req);
    ASSERT_TRUE(res.has_value());
    EXPECT_EQ(2, res.value().size());
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

    P2PGetWriteRouteRequest req;
    req.key = "key1";
    req.client_id = generate_uuid();
    req.object_size = 1024;
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
    local_seg.usage = 900;  // free 100 -> free_ratio 0.1
    auto local_id = generate_uuid();
    RegisterP2PClient(*service, local_id, {local_seg}, "10.0.0.1", 50051);

    // Several near-empty, high-capacity remotes (visited first).
    for (int i = 0; i < 3; ++i) {
        auto seg = MakeP2PSegment("remote_" + std::to_string(i), 100000, {}, 1);
        RegisterP2PClient(*service, generate_uuid(), {seg},
                          "10.0.0." + std::to_string(i + 2), 50052 + i);
    }

    P2PGetWriteRouteRequest req;
    req.key = "k";
    req.client_id = local_id;  // the requesting client is the local one
    req.object_size = 50;
    req.config.max_candidates = 1;
    req.config.early_return = false;  // collect all, then sort
    req.config.remote_weight = 0.02;  // strong local preference

    auto res = service->GetWriteRoute(req);
    ASSERT_TRUE(res.has_value()) << res.error();
    ASSERT_EQ(1u, res.value().size());
    EXPECT_EQ(local_id, res.value()[0].client_id);
}

// With a weak local preference, a much-emptier remote out-scores the
// nearly-full local client after sorting.
TEST_F(P2PMasterServiceTest, GetWriteRouteWeightedRemoteCanWin) {
    auto service = CreateService();
    auto local_seg = MakeP2PSegment("local", 1000, {}, 1);
    local_seg.usage = 900;  // free_ratio 0.1
    auto local_id = generate_uuid();
    RegisterP2PClient(*service, local_id, {local_seg}, "10.0.0.1", 50051);

    auto remote_seg =
        MakeP2PSegment("remote", 100000, {}, 1);  // free_ratio 1.0
    auto remote_id = generate_uuid();
    RegisterP2PClient(*service, remote_id, {remote_seg}, "10.0.0.2", 50052);

    P2PGetWriteRouteRequest req;
    req.key = "k";
    req.client_id = local_id;
    req.object_size = 50;
    req.config.max_candidates = 1;
    req.config.early_return = false;  // collect all, then sort
    req.config.remote_weight = 0.4;   // weak local preference

    auto res = service->GetWriteRoute(req);
    ASSERT_TRUE(res.has_value()) << res.error();
    ASSERT_EQ(1u, res.value().size());
    EXPECT_EQ(remote_id, res.value()[0].client_id);
}

// Strategy traversal precedes early stop; only the selected candidates are
// ranked by weighted score. Moving score ranking before early stop changes
// which client wins this request.
TEST_F(P2PMasterServiceTest, GetWriteRouteEarlyReturnStopsAtFirstCandidate) {
    auto service = CreateService();
    auto local_seg = MakeP2PSegment("local", 1000, {}, 1);
    local_seg.usage = 900;  // free_ratio 0.1
    auto local_id = generate_uuid();
    RegisterP2PClient(*service, local_id, {local_seg}, "10.0.0.1", 50051);

    auto remote_seg =
        MakeP2PSegment("remote", 100000, {}, 1);  // free_ratio 1.0
    auto remote_id = generate_uuid();
    RegisterP2PClient(*service, remote_id, {remote_seg}, "10.0.0.2", 50052);

    P2PGetWriteRouteRequest req;
    req.key = "k";
    req.client_id = local_id;
    req.object_size = 50;
    req.config.max_candidates = 1;
    req.config.early_return = true;
    req.config.remote_weight = 0.02;

    auto res = service->GetWriteRoute(req);
    ASSERT_TRUE(res.has_value()) << res.error();
    ASSERT_EQ(1u, res.value().size());
    // CAPACITY_PRIORITY visits the 100 000-capacity remote first; early stop.
    EXPECT_EQ(remote_id, res.value()[0].client_id);

    // Without early stop, the local score (0.1 * 0.98) beats the remote
    // score (1.0 * 0.02), despite having less absolute available capacity.
    req.config.early_return = false;
    auto all_candidates = service->GetWriteRoute(req);
    ASSERT_TRUE(all_candidates.has_value());
    ASSERT_EQ(all_candidates->size(), 1);
    EXPECT_EQ(local_id, all_candidates->front().client_id);
}

// Problem 3: top_tier_only changes which client wins by scoring only the
// highest-priority tier's free ratio instead of summing all tiers.
TEST_F(P2PMasterServiceTest, GetWriteRouteTopTierCapacityAffectsScore) {
    auto service = CreateService();

    // Client A: small high-prio DRAM mostly free; large low-prio NVMe mostly
    // full.
    auto a_dram = MakeP2PSegment("a_dram", 1000, {}, 10);
    a_dram.usage = 100;  // free 900 -> top-tier ratio 0.9
    auto a_nvme = MakeP2PSegment("a_nvme", 100000, {}, 0);
    a_nvme.usage = 99000;  // free 1000
    auto a_id = generate_uuid();
    RegisterP2PClient(*service, a_id, {a_dram, a_nvme}, "10.0.0.1", 50051);

    // Client B: high-prio DRAM half free; large low-prio NVMe fully free.
    auto b_dram = MakeP2PSegment("b_dram", 1000, {}, 10);
    b_dram.usage = 500;  // free 500 -> top-tier ratio 0.5
    auto b_nvme = MakeP2PSegment("b_nvme", 100000, {}, 0);
    b_nvme.usage = 0;  // free 100000
    auto b_id = generate_uuid();
    RegisterP2PClient(*service, b_id, {b_dram, b_nvme}, "10.0.0.2", 50052);

    P2PGetWriteRouteRequest req;
    req.key = "k";
    req.client_id = generate_uuid();  // non-local requester
    req.object_size = 50;
    req.config.max_candidates = P2PWriteRouteConfig::RETURN_ALL_CANDIDATES;
    req.config.early_return = false;
    req.config.remote_weight = 0.5;

    // All tiers: B is much emptier overall -> B wins.
    req.config.top_tier_only = false;
    auto res_all = service->GetWriteRoute(req);
    ASSERT_TRUE(res_all.has_value()) << res_all.error();
    EXPECT_EQ(b_id, res_all.value()[0].client_id);

    // Top tier only: A's DRAM tier is emptier than B's -> A wins.
    req.config.top_tier_only = true;
    auto res_top = service->GetWriteRoute(req);
    ASSERT_TRUE(res_top.has_value()) << res_top.error();
    EXPECT_EQ(a_id, res_top.value()[0].client_id);
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

    P2PGetWriteRouteRequest req;
    req.key = "k";
    req.client_id = generate_uuid();  // non-local, non-owner requester
    req.object_size = 1024;
    req.config.max_candidates = P2PWriteRouteConfig::RETURN_ALL_CANDIDATES;
    req.config.early_return = false;

    auto res = service->GetWriteRoute(req);
    ASSERT_TRUE(res.has_value()) << res.error();
    ASSERT_EQ(1u, res.value().size());
    EXPECT_EQ(other, res.value()[0].client_id);
    for (const auto& c : res.value()) {
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

    P2PGetWriteRouteRequest req;
    req.key = "k";
    req.client_id = self;  // requester is the existing owner
    req.object_size = 1024;
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

    P2PGetWriteRouteRequest req;
    req.key = "k";
    req.object_size = 1024;
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

    P2PGetWriteRouteRequest req;
    req.key = "k";
    req.client_id = client_id;
    req.object_size = 1024;
    req.config.max_candidates = 1;
    req.config.remote_weight = 0.0;

    auto res = service->GetWriteRoute(req);
    ASSERT_TRUE(res.has_value()) << res.error();
    ASSERT_EQ(1u, res.value().size());
    EXPECT_EQ(client_id, res.value()[0].client_id);
}

// w=0 fallback via master with no registered clients: NO_AVAILABLE_CANDIDATE.
TEST_F(P2PMasterServiceTest, GetWriteRouteLocalOnlyFallbackNoClient) {
    auto service = CreateService();

    P2PGetWriteRouteRequest req;
    req.key = "k";
    req.client_id = generate_uuid();  // not registered
    req.object_size = 1024;
    req.config.max_candidates = 1;
    req.config.remote_weight = 0.0;

    auto res = service->GetWriteRoute(req);
    EXPECT_FALSE(res.has_value());
    EXPECT_EQ(ErrorCode::NO_AVAILABLE_CANDIDATE, res.error());
}

// Invalid config (waterline=0 + remote_weight=0, a dead-end combo) is
// rejected at entry.
TEST_F(P2PMasterServiceTest, GetWriteRouteInvalidConfig) {
    auto service = CreateService();
    auto seg = MakeP2PSegment("seg1", kDefaultSegmentSize, {}, 1);
    auto client_id = generate_uuid();
    RegisterP2PClient(*service, client_id, {seg}, "10.0.0.1", 50051);

    P2PGetWriteRouteRequest req;
    req.key = "k";
    req.client_id = client_id;
    req.object_size = 1024;
    req.config.remote_weight = 0.0;
    req.config.local_write_waterline = 0.0;  // contradictory: dead end

    auto res = service->GetWriteRoute(req);
    EXPECT_FALSE(res.has_value());
    EXPECT_EQ(ErrorCode::INVALID_PARAMS, res.error());
}

// Invalid config (waterline=1 + remote_weight=1, a dead-end combo) is
// also rejected at entry.
TEST_F(P2PMasterServiceTest, GetWriteRouteInvalidConfigSecondContradiction) {
    auto service = CreateService();
    auto seg = MakeP2PSegment("seg1", kDefaultSegmentSize, {}, 1);
    auto client_id = generate_uuid();
    RegisterP2PClient(*service, client_id, {seg}, "10.0.0.1", 50051);

    P2PGetWriteRouteRequest req;
    req.key = "k";
    req.client_id = client_id;
    req.object_size = 1024;
    req.config.remote_weight = 1.0;
    req.config.local_write_waterline = 1.0;  // contradictory: dead end

    auto res = service->GetWriteRoute(req);
    EXPECT_FALSE(res.has_value());
    EXPECT_EQ(ErrorCode::INVALID_PARAMS, res.error());
}

TEST_F(P2PMasterServiceTest, BatchWriteRoutesMatchSingleRequests) {
    auto service = CreateService(/*max_client_per_key=*/2);
    service->GetClientManager().Stop();
    const UUID local_id{1, 1};
    const UUID remote_id{2, 2};
    auto local_top = MakeP2PSegment("local-top", 1000, {}, 10);
    local_top.usage = 100;
    auto local_disk = MakeP2PSegment("local-disk", 10000, {}, 1);
    local_disk.usage = 8000;
    auto remote = MakeP2PSegment("remote", 4000, {}, 5);
    remote.usage = 1000;
    auto excluded = MakeP2PSegment("excluded", 20000, {"excluded"}, 2);
    RegisterP2PClient(*service, local_id, {local_top, local_disk});
    RegisterP2PClient(*service, remote_id, {remote});
    RegisterP2PClient(*service, UUID{3, 3}, {excluded});
    AddReplicaHelper(*service, "owned", 64, local_id, local_top.id);
    AddReplicaHelper(*service, "capped", 64, local_id, local_top.id);
    AddReplicaHelper(*service, "capped", 64, remote_id, remote.id);

    P2PBatchGetWriteRouteRequest request;
    request.client_id = local_id;
    request.keys = {"new", "owned", "large", "capped", "", "new"};
    request.object_sizes = {64, 64, 100000, 64, 64, 64};
    for (auto strategy : {P2PClientSelectionStrategy::ORDERED,
                          P2PClientSelectionStrategy::CAPACITY_PRIORITY,
                          P2PClientSelectionStrategy::RANDOM}) {
        for (bool early_return : {false, true}) {
            for (double weight : {0.0, 0.2, 1.0}) {
                request.config.strategy = strategy;
                request.config.early_return = early_return;
                // Random early selection has no deterministic single-request
                // oracle. Its limited-candidate behavior is tested separately.
                request.config.max_candidates =
                    strategy == P2PClientSelectionStrategy::RANDOM ? 0 : 1;
                request.config.remote_weight = weight;
                request.config.top_tier_only = early_return;
                request.config.tag_filters =
                    early_return ? std::vector<std::string>{"excluded"}
                                 : std::vector<std::string>{};
                request.config.priority_limit = early_return ? 4 : 0;

                auto batch = service->BatchGetWriteRoute(request);
                ASSERT_EQ(batch.responses.size(), request.keys.size());
                ASSERT_EQ(batch.error_codes.size(), request.keys.size());
                EXPECT_EQ(batch.error_codes[2], ErrorCode::NO_AVAILABLE_CANDIDATE);
                EXPECT_EQ(batch.error_codes[3], ErrorCode::REPLICA_NUM_EXCEEDED);
                for (size_t i = 0; i < request.keys.size(); ++i) {
                    auto single = service->GetWriteRoute(
                        {.key = request.keys[i],
                         .client_id = request.client_id,
                         .object_size = request.object_sizes[i],
                         .config = request.config});
                    EXPECT_EQ(batch.error_codes[i],
                              single ? ErrorCode::OK : single.error());
                    if (!single) {
                        EXPECT_TRUE(batch.responses[i].empty());
                        continue;
                    }
                    ASSERT_EQ(batch.responses[i].size(), single->size());
                    for (size_t j = 0; j < single->size(); ++j) {
                        const auto& actual = batch.responses[i][j];
                        const auto& expected = (*single)[j];
                        EXPECT_EQ(actual.client_id, expected.client_id);
                        EXPECT_EQ(actual.ip_address, expected.ip_address);
                        EXPECT_EQ(actual.rpc_port, expected.rpc_port);
                        EXPECT_EQ(actual.available_capacity,
                                  expected.available_capacity);
                        EXPECT_DOUBLE_EQ(actual.score, expected.score);
                    }
                }
            }
        }
    }
}

TEST_F(P2PMasterServiceTest, BatchWriteRouteHandlesEmptyInvalidAndSingleInput) {
    auto service = CreateService(/*max_client_per_key=*/1);
    service->GetClientManager().Stop();
    const UUID client_id{1, 1};
    auto segment = MakeP2PSegment("segment", 4096);
    RegisterP2PClient(*service, client_id, {segment});
    AddReplicaHelper(*service, "capped", 64, client_id, segment.id);

    P2PBatchGetWriteRouteRequest request;
    request.client_id = client_id;
    EXPECT_TRUE(service->BatchGetWriteRoute(request).responses.empty());
    request.keys = {"new", "capped"};
    request.object_sizes = {64};
    auto result = service->BatchGetWriteRoute(request);
    EXPECT_EQ(result.error_codes,
              (std::vector<ErrorCode>(2, ErrorCode::INVALID_PARAMS)));

    request.object_sizes.push_back(64);
    request.config.remote_weight = 0;
    request.config.local_write_waterline = 0;
    result = service->BatchGetWriteRoute(request);
    EXPECT_EQ(result.error_codes,
              (std::vector<ErrorCode>(2, ErrorCode::INVALID_PARAMS)));

    request.config = P2PWriteRouteConfig{};
    request.config.strategy = static_cast<P2PClientSelectionStrategy>(99);
    result = service->BatchGetWriteRoute(request);
    // The manager rejects the strategy while preparing clients, before any
    // per-key selection or owner-limit check can run.
    EXPECT_EQ(result.error_codes,
              (std::vector<ErrorCode>(2, ErrorCode::INTERNAL_ERROR)));

    request.config = P2PWriteRouteConfig{};
    request.keys = {"new"};
    request.object_sizes = {64};
    result = service->BatchGetWriteRoute(request);
    ASSERT_EQ(result.error_codes, (std::vector<ErrorCode>{ErrorCode::OK}));
    ASSERT_EQ(result.responses.front().size(), 1);
    EXPECT_EQ(result.responses.front().front().client_id, client_id);
    request.keys = {"capped", "capped"};
    request.object_sizes = {64, 64};
    EXPECT_EQ(service->BatchGetWriteRoute(request).error_codes,
              (std::vector<ErrorCode>(2, ErrorCode::REPLICA_NUM_EXCEEDED)));
}

TEST_F(P2PMasterServiceTest, BatchWriteRouteChecksEachObjectSize) {
    auto service = CreateService();
    service->GetClientManager().Stop();
    const UUID client_id{1, 1};
    auto segment = MakeP2PSegment("segment", 1000);
    segment.usage = 900;
    RegisterP2PClient(*service, client_id, {segment});

    P2PBatchGetWriteRouteRequest request;
    request.client_id = client_id;
    request.keys = {"too-large", "fits", "zero"};
    request.object_sizes = {101, 100, 0};
    auto result = service->BatchGetWriteRoute(request);
    EXPECT_EQ(result.error_codes,
              (std::vector<ErrorCode>{ErrorCode::NO_AVAILABLE_CANDIDATE,
                                      ErrorCode::OK, ErrorCode::OK}));
    ASSERT_EQ(result.responses.size(), 3);
    EXPECT_TRUE(result.responses[0].empty());
    for (size_t i : {size_t{1}, size_t{2}}) {
        ASSERT_EQ(result.responses[i].size(), 1);
        EXPECT_EQ(result.responses[i].front().client_id, client_id);
        EXPECT_EQ(result.responses[i].front().available_capacity, 100);
    }

    auto client = service->GetClientManager().GetClient(client_id);
    ASSERT_NE(client, nullptr);
    auto update = client->UpdateSegmentUsages({{segment.id, 1000}});
    ASSERT_EQ(update.sub_results.size(), 1);
    ASSERT_EQ(update.sub_results.front().error, ErrorCode::OK);
    request.keys = {"positive", "zero"};
    request.object_sizes = {1, 0};
    result = service->BatchGetWriteRoute(request);
    EXPECT_EQ(result.error_codes,
              (std::vector<ErrorCode>{ErrorCode::NO_AVAILABLE_CANDIDATE,
                                      ErrorCode::OK}));
    ASSERT_EQ(result.responses.size(), 2);
    EXPECT_TRUE(result.responses[0].empty());
    ASSERT_EQ(result.responses[1].size(), 1);
    EXPECT_EQ(result.responses[1].front().available_capacity, 0);
    EXPECT_DOUBLE_EQ(result.responses[1].front().score, 0);
}

TEST_F(P2PMasterServiceTest, RandomBatchWriteRouteReusesPreparedOrder) {
    auto service = CreateService();
    service->GetClientManager().Stop();
    const UUID owner{1, 1};
    auto owner_segment = MakeP2PSegment("owner", 4096);
    RegisterP2PClient(*service, owner, {owner_segment});
    AddReplicaHelper(*service, "owned", 64, owner, owner_segment.id);
    for (uint64_t i = 2; i <= 4; ++i) {
        RegisterP2PClient(*service, UUID{i, i},
                         {MakeP2PSegment("remote-" + std::to_string(i),
                                         i * 4096)});
    }
    P2PBatchGetWriteRouteRequest request;
    request.client_id = owner;
    request.keys = {"owned", "owned", "owned", "owned"};
    request.object_sizes.assign(request.keys.size(), 64);
    request.config.strategy = P2PClientSelectionStrategy::RANDOM;
    request.config.early_return = true;
    request.config.max_candidates = 2;
    auto result = service->BatchGetWriteRoute(request);
    ASSERT_EQ(result.responses.size(), request.keys.size());
    for (size_t i = 0; i < result.responses.size(); ++i) {
        ASSERT_EQ(result.error_codes[i], ErrorCode::OK);
        const auto& candidates = result.responses[i];
        ASSERT_EQ(candidates.size(), 2);
        EXPECT_NE(candidates[0].client_id, candidates[1].client_id);
        EXPECT_EQ(candidates[0].client_id,
                  result.responses.front()[0].client_id);
        EXPECT_EQ(candidates[1].client_id,
                  result.responses.front()[1].client_id);
        for (const auto& candidate : candidates) {
            EXPECT_NE(candidate.client_id, owner);
            EXPECT_GE(candidate.available_capacity, 64);
        }
    }
}

TEST_F(P2PMasterServiceTest, PreparedWriteClientsFreezeCapacityButRecheckHealth) {
    auto service = CreateService();
    service->GetClientManager().Stop();
    const UUID client_id{1, 1};
    auto segment = MakeP2PSegment("segment", 1000);
    segment.usage = 100;
    RegisterP2PClient(*service, client_id, {segment});
    auto client = service->GetClientManager().GetClient(client_id);
    ASSERT_NE(client, nullptr);
    client->health_state_.last_heartbeat = std::chrono::steady_clock::now() -
        std::chrono::seconds(P2PClientMeta::disconnect_timeout_sec_ + 1);
    ASSERT_EQ(client->CheckHealth().second, P2PClientStatus::DISCONNECTION);

    P2PGetWriteRouteRequest request{
        .key = "new", .client_id = client_id, .object_size = 100};
    request.config.remote_weight = 0.2;
    const auto clients =
        service->GetWriteRouteClients(request.client_id, request.config);
    ASSERT_TRUE(clients.has_value());
    ASSERT_EQ(clients->size(), 1);
    auto select = [&] {
        return service->InnerGetWriteRoute(request, *clients);
    };
    auto result = select();
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error(), ErrorCode::NO_AVAILABLE_CANDIDATE);

    ASSERT_EQ(client->Heartbeat().second, P2PClientStatus::HEALTH);
    auto usage = client->UpdateSegmentUsages({{segment.id, 1000}});
    ASSERT_EQ(usage.sub_results.front().error, ErrorCode::OK);
    for (int i = 0; i < 2; ++i) {
        result = select();
        ASSERT_TRUE(result.has_value());
        ASSERT_EQ(result->size(), 1);
        EXPECT_EQ(result->front().available_capacity, 900);
        EXPECT_DOUBLE_EQ(result->front().score, 0.9 * 0.8);
    }
    auto fresh = service->GetWriteRoute(request);
    ASSERT_FALSE(fresh.has_value());
    EXPECT_EQ(fresh.error(), ErrorCode::NO_AVAILABLE_CANDIDATE);

    AddReplicaHelper(*service, "new", 100, client_id, segment.id);
    result = select();
    EXPECT_FALSE(result.has_value());

    request.key = "unowned";
    client->health_state_.last_heartbeat = std::chrono::steady_clock::now() -
        std::chrono::seconds(P2PClientMeta::disconnect_timeout_sec_ + 1);
    ASSERT_EQ(client->CheckHealth().second, P2PClientStatus::DISCONNECTION);
    EXPECT_FALSE(select().has_value());
    ASSERT_EQ(client->Heartbeat().second, P2PClientStatus::HEALTH);
    EXPECT_TRUE(select().has_value());
    ASSERT_TRUE(service->UnregisterClient(client_id).has_value());
    EXPECT_FALSE(select().has_value());
}

TEST_F(P2PMasterServiceTest, WriteRouteClientsFilterConfigBeforeKeySelection) {
    auto service = CreateService();
    service->GetClientManager().Stop();
    const UUID first_id{1, 1};
    const UUID second_id{2, 2};
    auto first = MakeP2PSegment("first", 1000, {}, 10);
    first.usage = 100;
    auto second_top = MakeP2PSegment("second-top", 100, {}, 10);
    auto second_low = MakeP2PSegment("second-low", 10000, {}, 1);
    auto excluded = MakeP2PSegment("excluded", 50000, {"skip"}, 10);
    RegisterP2PClient(*service, first_id, {first});
    RegisterP2PClient(*service, second_id, {second_top, second_low});
    RegisterP2PClient(*service, UUID{3, 3}, {excluded});
    auto second = service->GetClientManager().GetClient(second_id);
    ASSERT_NE(second, nullptr);
    second->health_state_.last_heartbeat = std::chrono::steady_clock::now() -
        std::chrono::seconds(P2PClientMeta::disconnect_timeout_sec_ + 1);
    ASSERT_EQ(second->CheckHealth().second, P2PClientStatus::DISCONNECTION);

    P2PGetWriteRouteRequest request{
        .key = "new", .client_id = UUID{99, 99}, .object_size = 64};
    request.config.tag_filters = {"skip"};
    request.config.priority_limit = 5;
    request.config.max_candidates = 1;
    request.config.early_return = true;
    const auto clients =
        service->GetWriteRouteClients(request.client_id, request.config);
    ASSERT_TRUE(clients.has_value());
    ASSERT_EQ(clients->size(), 2);
    // Total capacity determines traversal order, while config selects the
    // eligible tiers. Disconnected clients remain available for later recovery.
    EXPECT_EQ((*clients)[0].client->get_client_id(), second_id);
    EXPECT_EQ((*clients)[1].client->get_client_id(), first_id);

    auto result = service->InnerGetWriteRoute(request, *clients);
    ASSERT_TRUE(result.has_value());
    ASSERT_EQ(result->size(), 1);
    EXPECT_EQ(result->front().client_id, first_id);

    ASSERT_EQ(second->Heartbeat().second, P2PClientStatus::HEALTH);
    result = service->InnerGetWriteRoute(request, *clients);
    ASSERT_TRUE(result.has_value());
    ASSERT_EQ(result->size(), 1);
    EXPECT_EQ(result->front().client_id, second_id);

    request.object_size = 128;
    result = service->InnerGetWriteRoute(request, *clients);
    ASSERT_TRUE(result.has_value());
    ASSERT_EQ(result->size(), 1);
    EXPECT_EQ(result->front().client_id, first_id);
}

TEST_F(P2PMasterServiceTest, WriteRouteClientsApplyLocalRemoteConfig) {
    auto service = CreateService();
    service->GetClientManager().Stop();
    const UUID local_id{1, 1};
    const UUID remote_id{2, 2};
    auto local = MakeP2PSegment("local", 1000);
    local.usage = 200;
    auto remote = MakeP2PSegment("remote", 2000);
    remote.usage = 1000;
    auto excluded = MakeP2PSegment("excluded", 10000, {"skip"});
    RegisterP2PClient(*service, local_id, {local});
    RegisterP2PClient(*service, remote_id, {remote});
    RegisterP2PClient(*service, UUID{3, 3}, {excluded});

    P2PWriteRouteConfig config;
    config.tag_filters = {"skip"};
    config.remote_weight = 0;
    auto clients = service->GetWriteRouteClients(local_id, config);
    ASSERT_TRUE(clients.has_value());
    ASSERT_EQ(clients->size(), 1);
    EXPECT_EQ(clients->front().client->get_client_id(), local_id);
    EXPECT_DOUBLE_EQ(clients->front().candidate.score, 0.8);

    config.remote_weight = 1;
    clients = service->GetWriteRouteClients(local_id, config);
    ASSERT_TRUE(clients.has_value());
    ASSERT_EQ(clients->size(), 1);
    EXPECT_EQ(clients->front().client->get_client_id(), remote_id);
    EXPECT_DOUBLE_EQ(clients->front().candidate.score, 0.5);

    config.remote_weight = 0.25;
    clients = service->GetWriteRouteClients(local_id, config);
    ASSERT_TRUE(clients.has_value());
    ASSERT_EQ(clients->size(), 2);
    for (const auto& entry : *clients) {
        EXPECT_DOUBLE_EQ(entry.candidate.score,
                         entry.client->get_client_id() == local_id
                             ? 0.8 * 0.75
                             : 0.5 * 0.25);
    }
}

// ============================================================
// PublishRoute Tests
// ============================================================

TEST_F(P2PMasterServiceTest, AddReplicaBasic) {
    auto service = CreateService();
    auto seg = MakeP2PSegment("seg1", kDefaultSegmentSize, {}, 1);
    auto client_id = generate_uuid();
    RegisterP2PClient(*service, client_id, {seg}, "127.0.0.1", 50051);

    // Add a replica
    P2PPublishRouteRequest req;
    req.key = "key1";
    req.object_size = 1024;
    req.client_id = client_id;
    req.segment_id = seg.id;
    auto res = service->PublishRoute(req);
    ASSERT_TRUE(res.has_value());

    // Verify it shows up in GetReadRoute
    auto get_res = service->GetReadRoute(req.key);
    ASSERT_TRUE(get_res.has_value());
    EXPECT_EQ(1, get_res.value().size());

    auto& desc = get_res.value()[0];
    EXPECT_EQ(client_id, desc.client_id);
    EXPECT_EQ(seg.id, desc.segment_id);
}

TEST_F(P2PMasterServiceTest, AddReplicaDuplicate) {
    auto service = CreateService();
    auto seg = MakeP2PSegment();
    auto client_id = generate_uuid();
    RegisterP2PClient(*service, client_id, {seg}, "127.0.0.1", 50051);

    P2PPublishRouteRequest req;
    req.key = "key1";
    req.object_size = 1024;
    req.client_id = client_id;
    req.segment_id = seg.id;

    // First add
    auto res1 = service->PublishRoute(req);
    ASSERT_TRUE(res1.has_value());

    // Duplicate add
    auto res2 = service->PublishRoute(req);
    EXPECT_FALSE(res2.has_value());
    EXPECT_EQ(ErrorCode::REPLICA_ALREADY_EXISTS, res2.error());
}

TEST_F(P2PMasterServiceTest, AddReplicaRejectsObjectSizeMismatch) {
    auto service = CreateService();
    auto seg1 = MakeP2PSegment("seg1");
    auto seg2 = MakeP2PSegment("seg2");
    auto client1 = generate_uuid();
    auto client2 = generate_uuid();
    RegisterP2PClient(*service, client1, {seg1}, "10.0.0.1", 50051);
    RegisterP2PClient(*service, client2, {seg2}, "10.0.0.2", 50052);

    AddReplicaHelper(*service, "key1", 1024, client1, seg1.id);
    P2PPublishRouteRequest mismatch;
    mismatch.key = "key1";
    mismatch.object_size = 2048;
    mismatch.client_id = client2;
    mismatch.segment_id = seg2.id;
    auto result = service->PublishRoute(mismatch);
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error(), ErrorCode::INVALID_PARAMS);

    auto route = service->GetReadRoute("key1");
    ASSERT_TRUE(route.has_value());
    EXPECT_EQ(route.value().size(), 1);
    EXPECT_EQ(route.value().front().object_size, 1024);
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

    // GetReadRoute aggregates per client: client1's two segment-replicas
    // collapse to one route, plus client2 -> 2 routes. (Both PublishRoute calls
    // on client1 already succeeded above, confirming multiple replicas per
    // client are allowed.)
    auto get_res = service->GetReadRoute("key1");
    ASSERT_TRUE(get_res.has_value());
    EXPECT_EQ(2, get_res.value().size());

    // The third owner client should exceed the limit.
    P2PPublishRouteRequest req;
    req.key = "key1";
    req.object_size = 1024;
    req.client_id = client3;
    req.segment_id = seg3.id;
    auto res = service->PublishRoute(req);
    EXPECT_FALSE(res.has_value());
    EXPECT_EQ(ErrorCode::REPLICA_NUM_EXCEEDED, res.error());
}

TEST_F(P2PMasterServiceTest, AddReplicaClientNotFound) {
    auto service = CreateService();
    auto seg = MakeP2PSegment();

    P2PPublishRouteRequest req;
    req.key = "key1";
    req.object_size = 1024;
    req.client_id = generate_uuid();  // non-existent
    req.segment_id = seg.id;
    auto res = service->PublishRoute(req);
    EXPECT_FALSE(res.has_value());
    EXPECT_EQ(ErrorCode::CLIENT_NOT_FOUND, res.error());
}

TEST_F(P2PMasterServiceTest, AddReplicaSegmentNotFound) {
    auto service = CreateService();
    auto seg = MakeP2PSegment();
    auto client_id = generate_uuid();
    RegisterP2PClient(*service, client_id, {seg}, "127.0.0.1", 50051);

    P2PPublishRouteRequest req;
    req.key = "key1";
    req.object_size = 1024;
    req.client_id = client_id;
    req.segment_id = generate_uuid();  // non-existent segment
    auto res = service->PublishRoute(req);
    EXPECT_FALSE(res.has_value());
    EXPECT_EQ(ErrorCode::SEGMENT_NOT_FOUND, res.error());
}

// ============================================================
// WithdrawRoute Tests
// ============================================================

TEST_F(P2PMasterServiceTest, RemoveReplicaBasic) {
    auto service = CreateService();
    auto seg = MakeP2PSegment();
    auto client_id = generate_uuid();
    RegisterP2PClient(*service, client_id, {seg}, "127.0.0.1", 50051);

    AddReplicaHelper(*service, "key1", 1024, client_id, seg.id);

    // Remove the replica
    P2PWithdrawRouteRequest req;
    req.key = "key1";
    req.client_id = client_id;
    req.segment_id = seg.id;
    auto res = service->WithdrawRoute(req);
    ASSERT_TRUE(res.has_value());

    // Verify key is gone (last replica removed → object removed)
    auto get_res = service->GetReadRoute("key1");
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
    P2PWithdrawRouteRequest req;
    req.key = "key1";
    req.client_id = client1;
    req.segment_id = seg1.id;
    auto res = service->WithdrawRoute(req);
    ASSERT_TRUE(res.has_value());

    // Object still exists with one replica
    auto get_res = service->GetReadRoute("key1");
    ASSERT_TRUE(get_res.has_value());
    EXPECT_EQ(1, get_res.value().size());
}

TEST_F(P2PMasterServiceTest, RemoveReplicaNotFound) {
    auto service = CreateService();
    auto seg = MakeP2PSegment();
    auto client_id = generate_uuid();
    RegisterP2PClient(*service, client_id, {seg}, "127.0.0.1", 50051);

    AddReplicaHelper(*service, "key1", 1024, client_id, seg.id);

    // Try removing non-existent replica
    P2PWithdrawRouteRequest req;
    req.key = "key1";
    req.client_id = client_id;
    req.segment_id = generate_uuid();  // wrong segment
    auto res = service->WithdrawRoute(req);
    EXPECT_FALSE(res.has_value());
    EXPECT_EQ(ErrorCode::REPLICA_NOT_FOUND, res.error());
}

TEST_F(P2PMasterServiceTest, RemoveReplicaObjectNotFound) {
    auto service = CreateService();

    P2PWithdrawRouteRequest req;
    req.key = "non_existent_key";
    req.client_id = generate_uuid();
    req.segment_id = generate_uuid();
    auto res = service->WithdrawRoute(req);
    EXPECT_FALSE(res.has_value());
    EXPECT_EQ(ErrorCode::OBJECT_NOT_FOUND, res.error());
}

TEST_F(P2PMasterServiceTest, BatchSyncRoutesAppliesMixedOperations) {
    auto service = CreateService();
    auto segment = MakeP2PSegment();
    auto client_id = generate_uuid();
    RegisterP2PClient(*service, client_id, {segment});
    AddReplicaHelper(*service, "existing", 1024, client_id, segment.id);

    P2PBatchSyncRoutesRequest request;
    request.client_id = client_id;
    request.publish_operations = {
        {.key = "new", .object_size = 1024, .segment_id = segment.id},
        {.key = "invalid-segment",
         .object_size = 1024,
         .segment_id = generate_uuid()},
        {.key = "cycle", .object_size = 1024, .segment_id = segment.id},
    };
    request.withdraw_operations = {
        {.key = "existing", .segment_id = segment.id},
        {.key = "missing", .segment_id = segment.id},
        {.key = "cycle", .segment_id = segment.id},
    };

    auto response = service->BatchSyncRoutes(request);
    EXPECT_EQ(response.publish_results,
              (std::vector<ErrorCode>{ErrorCode::OK,
                                      ErrorCode::SEGMENT_NOT_FOUND,
                                      ErrorCode::OK}));
    EXPECT_EQ(response.withdraw_results,
              (std::vector<ErrorCode>{ErrorCode::OK, ErrorCode::OK,
                                      ErrorCode::OK}));
    EXPECT_TRUE(*service->ExistKey("new"));
    EXPECT_FALSE(*service->ExistKey("existing"));
    EXPECT_FALSE(*service->ExistKey("cycle"));
}

TEST_F(P2PMasterServiceTest, BatchSyncRoutesPreservesDuplicateErrorsAcrossShards) {
    auto service = CreateService();
    const auto client = generate_uuid();
    const auto segment = MakeP2PSegment();
    RegisterP2PClient(*service, client, {segment});
    P2PBatchSyncRoutesRequest request;
    request.client_id = client;
    auto response = service->BatchSyncRoutes(request);
    EXPECT_TRUE(response.publish_results.empty());
    EXPECT_TRUE(response.withdraw_results.empty());

    // Pick two keys in the same shard and a third in another shard.
    const std::string first = "batch-shard-key";
    std::string same, other;
    for (size_t i = 0; i < 100000 && (same.empty() || other.empty()); ++i) {
        std::string key = "batch-shard-" + std::to_string(i);
        if (service->GetRouteShardIndex(key) == service->GetRouteShardIndex(first)) {
            if (same.empty()) same = key;
        } else if (other.empty()) {
            other = key;
        }
    }
    ASSERT_FALSE(same.empty());
    ASSERT_FALSE(other.empty());
    request.publish_operations = {
        {.key = first, .object_size = 64, .segment_id = segment.id},
        {.key = other, .object_size = 64, .segment_id = segment.id},
        {.key = first, .object_size = 64, .segment_id = segment.id},
        {.key = same, .object_size = 64, .segment_id = segment.id},
        {.key = first, .object_size = 65, .segment_id = segment.id},
    };
    response = service->BatchSyncRoutes(request);
    EXPECT_EQ(response.publish_results,
              (std::vector<ErrorCode>{ErrorCode::OK, ErrorCode::OK,
                                      ErrorCode::REPLICA_ALREADY_EXISTS,
                                      ErrorCode::OK, ErrorCode::INVALID_PARAMS}));
    EXPECT_TRUE(response.withdraw_results.empty());
    EXPECT_EQ(service->GetKeyCount(), 3u);
    auto route = service->GetReadRoute(first);
    ASSERT_TRUE(route.has_value());
    ASSERT_EQ(route->size(), 1u);
    EXPECT_EQ(route->front().object_size, 64u);

    request.publish_operations.clear();
    request.withdraw_operations = {
        {.key = other, .segment_id = segment.id},
        {.key = first, .segment_id = segment.id},
        {.key = first, .segment_id = segment.id},
        {.key = same, .segment_id = segment.id},
    };
    for (int replay = 0; replay < 2; ++replay) {
        response = service->BatchSyncRoutes(request);
        EXPECT_TRUE(response.publish_results.empty());
        EXPECT_EQ(response.withdraw_results,
                  std::vector<ErrorCode>(request.withdraw_operations.size(),
                                          ErrorCode::OK));
        EXPECT_EQ(service->GetKeyCount(), 0u);
    }
}

TEST_F(P2PMasterServiceTest, RegexAndRoutesRemainConsistentDuringClientRemoval) {
    auto service = CreateService();
    service->GetClientManager().Stop();
    auto& metrics = P2PMasterMetricManager::instance();
    const auto initial_keys = metrics.get_key_count();
    const UUID retiring{1, 1}, writer{2, 2};
    auto old_segment = MakeP2PSegment("retiring");
    auto live_segment = MakeP2PSegment("writer");
    RegisterP2PClient(*service, retiring, {old_segment});
    RegisterP2PClient(*service, writer, {live_segment});
    AddReplicaHelper(*service, "live", 64, retiring, old_segment.id);
    AddReplicaHelper(*service, "retained", 64, writer, live_segment.id);
    std::barrier start(4);
    std::jthread mutations([&] {
        start.arrive_and_wait();
        for (int i = 0; i < 128; ++i) {
            auto published = service->PublishRoute(
                {.key = "live", .object_size = 64,
                 .client_id = writer, .segment_id = live_segment.id});
            EXPECT_TRUE(published.has_value());
            auto withdrawn = service->WithdrawRoute(
                {.key = "live", .client_id = writer,
                 .segment_id = live_segment.id});
            if (!withdrawn) {
                EXPECT_TRUE(withdrawn.error() == ErrorCode::OBJECT_NOT_FOUND ||
                            withdrawn.error() == ErrorCode::REPLICA_NOT_FOUND);
            }
        }
    });
    std::jthread reads([&] {
        start.arrive_and_wait();
        for (int i = 0; i < 128; ++i) {
            auto routes = service->GetReadRoute("live");
            if (routes) {
                for (const auto& route : *routes) {
                    EXPECT_EQ(route.object_size, 64u);
                    EXPECT_TRUE(route.client_id == retiring || route.client_id == writer);
                }
            } else {
                EXPECT_TRUE(routes.error() == ErrorCode::OBJECT_NOT_FOUND ||
                            routes.error() == ErrorCode::REPLICA_IS_NOT_READY);
            }
            auto matched = service->GetReadRouteByRegex("^live$");
            EXPECT_TRUE(matched.has_value());
            auto removed = service->RemoveByRegex("^live$");
            EXPECT_TRUE(removed.has_value());
        }
    });
    std::jthread lifecycle([&] {
        start.arrive_and_wait();
        EXPECT_TRUE(service->UnmountSegment(old_segment.id, retiring).has_value());
        EXPECT_TRUE(service->UnregisterClient(retiring).has_value());
    });
    start.arrive_and_wait();
    mutations.join();
    reads.join();
    lifecycle.join();
    EXPECT_EQ(service->GetKeyCount(), 1u);
    EXPECT_EQ(metrics.get_key_count(), initial_keys + 1);
    AddReplicaHelper(*service, "live", 64, writer, live_segment.id);
    ASSERT_TRUE(service->UnregisterClient(writer).has_value());
    EXPECT_EQ(service->GetKeyCount(), 0u);
    EXPECT_EQ(metrics.get_key_count(), initial_keys);
}

TEST_F(P2PMasterServiceTest, RouteShardLockSerializesConcurrentMutations) {
    constexpr size_t kReplicaCount = 16;
    auto service = CreateService();
    auto client_id = generate_uuid();
    std::vector<P2PSegment> segments;
    segments.reserve(kReplicaCount);
    for (size_t index = 0; index < kReplicaCount; ++index) {
        segments.push_back(MakeP2PSegment("segment-" +
                                         std::to_string(index)));
    }
    RegisterP2PClient(*service, client_id, segments);

    const std::string key = "concurrent-route";
    std::vector<ErrorCode> errors(kReplicaCount, ErrorCode::INTERNAL_ERROR);
    std::vector<std::thread> workers;
    workers.reserve(kReplicaCount);
    for (size_t index = 0; index < kReplicaCount; ++index) {
        workers.emplace_back([&, index] {
            P2PPublishRouteRequest request;
            request.key = key;
            request.object_size = 1024;
            request.client_id = client_id;
            request.segment_id = segments[index].id;
            auto result = service->PublishRoute(request);
            errors[index] =
                result.has_value() ? ErrorCode::OK : result.error();
        });
    }
    for (auto& worker : workers) {
        worker.join();
    }
    EXPECT_TRUE(std::all_of(errors.begin(), errors.end(), [](ErrorCode error) {
        return error == ErrorCode::OK;
    }));
    EXPECT_TRUE(*service->ExistKey(key));

    std::fill(errors.begin(), errors.end(), ErrorCode::INTERNAL_ERROR);
    workers.clear();
    for (size_t index = 0; index < kReplicaCount; ++index) {
        workers.emplace_back([&, index] {
            P2PWithdrawRouteRequest request;
            request.key = key;
            request.client_id = client_id;
            request.segment_id = segments[index].id;
            auto result = service->WithdrawRoute(request);
            errors[index] =
                result.has_value() ? ErrorCode::OK : result.error();
        });
    }
    for (auto& worker : workers) {
        worker.join();
    }
    EXPECT_TRUE(std::all_of(errors.begin(), errors.end(), [](ErrorCode error) {
        return error == ErrorCode::OK;
    }));
    EXPECT_FALSE(*service->ExistKey(key));
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
    ASSERT_TRUE(service->GetReadRoute("key1").has_value());
    ASSERT_TRUE(service->QuerySegments(seg.name).has_value());
    ASSERT_NE(service->GetClientManager().GetClient(client_id), nullptr);

    // Unregister cascades: segment unmount -> replica/object removal.
    auto res = service->UnregisterClient(client_id);
    ASSERT_TRUE(res.has_value());

    EXPECT_EQ(service->GetClientManager().GetClient(client_id), nullptr);
    EXPECT_FALSE(service->QuerySegments(seg.name).has_value());
    auto get_res = service->GetReadRoute("key1");
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
    ASSERT_TRUE(service->UnregisterClient(client1).has_value());

    EXPECT_EQ(service->GetClientManager().GetClient(client1), nullptr);
    EXPECT_NE(service->GetClientManager().GetClient(client2), nullptr);

    auto get_res = service->GetReadRoute("key1");
    ASSERT_TRUE(get_res.has_value());
    EXPECT_EQ(1, get_res.value().size());
    EXPECT_EQ(client2,
              get_res.value()[0].client_id);
}

TEST_F(P2PMasterServiceTest,
       UnregisterClientDoesNotCleanSameSegmentIdOnOtherClient) {
    auto service = CreateService();
    const UUID shared_segment_id = generate_uuid();
    auto seg1 = MakeP2PSegment("seg1");
    auto seg2 = MakeP2PSegment("seg2");
    seg1.id = shared_segment_id;
    seg2.id = shared_segment_id;
    auto client1 = generate_uuid();
    auto client2 = generate_uuid();
    RegisterP2PClient(*service, client1, {seg1}, "10.0.0.1", 50051);
    RegisterP2PClient(*service, client2, {seg2}, "10.0.0.2", 50052);
    AddReplicaHelper(*service, "key1", 1024, client1, shared_segment_id);
    AddReplicaHelper(*service, "key1", 1024, client2, shared_segment_id);

    ASSERT_TRUE(service->UnregisterClient(client1).has_value());

    auto route = service->GetReadRoute("key1");
    ASSERT_TRUE(route.has_value());
    ASSERT_EQ(route.value().size(), 1);
    EXPECT_EQ(route.value().front().client_id,
              client2);
}

TEST_F(P2PMasterServiceTest, UnregisterClientIdempotent) {
    auto service = CreateService();
    auto seg = MakeP2PSegment();
    auto client_id = generate_uuid();
    RegisterP2PClient(*service, client_id, {seg}, "127.0.0.1", 50051);

    ASSERT_TRUE(service->UnregisterClient(client_id).has_value());
    // Second call: client already gone -> still OK (idempotent).
    EXPECT_TRUE(service->UnregisterClient(client_id).has_value());
}

TEST_F(P2PMasterServiceTest, CapturedClientCannotPublishAfterUnregister) {
    auto service = CreateService();
    service->GetClientManager().Stop();
    auto segment = MakeP2PSegment("segment");
    const UUID client_id{1, 1};
    RegisterP2PClient(*service, client_id, {segment});
    auto old_client = service->GetClientManager().GetClient(client_id);
    ASSERT_NE(old_client, nullptr);
    AddReplicaHelper(*service, "existing", 64, client_id, segment.id);
    ASSERT_TRUE(service->UnregisterClient(client_id).has_value());

    // Model requests that captured the old shared_ptr before unregister and
    // resume after its cleanup has completed.
    auto mount = old_client->MountSegment(segment);
    ASSERT_FALSE(mount.has_value());
    EXPECT_EQ(mount.error(), ErrorCode::CLIENT_UNHEALTHY);
    auto publish = service->InnerPublishRoute("late", client_id, segment.id,
                                               64, old_client);
    ASSERT_FALSE(publish.has_value());
    EXPECT_EQ(publish.error(), ErrorCode::CLIENT_UNHEALTHY);
    EXPECT_EQ(old_client->GetAvailableCapacity(), 0);
    EXPECT_EQ(service->GetKeyCount(), 0);
    EXPECT_FALSE(*service->ExistKey("late"));
    EXPECT_FALSE(*service->ExistKey("existing"));
}

// ============================================================
// GetReadRoute + FilterReplicas Tests
// ============================================================

TEST_F(P2PMasterServiceTest, GetReplicaListBasic) {
    auto service = CreateService();
    auto seg = MakeP2PSegment("seg", kDefaultSegmentSize, {"fast"}, 5,
                              MemoryType::DRAM);
    auto client_id = generate_uuid();
    RegisterP2PClient(*service, client_id, {seg}, "127.0.0.1", 50051);
    AddReplicaHelper(*service, "key1", 1024, client_id, seg.id);

    auto res = service->GetReadRoute("key1");
    ASSERT_TRUE(res.has_value());
    EXPECT_EQ(1, res.value().size());
}

TEST_F(P2PMasterServiceTest, GetReplicaListNotFound) {
    auto service = CreateService();
    auto res = service->GetReadRoute("non_existent");
    EXPECT_FALSE(res.has_value());
    EXPECT_EQ(ErrorCode::OBJECT_NOT_FOUND, res.error());
}

TEST_F(P2PMasterServiceTest, GetReadRouteByRegexReturnsMatchingRoutes) {
    auto service = CreateService();
    auto segment = MakeP2PSegment();
    auto client_id = generate_uuid();
    RegisterP2PClient(*service, client_id, {segment});
    AddReplicaHelper(*service, "prefix-a", 1024, client_id, segment.id);
    AddReplicaHelper(*service, "prefix-b", 1024, client_id, segment.id);
    AddReplicaHelper(*service, "other", 1024, client_id, segment.id);

    auto result = service->GetReadRouteByRegex("^prefix-");
    ASSERT_TRUE(result.has_value());
    EXPECT_EQ(result->size(), 2);
    EXPECT_TRUE(result->contains("prefix-a"));
    EXPECT_TRUE(result->contains("prefix-b"));
    EXPECT_FALSE(result->contains("other"));
}

TEST_F(P2PMasterServiceTest, RegexHandlesEmptyInvalidAndUnmatchedPatterns) {
    auto service = CreateService();
    auto empty = service->GetReadRouteByRegex(".*");
    ASSERT_TRUE(empty.has_value());
    EXPECT_TRUE(empty->empty());
    auto removed = service->RemoveByRegex(".*");
    ASSERT_TRUE(removed.has_value());
    EXPECT_EQ(*removed, 0);

    const auto client = generate_uuid();
    const auto segment = MakeP2PSegment();
    RegisterP2PClient(*service, client, {segment});
    AddReplicaHelper(*service, "retained", 64, client, segment.id);
    auto invalid_read = service->GetReadRouteByRegex("[");
    ASSERT_FALSE(invalid_read.has_value());
    EXPECT_EQ(invalid_read.error(), ErrorCode::INVALID_PARAMS);
    auto invalid_remove = service->RemoveByRegex("[");
    ASSERT_FALSE(invalid_remove.has_value());
    EXPECT_EQ(invalid_remove.error(), ErrorCode::INVALID_PARAMS);
    auto unmatched = service->GetReadRouteByRegex("^absent$");
    ASSERT_TRUE(unmatched.has_value());
    EXPECT_TRUE(unmatched->empty());
    removed = service->RemoveByRegex("^absent$");
    ASSERT_TRUE(removed.has_value());
    EXPECT_EQ(*removed, 0);
    EXPECT_EQ(service->GetKeyCount(), 1);
    EXPECT_EQ(service->RemoveAll(), 1);
}

TEST_F(P2PMasterServiceTest, RegexPreservesLongKeysAndAllLocationsAcrossShards) {
    auto service = CreateService();
    auto& metrics = P2PMasterMetricManager::instance();
    const auto initial_key_count = metrics.get_key_count();
    const auto client = generate_uuid();
    const auto first = MakeP2PSegment("first");
    const auto second = MakeP2PSegment("second");
    RegisterP2PClient(*service, client, {first, second});
    std::vector<std::string> keys;
    std::unordered_set<size_t> shards;
    for (size_t i = 0; i < 128; ++i) {
        keys.push_back("match-" + std::string(256, 'x') + std::to_string(i));
        shards.insert(service->GetRouteShardIndex(keys.back()));
        AddReplicaHelper(*service, keys.back(), 64, client, first.id);
        AddReplicaHelper(*service, keys.back(), 64, client, second.id);
    }
    ASSERT_GT(shards.size(), 1u);
    AddReplicaHelper(*service, "retained", 64, client, first.id);

    auto routes = service->GetReadRouteByRegex("^match-");
    ASSERT_TRUE(routes.has_value());
    ASSERT_EQ(routes->size(), keys.size());
    for (const auto& key : keys) {
        ASSERT_TRUE(routes->contains(key));
        const auto& locations = routes->at(key);
        ASSERT_EQ(locations.size(), 2u);
        std::unordered_set<UUID, boost::hash<UUID>> segments;
        for (const auto& route : locations) {
            EXPECT_EQ(route.client_id, client);
            EXPECT_EQ(route.object_size, 64u);
            EXPECT_EQ(route.ip_address, "127.0.0.1");
            EXPECT_EQ(route.rpc_port, 50051);
            segments.insert(route.segment_id);
        }
        EXPECT_TRUE(segments.contains(first.id));
        EXPECT_TRUE(segments.contains(second.id));
    }
    auto removed = service->RemoveByRegex("^match-");
    ASSERT_TRUE(removed.has_value());
    EXPECT_EQ(*removed, static_cast<long>(keys.size()));
    EXPECT_EQ(service->GetKeyCount(), 1u);
    EXPECT_EQ(metrics.get_key_count(), initial_key_count + 1);
    removed = service->RemoveByRegex("^match-");
    ASSERT_TRUE(removed.has_value());
    EXPECT_EQ(*removed, 0);
    removed = service->RemoveByRegex("");
    ASSERT_TRUE(removed.has_value());
    EXPECT_EQ(*removed, 1);
    EXPECT_EQ(service->GetKeyCount(), 0u);
    EXPECT_EQ(metrics.get_key_count(), initial_key_count);
    auto all = service->GetReadRouteByRegex("");
    ASSERT_TRUE(all.has_value());
    EXPECT_TRUE(all->empty());
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
    P2PReadRouteConfig config;
    config.max_candidates = 10;
    config.tag_filters = {"memory"};
    config.priority_limit = 2;

    auto res = service->GetReadRoute("key1", config);
    ASSERT_TRUE(res.has_value());
    ASSERT_EQ(res->size(), 1u);
    EXPECT_EQ(res->front().client_id, client2);
    config.priority_limit = 3;
    res = service->GetReadRoute("key1", config);
    ASSERT_FALSE(res.has_value());
    EXPECT_EQ(res.error(), ErrorCode::REPLICA_IS_NOT_READY);
    config.tag_filters.clear();
    config.priority_limit = 10;
    res = service->GetReadRoute("key1", config);
    ASSERT_TRUE(res.has_value());
    ASSERT_EQ(res->size(), 1u);
    EXPECT_EQ(res->front().client_id, client1);
    config.priority_limit = 11;
    res = service->GetReadRoute("key1", config);
    ASSERT_FALSE(res.has_value());
    EXPECT_EQ(res.error(), ErrorCode::REPLICA_IS_NOT_READY);
}

TEST_F(P2PMasterServiceTest, ReadRouteChoosesEligibleTierAndRechecksHealth) {
    auto service = CreateService();
    service->GetClientManager().Stop();
    const auto client_id = generate_uuid();
    auto high = MakeP2PSegment("high", 4096, {"skip"}, 10);
    auto low = MakeP2PSegment("low", 4096, {}, 5);
    RegisterP2PClient(*service, client_id, {high, low});
    AddReplicaHelper(*service, "key", 64, client_id, high.id);
    AddReplicaHelper(*service, "key", 64, client_id, low.id);
    P2PReadRouteConfig config;
    config.tag_filters = {"skip"};
    auto route = service->GetReadRoute("key", config);
    ASSERT_TRUE(route.has_value());
    ASSERT_EQ(route->size(), 1u);
    EXPECT_EQ(route->front().segment_id, low.id);
    config.tag_filters.clear();
    route = service->GetReadRoute("key", config);
    ASSERT_TRUE(route.has_value());
    ASSERT_EQ(route->size(), 1u);
    EXPECT_EQ(route->front().segment_id, high.id);
    auto client = service->GetClientManager().GetClient(client_id);
    ASSERT_NE(client, nullptr);
    client->health_state_.last_heartbeat = std::chrono::steady_clock::now() -
        std::chrono::seconds(P2PClientMeta::disconnect_timeout_sec_ + 1);
    ASSERT_EQ(client->CheckHealth().second, P2PClientStatus::DISCONNECTION);
    route = service->GetReadRoute("key", config);
    ASSERT_FALSE(route.has_value());
    EXPECT_EQ(route.error(), ErrorCode::REPLICA_IS_NOT_READY);
    ASSERT_EQ(client->Heartbeat().second, P2PClientStatus::HEALTH);
    EXPECT_TRUE(service->GetReadRoute("key", config).has_value());
    EXPECT_EQ(service->RemoveAll(), 1);
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
    P2PReadRouteConfig config;
    config.max_candidates = 3;
    config.priority_limit = 0;

    auto res = service->GetReadRoute("key1", config);
    ASSERT_TRUE(res.has_value());
    ASSERT_EQ(res->size(), 3u);
    for (size_t i = 0; i < res->size(); ++i) {
        EXPECT_EQ((*res)[i].client_id, client_ids[4 - i]);
    }
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

    auto res = service->GetReadRoute("key1");
    ASSERT_TRUE(res.has_value());
    // Two segment-replicas on the same client collapse to one route.
    EXPECT_EQ(1, res.value().size());
    EXPECT_EQ(client_id,
              res.value()[0].client_id);
    // Representative is the highest-priority segment.
    EXPECT_EQ(seg_hi.id,
              res.value()[0].segment_id);
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

TEST_F(P2PMasterServiceTest, RemoveByRegexRemovesOnlyMatchingRoutes) {
    auto service = CreateService();
    auto segment = MakeP2PSegment();
    auto client_id = generate_uuid();
    RegisterP2PClient(*service, client_id, {segment});
    AddReplicaHelper(*service, "prefix-a", 1024, client_id, segment.id);
    AddReplicaHelper(*service, "prefix-b", 1024, client_id, segment.id);
    AddReplicaHelper(*service, "other", 1024, client_id, segment.id);

    auto result = service->RemoveByRegex("^prefix-");
    ASSERT_TRUE(result.has_value());
    EXPECT_EQ(*result, 2);
    EXPECT_FALSE(*service->ExistKey("prefix-a"));
    EXPECT_FALSE(*service->ExistKey("prefix-b"));
    EXPECT_TRUE(*service->ExistKey("other"));
}

TEST_F(P2PMasterServiceTest, GetKeyCountEmpty) {
    auto service = CreateService();
    EXPECT_EQ(0u, service->GetKeyCount());
}

TEST_F(P2PMasterServiceTest, GetKeyCountsDistinctKeys) {
    auto service = CreateService();
    auto seg1 = MakeP2PSegment("seg1");
    auto seg2 = MakeP2PSegment("seg2");
    auto client_id = generate_uuid();
    RegisterP2PClient(*service, client_id, {seg1, seg2}, "127.0.0.1", 50051);

    // Two replicas of the same key on different segments count only once.
    AddReplicaHelper(*service, "key1", 1024, client_id, seg1.id);
    AddReplicaHelper(*service, "key1", 1024, client_id, seg2.id);
    AddReplicaHelper(*service, "key2", 1024, client_id, seg1.id);

    EXPECT_EQ(2u, service->GetKeyCount());

    auto all_keys = service->GetAllKeys();
    ASSERT_TRUE(all_keys.has_value());
    EXPECT_EQ(2u, all_keys.value().size());
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
    P2PGetWriteRouteRequest w_req;
    w_req.key = "data_001";
    w_req.client_id = reader_id;  // reader asks for write route
    w_req.object_size = 4096;
    w_req.config.max_candidates = 1;

    auto w_res = service->GetWriteRoute(w_req);
    ASSERT_TRUE(w_res.has_value());
    EXPECT_EQ(1, w_res.value().size());

    auto& candidate = w_res.value()[0];
    EXPECT_EQ(writer_id, candidate.client_id);

    // Step 2: Add replica (simulate write completion). The route is
    // client-only; the client registers the concrete segment it actually wrote
    // (here seg.id).
    P2PPublishRouteRequest a_req;
    a_req.key = "data_001";
    a_req.object_size = 4096;
    a_req.client_id = candidate.client_id;
    a_req.segment_id = seg.id;
    auto a_res = service->PublishRoute(a_req);
    ASSERT_TRUE(a_res.has_value());

    // Step 3: Read — GetReadRoute
    auto r_res = service->GetReadRoute("data_001");
    ASSERT_TRUE(r_res.has_value());
    EXPECT_EQ(1, r_res.value().size());

    // Step 4: Remove
    P2PWithdrawRouteRequest rm_req;
    rm_req.key = "data_001";
    rm_req.client_id = candidate.client_id;
    rm_req.segment_id = seg.id;
    auto rm_res = service->WithdrawRoute(rm_req);
    ASSERT_TRUE(rm_res.has_value());

    // Verify gone
    auto r_res2 = service->GetReadRoute("data_001");
    EXPECT_FALSE(r_res2.has_value());
    EXPECT_EQ(ErrorCode::OBJECT_NOT_FOUND, r_res2.error());
}

// ============================================================
// CompleteRouteSync Tests
// ============================================================

TEST_F(P2PMasterServiceTest, SetSyncCompletedSuccess) {
    auto service = CreateService();
    auto seg = MakeP2PSegment();
    auto client_id = generate_uuid();
    RegisterP2PClient(*service, client_id, {seg}, "127.0.0.1", 50051);

    // Registration marks the client as syncing.
    auto p2p_client = service->client_manager_->GetClient(client_id);
    ASSERT_NE(p2p_client, nullptr);
    EXPECT_TRUE(p2p_client->IsSyncing());

    // CompleteRouteSync should clear is_syncing
    auto result = service->CompleteRouteSync(client_id);
    ASSERT_TRUE(result.has_value());
    EXPECT_FALSE(p2p_client->IsSyncing());
}

TEST_F(P2PMasterServiceTest, SetSyncCompletedClientNotFound) {
    auto service = CreateService();
    auto result = service->CompleteRouteSync(generate_uuid());
    EXPECT_FALSE(result.has_value());
    EXPECT_EQ(result.error(), ErrorCode::CLIENT_NOT_FOUND);
}

TEST_F(P2PMasterServiceTest, SetSyncCompletedIdempotent) {
    auto service = CreateService();
    auto seg = MakeP2PSegment();
    auto client_id = generate_uuid();
    RegisterP2PClient(*service, client_id, {seg}, "127.0.0.1", 50051);

    // Call twice — should succeed both times
    EXPECT_TRUE(service->CompleteRouteSync(client_id).has_value());
    EXPECT_TRUE(service->CompleteRouteSync(client_id).has_value());

    auto p2p_client = service->client_manager_->GetClient(client_id);
    EXPECT_FALSE(p2p_client->IsSyncing());
}

}  // namespace mooncake::test
