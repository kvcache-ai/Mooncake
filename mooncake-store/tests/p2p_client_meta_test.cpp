#include <gtest/gtest.h>
#include <glog/logging.h>
#include <thread>
#include <atomic>
#include <random>
#define private public
#define protected public
#include "p2p_client_meta.h"
#undef private
#undef protected

namespace mooncake {

class P2PClientMetaTest : public ::testing::Test {
   protected:
    void SetUp() override {
        google::InitGoogleLogging("P2PClientMetaTest");
        FLAGS_logtostderr = 1;
        // Set short timeouts for health state testing
        ClientMeta::SetTimeouts(2, 5);
    }

    void TearDown() override { google::ShutdownGoogleLogging(); }

    std::shared_ptr<P2PClientMeta> CreateMeta(
        UUID id = {12345, 67890}, const std::string& ip = "192.168.1.1",
        uint16_t rpc_port = 50051) {
        return std::make_shared<P2PClientMeta>(id, ip, rpc_port);
    }

    mooncake::Segment MakeP2PSegment(mooncake::UUID id = {1, 1},
                                     const std::string& name = "seg1",
                                     size_t size = 1024 * 1024,
                                     int priority = 1, size_t usage = 0) {
        mooncake::Segment seg;
        seg.id = id;
        seg.name = name;
        seg.size = size;
        seg.extra = mooncake::P2PSegmentExtraData{
            .priority = priority,
            .tags = {},
            .memory_type = mooncake::MemoryType::DRAM,
            .usage = usage,
        };
        return seg;
    }

    void CompareSegment(const mooncake::Segment& actual,
                        const mooncake::Segment& expected) {
        EXPECT_EQ(actual.id, expected.id);
        EXPECT_EQ(actual.name, expected.name);
        EXPECT_EQ(actual.size, expected.size);
        EXPECT_EQ(actual.GetP2PExtra().priority,
                  expected.GetP2PExtra().priority);
        EXPECT_EQ(actual.GetP2PExtra().memory_type,
                  expected.GetP2PExtra().memory_type);
    }
};

class P2PClientMetaExtendedTest : public P2PClientMetaTest {
   protected:
    std::shared_ptr<mooncake::P2PClientMeta> CreateExtendedMeta(
        mooncake::UUID id = {12345, 67890},
        const std::string& ip = "192.168.1.1", uint16_t rpc_port = 50051) {
        return std::make_shared<mooncake::P2PClientMeta>(id, ip, rpc_port);
    }
};

// ============================================================
// Mount/Unmount (testing parent ClientMeta interface)
// ============================================================

TEST_F(P2PClientMetaTest, MountSegmentSuccess) {
    auto meta = CreateMeta();
    auto seg = MakeP2PSegment();
    meta->Heartbeat();

    auto res = meta->MountSegment(seg);
    EXPECT_TRUE(res.has_value());

    // Verification: Query the segment to ensure it exists and matches
    auto query_res = meta->QuerySegment(seg.id);
    ASSERT_TRUE(query_res.has_value());
    CompareSegment(**query_res, seg);
}

TEST_F(P2PClientMetaTest, MountDuplicateSegmentIsIdempotent) {
    auto meta = CreateMeta();
    auto seg = MakeP2PSegment();
    meta->Heartbeat();
    ASSERT_TRUE(meta->MountSegment(seg).has_value());

    // ClientMeta absorbs SEGMENT_ALREADY_EXISTS, returns OK (idempotent)
    auto res = meta->MountSegment(seg);
    EXPECT_TRUE(res.has_value());

    // Verification: Segment should still exist
    auto query_res = meta->QuerySegment(seg.id);
    ASSERT_TRUE(query_res.has_value());
    CompareSegment(**query_res, seg);
}

TEST_F(P2PClientMetaTest, UnmountSegmentSuccess) {
    auto meta = CreateMeta();
    auto seg = MakeP2PSegment();
    meta->Heartbeat();
    ASSERT_TRUE(meta->MountSegment(seg).has_value());

    auto res = meta->UnmountSegment(seg.id);
    EXPECT_TRUE(res.has_value());

    auto query_res = meta->QuerySegment(seg.id);
    EXPECT_FALSE(query_res.has_value());
    EXPECT_EQ(query_res.error(), mooncake::ErrorCode::SEGMENT_NOT_FOUND);
}

TEST_F(P2PClientMetaTest, UnmountNonexistentSegmentIsIdempotent) {
    auto meta = CreateMeta();
    meta->Heartbeat();

    // ClientMeta absorbs SEGMENT_NOT_FOUND, returns OK (idempotent)
    UUID nonexistent = {999, 999};
    auto res = meta->UnmountSegment(nonexistent);
    EXPECT_TRUE(res.has_value());
}

// ============================================================
// Health state checks (testing parent ClientMeta interface)
// ============================================================

TEST_F(P2PClientMetaTest, HealthyAfterHeartbeat) {
    auto meta = CreateMeta();
    meta->Heartbeat();
    EXPECT_TRUE(meta->is_health());
}

TEST_F(P2PClientMetaTest, MountFailsWhenUnhealthy) {
    auto meta = CreateMeta();

    // Explicitly set short timeouts
    constexpr int kDisconnectTimeoutSec = 1;
    constexpr int kCrashTimeoutSec = 2;
    ClientMeta::SetTimeouts(kDisconnectTimeoutSec, kCrashTimeoutSec);

    meta->Heartbeat();
    auto seg = MakeP2PSegment();

    // 1. DISCONNECTION state
    constexpr int kWaitMsDisc = kDisconnectTimeoutSec * 1000 + 100;
    std::this_thread::sleep_for(std::chrono::milliseconds(kWaitMsDisc));
    ASSERT_EQ(meta->CheckHealth().second, ClientStatus::DISCONNECTION);

    auto res_disc = meta->MountSegment(seg);
    EXPECT_FALSE(res_disc.has_value());
    EXPECT_EQ(res_disc.error(), mooncake::ErrorCode::CLIENT_UNHEALTHY);

    // 2. CRASHED state
    constexpr int kWaitMsCrash =
        (kCrashTimeoutSec - kDisconnectTimeoutSec) * 1000 + 100;
    std::this_thread::sleep_for(std::chrono::milliseconds(kWaitMsCrash));
    ASSERT_EQ(meta->CheckHealth().second, ClientStatus::CRASHED);

    auto res_crash = meta->MountSegment(seg);
    EXPECT_FALSE(res_crash.has_value());
    EXPECT_EQ(res_crash.error(), mooncake::ErrorCode::CLIENT_UNHEALTHY);
}

TEST_F(P2PClientMetaExtendedTest, UnmountSegmentCheckHealth) {
    auto meta = CreateExtendedMeta();
    auto seg = MakeP2PSegment();
    constexpr int kDiscTimeout = 1;
    constexpr int kCrashTimeout = 2;
    ClientMeta::SetTimeouts(kDiscTimeout, kCrashTimeout);

    // Case 1: Health -> OK
    meta->Heartbeat();
    ASSERT_TRUE(meta->MountSegment(seg).has_value());
    EXPECT_TRUE(meta->UnmountSegment(seg.id).has_value());

    // Case 2: DISCONNECTION -> Fail
    ASSERT_TRUE(meta->MountSegment(seg).has_value());
    std::this_thread::sleep_for(
        std::chrono::milliseconds(kDiscTimeout * 1000 + 100));
    ASSERT_EQ(meta->CheckHealth().second, ClientStatus::DISCONNECTION);

    auto res = meta->UnmountSegment(seg.id);
    EXPECT_FALSE(res.has_value());
    EXPECT_EQ(res.error(), mooncake::ErrorCode::CLIENT_UNHEALTHY);

    // Case 3: CRASHED -> Fail
    // Wait for crash timeout
    std::this_thread::sleep_for(
        std::chrono::milliseconds((kCrashTimeout - kDiscTimeout) * 1000 + 100));
    ASSERT_EQ(meta->CheckHealth().second, ClientStatus::CRASHED);

    auto res_crash = meta->UnmountSegment(seg.id);
    EXPECT_FALSE(res_crash.has_value());
    EXPECT_EQ(res_crash.error(), mooncake::ErrorCode::CLIENT_UNHEALTHY);
}

// ============================================================
// Segment Management (Get/Query)
// ============================================================

TEST_F(P2PClientMetaTest, GetSegments) {
    auto meta = CreateMeta();
    meta->Heartbeat();
    auto s1 = MakeP2PSegment({1, 1}, "seg1", 1000, 1);
    auto s2 = MakeP2PSegment({2, 2}, "seg2", 2000, 2);
    ASSERT_TRUE(meta->MountSegment(s1).has_value());
    ASSERT_TRUE(meta->MountSegment(s2).has_value());

    auto res = meta->GetSegments();
    ASSERT_TRUE(res.has_value());
    EXPECT_EQ(res->size(), 2);

    for (const auto& s : *res) {
        if (s.id == s1.id) {
            CompareSegment(s, s1);
        } else if (s.id == s2.id) {
            CompareSegment(s, s2);
        }
    }
}

TEST_F(P2PClientMetaTest, QuerySegmentsByName) {
    auto meta = CreateMeta();
    meta->Heartbeat();
    auto s1 = MakeP2PSegment({1, 1}, "test_query_seg", 4096);
    ASSERT_TRUE(meta->MountSegment(s1).has_value());

    // Query by name returns optional<pair<addr, size>>
    auto res = meta->QuerySegments("test_query_seg");
    ASSERT_TRUE(res.has_value());
    EXPECT_EQ(res->second, 4096);

    // Deep verification via QuerySegment(id)
    auto query_full = meta->QuerySegment(s1.id);
    ASSERT_TRUE(query_full.has_value());
    CompareSegment(**query_full, s1);
}

TEST_F(P2PClientMetaTest, QuerySegmentById) {
    auto meta = CreateMeta();
    meta->Heartbeat();
    auto s1 = MakeP2PSegment({3, 3}, "id_query", 8192, 5);
    ASSERT_TRUE(meta->MountSegment(s1).has_value());

    auto res = meta->QuerySegment(s1.id);
    ASSERT_TRUE(res.has_value());
    CompareSegment(**res, s1);
}

// ============================================================
// Callback mechanism
// ============================================================

TEST_F(P2PClientMetaTest, SegmentRemovalCallbackTest) {
    auto meta = CreateMeta();
    meta->Heartbeat();
    auto s1 = MakeP2PSegment();
    ASSERT_TRUE(meta->MountSegment(s1).has_value());

    bool called = false;
    meta->SetSegmentRemovalCallback([&](const mooncake::UUID& id) {
        if (id == s1.id) called = true;
    });

    ASSERT_TRUE(meta->UnmountSegment(s1.id).has_value());
    EXPECT_TRUE(called);
}

// ============================================================
// Health State Machine
// ============================================================

TEST_F(P2PClientMetaExtendedTest, HealthStateMachineTransitions) {
    auto meta = CreateExtendedMeta();
    constexpr int64_t kDisconnectSec = 1;
    constexpr int64_t kCrashSec = 2;
    ClientMeta::SetTimeouts(kDisconnectSec, kCrashSec);

    meta->Heartbeat();
    EXPECT_TRUE(meta->is_health());

    // 1. Wait for DISCONNECTION
    std::this_thread::sleep_for(
        std::chrono::milliseconds(kDisconnectSec * 1000 + 100));
    EXPECT_EQ(meta->CheckHealth().second, ClientStatus::DISCONNECTION);
    EXPECT_FALSE(meta->is_health());

    // 2. Recover via Heartbeat
    auto res = meta->Heartbeat();
    EXPECT_EQ(res.first, ClientStatus::DISCONNECTION);
    EXPECT_EQ(res.second, ClientStatus::HEALTH);
    EXPECT_TRUE(meta->is_health());

    // 3. Wait for CRASHED
    std::this_thread::sleep_for(
        std::chrono::milliseconds(kCrashSec * 1000 + 100));
    EXPECT_EQ(meta->CheckHealth().second, ClientStatus::CRASHED);

    // 4. Irreversible CRASHED
    EXPECT_EQ(meta->Heartbeat().second, ClientStatus::CRASHED);
}

// ============================================================
// UpdateSegmentUsages
// ============================================================

TEST_F(P2PClientMetaTest, UpdateSegmentUsagesBasic) {
    auto meta = CreateMeta();
    meta->Heartbeat();

    auto seg1 = MakeP2PSegment({1, 1}, "seg1", 1000);
    auto seg2 = MakeP2PSegment({2, 2}, "seg2", 2000);
    ASSERT_TRUE(meta->MountSegment(seg1).has_value());
    ASSERT_TRUE(meta->MountSegment(seg2).has_value());

    // Update usages
    std::vector<TierUsageInfo> usages = {
        TierUsageInfo{.segment_id = {1, 1}, .usage = 300},
        TierUsageInfo{.segment_id = {2, 2}, .usage = 500},
    };
    meta->UpdateSegmentUsages(usages);

    // Available = (1000 - 300) + (2000 - 500) = 700 + 1500 = 2200
    EXPECT_EQ(meta->GetAvailableCapacity(), 2200);
}

TEST_F(P2PClientMetaTest, UpdateSegmentUsagesEmpty) {
    auto meta = CreateMeta();
    meta->Heartbeat();

    auto seg = MakeP2PSegment({1, 1}, "seg1", 1000);
    ASSERT_TRUE(meta->MountSegment(seg).has_value());

    // Empty usages → capacity and usage should remain at initial values
    std::vector<TierUsageInfo> usages;
    meta->UpdateSegmentUsages(usages);

    // After mount: capacity = seg.size = 1000, usage = 0
    EXPECT_EQ(meta->GetAvailableCapacity(), 1000);
}

TEST_F(P2PClientMetaTest, UpdateSegmentUsagesFullCapacity) {
    auto meta = CreateMeta();
    meta->Heartbeat();

    auto seg = MakeP2PSegment({1, 1}, "seg1", 1000);
    ASSERT_TRUE(meta->MountSegment(seg).has_value());

    std::vector<TierUsageInfo> usages = {
        TierUsageInfo{.segment_id = {1, 1}, .usage = 1000},
    };
    meta->UpdateSegmentUsages(usages);

    EXPECT_EQ(meta->GetAvailableCapacity(), 0);
}

TEST_F(P2PClientMetaTest, UpdateSegmentUsagesNonexistentSegment) {
    auto meta = CreateMeta();
    meta->Heartbeat();

    auto seg = MakeP2PSegment({1, 1}, "seg1", 1000);
    ASSERT_TRUE(meta->MountSegment(seg).has_value());

    // Update nonexistent segment ID
    std::vector<TierUsageInfo> usages = {
        TierUsageInfo{.segment_id = {999, 999}, .usage = 500},
    };
    auto result = meta->UpdateSegmentUsages(usages);

    ASSERT_EQ(result.sub_results.size(), 1);
    EXPECT_EQ(result.sub_results[0].error, ErrorCode::SEGMENT_NOT_FOUND);
    // Capacity should remain unchanged (1000)
    EXPECT_EQ(meta->GetAvailableCapacity(), 1000);
}

TEST_F(P2PClientMetaTest, UpdateSegmentUsagesExceedSize) {
    auto meta = CreateMeta();
    meta->Heartbeat();

    auto seg = MakeP2PSegment({1, 1}, "seg1", 1000);
    ASSERT_TRUE(meta->MountSegment(seg).has_value());

    // Usage (1500) > Size (1000)
    std::vector<TierUsageInfo> usages = {
        TierUsageInfo{.segment_id = {1, 1}, .usage = 1500},
    };
    auto result = meta->UpdateSegmentUsages(usages);

    ASSERT_EQ(result.sub_results.size(), 1);
    EXPECT_EQ(result.sub_results[0].error, ErrorCode::INVALID_PARAMS);
    // Capacity should remain unchanged (1000)
    EXPECT_EQ(meta->GetAvailableCapacity(), 1000);
}

TEST_F(P2PClientMetaTest, UpdateSegmentUsagesMixedResults) {
    auto meta = CreateMeta();
    meta->Heartbeat();

    auto seg1 = MakeP2PSegment({1, 1}, "seg1", 1000);
    auto seg2 = MakeP2PSegment({2, 2}, "seg2", 2000);
    ASSERT_TRUE(meta->MountSegment(seg1).has_value());
    ASSERT_TRUE(meta->MountSegment(seg2).has_value());

    // Update: one valid, one nonexistent, one exceed size
    std::vector<TierUsageInfo> usages = {
        TierUsageInfo{.segment_id = {1, 1},
                      .usage = 400},  // OK: 1000 - 400 = 600
        TierUsageInfo{.segment_id = {99, 99}, .usage = 100},  // Fail: Not found
        TierUsageInfo{.segment_id = {2, 2},
                      .usage = 2500},  // Fail: Invalid params
    };
    auto result = meta->UpdateSegmentUsages(usages);

    ASSERT_EQ(result.sub_results.size(), 3);
    EXPECT_EQ(result.sub_results[0].error, ErrorCode::OK);
    EXPECT_EQ(result.sub_results[1].error, ErrorCode::SEGMENT_NOT_FOUND);
    EXPECT_EQ(result.sub_results[2].error, ErrorCode::INVALID_PARAMS);

    // Available = (1000 - 400) + (2000 - 0) = 600 + 2000 = 2600
    EXPECT_EQ(meta->GetAvailableCapacity(), 2600);
}

// ============================================================
// QueryIp
// ============================================================

TEST_F(P2PClientMetaTest, QueryIpReturnsAddress) {
    UUID id = {111, 222};
    std::string expected_ip = "192.168.1.100";
    auto meta = CreateMeta(id, expected_ip, 8080);
    meta->Heartbeat();

    auto res = meta->QueryIp(id);
    EXPECT_TRUE(res.has_value());
    ASSERT_FALSE(res.value().empty());
    EXPECT_EQ(res.value()[0], expected_ip);
}

TEST_F(P2PClientMetaExtendedTest, QueryIpCheckHealth) {
    auto meta = CreateExtendedMeta();
    UUID id = meta->get_client_id();
    constexpr int kDiscTimeout = 1;
    constexpr int kCrashTimeout = 2;
    ClientMeta::SetTimeouts(kDiscTimeout, kCrashTimeout);

    meta->Heartbeat();
    auto res_healthy = meta->QueryIp(id);
    EXPECT_TRUE(res_healthy.has_value());
    ASSERT_FALSE(res_healthy->empty());
    EXPECT_EQ(res_healthy.value()[0], "192.168.1.1");

    // 1. DISCONNECTION
    std::this_thread::sleep_for(
        std::chrono::milliseconds(kDiscTimeout * 1000 + 100));
    ASSERT_EQ(meta->CheckHealth().second, ClientStatus::DISCONNECTION);
    auto res_disconnected = meta->QueryIp(id);
    EXPECT_FALSE(res_disconnected.has_value());
    EXPECT_EQ(res_disconnected.error(), mooncake::ErrorCode::CLIENT_UNHEALTHY);

    // 2. CRASHED
    std::this_thread::sleep_for(
        std::chrono::milliseconds((kCrashTimeout - kDiscTimeout) * 1000 + 100));
    ASSERT_EQ(meta->CheckHealth().second, ClientStatus::CRASHED);
    auto res_crashed = meta->QueryIp(id);
    EXPECT_FALSE(res_crashed.has_value());
    EXPECT_EQ(res_crashed.error(), mooncake::ErrorCode::CLIENT_UNHEALTHY);
}

// ============================================================
// CollectWriteRouteCandidates
// ============================================================

TEST_F(P2PClientMetaTest, CollectCandidatesMultipleSegments) {
    UUID client_id = {111, 222};
    auto meta = CreateMeta(client_id, "10.0.0.1", 50051);
    meta->Heartbeat();

    auto s1 = MakeP2PSegment({1, 1}, "seg1", 10000, 5);
    auto s2 = MakeP2PSegment({2, 2}, "seg2", 20000, 3);
    auto s3 = MakeP2PSegment({3, 3}, "seg3", 30000, 4);
    ASSERT_TRUE(meta->MountSegment(s1).has_value());
    ASSERT_TRUE(meta->MountSegment(s2).has_value());
    ASSERT_TRUE(meta->MountSegment(s3).has_value());

    WriteRouteRequest req;
    req.key = "test_key";
    req.client_id = {999, 999};
    req.size = 100;
    req.config.allow_local = true;

    // 1. No filter conditions
    std::vector<WriteCandidate> candidates;
    auto result = meta->CollectWriteRouteCandidates(req, candidates);
    EXPECT_TRUE(result.has_value());
    ASSERT_EQ(candidates.size(), 3);
    // Verify specific segments are returned
    std::set<UUID> returned_ids;
    for (const auto& cand : candidates)
        returned_ids.insert(cand.replica.segment_id);
    EXPECT_TRUE(returned_ids.count(s1.id));
    EXPECT_TRUE(returned_ids.count(s2.id));
    EXPECT_TRUE(returned_ids.count(s3.id));

    // 2. Test max_candidates = 2 with early_return = true (Should return 2)
    // If `early_return`, it will stop when candidates.size() >= max_candidates.
    // Otherwise, it will continue to process all segments.
    // Actually, the `max_candidates` limits the candidates number in the
    // GetWriteRoute() of P2PMasterService, which is the caller of
    // CollectWriteRouteCandidates().
    // However, here we are testing CollectWriteRouteCandidates() and the filter
    // logic is processed in GetWriteRoute(). Thus, if does not set
    // `early_return`, the `max_candidates` is not used.
    req.config.max_candidates = 2;
    req.config.early_return = true;
    candidates.clear();
    result = meta->CollectWriteRouteCandidates(req, candidates);
    EXPECT_TRUE(result.has_value());
    EXPECT_EQ(candidates.size(), 2);
}

TEST_F(P2PClientMetaTest, CollectCandidatesCapacityFilter) {
    UUID client_id = {111, 222};
    auto meta = CreateMeta(client_id, "10.0.0.1", 50051);
    meta->Heartbeat();

    auto s1 = MakeP2PSegment({1, 1}, "small", 100, 5);   // 100 bytes
    auto s2 = MakeP2PSegment({2, 2}, "large", 1000, 5);  // 1000 bytes
    ASSERT_TRUE(meta->MountSegment(s1).has_value());
    ASSERT_TRUE(meta->MountSegment(s2).has_value());

    WriteRouteRequest req;
    req.key = "test_key";
    req.size = 500;  // Requires 500 bytes
    req.config.allow_local = true;

    std::vector<WriteCandidate> candidates;
    auto result = meta->CollectWriteRouteCandidates(req, candidates);
    EXPECT_TRUE(result.has_value());
    ASSERT_EQ(candidates.size(), 1);
    EXPECT_EQ(candidates[0].replica.segment_id, s2.id);
}

TEST_F(P2PClientMetaTest, CollectCandidatesAllowLocalFilter) {
    UUID client_id = {111, 222};
    auto meta = CreateMeta(client_id, "10.0.0.1", 50051);
    meta->Heartbeat();

    auto seg = MakeP2PSegment({1, 1}, "seg1", 10000, 5);
    ASSERT_TRUE(meta->MountSegment(seg).has_value());

    WriteRouteRequest req;
    req.key = "test_key";
    req.client_id = client_id;  // Same as meta client_id
    req.size = 100;
    req.config.allow_local = false;  // Disallow local

    std::vector<WriteCandidate> candidates;
    auto result = meta->CollectWriteRouteCandidates(req, candidates);
    EXPECT_TRUE(result.has_value());
    EXPECT_EQ(candidates.size(), 0);

    // Allow local should return the candidate
    req.config.allow_local = true;
    candidates.clear();
    result = meta->CollectWriteRouteCandidates(req, candidates);
    EXPECT_TRUE(result.has_value());
    EXPECT_EQ(candidates.size(), 1);
}

TEST_F(P2PClientMetaTest, CollectCandidatesTagFilter) {
    UUID client_id = {111, 222};
    auto meta = CreateMeta(client_id, "10.0.0.1", 50051);
    meta->Heartbeat();

    // S1: ssd, fast
    auto s1 = MakeP2PSegment({1, 1}, "s1", 10000);
    s1.GetP2PExtra().tags = {"ssd", "fast"};
    // S2: hdd
    auto s2 = MakeP2PSegment({2, 2}, "s2", 10000);
    s2.GetP2PExtra().tags = {"hdd"};
    // S3: ssd, slow
    auto s3 = MakeP2PSegment({3, 3}, "s3", 10000);
    s3.GetP2PExtra().tags = {"ssd", "slow"};
    // S4: fast (no ssd)
    auto s4 = MakeP2PSegment({4, 4}, "s4", 10000);
    s4.GetP2PExtra().tags = {"fast"};

    ASSERT_TRUE(meta->MountSegment(s1).has_value());
    ASSERT_TRUE(meta->MountSegment(s2).has_value());
    ASSERT_TRUE(meta->MountSegment(s3).has_value());
    ASSERT_TRUE(meta->MountSegment(s4).has_value());

    WriteRouteRequest req;
    req.size = 100;
    req.config.allow_local = true;

    // Filter "ssd": S1, S3 contain "ssd", should be excluded. S2, S4 remain.
    req.config.tag_filters = {"ssd"};
    std::vector<WriteCandidate> candidates;
    auto result = meta->CollectWriteRouteCandidates(req, candidates);
    EXPECT_TRUE(result.has_value());
    EXPECT_EQ(candidates.size(), 2);
    for (const auto& cand : candidates) {
        EXPECT_TRUE(cand.replica.segment_id == s2.id ||
                    cand.replica.segment_id == s4.id);
    }

    // Filter "ssd", "fast": Only S12 remain.
    req.config.tag_filters = {"ssd", "fast"};
    candidates.clear();
    result = meta->CollectWriteRouteCandidates(req, candidates);
    EXPECT_TRUE(result.has_value());
    EXPECT_EQ(candidates.size(), 1);
    EXPECT_TRUE(candidates[0].replica.segment_id == s2.id);

    // Filter "memory": No segment has "memory", none excluded. All 4 remain.
    req.config.tag_filters = {"memory"};
    candidates.clear();
    result = meta->CollectWriteRouteCandidates(req, candidates);
    EXPECT_TRUE(result.has_value());
    EXPECT_EQ(candidates.size(), 4);
}

TEST_F(P2PClientMetaTest, CollectCandidatesPreferLocalPriorityBoost) {
    UUID client_id = {111, 222};
    auto meta = CreateMeta(client_id, "10.0.0.1", 50051);
    meta->Heartbeat();
    const int base_priority = 10;
    auto seg = MakeP2PSegment({1, 1}, "seg1", 10000,
                              base_priority);  // Base priority 10
    ASSERT_TRUE(meta->MountSegment(seg).has_value());

    WriteRouteRequest req;
    req.size = 100;

    // 1. Local client + prefer_local = true -> Boosted priority
    req.client_id = client_id;
    req.config.allow_local = true;
    req.config.prefer_local = true;

    std::vector<WriteCandidate> candidates;
    auto result = meta->CollectWriteRouteCandidates(req, candidates);
    EXPECT_TRUE(result.has_value());
    ASSERT_EQ(candidates.size(), 1);
    EXPECT_EQ(candidates[0].priority,
              base_priority + P2PClientMeta::INF_PRIORITY);

    // 2. Non-local client + prefer_local = true -> No boost
    req.client_id = {999, 999};
    candidates.clear();
    result = meta->CollectWriteRouteCandidates(req, candidates);
    EXPECT_TRUE(result.has_value());
    ASSERT_EQ(candidates.size(), 1);
    EXPECT_EQ(candidates[0].priority, base_priority);

    // 3. Local client + prefer_local = false -> No boost
    req.client_id = client_id;
    req.config.prefer_local = false;
    candidates.clear();
    result = meta->CollectWriteRouteCandidates(req, candidates);
    EXPECT_TRUE(result.has_value());
    ASSERT_EQ(candidates.size(), 1);
    EXPECT_EQ(candidates[0].priority, base_priority);

    // 4. Local client + allow_local = false -> No candidate
    req.config.allow_local = false;
    req.config.prefer_local = true;
    candidates.clear();
    result = meta->CollectWriteRouteCandidates(req, candidates);
    EXPECT_TRUE(result.has_value());
    EXPECT_EQ(candidates.size(), 0);
}

TEST_F(P2PClientMetaTest, CollectCandidatesCompositeFilters) {
    UUID client_id = {111, 222};
    auto meta = CreateMeta(client_id, "10.0.0.1", 50051);
    meta->Heartbeat();

    // S1: Sufficient space, correct tags, low priority
    auto s1 = MakeP2PSegment({1, 1}, "s1", 10000, 1);
    s1.GetP2PExtra().tags = {"ssd"};

    // S2: Sufficient space, wrong tags, high priority
    auto s2 = MakeP2PSegment({2, 2}, "s2", 10000, 10);
    s2.GetP2PExtra().tags = {"hdd"};

    // S3: Insufficient space, correct tags, high priority
    auto s3 = MakeP2PSegment({3, 3}, "s3", 50, 10);
    s3.GetP2PExtra().tags = {"ssd"};

    // S4: Sufficient space, correct tags, high priority -> The only valid one
    auto s4 = MakeP2PSegment({4, 4}, "s4", 10000, 10);
    s4.GetP2PExtra().tags = {"ssd"};

    ASSERT_TRUE(meta->MountSegment(s1).has_value());
    ASSERT_TRUE(meta->MountSegment(s2).has_value());
    ASSERT_TRUE(meta->MountSegment(s3).has_value());
    ASSERT_TRUE(meta->MountSegment(s4).has_value());

    WriteRouteRequest req;
    req.size = 100;  // Filter S3
    req.config.allow_local = true;
    req.config.priority_limit = 5;     // Filter S1
    req.config.tag_filters = {"hdd"};  // Filter S2

    std::vector<WriteCandidate> candidates;
    auto result = meta->CollectWriteRouteCandidates(req, candidates);
    EXPECT_TRUE(result.has_value());
    ASSERT_EQ(candidates.size(), 1);
    EXPECT_EQ(candidates[0].replica.segment_id, s4.id);
}

// ============================================================
// Concurrent mount + health changes
// ============================================================

TEST_F(P2PClientMetaExtendedTest, ConcurrentHealthChangeAndMount) {
    auto meta = CreateExtendedMeta();
    std::atomic<bool> stop{false};
    std::atomic<int> success_count{0};
    std::atomic<int> fail_unhealthy_count{0};
    std::atomic<int> op_count{0};

    // Thread A: Toggles Health Status
    std::thread state_thread([&]() {
        std::random_device rd;
        std::mt19937 rng(rd());
        std::uniform_int_distribution<int> dist(
            0, 2);  // 0: HEALTH, 1: DISC, 2: CRASHED
        while (!stop) {
            int state = dist(rng);
            ClientStatus s = ClientStatus::HEALTH;
            if (state == 1) s = ClientStatus::DISCONNECTION;
            if (state == 2) s = ClientStatus::CRASHED;

            {
                SharedMutexLocker lock(&meta->client_mutex_);
                meta->health_state_.status = s;
            }
        }
    });

    // Thread B: Attempts to Mount Segments
    std::thread mount_thread([&]() {
        int i = 0;
        while (!stop) {
            i++;
            auto segment =
                MakeP2PSegment({100, (uint64_t)i}, "seg_" + std::to_string(i));
            auto res = meta->MountSegment(segment);
            if (res.has_value()) {
                success_count.fetch_add(1);
            } else if (res.error() == mooncake::ErrorCode::CLIENT_UNHEALTHY) {
                fail_unhealthy_count.fetch_add(1);
            } else {
                LOG(ERROR) << "MountSegment failed with error: " << res.error();
            }
            op_count.fetch_add(1);
        }
    });

    // Run for 1 second
    std::this_thread::sleep_for(std::chrono::seconds(1));
    stop = true;
    state_thread.join();
    mount_thread.join();

    EXPECT_EQ(success_count.load() + fail_unhealthy_count.load(),
              op_count.load());
}

}  // namespace mooncake
