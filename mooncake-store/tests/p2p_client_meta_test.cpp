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
// GetWriteRouteCandidate
// ============================================================

TEST_F(P2PClientMetaTest, GetWriteRouteCandidateAggregatesToOneClient) {
    UUID client_id = {111, 222};
    auto meta = CreateMeta(client_id, "10.0.0.1", 50051);
    meta->Heartbeat();

    // Three segments (tiers) on one client, all empty.
    auto s1 = MakeP2PSegment({1, 1}, "seg1", 10000, 5);
    auto s2 = MakeP2PSegment({2, 2}, "seg2", 20000, 3);
    auto s3 = MakeP2PSegment({3, 3}, "seg3", 30000, 4);
    ASSERT_TRUE(meta->MountSegment(s1).has_value());
    ASSERT_TRUE(meta->MountSegment(s2).has_value());
    ASSERT_TRUE(meta->MountSegment(s3).has_value());

    WriteRouteRequest req;
    req.key = "test_key";
    req.client_id = {999, 999};  // non-local requester
    req.size = 100;
    req.config.max_candidates = WriteRouteRequestConfig::RETURN_ALL_CANDIDATES;
    req.config.top_tier_only = false;  // aggregate all tiers
    auto result = meta->GetWriteRouteCandidate(req);
    // Client granularity: one candidate per client regardless of tier count.
    ASSERT_TRUE(result.has_value());
    EXPECT_EQ(result->client_id, client_id);
    // available_capacity aggregates all eligible tiers' free space.
    EXPECT_EQ(result->available_capacity, 60000u);
}

TEST_F(P2PClientMetaTest, GetWriteRouteCandidateCapacityGate) {
    UUID client_id = {111, 222};
    auto meta = CreateMeta(client_id, "10.0.0.1", 50051);
    meta->Heartbeat();

    auto s1 = MakeP2PSegment({1, 1}, "small", 100, 5);   // 100 bytes
    auto s2 = MakeP2PSegment({2, 2}, "large", 1000, 5);  // 1000 bytes
    ASSERT_TRUE(meta->MountSegment(s1).has_value());
    ASSERT_TRUE(meta->MountSegment(s2).has_value());
    // client-level free = 100 + 1000 = 1100

    WriteRouteRequest req;
    req.key = "test_key";

    // Fits in the client's aggregate free capacity -> a candidate.
    req.size = 500;
    auto result = meta->GetWriteRouteCandidate(req);
    ASSERT_TRUE(result.has_value());
    EXPECT_EQ(result->available_capacity, 1100u);

    // Larger than the client's total free -> not a candidate.
    req.size = 2000;
    result = meta->GetWriteRouteCandidate(req);
    EXPECT_FALSE(result.has_value());
}

// The client meta does NOT exclude the local client based on remote_weight;
// that decision belongs to the master. The client only reports its capacity.
TEST_F(P2PClientMetaTest, GetWriteRouteCandidateLocalAlwaysEligible) {
    UUID client_id = {111, 222};
    auto meta = CreateMeta(client_id, "10.0.0.1", 50051);
    meta->Heartbeat();

    auto seg = MakeP2PSegment({1, 1}, "seg1", 10000, 5);
    ASSERT_TRUE(meta->MountSegment(seg).has_value());

    WriteRouteRequest req;
    req.key = "test_key";
    req.client_id = client_id;  // Same as meta client_id
    req.size = 100;

    // remote_weight does not affect eligibility at the client level.
    req.config.remote_weight = 1.0;
    auto result = meta->GetWriteRouteCandidate(req);
    ASSERT_TRUE(result.has_value());

    req.config.remote_weight = 0.5;
    result = meta->GetWriteRouteCandidate(req);
    ASSERT_TRUE(result.has_value());

    req.config.remote_weight = 0.0;
    result = meta->GetWriteRouteCandidate(req);
    ASSERT_TRUE(result.has_value());
}

TEST_F(P2PClientMetaTest, GetWriteRouteCandidateTagFilter) {
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
    req.config.max_candidates = WriteRouteRequestConfig::RETURN_ALL_CANDIDATES;

    // tag_filters scope the client's capacity score. Filter "ssd" excludes
    // S1,S3; S2,S4 remain eligible -> one candidate, capacity = S2+S4 free.
    req.config.tag_filters = {"ssd"};
    auto result = meta->GetWriteRouteCandidate(req);
    ASSERT_TRUE(result.has_value());
    EXPECT_EQ(result->client_id, client_id);
    EXPECT_EQ(result->available_capacity, 20000u);  // S2 + S4

    // Every segment carries a filtered tag -> no eligible tier -> not a
    // candidate.
    req.config.tag_filters = {"ssd", "fast", "hdd", "slow"};
    result = meta->GetWriteRouteCandidate(req);
    EXPECT_FALSE(result.has_value());
}

// The candidate's score is the client-level free ratio ONLY. The local bias
// The remote_weight scaling is applied by the master, not here, so this test
// verifies the raw free ratio is returned regardless of remote_weight. Local
// bias behavior is covered by P2PMasterServiceTest.
TEST_F(P2PClientMetaTest, GetWriteRouteCandidateScoreIsRawFreeRatio) {
    UUID client_id = {111, 222};
    auto meta = CreateMeta(client_id, "10.0.0.1", 50051);
    meta->Heartbeat();
    const int base_priority = 10;
    // 10000 total, 5000 used -> free_ratio = 0.5
    auto seg = MakeP2PSegment({1, 1}, "seg1", 10000, base_priority,
                              /*usage=*/5000);
    ASSERT_TRUE(meta->MountSegment(seg).has_value());
    const double free_ratio = 0.5;

    WriteRouteRequest req;
    req.size = 100;

    // 1. Local client + remote_weight = 0 (force local) -> score is free_ratio
    //    (the +1 local bias is added by the master, not by the client).
    req.client_id = client_id;
    req.config.remote_weight = 0.0;
    auto result = meta->GetWriteRouteCandidate(req);
    ASSERT_TRUE(result.has_value());
    EXPECT_DOUBLE_EQ(result->score, free_ratio);

    // 2. Local client + remote_weight = 0.5 (balanced) -> free_ratio, no bias.
    req.config.remote_weight = 0.5;
    result = meta->GetWriteRouteCandidate(req);
    ASSERT_TRUE(result.has_value());
    EXPECT_DOUBLE_EQ(result->score, free_ratio);

    // 3. Non-local client -> free_ratio, no bias (never local).
    req.client_id = {999, 999};
    req.config.remote_weight = 0.0;
    result = meta->GetWriteRouteCandidate(req);
    ASSERT_TRUE(result.has_value());
    EXPECT_DOUBLE_EQ(result->score, free_ratio);

    // 4. Local client + remote_weight = 1 -> still a candidate at client level
    //    (the master applies the weight and decides exclusion).
    req.client_id = client_id;
    req.config.remote_weight = 1.0;
    result = meta->GetWriteRouteCandidate(req);
    ASSERT_TRUE(result.has_value());
    EXPECT_DOUBLE_EQ(result->score, free_ratio);
}

TEST_F(P2PClientMetaTest, GetWriteRouteCandidateCompositeFilters) {
    UUID client_id = {111, 222};
    auto meta = CreateMeta(client_id, "10.0.0.1", 50051);
    meta->Heartbeat();

    // S1: low priority (excluded by priority_limit)
    auto s1 = MakeP2PSegment({1, 1}, "s1", 10000, 1);
    s1.GetP2PExtra().tags = {"ssd"};

    // S2: wrong tag (excluded by tag_filters), high priority
    auto s2 = MakeP2PSegment({2, 2}, "s2", 10000, 10);
    s2.GetP2PExtra().tags = {"hdd"};

    // S3: eligible tier, small
    auto s3 = MakeP2PSegment({3, 3}, "s3", 50, 10);
    s3.GetP2PExtra().tags = {"ssd"};

    // S4: eligible tier, large
    auto s4 = MakeP2PSegment({4, 4}, "s4", 10000, 10);
    s4.GetP2PExtra().tags = {"ssd"};

    ASSERT_TRUE(meta->MountSegment(s1).has_value());
    ASSERT_TRUE(meta->MountSegment(s2).has_value());
    ASSERT_TRUE(meta->MountSegment(s3).has_value());
    ASSERT_TRUE(meta->MountSegment(s4).has_value());

    WriteRouteRequest req;
    req.size = 100;
    req.config.priority_limit = 5;     // excludes S1
    req.config.tag_filters = {"hdd"};  // excludes S2

    // Eligible tiers = S3 + S4; client is a candidate with their aggregate
    // free. (The client itself picks the concrete tier that fits at write
    // time.)
    auto result = meta->GetWriteRouteCandidate(req);
    ASSERT_TRUE(result.has_value());
    EXPECT_EQ(result->client_id, client_id);
    EXPECT_EQ(result->available_capacity, 10050u);  // S3(50) + S4(10000)
}

TEST_F(P2PClientMetaTest, GetWriteScoreCapacity) {
    UUID client_id = {111, 222};
    auto meta = CreateMeta(client_id, "10.0.0.1", 50051);
    meta->Heartbeat();

    // High-priority (10) DRAM tier: small, mostly free.
    auto dram = MakeP2PSegment({1, 1}, "dram", 1000, /*priority=*/10,
                               /*usage=*/200);
    dram.GetP2PExtra().tags = {"fast"};
    // Low-priority (0) NVMe tier: large, mostly free.
    auto nvme = MakeP2PSegment({2, 2}, "nvme", 10000, /*priority=*/0,
                               /*usage=*/1000);
    ASSERT_TRUE(meta->MountSegment(dram).has_value());
    ASSERT_TRUE(meta->MountSegment(nvme).has_value());

    // All eligible tiers summed (no filters).
    auto all = meta->GetWriteScoreCapacity({}, 0, /*top_tier_only=*/false);
    EXPECT_EQ(all.total, 11000u);
    EXPECT_EQ(all.free, 800u + 9000u);

    // Only the highest-priority eligible tier (DRAM).
    auto top = meta->GetWriteScoreCapacity({}, 0, /*top_tier_only=*/true);
    EXPECT_EQ(top.total, 1000u);
    EXPECT_EQ(top.free, 800u);

    // priority_limit excludes the NVMe tier -> only DRAM contributes.
    auto by_priority =
        meta->GetWriteScoreCapacity({}, 5, /*top_tier_only=*/false);
    EXPECT_EQ(by_priority.total, 1000u);
    EXPECT_EQ(by_priority.free, 800u);

    // tag_filters excludes DRAM (carries "fast") -> only NVMe contributes.
    auto by_tag =
        meta->GetWriteScoreCapacity({"fast"}, 0, /*top_tier_only=*/false);
    EXPECT_EQ(by_tag.total, 10000u);
    EXPECT_EQ(by_tag.free, 9000u);
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
