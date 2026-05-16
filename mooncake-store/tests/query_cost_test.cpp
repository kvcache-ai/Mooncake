// Cost-aware routing — integration tests against MasterService::QueryCost.
//
// These run against an in-process MasterService (no network) but exercise
// the full Snapshot → ClassifyLink → ClassifyTier → Score → Sort pipeline.

#include <gtest/gtest.h>

#include "master_service.h"
#include "rpc_types.h"
#include "segment.h"
#include "types.h"

using namespace mooncake;

namespace {

class QueryCostTest : public ::testing::Test {
   protected:
    void SetUp() override {
        MasterServiceConfig config;
        config.enable_cost_aware = true;
        config.cluster_topology_json = R"({
            "zone-a": ["10.0.0.1", "10.0.0.2"],
            "zone-b": ["10.0.1.1"]
        })";
        master_ = std::make_unique<MasterService>(config);
        // Helper: pretend three segments are mounted at known endpoints.
        // We can't mount via the public RPC without a real client, so we
        // exercise the unmounted fallback paths + the inflight tracker.
    }

    std::unique_ptr<MasterService> master_;
};

}  // namespace

TEST_F(QueryCostTest, EmptyCandidatesReturnsErr) {
    QueryCostRequest request;
    auto resp = master_->QueryCost(request);
    ASSERT_FALSE(resp.has_value());
    EXPECT_EQ(resp.error(), ErrorCode::COST_REQUEST_EMPTY);
}

TEST_F(QueryCostTest, DisabledFlagShortCircuits) {
    master_->SetCostAwareEnabledForTesting(false);
    QueryCostRequest request;
    request.candidate_segment_names = {"seg-a"};
    auto resp = master_->QueryCost(request);
    ASSERT_FALSE(resp.has_value());
    EXPECT_EQ(resp.error(), ErrorCode::COST_QUERY_DISABLED);
    master_->SetCostAwareEnabledForTesting(true);
}

TEST_F(QueryCostTest, UnmountedCandidatesDroppedByDefault) {
    QueryCostRequest request;
    request.candidate_segment_names = {"missing-1", "missing-2"};
    auto resp = master_->QueryCost(request);
    ASSERT_TRUE(resp.has_value());
    EXPECT_TRUE(resp->candidates.empty());
}

TEST_F(QueryCostTest, UnmountedCandidatesIncludedWhenRequested) {
    QueryCostRequest request;
    request.candidate_segment_names = {"missing-1", "missing-2"};
    request.include_unmounted = true;
    auto resp = master_->QueryCost(request);
    ASSERT_TRUE(resp.has_value());
    ASSERT_EQ(resp->candidates.size(), 2u);
    for (const auto& c : resp->candidates) {
        EXPECT_FALSE(c.found);
    }
}

TEST_F(QueryCostTest, RequestTooLargeRejected) {
    QueryCostRequest request;
    request.candidate_segment_names.assign(kMaxCostCandidates + 1, "x");
    auto resp = master_->QueryCost(request);
    ASSERT_FALSE(resp.has_value());
    EXPECT_EQ(resp.error(), ErrorCode::COST_REQUEST_TOO_LARGE);
}

TEST_F(QueryCostTest, InflightTrackerRoundtrip) {
    auto a = master_->InflightBegin("seg-a");
    ASSERT_TRUE(a.has_value());
    EXPECT_EQ(*a, 1u);
    auto a2 = master_->InflightBegin("seg-a");
    ASSERT_TRUE(a2.has_value());
    EXPECT_EQ(*a2, 2u);
    auto b = master_->InflightBegin("seg-b");
    ASSERT_TRUE(b.has_value());
    EXPECT_EQ(*b, 1u);
    auto a3 = master_->InflightEnd("seg-a");
    ASSERT_TRUE(a3.has_value());
    EXPECT_EQ(*a3, 1u);

    QueryCostRequest request;
    request.candidate_segment_names = {"seg-a"};
    request.include_unmounted = true;
    auto resp = master_->QueryCost(request);
    ASSERT_TRUE(resp.has_value());
    ASSERT_EQ(resp->candidates.size(), 1u);
    // seg-a is unmounted but inflight is reported on the master side
    // via total_inflight; the per-candidate inflight is 0 because
    // the entry is unmounted.
    EXPECT_EQ(resp->total_inflight, 2u);

    master_->ResetInflightTrackerForTesting();
}

TEST_F(QueryCostTest, InflightBeginRejectsEmptyName) {
    auto rc = master_->InflightBegin("");
    ASSERT_FALSE(rc.has_value());
    EXPECT_EQ(rc.error(), ErrorCode::COST_REQUEST_EMPTY);
}

namespace {

constexpr size_t kSegSize = 64ul * 1024 * 1024;
constexpr size_t kSegBase = 0x10000000ul;

Segment MakeSegmentAtHost(const std::string& name, const std::string& host) {
    Segment s;
    s.id = generate_uuid();
    s.name = name;
    s.te_endpoint = host + ":12345";
    s.base = kSegBase;
    s.size = kSegSize;
    return s;
}

}  // namespace

// e2e: candidates that are actually mounted should be ranked ahead of
// unmounted candidates, and within the mounted set the LinkClass order
// (LOCAL_HOST < SAME_ZONE < CROSS_ZONE) drives the ranking.
TEST_F(QueryCostTest, MountedAndUnmountedMixedOrdering) {
    Segment local_seg = MakeSegmentAtHost("seg-local", "10.0.0.1");
    Segment same_zone_seg = MakeSegmentAtHost("seg-same-zone", "10.0.0.2");
    Segment cross_zone_seg = MakeSegmentAtHost("seg-cross-zone", "10.0.1.1");

    UUID client_id = generate_uuid();
    ASSERT_TRUE(master_->MountSegment(local_seg, client_id).has_value());
    ASSERT_TRUE(master_->MountSegment(same_zone_seg, client_id).has_value());
    ASSERT_TRUE(master_->MountSegment(cross_zone_seg, client_id).has_value());

    QueryCostRequest request;
    // Deliberately shuffle the candidate order + sprinkle in an unmounted
    // name to confirm the sort is independent of input order and that
    // unmounted entries land last.
    request.candidate_segment_names = {"seg-cross-zone", "missing-seg",
                                       "seg-same-zone", "seg-local"};
    request.client_host = "10.0.0.1";
    request.client_zone = "zone-a";
    request.include_unmounted = true;

    auto resp = master_->QueryCost(request);
    ASSERT_TRUE(resp.has_value());
    ASSERT_EQ(resp->candidates.size(), 4u);

    EXPECT_EQ(resp->candidates[0].segment_name, "seg-local");
    EXPECT_TRUE(resp->candidates[0].found);
    EXPECT_EQ(resp->candidates[0].link_class,
              static_cast<int32_t>(LinkClass::LOCAL_HOST));
    EXPECT_EQ(resp->candidates[1].segment_name, "seg-same-zone");
    EXPECT_TRUE(resp->candidates[1].found);
    EXPECT_EQ(resp->candidates[1].link_class,
              static_cast<int32_t>(LinkClass::SAME_ZONE));
    EXPECT_EQ(resp->candidates[2].segment_name, "seg-cross-zone");
    EXPECT_TRUE(resp->candidates[2].found);
    EXPECT_EQ(resp->candidates[2].link_class,
              static_cast<int32_t>(LinkClass::CROSS_ZONE));
    EXPECT_EQ(resp->candidates[3].segment_name, "missing-seg");
    EXPECT_FALSE(resp->candidates[3].found);
}

// include_unmounted=false (the default) drops every candidate that is not
// currently mounted, even if other candidates ARE mounted.
TEST_F(QueryCostTest, MountedOnlyResponseExcludesUnmounted) {
    Segment local_seg = MakeSegmentAtHost("seg-local", "10.0.0.1");
    UUID client_id = generate_uuid();
    ASSERT_TRUE(master_->MountSegment(local_seg, client_id).has_value());

    QueryCostRequest request;
    request.candidate_segment_names = {"missing-1", "seg-local", "missing-2"};
    request.client_host = "10.0.0.1";
    request.client_zone = "zone-a";
    request.include_unmounted = false;

    auto resp = master_->QueryCost(request);
    ASSERT_TRUE(resp.has_value());
    ASSERT_EQ(resp->candidates.size(), 1u);
    EXPECT_EQ(resp->candidates[0].segment_name, "seg-local");
    EXPECT_TRUE(resp->candidates[0].found);
}
