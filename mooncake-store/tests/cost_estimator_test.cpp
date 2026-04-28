// Cost-aware routing — unit tests for CostEstimator + helpers.
//
// These tests are pure CPU; they do NOT spin up a master_service.

#include <gtest/gtest.h>

#include <string>

#include "cost_estimator.h"

using namespace mooncake;

namespace {

ClusterTopology BuildTopology() {
    ClusterTopology t;
    t.zone_to_hosts["zone-a"].insert("10.0.0.1");
    t.zone_to_hosts["zone-a"].insert("10.0.0.2");
    t.zone_to_hosts["zone-b"].insert("10.0.1.1");
    t.host_to_zone["10.0.0.1"] = "zone-a";
    t.host_to_zone["10.0.0.2"] = "zone-a";
    t.host_to_zone["10.0.1.1"] = "zone-b";
    return t;
}

}  // namespace

TEST(CostEstimator, ScoreOrdersByLinkClass) {
    CostEstimator est;
    CostInputs base{};
    base.storage_tier = StorageTier::DRAM;
    base.inflight = 0;
    base.request_size_bytes = 0;

    base.link_class = LinkClass::LOCAL_HOST;
    const double local = est.Score(base);
    base.link_class = LinkClass::SAME_ZONE;
    const double same_zone = est.Score(base);
    base.link_class = LinkClass::CROSS_ZONE;
    const double cross_zone = est.Score(base);
    base.link_class = LinkClass::UNKNOWN;
    const double unknown = est.Score(base);

    EXPECT_LT(local, same_zone);
    EXPECT_LT(same_zone, cross_zone);
    EXPECT_LE(cross_zone, unknown);
}

TEST(CostEstimator, ScoreOrdersByStorageTier) {
    CostEstimator est;
    CostInputs base{};
    base.link_class = LinkClass::LOCAL_HOST;
    base.inflight = 0;

    base.storage_tier = StorageTier::DRAM;
    const double dram = est.Score(base);
    base.storage_tier = StorageTier::SSD;
    const double ssd = est.Score(base);
    base.storage_tier = StorageTier::FILE;
    const double file = est.Score(base);

    EXPECT_LT(dram, ssd);
    EXPECT_LT(ssd, file);
}

TEST(CostEstimator, InflightAddsLinearPenalty) {
    CostEstimator est;
    CostInputs base{};
    base.link_class = LinkClass::LOCAL_HOST;
    base.storage_tier = StorageTier::DRAM;

    base.inflight = 0;
    const double zero = est.Score(base);
    base.inflight = 1;
    const double one = est.Score(base);
    base.inflight = 10;
    const double ten = est.Score(base);

    EXPECT_GT(one, zero);
    EXPECT_GT(ten, one);
    // Linear: 10*delta == ten-zero, 1*delta == one-zero.
    EXPECT_NEAR(ten - zero, 10.0 * (one - zero), 1e-9);
}

TEST(ClusterTopology, ParseValidJson) {
    const std::string json = R"({
        "zone-a": ["10.0.0.1", "10.0.0.2"],
        "zone-b": ["10.0.1.1"]
    })";
    ClusterTopology t;
    ASSERT_TRUE(ClusterTopology::ParseFromJson(json, &t));
    EXPECT_EQ(t.zone_to_hosts.size(), 2u);
    EXPECT_EQ(t.host_to_zone.at("10.0.0.1"), "zone-a");
    EXPECT_EQ(t.host_to_zone.at("10.0.1.1"), "zone-b");
}

TEST(ClusterTopology, ParseEmptyJsonReturnsTrueWithEmptyTopology) {
    ClusterTopology t;
    ASSERT_TRUE(ClusterTopology::ParseFromJson("{}", &t));
    EXPECT_TRUE(t.empty());
}

TEST(ClusterTopology, ParseGarbageReturnsFalse) {
    ClusterTopology t;
    EXPECT_FALSE(ClusterTopology::ParseFromJson("not json", &t));
}

TEST(CostEstimator, ClassifyLinkLocalHost) {
    auto topo = BuildTopology();
    EXPECT_EQ(CostEstimator::ClassifyLink("10.0.0.1:12345", "10.0.0.1",
                                          "zone-a", topo),
              LinkClass::LOCAL_HOST);
}

TEST(CostEstimator, ClassifyLinkSameZone) {
    auto topo = BuildTopology();
    EXPECT_EQ(CostEstimator::ClassifyLink("10.0.0.2:12345", "10.0.0.1",
                                          "zone-a", topo),
              LinkClass::SAME_ZONE);
}

TEST(CostEstimator, ClassifyLinkCrossZone) {
    auto topo = BuildTopology();
    EXPECT_EQ(CostEstimator::ClassifyLink("10.0.1.1:12345", "10.0.0.1",
                                          "zone-a", topo),
              LinkClass::CROSS_ZONE);
}

TEST(CostEstimator, ClassifyLinkUnknownWithoutTopology) {
    ClusterTopology empty_topo;
    EXPECT_EQ(CostEstimator::ClassifyLink("10.0.1.1:12345", "10.0.0.1",
                                          "zone-a", empty_topo),
              LinkClass::UNKNOWN);
}

TEST(CostEstimator, ClassifyTier) {
    EXPECT_EQ(CostEstimator::ClassifyTier("rdma"), StorageTier::DRAM);
    EXPECT_EQ(CostEstimator::ClassifyTier("tcp"), StorageTier::DRAM);
    EXPECT_EQ(CostEstimator::ClassifyTier("ssd"), StorageTier::SSD);
    EXPECT_EQ(CostEstimator::ClassifyTier("nvme"), StorageTier::SSD);
    EXPECT_EQ(CostEstimator::ClassifyTier("file"), StorageTier::FILE);
    EXPECT_EQ(CostEstimator::ClassifyTier("3fs"), StorageTier::FILE);
    EXPECT_EQ(CostEstimator::ClassifyTier("unknown_protocol"),
              StorageTier::DRAM);
}

TEST(SegmentInflightTracker, BeginEndSnapshot) {
    SegmentInflightTracker tracker;
    EXPECT_EQ(tracker.Get("seg-a"), 0u);
    EXPECT_EQ(tracker.Begin("seg-a"), 1u);
    EXPECT_EQ(tracker.Begin("seg-a"), 2u);
    EXPECT_EQ(tracker.Begin("seg-b"), 1u);
    EXPECT_EQ(tracker.TotalInflight(), 3u);
    EXPECT_EQ(tracker.End("seg-a"), 1u);
    EXPECT_EQ(tracker.End("seg-a"), 0u);
    EXPECT_EQ(tracker.End("seg-a"), 0u);  // saturating at zero
    EXPECT_EQ(tracker.TotalInflight(), 1u);
}
