#include <glog/logging.h>
#include <gtest/gtest.h>

#include <optional>
#include <string>

#include "tiered_cache/event_driven_scheduler/multi_lru_stats_collector.h"

namespace mooncake {
namespace {

constexpr UUID kFast{1, 1};
constexpr UUID kSlow{2, 2};

// Find a key's current band by scanning the cold-first candidate list.
std::optional<HeatBand> BandOfKey(const MultiLRUStatsCollector& c,
                                  const std::string& key) {
    for (const auto& e : c.CollectEvictionCandidates(100000)) {
        if (e.key == key) {
            return e.band;
        }
    }
    return std::nullopt;
}

TEST(MultiLRUTest, BandRisesWithFrequency) {
    MultiLRUStatsCollector c(4096);
    c.SetFastTier(kFast);
    c.OnCommit("k", kFast, 100);  // committed at freq 0 -> cold
    EXPECT_EQ(BandOfKey(c, "k"), HeatBand::kCold);

    c.OnAccess("k", kFast);
    c.OnAccess("k", kFast);  // freq 2 -> warm
    EXPECT_EQ(BandOfKey(c, "k"), HeatBand::kWarm);

    c.OnAccess("k", kFast);
    c.OnAccess("k", kFast);  // freq 4 -> hot
    EXPECT_EQ(BandOfKey(c, "k"), HeatBand::kHot);

    for (int i = 0; i < 4; ++i) {
        c.OnAccess("k", kFast);  // freq 8 -> very hot
    }
    EXPECT_EQ(BandOfKey(c, "k"), HeatBand::kVeryHot);
}

TEST(MultiLRUTest, CollectHotReturnsHottestFirstBounded) {
    MultiLRUStatsCollector c(4096);
    c.SetFastTier(kFast);
    c.OnCommit("cold", kFast, 10);
    c.OnCommit("hot", kFast, 10);
    for (int i = 0; i < 5; ++i) c.OnAccess("hot", kFast);  // hot band
    c.OnCommit("veryhot", kFast, 10);
    for (int i = 0; i < 10; ++i) c.OnAccess("veryhot", kFast);  // very hot

    AccessStats stats = c.GetHotKeyStats(2);
    ASSERT_EQ(stats.hot_keys.size(), 2u);
    EXPECT_EQ(stats.metric, AccessStatMetric::kFrequency);
    EXPECT_EQ(stats.hot_keys[0].key, "veryhot");  // hottest first
    EXPECT_EQ(stats.hot_keys[1].key, "hot");
    EXPECT_GT(stats.hot_keys[0].estimated_frequency,
              stats.hot_keys[1].estimated_frequency);
}

TEST(MultiLRUTest, RemoveDropsFromBandsButSketchRemembers) {
    MultiLRUStatsCollector c(4096);
    c.SetFastTier(kFast);
    c.OnCommit("k", kFast, 10);
    for (int i = 0; i < 6; ++i) c.OnAccess("k", kFast);
    ASSERT_TRUE(BandOfKey(c, "k").has_value());
    const uint64_t freq_before = c.GetAccessFrequency("k");
    EXPECT_GE(freq_before, 6u);

    c.OnDelete("k", kFast);
    EXPECT_FALSE(BandOfKey(c, "k").has_value());        // gone from bands
    EXPECT_EQ(c.GetAccessFrequency("k"), freq_before);  // sketch remembers
}

TEST(MultiLRUTest, NonFastTierAccessAndCommitDoNotEnterBands) {
    MultiLRUStatsCollector c(4096);
    c.SetFastTier(kFast);
    c.OnCommit("s", kSlow, 10);  // committed to the slow tier
    EXPECT_FALSE(BandOfKey(c, "s").has_value());
    for (int i = 0; i < 5; ++i) c.OnAccess("s", kSlow);  // slow-tier hits
    EXPECT_FALSE(BandOfKey(c, "s").has_value());
    EXPECT_GE(c.GetAccessFrequency("s"), 5u);  // frequency still tracked
}

TEST(MultiLRUTest, TierScopedDeleteGuardsGap1) {
    MultiLRUStatsCollector c(4096);
    c.SetFastTier(kFast);
    c.OnCommit("k", kFast, 10);
    ASSERT_TRUE(BandOfKey(c, "k").has_value());

    // A delete on the SLOW tier must NOT evict the fast-tier resident.
    c.OnDelete("k", kSlow);
    EXPECT_TRUE(BandOfKey(c, "k").has_value());

    // A delete on the FAST tier removes it.
    c.OnDelete("k", kFast);
    EXPECT_FALSE(BandOfKey(c, "k").has_value());
}

TEST(MultiLRUTest, FullKeyDeleteRemoves) {
    MultiLRUStatsCollector c(4096);
    c.SetFastTier(kFast);
    c.OnCommit("k", kFast, 10);
    c.OnDelete("k", std::nullopt);  // full-key delete
    EXPECT_FALSE(BandOfKey(c, "k").has_value());
}

}  // namespace
}  // namespace mooncake
