#include <glog/logging.h>
#include <gtest/gtest.h>

#include "tiered_cache/event_driven_scheduler/frequency_sketch.h"

namespace mooncake {
namespace {

TEST(TinyLFUSketchTest, IncrementAndEstimate) {
    FrequencySketch s(100);
    EXPECT_EQ(s.Estimate(42), 0u);
    s.Increment(42);
    EXPECT_EQ(s.Estimate(42), 1u);
    s.Increment(42);
    s.Increment(42);
    EXPECT_EQ(s.Estimate(42), 3u);
}

TEST(TinyLFUSketchTest, SaturatesAt15) {
    FrequencySketch s(100);
    for (int i = 0; i < 50; ++i) {
        s.Increment(7);
    }
    EXPECT_EQ(s.Estimate(7), 15u);  // 4-bit counters saturate at 15
}

TEST(TinyLFUSketchTest, MonotoneNonDecreasingBeforeReset) {
    FrequencySketch s(/*capacity=*/1000, /*sample_size=*/1'000'000);
    uint32_t prev = 0;
    for (int i = 0; i < 15; ++i) {
        s.Increment(123);
        const uint32_t cur = s.Estimate(123);
        EXPECT_GE(cur, prev);
        prev = cur;
    }
}

TEST(TinyLFUSketchTest, ManualResetHalvesCounters) {
    FrequencySketch s(/*capacity=*/256, /*sample_size=*/1'000'000);  // no auto
    for (int i = 0; i < 10; ++i) {
        s.Increment(5);
    }
    const uint32_t before = s.Estimate(5);
    EXPECT_EQ(before, 10u);
    s.Reset();
    EXPECT_EQ(s.Estimate(5), before / 2);  // 10 -> 5
}

TEST(TinyLFUSketchTest, AutoResetReducesCounters) {
    FrequencySketch s(/*capacity=*/64, /*sample_size=*/4);
    for (int i = 0; i < 4; ++i) {
        s.Increment(9);  // the 4th increment trips the decay threshold
    }
    EXPECT_LT(s.size(), 4u);       // size counter was reduced by the reset
    EXPECT_LE(s.Estimate(9), 2u);  // counters halved (4 -> 2)
}

TEST(TinyLFUSketchTest, IndependentRowsKeepUnrelatedKeysLow) {
    FrequencySketch s(4096);
    for (int i = 0; i < 1000; ++i) {
        s.Increment(0xC0FFEEULL);
    }
    // Keys unrelated to the hammered one estimate ~0 (within a tiny collision
    // bound) — guards the independence of the 4 splitmix64 mixers.
    EXPECT_LE(s.Estimate(0xAAAA1111ULL), 2u);
    EXPECT_LE(s.Estimate(0xBBBB2222ULL), 2u);
    EXPECT_EQ(s.Estimate(0xC0FFEEULL), 15u);
}

// The fused hot-path method must be observably identical to Increment() then
// Estimate() — same return value and same resulting table state.
TEST(TinyLFUSketchTest, FusedIncrementAndEstimateMatchesSeparate) {
    FrequencySketch fused(/*capacity=*/1000, /*sample_size=*/1'000'000);
    FrequencySketch split(/*capacity=*/1000, /*sample_size=*/1'000'000);
    for (uint64_t k = 0; k < 50; ++k) {
        for (int i = 0; i < 5; ++i) {
            split.Increment(k);
            const uint32_t expect = split.Estimate(k);
            EXPECT_EQ(fused.IncrementAndEstimate(k), expect)
                << "key=" << k << " i=" << i;
        }
    }
    for (uint64_t k = 0; k < 50; ++k) {
        EXPECT_EQ(fused.Estimate(k), split.Estimate(k)) << "key=" << k;
    }
}

// Equivalence must hold even when the fused increment trips the auto-reset:
// the returned estimate must reflect the post-reset (halved) table.
TEST(TinyLFUSketchTest, FusedIncrementAndEstimateAcrossAutoReset) {
    FrequencySketch fused(/*capacity=*/64, /*sample_size=*/4);
    FrequencySketch split(/*capacity=*/64, /*sample_size=*/4);
    for (int i = 0; i < 8; ++i) {
        split.Increment(9);
        EXPECT_EQ(fused.IncrementAndEstimate(9), split.Estimate(9))
            << "i=" << i;
    }
    EXPECT_EQ(fused.size(), split.size());
}

TEST(TinyLFUSketchTest, MemoryApproxCapacityOverFour) {
    FrequencySketch s(1u << 16);
    EXPECT_EQ(s.MemoryBytes(), 8u * ((1u << 16) / 4));  // 8 bytes/word
}

TEST(TinyLFUSketchTest, FixedDecayFromCapacity) {
    auto p = FixedDecayPolicy::FromCapacity(10);
    EXPECT_FALSE(p.ShouldDecay(99));
    EXPECT_TRUE(p.ShouldDecay(100));  // 10 * 10 = 100
    EXPECT_TRUE(p.ShouldDecay(101));
}

}  // namespace
}  // namespace mooncake
