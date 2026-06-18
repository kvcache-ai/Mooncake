// Copyright 2026 KVCache.AI
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <gtest/gtest.h>

#include <cstdint>
#include <string>
#include <vector>

#include "tent/runtime/segment.h"
#include "tent/runtime/topology.h"

namespace mooncake {
namespace tent {
namespace {

constexpr size_t kKiB = static_cast<size_t>(1) << 10;
constexpr size_t kMiB = static_cast<size_t>(1) << 20;
constexpr size_t kGiB = static_cast<size_t>(1) << 30;

// Build an entries vector where each RangeLocation is address-adjacent to
// the previous one. Mirrors what Platform::getLoader().getLocation produces.
std::vector<RangeLocation> makeEntries(
    const std::vector<std::pair<size_t, std::string>>& segs) {
    std::vector<RangeLocation> out;
    uint64_t cursor = 0;
    out.reserve(segs.size());
    for (auto& [len, loc] : segs) {
        out.push_back(RangeLocation{cursor, len, loc});
        cursor += len;
    }
    return out;
}

size_t sumLens(const std::vector<RangeLocation>& entries) {
    size_t s = 0;
    for (auto& e : entries) s += e.len;
    return s;
}

size_t sumSizes(const std::vector<Region>& regions) {
    size_t s = 0;
    for (auto& r : regions) s += r.size;
    return s;
}

// All these inputs are well under (128 buckets × 1 MiB floor), so the
// effective bucket size is the 1 MiB default min_bucket_bytes floor.
TEST(CoalesceRegionsTest, TieBreakIsDeterministic) {
    auto entries = makeEntries({{256 * kKiB, "cpu:0"},
                                {256 * kKiB, "cpu:1"},
                                {256 * kKiB, "cpu:0"},
                                {256 * kKiB, "cpu:1"}});
    auto first = coalesceRegions(entries);
    ASSERT_EQ(first.size(), 1u);
    EXPECT_EQ(first[0].size, 1 * kMiB);
    EXPECT_EQ(first[0].location, "cpu:0");

    for (int i = 0; i < 100; ++i) {
        auto run = coalesceRegions(entries);
        ASSERT_EQ(run.size(), 1u);
        EXPECT_EQ(run[0].location, "cpu:0") << "run " << i;
    }
}

TEST(CoalesceRegionsTest, MajorityWins) {
    auto entries = makeEntries({{900 * kKiB, "cpu:0"}, {100 * kKiB, "cpu:1"}});
    auto out = coalesceRegions(entries);
    ASSERT_EQ(out.size(), 1u);
    EXPECT_EQ(out[0].size, 1000 * kKiB);
    EXPECT_EQ(out[0].location, "cpu:0");
}

TEST(CoalesceRegionsTest, SameLocationCoalesces) {
    auto entries = makeEntries({{256 * kKiB, "cpu:0"},
                                {256 * kKiB, "cpu:0"},
                                {256 * kKiB, "cpu:0"},
                                {256 * kKiB, "cpu:0"}});
    auto out = coalesceRegions(entries);
    ASSERT_EQ(out.size(), 1u);
    EXPECT_EQ(out[0].size, 1 * kMiB);
    EXPECT_EQ(out[0].location, "cpu:0");
}

TEST(CoalesceRegionsTest, EmptyInput) {
    auto out = coalesceRegions({});
    EXPECT_TRUE(out.empty());
}

TEST(CoalesceRegionsTest, TotalBytesPreserved) {
    auto entries = makeEntries({{700 * kKiB, "cpu:0"},
                                {300 * kKiB, "cpu:1"},
                                {2 * kMiB, "cpu:0"},
                                {500 * kKiB, "cpu:1"},
                                {1500 * kKiB, "cpu:0"}});
    auto out = coalesceRegions(entries);
    EXPECT_EQ(sumSizes(out), sumLens(entries));
}

TEST(CoalesceRegionsTest, AutoBucketBoundedBy128) {
    // 256 segments × 16 MiB alternating cpu:0 / cpu:1 = 4 GiB total.
    // Auto bucket should land near 4 GiB / 128 = 32 MiB → at most 128
    // regions.
    std::vector<std::pair<size_t, std::string>> segs;
    segs.reserve(256);
    for (int i = 0; i < 256; ++i) {
        segs.emplace_back(16 * kMiB, i % 2 == 0 ? "cpu:0" : "cpu:1");
    }
    auto entries = makeEntries(segs);
    auto out = coalesceRegions(entries);  // all defaults: auto, 128, 1 MiB
    EXPECT_LE(out.size(), 128u);
    EXPECT_EQ(sumSizes(out), sumLens(entries));
}

TEST(CoalesceRegionsTest, AutoBucketRespectsMinFloor) {
    auto entries = makeEntries({{50 * kKiB, "cpu:0"}, {50 * kKiB, "cpu:0"}});
    auto out = coalesceRegions(entries);  // auto: 1 MiB floor applies
    ASSERT_EQ(out.size(), 1u);
    EXPECT_EQ(out[0].size, 100 * kKiB);
    EXPECT_EQ(out[0].location, "cpu:0");
}

// `max_buckets=0` and `min_bucket_bytes=0` together would historically
// yield bucket_bytes=0 → divide-by-zero / take=0 infinite loop. Defensive
// floor inside the helper must keep the call well-defined.
TEST(CoalesceRegionsTest, DefensiveZeroParams) {
    auto entries = makeEntries({{256 * kKiB, "cpu:0"}, {256 * kKiB, "cpu:1"}});
    auto out = coalesceRegions(entries,
                               /*max_buckets=*/0,
                               /*min_bucket_bytes=*/0);
    EXPECT_FALSE(out.empty());
    EXPECT_EQ(sumSizes(out), sumLens(entries));
}

// Documents the intentional precision/size tradeoff: a tiny minority
// region inside a much larger bucket gets the majority label. This is the
// designed behavior, not a bug — kept here so any future change that
// "fixes" this case has to consciously remove the test.
TEST(CoalesceRegionsTest, AcceptsApproximation) {
    auto entries = makeEntries({{1 * kKiB, "cpu:0"}, {9 * kMiB, "cpu:1"}});
    auto out = coalesceRegions(entries);  // auto bucket >> 1 KiB
    ASSERT_FALSE(out.empty());
    EXPECT_EQ(out.front().location, "cpu:1");
    EXPECT_EQ(sumSizes(out), sumLens(entries));
}

// Verifies only spatially adjacent entries get merged into the same
// Region: a 1 MiB bucket size + 6 alternating 256 KiB segments yields
// (a) Regions in input order, (b) the second Region cannot pull cpu:0
// votes from the first bucket back across the boundary.
TEST(CoalesceRegionsTest, OnlyAdjacentMerged) {
    auto entries = makeEntries({{256 * kKiB, "cpu:0"},
                                {256 * kKiB, "cpu:1"},
                                {256 * kKiB, "cpu:0"},
                                {256 * kKiB, "cpu:1"},
                                {256 * kKiB, "cpu:0"},
                                {256 * kKiB, "cpu:1"}});
    auto out = coalesceRegions(entries);
    ASSERT_EQ(out.size(), 2u);
    EXPECT_EQ(out[0].size, 1 * kMiB);
    EXPECT_EQ(out[0].location, "cpu:0");  // 4-way tie, first-seen wins
    EXPECT_EQ(out[1].size, 512 * kKiB);
    EXPECT_EQ(out[1].location, "cpu:0");  // 2-way tie, first-seen wins
    EXPECT_EQ(out[0].size + out[1].size, sumLens(entries));
}

}  // namespace
}  // namespace tent
}  // namespace mooncake
