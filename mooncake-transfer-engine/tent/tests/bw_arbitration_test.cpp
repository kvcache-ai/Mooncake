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
//
// Unit tests for the deadline-aware bandwidth arbitration ordering (#2792).

#include "tent/transport/rdma/bw_arbitration.h"

#include <gtest/gtest.h>

#include <cstdint>
#include <vector>

namespace mooncake {
namespace tent {
namespace {

constexpr uint64_t kNow = 1'000'000'000;  // 1s in ns
constexpr double kBw = 1e9;               // 1 GB/s -> 16 B takes 16 ns

// A flow whose window is `window_ns` from now, transferring `len` bytes.
ArbFlow flow(uint64_t window_ns, size_t len) {
    return ArbFlow{kNow + window_ns, len};
}

TEST(BwArbitrationTest, TighterDeadlineSortsFirst) {
    std::vector<ArbFlow> flows = {
        flow(1'000'000, 4096),  // idx0: loose (1ms window)
        flow(10'000, 4096),     // idx1: tight (10us window) -> most urgent
        flow(100'000, 4096),    // idx2: medium
    };
    auto order = OrderByUrgency(flows, kNow, kBw);
    ASSERT_EQ(order.size(), 3u);
    EXPECT_EQ(order[0], 1u);  // tightest first
    EXPECT_EQ(order[1], 2u);
    EXPECT_EQ(order[2], 0u);  // loosest last
}

TEST(BwArbitrationTest, NoDeadlineSortsLast) {
    std::vector<ArbFlow> flows = {
        ArbFlow{0, 4096},    // idx0: no deadline -> least urgent
        flow(50'000, 4096),  // idx1: has deadline -> first
    };
    auto order = OrderByUrgency(flows, kNow, kBw);
    EXPECT_EQ(order[0], 1u);
    EXPECT_EQ(order[1], 0u);
}

TEST(BwArbitrationTest, PastDeadlineSortsFirst) {
    std::vector<ArbFlow> flows = {
        flow(50'000, 4096),       // idx0: still feasible
        ArbFlow{kNow - 1, 4096},  // idx1: already past -> most urgent
    };
    auto order = OrderByUrgency(flows, kNow, kBw);
    EXPECT_EQ(order[0], 1u);
    EXPECT_EQ(order[1], 0u);
}

TEST(BwArbitrationTest, TiesKeepFifoOrder) {
    // Identical deadlines/lengths -> stable sort preserves original order.
    std::vector<ArbFlow> flows = {
        flow(50'000, 4096),  // idx0
        flow(50'000, 4096),  // idx1
        flow(50'000, 4096),  // idx2
    };
    auto order = OrderByUrgency(flows, kNow, kBw);
    EXPECT_EQ(order, (std::vector<size_t>{0, 1, 2}));
}

TEST(BwArbitrationTest, AllNoDeadlineKeepsFifoOrder) {
    // No flow has a deadline -> byte-identical to today's order (no reorder).
    std::vector<ArbFlow> flows = {ArbFlow{0, 1}, ArbFlow{0, 2}, ArbFlow{0, 3}};
    auto order = OrderByUrgency(flows, kNow, kBw);
    EXPECT_EQ(order, (std::vector<size_t>{0, 1, 2}));
}

TEST(BwArbitrationTest, ZeroBandwidthDisablesReorder) {
    // bw<=0 -> prediction disabled -> original order preserved.
    std::vector<ArbFlow> flows = {
        flow(1'000'000, 4096),
        flow(10'000, 4096),
    };
    auto order = OrderByUrgency(flows, kNow, /*bw_bps=*/0.0);
    EXPECT_EQ(order, (std::vector<size_t>{0, 1}));
}

TEST(BwArbitrationTest, LongerTransferIsMoreUrgentAtSameDeadline) {
    // Same window, but a bigger transfer has higher predicted MLU (needs more
    // of the shared bandwidth to finish in time).
    std::vector<ArbFlow> flows = {
        flow(50'000, 4096),   // idx0: small
        flow(50'000, 65536),  // idx1: large -> more urgent
    };
    auto order = OrderByUrgency(flows, kNow, kBw);
    EXPECT_EQ(order[0], 1u);
    EXPECT_EQ(order[1], 0u);
}

}  // namespace
}  // namespace tent
}  // namespace mooncake
