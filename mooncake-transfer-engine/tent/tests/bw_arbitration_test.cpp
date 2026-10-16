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
#include "tent/runtime/deadline_mlu.h"

#include <gtest/gtest.h>

#include <cstdint>
#include <limits>
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

// The admission queue's drop predictor and the NIC arbitration must compute
// the same MLU from the same inputs; DeadlineMlu is that one definition.
TEST(DeadlineMluTest, PredictedTimeOverWindow) {
    // 16 B at 1 GB/s = 16 ns; a 32 ns window is half used.
    EXPECT_DOUBLE_EQ(DeadlineMlu(0, 16, kNow + 32, kNow, kBw), 0.5);
}

// A deadline is absolute, so time spent behind bytes already in the pipeline
// counts against the window: it is an additive delay, not a slower link.
TEST(DeadlineMluTest, BytesAheadAddToPredictedTime) {
    EXPECT_DOUBLE_EQ(DeadlineMlu(16, 16, kNow + 32, kNow, kBw), 1.0);
    // Queueing alone can exhaust the window even for a tiny request.
    EXPECT_DOUBLE_EQ(DeadlineMlu(64, 0, kNow + 32, kNow, kBw), 2.0);
}

TEST(DeadlineMluTest, NoDeadlineOrNoBandwidthIsNotUrgent) {
    EXPECT_DOUBLE_EQ(DeadlineMlu(0, 16, 0, kNow, kBw), 0.0);
    EXPECT_DOUBLE_EQ(DeadlineMlu(0, 16, kNow + 32, kNow, 0.0), 0.0);
    EXPECT_DOUBLE_EQ(DeadlineMlu(0, 16, kNow + 32, kNow, -1.0), 0.0);
}

TEST(DeadlineMluTest, PastDeadlineIsInfinitelyUrgent) {
    EXPECT_EQ(DeadlineMlu(0, 16, kNow, kNow, kBw),
              std::numeric_limits<double>::max());
    EXPECT_EQ(DeadlineMlu(0, 16, kNow - 1, kNow, kBw),
              std::numeric_limits<double>::max());
}

TEST(DeadlineMluTest, ArbitrationUsesTheSameDefinition) {
    const ArbFlow f = flow(32, 16);
    EXPECT_DOUBLE_EQ(PredictedMlu(f, kNow, kBw),
                     DeadlineMlu(0, f.length, f.deadline_ns, kNow, kBw));
    EXPECT_DOUBLE_EQ(PredictedMlu(f, kNow, kBw, 48),
                     DeadlineMlu(48, f.length, f.deadline_ns, kNow, kBw));
}

TEST(BwArbitrationTest, BytesAheadFavorTheTighterWindow) {
    std::vector<ArbFlow> flows = {
        flow(100'000, 4096),  // idx0: 4.1us of 100us  -> MLU 0.041
        flow(30'000, 1024),   // idx1: 1.0us of 30us   -> MLU 0.034
    };
    // On an idle NIC the bigger request is (slightly) more urgent...
    auto idle = OrderByUrgency(flows, kNow, kBw);
    EXPECT_EQ(idle[0], 0u);
    // ...but behind 64 KiB already queued (65.5us) the 30us window is gone
    // while the 100us one still has room, so the small flow must go first.
    auto busy = OrderByUrgency(flows, kNow, kBw, 65536);
    EXPECT_EQ(busy[0], 1u);
    EXPECT_EQ(busy[1], 0u);
}

// Ordering is decided one slot at a time: once a flow takes a slot, the
// flows still waiting sit behind its bytes too, so the queue-ahead term
// grows as the order is built. Scoring everyone once against the same value
// would miss that.
TEST(BwArbitrationTest, EachSlotIsScoredBehindTheOnesBeforeIt) {
    // 1 GB/s: 1 byte == 1 ns of wire time.
    std::vector<ArbFlow> flows = {
        flow(40'000, 100'000),  // idx0: 100us of a 40us window -> 2.50
        flow(100'000, 20'000),  // idx1: 20us of a 100us window -> 0.20
        flow(50'000, 5'000),    // idx2: 5us of a 50us window   -> 0.10
    };
    // Scored once, the order would be its own MLU ranking: 0, 1, 2.
    // Behind idx0's 100us, though, idx2's 50us window is blown (105/50 =
    // 2.10) while idx1 still fits (120/100 = 1.20), so idx2 goes first.
    auto order = OrderByUrgency(flows, kNow, kBw);
    ASSERT_EQ(order.size(), 3u);
    EXPECT_EQ(order[0], 0u);
    EXPECT_EQ(order[1], 2u);
    EXPECT_EQ(order[2], 1u);
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
