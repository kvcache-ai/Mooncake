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
// Pins both promotion policies used by Workers::promoteTimedOutRequests
// (issue #2528). Head-only is the default; per-entry is opt-in via
// transports/rdma/priority_promotion_per_entry. Contrasted on the same inputs
// so the default "flush the tier" behavior stays unambiguous (no RDMA stack,
// no timing noise).

#include "tent/transport/rdma/promotion_policy.h"

#include <gtest/gtest.h>

#include <cstdint>
#include <vector>

namespace mooncake {
namespace tent {
namespace {

constexpr uint64_t kTimeout = 10'000;  // 10us promotion timeout
constexpr uint64_t kNow = 1'000'000;   // fixed "now"

// One entry that has clearly timed out.
uint64_t timedOut() { return kNow - kTimeout - 1; }
// One entry that was just enqueued and is nowhere near the timeout.
uint64_t fresh() { return kNow - 1; }

// --- Issue #2528 point 1: head-only decision, whole-queue promotion --------

TEST(PromotionPolicyTest, HeadOnlyPromotesFreshEntriesWhenHeadTimedOut) {
    // Queue head is starving; the two entries behind it were just enqueued.
    std::vector<uint64_t> q = {timedOut(), fresh(), fresh()};

    auto d = DecidePromotionHeadOnly(q, kNow, kTimeout);

    // Default policy: all three are promoted, including the two fresh ones.
    EXPECT_EQ(d.promote_indices, (std::vector<size_t>{0, 1, 2}));

    // The per-entry policy promotes only the genuinely starving head.
    auto fixed = DecidePromotionPerEntry(q, kNow, kTimeout);
    EXPECT_EQ(fixed.promote_indices, (std::vector<size_t>{0}));
}

TEST(PromotionPolicyTest, HeadOnlyMissesTimedOutTailWhenHeadFresh) {
    // Head was just enqueued; entries behind it have been starving.
    std::vector<uint64_t> q = {fresh(), timedOut(), timedOut()};

    auto d = DecidePromotionHeadOnly(q, kNow, kTimeout);

    // Default policy: nothing is promoted even though indices 1 and 2 starve.
    EXPECT_TRUE(d.promote_indices.empty());

    auto fixed = DecidePromotionPerEntry(q, kNow, kTimeout);
    EXPECT_EQ(fixed.promote_indices, (std::vector<size_t>{1, 2}));
}

// --- Shared behavior both policies must keep ------------------------------

TEST(PromotionPolicyTest, EmptyQueuePromotesNothing) {
    std::vector<uint64_t> q;
    EXPECT_TRUE(
        DecidePromotionHeadOnly(q, kNow, kTimeout).promote_indices.empty());
    EXPECT_TRUE(
        DecidePromotionPerEntry(q, kNow, kTimeout).promote_indices.empty());
}

TEST(PromotionPolicyTest, ZeroTimestampNeverTimesOut) {
    // enqueue_ts == 0 means "no timestamp" and must never be promoted.
    std::vector<uint64_t> q = {0, 0};
    EXPECT_TRUE(
        DecidePromotionHeadOnly(q, kNow, kTimeout).promote_indices.empty());
    EXPECT_TRUE(
        DecidePromotionPerEntry(q, kNow, kTimeout).promote_indices.empty());
}

TEST(PromotionPolicyTest, AllTimedOutPromotesAllUnderBothPolicies) {
    // When every entry is starving the two policies agree — this is the case
    // the default head-only policy was designed around.
    std::vector<uint64_t> q = {timedOut(), timedOut(), timedOut()};
    EXPECT_EQ(DecidePromotionHeadOnly(q, kNow, kTimeout).promote_indices,
              (std::vector<size_t>{0, 1, 2}));
    EXPECT_EQ(DecidePromotionPerEntry(q, kNow, kTimeout).promote_indices,
              (std::vector<size_t>{0, 1, 2}));
}

}  // namespace
}  // namespace tent
}  // namespace mooncake
