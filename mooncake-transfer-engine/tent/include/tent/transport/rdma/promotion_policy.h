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
// Starvation-prevention promotion policy for the RDMA worker priority queues,
// factored out of Workers::promoteTimedOutRequests so the decision logic can be
// unit-tested without the full RDMA stack (issue #2528).
//
// A worker keeps three FIFO priority queues (HIGH > MEDIUM > LOW). To keep
// lower-priority requests from starving, every ~1ms a promotion pass looks at
// timed-out entries and moves them up one level. This header isolates *which*
// entries a pass decides to promote, given each entry's enqueue timestamp.

#pragma once

#include <cstddef>
#include <cstdint>
#include <vector>

namespace mooncake {
namespace tent {

// Result of a promotion decision for a single queue: the indices (into the
// drained queue order) that should move up one level. Empty == promote none.
struct PromotionDecision {
    std::vector<size_t> promote_indices;
    bool promoted_any() const { return !promote_indices.empty(); }
};

// Historical policy (as implemented in Workers::promoteTimedOutRequests today):
// the pass drains the whole queue, inspects ONLY the head entry, and if the
// head has timed out it promotes EVERY entry in the queue. This is the behavior
// #2528 flags as unintended:
//   * decision is head-only but applied to the whole queue -> freshly enqueued
//     entries that are not starving get promoted alongside the starving head;
//   * if the head has NOT timed out, later timed-out entries are not promoted.
//
// `enqueue_ts` is per drained entry in queue order (index 0 == head). A zero
// timestamp means "no timestamp" and never counts as timed out (matches the
// `enqueue_ts > 0` guard in the current code).
inline PromotionDecision DecidePromotionHeadOnly(
    const std::vector<uint64_t>& enqueue_ts, uint64_t current_ts,
    uint64_t promotion_timeout_ns) {
    PromotionDecision d;
    if (enqueue_ts.empty()) return d;
    const uint64_t head = enqueue_ts.front();
    // Guard current_ts >= head before subtracting: both are unsigned, so a
    // non-monotonic clock / race where current_ts < head would otherwise
    // underflow and spuriously mark the entry timed out.
    const bool head_timed_out = head > 0 && current_ts >= head &&
                                (current_ts - head) >= promotion_timeout_ns;
    if (head_timed_out) {
        d.promote_indices.reserve(enqueue_ts.size());
        for (size_t i = 0; i < enqueue_ts.size(); ++i) {
            d.promote_indices.push_back(i);  // promote ALL
        }
    }
    return d;
}

// Per-entry policy: promote exactly the entries that have themselves timed out,
// leaving freshly enqueued entries in place. This is the behavior #2528
// proposes; kept here next to the historical policy so a test can contrast the
// two and a follow-up fix can switch Workers over to it.
inline PromotionDecision DecidePromotionPerEntry(
    const std::vector<uint64_t>& enqueue_ts, uint64_t current_ts,
    uint64_t promotion_timeout_ns) {
    PromotionDecision d;
    for (size_t i = 0; i < enqueue_ts.size(); ++i) {
        const uint64_t ts = enqueue_ts[i];
        // See DecidePromotionHeadOnly: guard against unsigned underflow when
        // current_ts < ts.
        if (ts > 0 && current_ts >= ts &&
            (current_ts - ts) >= promotion_timeout_ns) {
            d.promote_indices.push_back(i);
        }
    }
    return d;
}

}  // namespace tent
}  // namespace mooncake
