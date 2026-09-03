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
// Deadline-aware NIC bandwidth arbitration WITHIN a priority tier (RFC #2792).
//
// TENT's QoS is otherwise vertical: SL/TC and priority tiers separate business
// classes. But when several flows in the SAME tier contend for one NIC, the
// NIC's bandwidth is split blindly/equally (measured: a ~388 Gb/s NIC gives
// ~97 Gb/s to each of 4 contending flows). There is no way to let a flow that
// is about to miss its deadline claim a larger share.
//
// This header isolates the pure ordering decision so it can be unit-tested
// without the RDMA stack: given the contending slices' (deadline_ns, length)
// and a bandwidth estimate, order them most-urgent-first by predicted MLU
// (predicted transfer time / remaining deadline window) — the same MLU used by
// the admission layer (#2618/#2764). Opt-in: when disabled, the original order
// is preserved exactly (byte-identical to today's equal split).

#pragma once

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <vector>

#include "tent/runtime/deadline_mlu.h"

namespace mooncake {
namespace tent {

// Minimal view of a contending slice for arbitration. Kept free of RdmaSlice
// so the policy is unit-testable in isolation.
struct ArbFlow {
    uint64_t deadline_ns;  // 0 == no deadline
    size_t length;         // bytes to transfer
};

// Predicted MLU for one contending flow; see DeadlineMlu for the semantics.
// `bytes_ahead` is what the NIC has to move before this flow: the bytes
// already posted to it, plus those of the flows OrderByUrgency has placed
// ahead of this one. Higher == more urgent (closer to / past its deadline).
// bw_bps <= 0 disables prediction (returns 0 for everyone == no reordering).
inline double PredictedMlu(const ArbFlow& f, uint64_t now_ns, double bw_bps,
                           size_t bytes_ahead = 0) {
    return DeadlineMlu(bytes_ahead, f.length, f.deadline_ns, now_ns, bw_bps);
}

// Return the indices of `flows` ordered most-urgent-first (highest predicted
// MLU first). Ties keep the original relative order, so with no deadlines
// anywhere the input order is preserved exactly. This never drops or admits
// anything; it only reorders selection among already-eligible, same-tier
// flows.
//
// The order is built one slot at a time: the flow that takes a slot is then
// part of what the remaining flows wait behind, so its bytes join
// `bytes_ahead` before the next slot is scored. `bytes_ahead` on entry is
// what the NIC owes that is *not* in `flows`. Scoring every flow once
// against the same value would instead treat all of them as simultaneous.
//
// Cost is O(n * kGreedySlots). Callers post a prefix of this order (the QP
// budget decides how long), so only the head is worth resolving exactly;
// past kGreedySlots the remainder is ranked in one pass against the bytes
// accumulated so far.
inline constexpr size_t kGreedySlots = 64;

inline std::vector<size_t> OrderByUrgency(const std::vector<ArbFlow>& flows,
                                          uint64_t now_ns, double bw_bps,
                                          size_t bytes_ahead = 0) {
    const size_t n = flows.size();
    std::vector<size_t> order;
    order.reserve(n);
    std::vector<bool> taken(n, false);

    const size_t greedy = std::min(n, kGreedySlots);
    for (size_t slot = 0; slot < greedy; ++slot) {
        size_t best = n;
        double best_mlu = 0.0;
        for (size_t i = 0; i < n; ++i) {
            if (taken[i]) continue;
            const double mlu =
                PredictedMlu(flows[i], now_ns, bw_bps, bytes_ahead);
            // Strictly greater keeps the lowest index on ties (FIFO).
            if (best == n || mlu > best_mlu) {
                best = i;
                best_mlu = mlu;
            }
        }
        taken[best] = true;
        order.push_back(best);
        bytes_ahead += flows[best].length;
    }
    if (order.size() == n) return order;

    std::vector<size_t> rest;
    rest.reserve(n - order.size());
    for (size_t i = 0; i < n; ++i)
        if (!taken[i]) rest.push_back(i);
    std::vector<double> mlu(n, 0.0);
    for (size_t i : rest)
        mlu[i] = PredictedMlu(flows[i], now_ns, bw_bps, bytes_ahead);
    std::stable_sort(rest.begin(), rest.end(),
                     [&](size_t a, size_t b) { return mlu[a] > mlu[b]; });
    order.insert(order.end(), rest.begin(), rest.end());
    return order;
}

}  // namespace tent
}  // namespace mooncake
