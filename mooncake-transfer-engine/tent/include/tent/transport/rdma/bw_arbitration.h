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
#include <limits>
#include <numeric>
#include <vector>

namespace mooncake {
namespace tent {

// Minimal view of a contending slice for arbitration. Kept free of RdmaSlice
// so the policy is unit-testable in isolation.
struct ArbFlow {
    uint64_t deadline_ns;  // 0 == no deadline
    size_t length;         // bytes to transfer
};

// Predicted Missed-Latency-per-Unit: predicted transfer time / remaining
// deadline window. Higher == more urgent (closer to / past its deadline).
// A flow with no deadline (deadline_ns == 0) is least urgent (MLU 0). A flow
// already past its deadline, or that cannot fit its window at the given
// bandwidth, gets a very high MLU so it sorts first. bw_bps <= 0 disables
// prediction (returns 0 for everyone == no reordering).
inline double PredictedMlu(const ArbFlow& f, uint64_t now_ns, double bw_bps) {
    if (f.deadline_ns == 0 || bw_bps <= 0.0) return 0.0;
    if (f.deadline_ns <= now_ns) return std::numeric_limits<double>::max();
    const double window_s = (f.deadline_ns - now_ns) / 1e9;
    const double predicted_time_s = static_cast<double>(f.length) / bw_bps;
    return predicted_time_s / window_s;
}

// Return the indices of `flows` ordered most-urgent-first (highest predicted
// MLU first). Ties and the no-deadline case keep original (FIFO) order — a
// stable sort — so a symmetric set degrades to today's behavior. This does not
// drop or admit anything; it only reorders selection among already-eligible,
// same-tier flows.
inline std::vector<size_t> OrderByUrgency(const std::vector<ArbFlow>& flows,
                                          uint64_t now_ns, double bw_bps) {
    std::vector<size_t> idx(flows.size());
    std::iota(idx.begin(), idx.end(), size_t{0});
    std::vector<double> mlu(flows.size());
    for (size_t i = 0; i < flows.size(); ++i) {
        mlu[i] = PredictedMlu(flows[i], now_ns, bw_bps);
    }
    std::stable_sort(idx.begin(), idx.end(), [&](size_t a, size_t b) {
        return mlu[a] > mlu[b];  // higher MLU first; stable keeps FIFO on ties
    });
    return idx;
}

}  // namespace tent
}  // namespace mooncake
