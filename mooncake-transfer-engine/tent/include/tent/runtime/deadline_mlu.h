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
// The one definition of a request's predicted deadline feasibility (MLU),
// shared by the admission queue's drop predictor (RFC #2519 step 3) and the
// RDMA workers' bandwidth arbitration (RFC #2792). Both must rank the same
// request the same way from the same bandwidth series, or the admission
// layer can drop a flow the NIC layer just promoted.

#pragma once

#include <cstddef>
#include <cstdint>
#include <limits>

namespace mooncake {
namespace tent {

// Predicted MLU = predicted completion time / remaining deadline window,
// where predicted completion time = (bytes_ahead + length) / bw_bps.
//
// The deadline is absolute, so the request must first wait for the bytes
// already in the pipeline ahead of it (`bytes_ahead`: dispatched but not
// completed) and then spend its own wire time. Queueing is therefore an
// additive term over the wire rate -- not folded into a slower bandwidth,
// which would scale the wait by the request's size.
//
// Higher == more urgent. No deadline (deadline_ns == 0) or no usable
// bandwidth (bw_bps <= 0) yields 0: nothing to predict, so never urgent and
// never dropped. A deadline already reached is infinitely urgent.
inline double DeadlineMlu(size_t bytes_ahead, size_t length,
                          uint64_t deadline_ns, uint64_t now_ns,
                          double bw_bps) {
    if (deadline_ns == 0 || bw_bps <= 0.0) return 0.0;
    if (deadline_ns <= now_ns) return std::numeric_limits<double>::max();
    const double window_s = (deadline_ns - now_ns) / 1e9;
    const double predicted_time_s =
        (static_cast<double>(bytes_ahead) + static_cast<double>(length)) /
        bw_bps;
    return predicted_time_s / window_s;
}

}  // namespace tent
}  // namespace mooncake
