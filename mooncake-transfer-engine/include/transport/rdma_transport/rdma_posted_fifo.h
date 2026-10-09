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

#ifndef RDMA_POSTED_FIFO_H
#define RDMA_POSTED_FIFO_H

#include <algorithm>
#include <cstddef>
#include <deque>
#include <iterator>
#include <vector>

namespace mooncake {

// interval <= 1 keeps the historical "every WR is IBV_SEND_SIGNALED" path.
// Otherwise a WR is signaled when:
//   * unsignaled_since + 1 reaches the interval,
//   * it is the last WR of this ibv_post_send chain (one CQE retires the
//     burst so the SQ cannot fill with only unsignaled WRs), or
//   * unsignaled_since + 1 reaches max_wr / 2 (hard cap vs SQ deadlock).
inline bool shouldSignalRdmaWr(int unsignaled_since, int interval, int max_wr,
                               bool last_in_chain) {
    if (interval <= 1) return true;
    const int count = unsignaled_since + 1;
    if (count >= interval) return true;
    if (max_wr >= 2 && count >= max_wr / 2) return true;
    return last_in_chain;
}

// interval==1 (and invalid <=1) keeps the historical 1:1 CQE path: no FIFO.
inline bool useRdmaPostedFifo(int signal_interval) {
    return signal_interval > 1;
}

// How many WRs this CQE retires for cq_outstanding.
// Null wr_id: 0. No FIFO: 1 per non-null CQE. FIFO miss (stale): 0.
// FIFO hit: drained_n (the prefix through this CQE).
inline size_t wrRetiredByCqe(bool use_fifo, bool has_completed,
                             size_t drained_n) {
    if (!has_completed) return 0;
    if (!use_fifo) return 1;
    return drained_n;
}

// Pop posting-order WRs covered by this CQE. Failed and flushed WRs generate
// a CQE even without IBV_SEND_SIGNALED, so the tail stays in the FIFO for
// those later CQEs. On success the unsignaled prefix completed in RC order
// and is retired with this signaled CQE.
template <typename Slice>
size_t collectPostedFifo(std::deque<Slice *> &q, Slice *completed,
                         std::vector<Slice *> &out) {
    if (!completed || q.empty()) return 0;
    const auto found = std::find(q.begin(), q.end(), completed);
    if (found == q.end()) return 0;
    const size_t n = static_cast<size_t>(std::distance(q.begin(), found) + 1);
    out.insert(out.end(), q.begin(),
               std::next(q.begin(), static_cast<ptrdiff_t>(n)));
    q.erase(q.begin(), std::next(q.begin(), static_cast<ptrdiff_t>(n)));
    return n;
}

// How to stamp a drained slice from one CQE. The CQE's own WR (the last
// drained entry) always keeps the hardware status. Prefix WRs:
//   * SUCCESS CQE, or the first non-flush error on this QP: SUCCESS
//     (RC completed them on the wire before the failing WR).
//   * FLUSH_ERR, or any later error after the QP is already errored:
//     WR_FLUSH_ERR (those WRs did not complete; do not claim SUCCESS).
enum class DrainedSliceStatus {
    kKeepCqe,
    kSuccess,
    kFlushErr,
};

inline DrainedSliceStatus drainedSliceStatus(bool cqe_success,
                                             bool cqe_is_flush,
                                             bool qp_already_errored,
                                             size_t index, size_t drained_n) {
    if (index + 1 >= drained_n) return DrainedSliceStatus::kKeepCqe;
    if (cqe_success) return DrainedSliceStatus::kSuccess;
    if (!cqe_is_flush && !qp_already_errored)
        return DrainedSliceStatus::kSuccess;
    return DrainedSliceStatus::kFlushErr;
}

}  // namespace mooncake

#endif  // RDMA_POSTED_FIFO_H
