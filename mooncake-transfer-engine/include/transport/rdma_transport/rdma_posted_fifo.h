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

// Signal every k WRs and always the last WR of this ibv_post_send chain.
// k = min(interval, max_wr/2) so SQ slots free in two batches and later
// posts need not wait for the whole chain. interval<=1 is handled by the
// caller (every WR signaled, no FIFO). Each chain starts at index 0
// because the previous chain always ended signaled (or the endpoint was
// destroyed).
inline int rdmaSignalPeriod(int interval, int max_wr) {
    if (interval <= 1 || max_wr < 2) return 1;
    return std::max(1, std::min(interval, max_wr / 2));
}

inline bool shouldSignalRdmaWr(int index, int wr_count, int period) {
    return (index + 1) % period == 0 || index + 1 == wr_count;
}

// Pop posting-order WRs covered by this CQE. Failed and flushed WRs generate
// a CQE even without IBV_SEND_SIGNALED, so the tail stays in the FIFO for
// those later CQEs. On success the unsignaled prefix completed in RC order
// and is retired with this signaled CQE.
template <typename Slice>
size_t collectPostedFifo(std::deque<Slice *> &q, Slice *completed,
                         std::vector<Slice *> &out) {
    if (q.empty()) return 0;
    const auto found = std::find(q.begin(), q.end(), completed);
    if (found == q.end()) return 0;
    const size_t n = static_cast<size_t>(std::distance(q.begin(), found) + 1);
    out.insert(out.end(), q.begin(),
               std::next(q.begin(), static_cast<ptrdiff_t>(n)));
    q.erase(q.begin(), std::next(q.begin(), static_cast<ptrdiff_t>(n)));
    return n;
}

}  // namespace mooncake

#endif  // RDMA_POSTED_FIFO_H
