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

// Pop posting-order WRs covered by a signaled CQE. On success, everything up
// to `signaled` inclusive is retired (RC completion order). On error,
// `drain_rest` also retires WRs after it: they will not generate CQEs.
template <typename Slice>
size_t collectPostedFifo(std::deque<Slice *> &q, Slice *signaled,
                         bool drain_rest, std::vector<Slice *> &out) {
    if (!signaled || q.empty()) return 0;
    const auto found = std::find(q.begin(), q.end(), signaled);
    if (found == q.end()) return 0;
    const size_t n =
        drain_rest ? q.size()
                   : static_cast<size_t>(std::distance(q.begin(), found) + 1);
    out.insert(out.end(), q.begin(), std::next(q.begin(), static_cast<ptrdiff_t>(n)));
    q.erase(q.begin(), std::next(q.begin(), static_cast<ptrdiff_t>(n)));
    return n;
}

}  // namespace mooncake

#endif  // RDMA_POSTED_FIFO_H
