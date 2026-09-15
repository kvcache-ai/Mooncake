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

#ifndef RDMA_PENDING_READ_RESP_QUEUE_H_
#define RDMA_PENDING_READ_RESP_QUEUE_H_

#include <cstdint>
#include <deque>
#include <functional>
#include <mutex>

#include "error.h"

namespace mooncake {

// Serialized queue for READ_RESP frames deferred when the bounce pool is full.
//
// A naive "enqueue then hope a later SEND completion drains" path loses the
// wakeup when the last outstanding SEND completes between the failed post and
// the enqueue. This helper uses a single-drainer / recheck protocol:
//
//   - At most one thread runs the drain loop at a time (drain_running_).
//   - enqueue() and onSlotFreed() both kick(); a kick that finds an active
//     drainer sets recheck_ so that drainer re-examines the queue before exit.
//   - ERR_TOO_MANY_REQUESTS pushes the item back and exits the loop only when
//     no concurrent kick arrived; otherwise it retries (the kick may have been
//     a real slot free during try_send).
struct PendingReadResp {
    uint64_t task_id = 0;
    uint64_t addr = 0;
    uint32_t length = 0;
    uint32_t slice_seq = 0;
};

class PendingReadRespQueue {
   public:
    // Returns 0 on post success, ERR_TOO_MANY_REQUESTS when the pool is still
    // full, or another error to drop the item.
    using TrySendFn = std::function<int(const PendingReadResp &)>;

    // Hold `item` and participate in a serialized drain.
    void enqueue(PendingReadResp item, const TrySendFn &try_send);

    // A send bounce slot just freed (SEND completion or pool expand).
    void onSlotFreed(const TrySendFn &try_send);

    // Test / diagnostics.
    size_t size() const;
    bool drainRunningForTest() const;

   private:
    void kick(const TrySendFn &try_send);

    mutable std::mutex mutex_;
    std::deque<PendingReadResp> pending_;
    bool drain_running_ = false;
    bool recheck_ = false;
};

}  // namespace mooncake

#endif  // RDMA_PENDING_READ_RESP_QUEUE_H_
