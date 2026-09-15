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

#include "transport/rdma_twosided/pending_read_resp_queue.h"

namespace mooncake {

void PendingReadRespQueue::enqueue(PendingReadResp item,
                                   const TrySendFn &try_send) {
    {
        std::lock_guard<std::mutex> lock(mutex_);
        pending_.push_back(std::move(item));
    }
    kick(try_send);
}

void PendingReadRespQueue::onSlotFreed(const TrySendFn &try_send) {
    kick(try_send);
}

size_t PendingReadRespQueue::size() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return pending_.size();
}

bool PendingReadRespQueue::drainRunningForTest() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return drain_running_;
}

void PendingReadRespQueue::kick(const TrySendFn &try_send) {
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (drain_running_) {
            // A concurrent enqueue or slot free arrived while another thread
            // owns the drain loop. That owner must re-examine the queue before
            // clearing drain_running_, or this kick is lost.
            recheck_ = true;
            return;
        }
        drain_running_ = true;
    }

    for (;;) {
        PendingReadResp item;
        {
            std::lock_guard<std::mutex> lock(mutex_);
            if (pending_.empty()) {
                if (!recheck_) {
                    drain_running_ = false;
                    return;
                }
                recheck_ = false;
                continue;
            }
            item = pending_.front();
            pending_.pop_front();
        }

        int rc = try_send(item);
        if (rc == ERR_TOO_MANY_REQUESTS) {
            std::lock_guard<std::mutex> lock(mutex_);
            pending_.push_front(item);
            if (recheck_) {
                // Slot may have freed (or a new item arrived) during try_send.
                // Clear the flag and retry; if the pool is still full we will
                // hit TOO_MANY again with recheck_ false and exit cleanly.
                recheck_ = false;
                continue;
            }
            drain_running_ = false;
            return;
        }
        // Success (0) or hard error (item dropped by try_send / caller): keep
        // draining any remaining queued responses.
    }
}

}  // namespace mooncake
