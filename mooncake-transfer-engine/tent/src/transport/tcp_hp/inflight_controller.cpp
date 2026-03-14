// Copyright 2025 KVCache.AI
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

#include "tent/transport/tcp_hp/inflight_controller.h"

namespace mooncake {
namespace tent {
namespace tcp_hp {

InflightController::InflightController(size_t max_inflight)
    : max_inflight_(max_inflight) {}

bool InflightController::tryAcquire() {
    if (max_inflight_ == 0) return true;  // Unlimited.
    size_t cur = current_.load(std::memory_order_relaxed);
    while (cur < max_inflight_) {
        if (current_.compare_exchange_weak(cur, cur + 1,
                                           std::memory_order_acq_rel)) {
            return true;
        }
    }
    return false;
}

void InflightController::release() {
    if (max_inflight_ == 0) return;
    current_.fetch_sub(1, std::memory_order_acq_rel);
    dispatchPending();
}

void InflightController::enqueue(std::function<void()> task) {
    {
        std::lock_guard<std::mutex> lock(queue_mutex_);
        pending_.push(std::move(task));
    }
    // Try to dispatch immediately in case a slot freed up.
    dispatchPending();
}

void InflightController::dispatchPending() {
    while (true) {
        std::function<void()> task;
        {
            std::lock_guard<std::mutex> lock(queue_mutex_);
            if (pending_.empty()) return;
            // Try to acquire a slot for this pending task.
            size_t cur = current_.load(std::memory_order_relaxed);
            while (cur < max_inflight_) {
                if (current_.compare_exchange_weak(cur, cur + 1,
                                                   std::memory_order_acq_rel)) {
                    goto acquired;
                }
            }
            return;  // No slot available.
        acquired:
            task = std::move(pending_.front());
            pending_.pop();
        }
        task();
    }
}

size_t InflightController::pending() const {
    std::lock_guard<std::mutex> lock(
        const_cast<std::mutex&>(queue_mutex_));
    return pending_.size();
}

}  // namespace tcp_hp
}  // namespace tent
}  // namespace mooncake
