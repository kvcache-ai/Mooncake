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

#ifndef TCP_HP_INFLIGHT_CONTROLLER_H_
#define TCP_HP_INFLIGHT_CONTROLLER_H_

#include <atomic>
#include <functional>
#include <mutex>
#include <queue>

namespace mooncake {
namespace tent {
namespace tcp_hp {

// Limits the number of concurrent inflight transfers.
// When the limit is reached, new transfers are queued and dispatched
// as earlier transfers complete.
//
// Thread-safe: tryAcquire/release/enqueue can be called from any thread.
class InflightController {
   public:
    // max_inflight: maximum concurrent transfers (0 = unlimited)
    explicit InflightController(size_t max_inflight);

    // Try to acquire a slot. Returns true if under limit.
    bool tryAcquire();

    // Release a slot and dispatch the next pending task (if any).
    void release();

    // Enqueue a task to run when a slot becomes available.
    void enqueue(std::function<void()> task);

    size_t current() const {
        return current_.load(std::memory_order_relaxed);
    }

    size_t pending() const;

   private:
    void dispatchPending();

    size_t max_inflight_;
    std::atomic<size_t> current_{0};
    std::mutex queue_mutex_;
    std::queue<std::function<void()>> pending_;
};

}  // namespace tcp_hp
}  // namespace tent
}  // namespace mooncake

#endif  // TCP_HP_INFLIGHT_CONTROLLER_H_
