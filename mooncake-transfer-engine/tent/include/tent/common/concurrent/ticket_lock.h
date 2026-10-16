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

#ifndef TENT_TICKET_LOCK_H
#define TENT_TICKET_LOCK_H

#include <atomic>
#include <cstdint>
#include <thread>

#include "tent/common/types.h"
#include "tent/common/utils/os.h"

namespace mooncake {
namespace tent {
// FIFO spinlock for short, bounded critical sections: arrivals take a ticket
// and are served in order, so no waiter can be starved by a re-acquiring
// holder. Not reentrant, no try_lock.
class TicketLock {
   public:
    TicketLock() : next_ticket_(0), now_serving_(0) {}

    void lock() {
        const int my_ticket =
            next_ticket_.fetch_add(1, std::memory_order_relaxed);
        // A running holder leaves a short critical section well within the
        // spin budget, and pause keeps the loop from clogging the pipeline.
        // Past the budget the holder is most likely preempted, so the CPU is
        // better given away than burned; same policy as RWSpinlock.
        uint32_t spins = 0;
        while (now_serving_.load(std::memory_order_acquire) != my_ticket) {
            if (++spins < kSpinsBeforeYield)
                PAUSE();
            else
                std::this_thread::yield();
        }
    }

    void unlock() { now_serving_.fetch_add(1, std::memory_order_release); }

   private:
    friend class TicketLockTestPeer;  // tests read the ticket counters

    static constexpr uint32_t kSpinsBeforeYield = 1000;

    std::atomic<int> next_ticket_;
    std::atomic<int> now_serving_;
    uint64_t padding_[14];
};
}  // namespace tent
}  // namespace mooncake

#endif  // TENT_TICKET_LOCK_H
