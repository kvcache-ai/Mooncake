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

// Unit tests for TicketLock: mutual exclusion under contention, hand-off in
// arrival order, and hand-off after a hold long enough to push waiters past
// the spin budget into the yield path.

#include "tent/common/concurrent/ticket_lock.h"

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <mutex>
#include <thread>
#include <vector>

namespace mooncake {
namespace tent {

// Tickets are the lock's only observable state, and a test that wants to
// arrange waiters in a known order has to see them handed out: a sleep only
// makes the order likely, not certain.
class TicketLockTestPeer {
   public:
    static int ticketsIssued(const TicketLock& lock) {
        return lock.next_ticket_.load(std::memory_order_acquire);
    }
};

namespace {

using namespace std::chrono_literals;

// Spin until `n` tickets have been handed out, i.e. n-1 waiters are queued
// behind the holder.
void waitForTickets(const TicketLock& lock, int n) {
    while (TicketLockTestPeer::ticketsIssued(lock) < n)
        std::this_thread::yield();
}

TEST(TicketLockTest, UncontendedLockUnlockRoundTrips) {
    TicketLock lock;
    for (int i = 0; i < 1000; ++i) {
        std::lock_guard<TicketLock> guard(lock);
    }
}

// Eight threads bump a plain counter under the lock and check that no two
// of them are ever inside the critical section together. Overlap would lose
// increments or show a nesting depth above one.
TEST(TicketLockTest, ExcludesConcurrentCriticalSections) {
    constexpr int kThreads = 8;
    constexpr int kIters = 20000;
    TicketLock lock;
    long counter = 0;    // deliberately not atomic
    int in_section = 0;  // deliberately not atomic
    std::atomic<bool> overlap{false};

    std::vector<std::thread> threads;
    for (int t = 0; t < kThreads; ++t) {
        threads.emplace_back([&] {
            for (int i = 0; i < kIters; ++i) {
                std::lock_guard<TicketLock> guard(lock);
                if (++in_section != 1) overlap.store(true);
                ++counter;
                --in_section;
            }
        });
    }
    for (auto& t : threads) t.join();

    EXPECT_EQ(counter, static_cast<long>(kThreads) * kIters);
    EXPECT_FALSE(overlap.load());
}

// With the lock held (ticket 0), B queues up (ticket 1) and then C does
// (ticket 2). On release B must be served before C: tickets are handed out on
// arrival and served in order. Each waiter is started only once the previous
// one is seen holding its ticket, so the arrival order is a fact, not a race
// the test hopes to win.
TEST(TicketLockTest, ServesWaitersInArrivalOrder) {
    constexpr int kRounds = 20;
    for (int round = 0; round < kRounds; ++round) {
        TicketLock lock;
        std::vector<char> order;  // written under the lock

        lock.lock();
        std::thread b([&] {
            lock.lock();
            order.push_back('B');
            lock.unlock();
        });
        waitForTickets(lock, 2);  // B holds ticket 1
        std::thread c([&] {
            lock.lock();
            order.push_back('C');
            lock.unlock();
        });
        waitForTickets(lock, 3);  // C holds ticket 2
        lock.unlock();

        b.join();
        c.join();
        ASSERT_EQ(order, (std::vector<char>{'B', 'C'})) << "round " << round;
    }
}

// A hold of tens of milliseconds is far past the spin budget, so a waiter
// that is already queued falls through to yield(); it must still take the
// lock on release.
TEST(TicketLockTest, HandsOverAfterAHoldLongerThanTheSpinBudget) {
    TicketLock lock;
    std::atomic<bool> acquired{false};

    lock.lock();
    std::thread waiter([&] {
        lock.lock();
        acquired.store(true);
        lock.unlock();
    });
    waitForTickets(lock, 2);            // the waiter is queued...
    std::this_thread::sleep_for(20ms);  // ...and spins past its budget
    EXPECT_FALSE(acquired.load());      // still held here

    lock.unlock();
    waiter.join();
    EXPECT_TRUE(acquired.load());
}

}  // namespace
}  // namespace tent
}  // namespace mooncake
