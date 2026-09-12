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

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <mutex>
#include <thread>
#include <vector>

#include <gtest/gtest.h>

#include "error.h"
#include "transport/rdma_twosided/pending_read_resp_queue.h"

using namespace mooncake;

namespace {

PendingReadResp MakeItem(uint64_t task_id, uint32_t slice_seq = 0) {
    return PendingReadResp{task_id, /*addr=*/0, /*length=*/64, slice_seq};
}

}  // namespace

// Reviewer race: observe pool-full, then a slot frees and drains an empty
// queue, then enqueue. Without enqueue→kick the response would strand.
TEST(PendingReadRespQueueTest, EnqueueAfterEmptyDrainDoesNotLoseWakeup) {
    PendingReadRespQueue q;
    std::atomic<int> posts{0};
    std::atomic<bool> pool_full{true};

    auto try_send = [&](const PendingReadResp &) {
        if (pool_full.load(std::memory_order_acquire))
            return ERR_TOO_MANY_REQUESTS;
        posts.fetch_add(1, std::memory_order_relaxed);
        return 0;
    };

    // Immediate send would have failed; a completion drains while queue empty.
    ASSERT_EQ(try_send(MakeItem(1)), ERR_TOO_MANY_REQUESTS);
    pool_full.store(false, std::memory_order_release);
    q.onSlotFreed(try_send);  // empty: no-op
    EXPECT_EQ(posts.load(), 0);

    // Enqueue must kick drain itself so the response is posted without waiting
    // for another completion or pool expand.
    q.enqueue(MakeItem(1), try_send);
    EXPECT_EQ(posts.load(), 1);
    EXPECT_EQ(q.size(), 0u);
    EXPECT_FALSE(q.drainRunningForTest());
}

// Slot frees while drain is blocked inside try_send (still full). The free
// must arm recheck so the response is posted when try_send returns.
TEST(PendingReadRespQueueTest, SlotFreeDuringDrainIsNotLost) {
    PendingReadRespQueue q;
    std::mutex gate_mu;
    std::condition_variable gate_cv;
    bool entered_try_send = false;
    bool release_try_send = false;
    std::atomic<int> attempt{0};
    std::atomic<int> posts{0};

    auto try_send = [&](const PendingReadResp &) {
        const int n = attempt.fetch_add(1, std::memory_order_relaxed);
        if (n == 0) {
            {
                std::lock_guard<std::mutex> lock(gate_mu);
                entered_try_send = true;
            }
            gate_cv.notify_all();
            std::unique_lock<std::mutex> lock(gate_mu);
            gate_cv.wait(lock, [&] { return release_try_send; });
            return ERR_TOO_MANY_REQUESTS;
        }
        posts.fetch_add(1, std::memory_order_relaxed);
        return 0;
    };

    std::thread drainer([&] { q.enqueue(MakeItem(7), try_send); });

    {
        std::unique_lock<std::mutex> lock(gate_mu);
        ASSERT_TRUE(gate_cv.wait_for(lock, std::chrono::seconds(2),
                                     [&] { return entered_try_send; }));
    }
    // Mid-drain slot free: must set recheck_ rather than return as a no-op.
    q.onSlotFreed(try_send);
    {
        std::lock_guard<std::mutex> lock(gate_mu);
        release_try_send = true;
    }
    gate_cv.notify_all();
    drainer.join();

    EXPECT_EQ(posts.load(), 1);
    EXPECT_EQ(q.size(), 0u);
    EXPECT_FALSE(q.drainRunningForTest());
}

TEST(PendingReadRespQueueTest, StaysQueuedUntilSlotFrees) {
    PendingReadRespQueue q;
    std::atomic<bool> pool_full{true};
    std::atomic<int> posts{0};
    auto try_send = [&](const PendingReadResp &) {
        if (pool_full.load(std::memory_order_acquire))
            return ERR_TOO_MANY_REQUESTS;
        posts.fetch_add(1, std::memory_order_relaxed);
        return 0;
    };

    q.enqueue(MakeItem(3), try_send);
    EXPECT_EQ(posts.load(), 0);
    EXPECT_EQ(q.size(), 1u);

    pool_full.store(false, std::memory_order_release);
    q.onSlotFreed(try_send);
    EXPECT_EQ(posts.load(), 1);
    EXPECT_EQ(q.size(), 0u);
}

TEST(PendingReadRespQueueTest, DrainsFifoAcrossMultipleItems) {
    PendingReadRespQueue q;
    std::vector<uint64_t> order;
    auto try_send = [&](const PendingReadResp &item) {
        order.push_back(item.task_id);
        return 0;
    };

    // First enqueue drains immediately; subsequent items enqueue while the
    // first drain may still be conceptually "done" — just verify all post.
    q.enqueue(MakeItem(1), try_send);
    q.enqueue(MakeItem(2), try_send);
    q.enqueue(MakeItem(3), try_send);
    ASSERT_EQ(order.size(), 3u);
    EXPECT_EQ(order[0], 1u);
    EXPECT_EQ(order[1], 2u);
    EXPECT_EQ(order[2], 3u);
    EXPECT_EQ(q.size(), 0u);
}
