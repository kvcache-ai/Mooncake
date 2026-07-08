// Copyright 2024 KVCache.AI
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

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <future>
#include <thread>
#include <vector>

#include "transport/rdma_transport/rdma_transport.h"

namespace mooncake {
namespace {

// The number of concurrently "held" slots must never exceed the configured
// limit, regardless of how many threads contend for the limiter.
TEST(ConnectionLimiterTest, NeverExceedsMaxConcurrent) {
    constexpr int kMaxConcurrent = 4;
    constexpr int kNumThreads = 32;
    ConnectionLimiter limiter(kMaxConcurrent);

    std::atomic<int> active{0};
    std::atomic<int> max_observed{0};

    auto worker = [&]() {
        limiter.acquire();
        int now = active.fetch_add(1) + 1;
        // Track the peak number of simultaneous holders.
        int prev = max_observed.load();
        while (now > prev && !max_observed.compare_exchange_weak(prev, now)) {
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
        active.fetch_sub(1);
        limiter.release();
    };

    std::vector<std::thread> threads;
    threads.reserve(kNumThreads);
    for (int i = 0; i < kNumThreads; ++i) threads.emplace_back(worker);
    for (auto& t : threads) t.join();

    EXPECT_LE(max_observed.load(), kMaxConcurrent);
    EXPECT_EQ(active.load(), 0);
}

// acquire() must block once the limit is reached, and unblock only after a
// corresponding release().
TEST(ConnectionLimiterTest, AcquireBlocksWhenFullAndUnblocksOnRelease) {
    ConnectionLimiter limiter(1);
    limiter.acquire();  // Take the only available slot.

    std::atomic<bool> second_acquired{false};
    auto fut = std::async(std::launch::async, [&]() {
        limiter.acquire();
        second_acquired.store(true);
        limiter.release();
    });

    // The second acquire should still be blocked because the slot is taken.
    EXPECT_EQ(fut.wait_for(std::chrono::milliseconds(100)),
              std::future_status::timeout);
    EXPECT_FALSE(second_acquired.load());

    // Releasing the slot should let the blocked acquire proceed.
    limiter.release();
    ASSERT_EQ(fut.wait_for(std::chrono::seconds(5)), std::future_status::ready);
    EXPECT_TRUE(second_acquired.load());
}

// A limiter with a large limit should never block for a small number of
// sequential acquire/release cycles.
TEST(ConnectionLimiterTest, SequentialAcquireReleaseDoesNotDeadlock) {
    ConnectionLimiter limiter(16);
    for (int i = 0; i < 100; ++i) {
        limiter.acquire();
        limiter.release();
    }
    SUCCEED();
}

}  // namespace
}  // namespace mooncake
