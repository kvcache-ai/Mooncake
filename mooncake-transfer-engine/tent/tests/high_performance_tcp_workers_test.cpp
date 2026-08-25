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

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <mutex>
#include <vector>

#include "tent/transport/tcp/high_performance_tcp_workers.h"

namespace mooncake {
namespace tent {
namespace {

using namespace std::chrono_literals;

TEST(HighPerformanceTcpWorkersTest, PreservesExplicitWorkerAffinity) {
    HighPerformanceTcpWorkers workers({.worker_count = 2, .queue_capacity = 4});
    ASSERT_TRUE(workers.start().ok());

    std::mutex mutex;
    std::condition_variable cv;
    size_t completed = 0;
    size_t wrong_worker = 0;
    for (size_t expected_worker : {0u, 1u, 0u, 1u}) {
        ASSERT_TRUE(workers
                        .submitToWorker(expected_worker,
                                        [&, expected_worker](size_t worker_id) {
                                            std::lock_guard<std::mutex> guard(
                                                mutex);
                                            ++completed;
                                            if (worker_id != expected_worker) {
                                                ++wrong_worker;
                                            }
                                            cv.notify_all();
                                        })
                        .ok());
    }

    {
        std::unique_lock<std::mutex> lock(mutex);
        ASSERT_TRUE(cv.wait_for(lock, 2s, [&] { return completed == 4; }));
    }
    EXPECT_EQ(wrong_worker, 0u);
    EXPECT_TRUE(workers.stop().ok());
}

TEST(HighPerformanceTcpWorkersTest, EnforcesPerWorkerQueueBound) {
    HighPerformanceTcpWorkers workers({.worker_count = 1, .queue_capacity = 1});
    ASSERT_TRUE(workers.start().ok());

    std::mutex mutex;
    std::condition_variable cv;
    bool first_started = false;
    bool release_first = false;
    ASSERT_TRUE(
        workers
            .submitToWorker(0,
                            [&](size_t) {
                                std::unique_lock<std::mutex> lock(mutex);
                                first_started = true;
                                cv.notify_all();
                                cv.wait(lock, [&] { return release_first; });
                            })
            .ok());
    {
        std::unique_lock<std::mutex> lock(mutex);
        ASSERT_TRUE(cv.wait_for(lock, 2s, [&] { return first_started; }));
    }

    ASSERT_TRUE(workers.submitToWorker(0, [](size_t) {}).ok());
    EXPECT_TRUE(workers.submitToWorker(0, [](size_t) {}).IsTooManyRequests());

    {
        std::lock_guard<std::mutex> lock(mutex);
        release_first = true;
    }
    cv.notify_all();
    EXPECT_TRUE(workers.stop().ok());
}

TEST(HighPerformanceTcpWorkersTest, DoesNotStealFromAFullAffinityWorker) {
    HighPerformanceTcpWorkers workers({.worker_count = 2, .queue_capacity = 1});
    ASSERT_TRUE(workers.start().ok());

    std::mutex mutex;
    std::condition_variable cv;
    bool first_started = false;
    bool release_first = false;
    ASSERT_TRUE(
        workers
            .submitToWorker(0,
                            [&](size_t) {
                                std::unique_lock<std::mutex> lock(mutex);
                                first_started = true;
                                cv.notify_all();
                                cv.wait(lock, [&] { return release_first; });
                            })
            .ok());
    {
        std::unique_lock<std::mutex> lock(mutex);
        ASSERT_TRUE(cv.wait_for(lock, 2s, [&] { return first_started; }));
    }
    ASSERT_TRUE(workers.submitToWorker(0, [](size_t) {}).ok());

    // A connection owner must remain on worker 0.  A saturated mailbox is
    // backpressure, not permission to move its socket state to worker 1.
    EXPECT_TRUE(workers.submitToWorker(0, [](size_t) {}).IsTooManyRequests());

    {
        std::lock_guard<std::mutex> lock(mutex);
        release_first = true;
    }
    cv.notify_all();
    EXPECT_TRUE(workers.stop().ok());
}

TEST(HighPerformanceTcpWorkersTest, StopIsABarrierAndRejectsNewWork) {
    HighPerformanceTcpWorkers workers({.worker_count = 2, .queue_capacity = 4});
    ASSERT_TRUE(workers.start().ok());

    std::atomic<size_t> completed{0};
    for (size_t i = 0; i < 6; ++i) {
        ASSERT_TRUE(
            workers.submit([&](size_t) { completed.fetch_add(1); }).ok());
    }

    EXPECT_TRUE(workers.stop().ok());
    // Runtime shutdown cancels queued commands; the transport owns their
    // completion accounting and settles them before calling this barrier.
    EXPECT_LE(completed.load(), 6u);
    EXPECT_TRUE(workers.submit([](size_t) {}).IsInternalError());
}

TEST(HighPerformanceTcpWorkersTest, RejectsInvalidConfiguration) {
    HighPerformanceTcpWorkers no_workers(
        {.worker_count = 0, .queue_capacity = 1});
    EXPECT_TRUE(no_workers.start().IsInvalidArgument());

    HighPerformanceTcpWorkers no_queue(
        {.worker_count = 1, .queue_capacity = 0});
    EXPECT_TRUE(no_queue.start().IsInvalidArgument());
}

}  // namespace
}  // namespace tent
}  // namespace mooncake
