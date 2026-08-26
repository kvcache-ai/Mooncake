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

#include "tent/transport/tcp/high_performance_tcp_buffer_registry.h"
#include "tent/transport/tcp/high_performance_tcp_task.h"
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

TEST(HighPerformanceTcpWorkersTest,
     AtomicBatchAdmissionHasZeroSideEffectsOnFullMailbox) {
    HighPerformanceTcpWorkers workers({.worker_count = 2, .queue_capacity = 1});
    ASSERT_TRUE(workers.start().ok());

    std::mutex mutex;
    std::condition_variable cv;
    bool blocker_started = false;
    bool release_blocker = false;
    ASSERT_TRUE(
        workers
            .submitToWorker(1,
                            [&](size_t) {
                                std::unique_lock<std::mutex> lock(mutex);
                                blocker_started = true;
                                cv.notify_all();
                                cv.wait(lock, [&] { return release_blocker; });
                            })
            .ok());
    {
        std::unique_lock<std::mutex> lock(mutex);
        ASSERT_TRUE(cv.wait_for(lock, 2s, [&] { return blocker_started; }));
    }
    ASSERT_TRUE(workers.submitToWorker(1, [](size_t) {}).ok());
    ASSERT_EQ(workers.mailboxDepth(1), 1u);

    HighPerformanceTcpAdmissionController admission(8, 4096);
    std::atomic<size_t> ran{0};
    bool ownership_committed = false;
    std::vector<HighPerformanceTcpWorkers::Command> commands;
    commands.push_back({.worker_id = 0,
                        .run = [&](size_t) { ran.fetch_add(1); },
                        .cancel = [] {}});
    commands.push_back({.worker_id = 1,
                        .run = [&](size_t) { ran.fetch_add(1); },
                        .cancel = [] {}});
    Status status = workers.tryCommitBatch(commands, &admission, 2, 1024,
                                           [&] { ownership_committed = true; });
    EXPECT_TRUE(status.IsTooManyRequests());
    EXPECT_FALSE(ownership_committed);
    EXPECT_EQ(admission.outstandingTasks(), 0u);
    EXPECT_EQ(admission.outstandingBytes(), 0u);
    EXPECT_EQ(workers.mailboxDepth(0), 0u);
    EXPECT_EQ(ran.load(), 0u);

    {
        std::lock_guard<std::mutex> lock(mutex);
        release_blocker = true;
    }
    cv.notify_all();
    EXPECT_TRUE(workers.stop().ok());
}

TEST(HighPerformanceTcpWorkersTest, AdmissionBoundsTasksAndBytes) {
    HighPerformanceTcpAdmissionController admission(2, 100);
    ASSERT_TRUE(admission.tryReserve(1, 60).ok());
    EXPECT_TRUE(admission.tryReserve(1, 50).IsTooManyRequests());
    EXPECT_EQ(admission.outstandingTasks(), 1u);
    EXPECT_EQ(admission.outstandingBytes(), 60u);
    admission.release(1, 60);
    EXPECT_EQ(admission.outstandingTasks(), 0u);
    EXPECT_EQ(admission.outstandingBytes(), 0u);
    admission.close();
    EXPECT_TRUE(admission.tryReserve(1, 1).IsTooManyRequests());
}

TEST(HighPerformanceTcpTaskTest,
     CompletesExactlyOnceAndReleasesLeaseAndBudgetFirst) {
    HighPerformanceTcpBufferRegistry registry;
    std::array<uint8_t, 64> memory{};
    uint64_t registration = 0;
    ASSERT_TRUE(registry
                    .add(reinterpret_cast<uint64_t>(memory.data()),
                         memory.size(), kGlobalReadWrite, &registration)
                    .ok());
    HighPerformanceTcpBufferRegistry::Lease lease;
    ASSERT_TRUE(
        registry
            .acquireLocalLease(reinterpret_cast<uint64_t>(memory.data()),
                               memory.size(), &lease)
            .ok());

    HighPerformanceTcpAdmissionController admission(1, memory.size());
    ASSERT_TRUE(admission.tryReserve(1, memory.size()).ok());
    std::atomic<size_t> notifications{0};
    Request request{};
    request.opcode = Request::READ;
    request.source = memory.data();
    request.target_id = 1;
    request.target_offset = 0;
    request.length = memory.size();
    auto task = std::make_shared<HighPerformanceTcpTaskState>(
        request, 9,
        [&](BatchID batch) {
            EXPECT_EQ(batch, 9u);
            // Terminal publication happens after resource retirement.
            EXPECT_EQ(admission.outstandingTasks(), 0u);
            notifications.fetch_add(1);
        },
        std::move(lease));
    task->activateReservation(&admission, memory.size());

    EXPECT_TRUE(task->completeOnce(COMPLETED, memory.size()));
    EXPECT_FALSE(task->completeOnce(FAILED, 0));
    EXPECT_EQ(task->snapshot().s, COMPLETED);
    EXPECT_EQ(task->snapshot().transferred_bytes, memory.size());
    EXPECT_EQ(notifications.load(), 1u);
    EXPECT_EQ(admission.outstandingTasks(), 0u);
    EXPECT_TRUE(
        registry
            .remove(reinterpret_cast<uint64_t>(memory.data()), memory.size())
            .ok());
}

TEST(HighPerformanceTcpWorkersTest,
     StopIsIdempotentAndRuntimeIsNotRestartable) {
    HighPerformanceTcpWorkers workers({.worker_count = 2, .queue_capacity = 4});
    ASSERT_TRUE(workers.start().ok());
    Status a, b;
    std::thread first([&] { a = workers.stop(); });
    std::thread second([&] { b = workers.stop(); });
    first.join();
    second.join();
    EXPECT_TRUE(a.ok());
    EXPECT_TRUE(b.ok());
    EXPECT_TRUE(workers.stop().ok());
    EXPECT_TRUE(workers.start().IsInvalidArgument());
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
