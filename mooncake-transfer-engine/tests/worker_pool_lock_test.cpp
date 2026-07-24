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
#include <future>
#include <memory>
#include <string>
#include <thread>

#include "transport/rdma_transport/rdma_context.h"
#include "transport/rdma_transport/rdma_transport.h"
#include "transport/rdma_transport/worker_pool.h"

#if defined(__has_feature)
#define MC_HAS_FEATURE(x) __has_feature(x)
#else
#define MC_HAS_FEATURE(x) 0
#endif
#if defined(__SANITIZE_ADDRESS__) || MC_HAS_FEATURE(address_sanitizer)
#include <sanitizer/lsan_interface.h>
#define MC_LSAN_IGNORE_OBJECT(p) __lsan_ignore_object(p)
#else
#define MC_LSAN_IGNORE_OBJECT(p) ((void)(p))
#endif

using namespace mooncake;

namespace mooncake {

class WorkerPoolTestPeer {
   public:
    static std::unique_ptr<WorkerPool> createWithoutThreads(
        RdmaContext& context) {
        return std::unique_ptr<WorkerPool>(
            new WorkerPool(context, /*numa_socket_id=*/0,
                           /*start_workers=*/false));
    }

    static std::mutex& postPollMutexFor(WorkerPool& worker_pool,
                                        const std::string& peer_nic_path) {
        return worker_pool.postPollMutexFor(peer_nic_path);
    }
};

}  // namespace mooncake

namespace {

class WorkerPoolLockTest : public ::testing::Test {
   protected:
    WorkerPoolLockTest()
        : transport_(new RdmaTransport()),
          context_(std::make_unique<RdmaContext>(*transport_, "unused")),
          worker_pool_(WorkerPoolTestPeer::createWithoutThreads(*context_)) {
        // Intentional leak: ~RdmaTransport dereferences metadata_, which is
        // null until install(). The worker-pool lock tests only need a stable
        // engine reference for RdmaContext construction.
        MC_LSAN_IGNORE_OBJECT(transport_);
    }

    RdmaTransport* transport_;
    std::unique_ptr<RdmaContext> context_;
    std::unique_ptr<WorkerPool> worker_pool_;
};

TEST_F(WorkerPoolLockTest, SamePeerSerializesPostAndCompletion) {
    constexpr auto kWait = std::chrono::milliseconds(50);
    auto& mutex =
        WorkerPoolTestPeer::postPollMutexFor(*worker_pool_, "10.0.0.1@mlx5_0");

    mutex.lock();
    std::atomic<bool> acquired{false};
    std::promise<void> started;
    auto done = std::async(std::launch::async, [&] {
        started.set_value();
        std::lock_guard<std::mutex> lock(mutex);
        acquired.store(true, std::memory_order_release);
    });

    started.get_future().wait();
    std::this_thread::sleep_for(kWait);
    EXPECT_FALSE(acquired.load(std::memory_order_acquire))
        << "same-peer completion must wait while post-send owns the stripe";

    mutex.unlock();
    auto wait_status = done.wait_for(kWait);
    ASSERT_EQ(wait_status, std::future_status::ready);
    EXPECT_TRUE(acquired.load(std::memory_order_acquire));
}

TEST_F(WorkerPoolLockTest, DifferentStripesCanProceedConcurrently) {
    constexpr auto kWait = std::chrono::milliseconds(50);
    const std::string first_peer = "10.0.0.1@mlx5_0";
    auto& first_mutex =
        WorkerPoolTestPeer::postPollMutexFor(*worker_pool_, first_peer);

    std::mutex* second_mutex = nullptr;
    for (int i = 0; i < 1024; ++i) {
        auto candidate = "10.0.0.2@mlx5_" + std::to_string(i);
        auto& candidate_mutex =
            WorkerPoolTestPeer::postPollMutexFor(*worker_pool_, candidate);
        if (&candidate_mutex != &first_mutex) {
            second_mutex = &candidate_mutex;
            break;
        }
    }
    ASSERT_NE(second_mutex, nullptr)
        << "test setup should find two peers assigned to different stripes";

    first_mutex.lock();
    std::atomic<bool> acquired{false};
    auto done = std::async(std::launch::async, [&] {
        std::lock_guard<std::mutex> lock(*second_mutex);
        acquired.store(true, std::memory_order_release);
    });

    auto wait_status = done.wait_for(kWait);
    first_mutex.unlock();
    ASSERT_EQ(wait_status, std::future_status::ready)
        << "different lock stripes should not block each other";
    EXPECT_TRUE(acquired.load(std::memory_order_acquire));
}

}  // namespace
