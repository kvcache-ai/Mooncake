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
#include <memory>
#include <thread>
#include <vector>

#include "tent/common/config.h"
#include "tent/common/types.h"
#include "tent/transport/tcp/tcp_transport.h"

namespace mooncake {
namespace tent {
namespace {

// ---------------------------------------------------------------------------
// TcpParams unit tests
// ---------------------------------------------------------------------------

TEST(TcpParamsTest, DefaultValues) {
    TcpParams params;
    EXPECT_EQ(params.max_retry_count, 3);
    EXPECT_EQ(params.retry_base_delay_ms, 100ULL);
    EXPECT_EQ(params.retry_max_delay_ms, 2'000ULL);
    EXPECT_EQ(params.max_concurrent_tasks, 16);
}

// ---------------------------------------------------------------------------
// TcpTask unit tests — atomic semantics
// ---------------------------------------------------------------------------

TEST(TcpTaskTest, DefaultConstruction) {
    TcpTask task;
    EXPECT_EQ(task.status_word.load(), TransferStatusEnum::PENDING);
    EXPECT_EQ(task.transferred_bytes.load(), 0u);
    EXPECT_EQ(task.target_addr, 0u);
}

TEST(TcpTaskTest, AtomicStatusTransitions) {
    TcpTask task;
    EXPECT_EQ(task.status_word.load(std::memory_order_acquire),
              TransferStatusEnum::PENDING);

    task.status_word.store(TransferStatusEnum::COMPLETED,
                           std::memory_order_release);
    EXPECT_EQ(task.status_word.load(std::memory_order_acquire),
              TransferStatusEnum::COMPLETED);

    task.status_word.store(TransferStatusEnum::FAILED,
                           std::memory_order_release);
    EXPECT_EQ(task.status_word.load(std::memory_order_acquire),
              TransferStatusEnum::FAILED);
}

TEST(TcpTaskTest, AtomicTransferredBytes) {
    TcpTask task;
    constexpr size_t kPayload = 1024 * 1024;
    task.transferred_bytes.store(kPayload, std::memory_order_release);
    EXPECT_EQ(task.transferred_bytes.load(std::memory_order_acquire), kPayload);
}

TEST(TcpTaskTest, MoveConstruction) {
    TcpTask a;
    a.status_word.store(TransferStatusEnum::COMPLETED,
                        std::memory_order_relaxed);
    a.transferred_bytes.store(4096, std::memory_order_relaxed);
    a.target_addr = 0xdeadbeef;

    TcpTask b(std::move(a));
    EXPECT_EQ(b.status_word.load(), TransferStatusEnum::COMPLETED);
    EXPECT_EQ(b.transferred_bytes.load(), 4096u);
    EXPECT_EQ(b.target_addr, 0xdeadbeef);
}

// ---------------------------------------------------------------------------
// TcpSubBatch unit tests
// ---------------------------------------------------------------------------

TEST(TcpSubBatchTest, EmptyBatch) {
    TcpSubBatch batch;
    batch.max_size = 64;
    EXPECT_EQ(batch.size(), 0u);
}

TEST(TcpSubBatchTest, EmplaceAndSize) {
    TcpSubBatch batch;
    batch.max_size = 64;
    batch.task_list.reserve(batch.max_size);

    batch.task_list.emplace_back();
    batch.task_list.emplace_back();
    batch.task_list.emplace_back();
    EXPECT_EQ(batch.size(), 3u);
}

TEST(TcpSubBatchTest, PointerStabilityAfterReserve) {
    // Verify that pointers taken after reserve remain valid when more
    // tasks are emplaced (important for async dispatch correctness).
    TcpSubBatch batch;
    batch.max_size = 8;
    batch.task_list.reserve(batch.max_size);

    batch.task_list.emplace_back();
    TcpTask* first = &batch.task_list[0];
    first->target_addr = 42;

    // Add more tasks (within reserved capacity)
    for (int i = 1; i < 8; ++i) {
        batch.task_list.emplace_back();
    }

    // First pointer must still be valid
    EXPECT_EQ(first->target_addr, 42u);
    EXPECT_EQ(&batch.task_list[0], first);
}

// ---------------------------------------------------------------------------
// TcpTransport config loading test
// ---------------------------------------------------------------------------

TEST(TcpTransportConfigTest, ConfigOverridesDefaults) {
    auto conf = std::make_shared<Config>();
    conf->set("transports/tcp/max_retry_count", 5);
    conf->set("transports/tcp/retry_base_delay_ms", 200ULL);
    conf->set("transports/tcp/retry_max_delay_ms", 4000ULL);
    conf->set("transports/tcp/max_concurrent_tasks", 32);

    EXPECT_EQ(conf->get("transports/tcp/max_retry_count", 0), 5);
    EXPECT_EQ(conf->get("transports/tcp/retry_base_delay_ms", 0ULL), 200ULL);
    EXPECT_EQ(conf->get("transports/tcp/retry_max_delay_ms", 0ULL), 4000ULL);
    EXPECT_EQ(conf->get("transports/tcp/max_concurrent_tasks", 0), 32);
}

TEST(TcpTransportConfigTest, MissingConfigUsesDefaults) {
    auto conf = std::make_shared<Config>();

    TcpParams defaults;
    EXPECT_EQ(
        conf->get("transports/tcp/max_retry_count", defaults.max_retry_count),
        3);
    EXPECT_EQ(conf->get("transports/tcp/max_concurrent_tasks",
                        defaults.max_concurrent_tasks),
              16);
}

// ---------------------------------------------------------------------------
// Cross-thread visibility test (verifies atomic correctness)
// ---------------------------------------------------------------------------

TEST(TcpTaskTest, CrossThreadVisibility) {
    TcpTask task;
    std::atomic<bool> writer_done{false};

    std::thread writer([&]() {
        task.transferred_bytes.store(8192, std::memory_order_release);
        task.status_word.store(TransferStatusEnum::COMPLETED,
                               std::memory_order_release);
        writer_done.store(true, std::memory_order_release);
    });

    // Spin until writer signals done
    while (!writer_done.load(std::memory_order_acquire)) {
        std::this_thread::yield();
    }

    EXPECT_EQ(task.status_word.load(std::memory_order_acquire),
              TransferStatusEnum::COMPLETED);
    EXPECT_EQ(task.transferred_bytes.load(std::memory_order_acquire), 8192u);

    writer.join();
}

// ---------------------------------------------------------------------------
// Exponential backoff calculation test
// ---------------------------------------------------------------------------

TEST(TcpRetryBackoffTest, ExponentialGrowthWithCap) {
    // Simulate the backoff logic from doTransferWithRetry
    const uint64_t base = 100;
    const uint64_t cap = 2000;
    uint64_t delay = base;

    std::vector<uint64_t> delays;
    for (int attempt = 0; attempt < 6; ++attempt) {
        delays.push_back(delay);
        delay = std::min(delay * 2, cap);
    }

    // 100 → 200 → 400 → 800 → 1600 → 2000 (capped)
    EXPECT_EQ(delays[0], 100u);
    EXPECT_EQ(delays[1], 200u);
    EXPECT_EQ(delays[2], 400u);
    EXPECT_EQ(delays[3], 800u);
    EXPECT_EQ(delays[4], 1600u);
    EXPECT_EQ(delays[5], 2000u);  // capped at retry_max_delay_ms
}

}  // namespace
}  // namespace tent
}  // namespace mooncake
