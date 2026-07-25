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
#include <thread>

#include "tent/common/concurrent/rw_spinlock.h"

namespace mooncake {
namespace tent {
namespace {

using namespace std::chrono_literals;

TEST(RWSpinlockTest, AggressiveWriteLockProgressesAcrossTicketWraparound) {
    RWSpinlock lock;
    int protected_value = 0;

    constexpr int kIterations = 65536 + 3;
    for (int i = 0; i < kIterations; ++i) {
        lock.writeLockAggressive();
        ++protected_value;
        lock.unlock();
    }

    RWSpinlock::ReadGuard guard(lock);
    EXPECT_EQ(protected_value, kIterations);
}

TEST(RWSpinlockTest, DowngradePublishesToReadersAndBlocksWriters) {
    RWSpinlock lock;
    int protected_value = 0;
    std::atomic<bool> writer_started{false};
    std::atomic<bool> writer_entered{false};
    std::atomic<bool> reader_observed{false};

    lock.writeLockAggressive();
    protected_value = 42;
    lock.unlockAndLockShared();

    std::thread reader([&] {
        RWSpinlock::ReadGuard guard(lock);
        reader_observed.store(protected_value == 42, std::memory_order_release);
    });

    reader.join();
    EXPECT_TRUE(reader_observed.load(std::memory_order_acquire));

    std::thread writer([&] {
        writer_started.store(true, std::memory_order_release);
        lock.writeLockAggressive();
        writer_entered.store(true, std::memory_order_release);
        protected_value = 99;
        lock.unlock();
    });

    while (!writer_started.load(std::memory_order_acquire)) {
        std::this_thread::yield();
    }

    std::this_thread::sleep_for(10ms);
    EXPECT_FALSE(writer_entered.load(std::memory_order_acquire));

    lock.unlockShared();
    writer.join();

    RWSpinlock::ReadGuard guard(lock);
    EXPECT_TRUE(writer_entered.load(std::memory_order_acquire));
    EXPECT_EQ(protected_value, 99);
}

}  // namespace
}  // namespace tent
}  // namespace mooncake
