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

#ifndef TENT_RW_SPINLOCK_H
#define TENT_RW_SPINLOCK_H

#include <atomic>
#include <thread>

#include "tent/common/types.h"
#include "tent/common/utils/os.h"

namespace mooncake {
namespace tent {
class RWSpinlock {
    union RWTicket {
        constexpr RWTicket() : whole(0) {}
        constexpr RWTicket(uint64_t v) : whole(v) {}
        uint64_t whole;
        uint32_t readWrite;
        struct {
            uint16_t write;
            uint16_t read;
            uint16_t users;
        };
    };

    std::atomic<uint64_t> ticket;

   public:
    RWSpinlock() : ticket(0) {}

    RWSpinlock(RWSpinlock const &) = delete;
    RWSpinlock &operator=(RWSpinlock const &) = delete;

    void lock() { writeLockNice(); }

    bool tryLock() {
        RWTicket t, expected;
        expected.whole = ticket.load(std::memory_order_acquire);
        t.whole = expected.whole;
        if (t.users != t.write) return false;
        ++t.users;
        return ticket.compare_exchange_weak(expected.whole, t.whole,
                                            std::memory_order_acquire);
    }

    void writeLockAggressive() {
        uint32_t count = 0;
        RWTicket increment;
        increment.users = 1;
        RWTicket old(ticket.fetch_add(increment.whole,
                                      std::memory_order_acquire));
        uint16_t val = old.users;
        RWTicket t;
        while (val != (t.whole = ticket.load(std::memory_order_acquire),
                       t.write)) {
            PAUSE();
            if (++count > 1000) std::this_thread::yield();
        }
    }

    void writeLockNice() {
        uint32_t count = 0;
        while (!tryLock()) {
            PAUSE();
            if (++count > 1000) std::this_thread::yield();
        }
    }

    void unlockAndLockShared() {
        uint16_t val = fetch_add_read(1);
        (void)val;
    }

    void unlock() {
        uint64_t expected = ticket.load(std::memory_order_relaxed);
        uint64_t new_val;
        RWTicket t;
        do {
            t.whole = expected;
            ++t.read;
            ++t.write;
            new_val = t.whole;
        } while (!ticket.compare_exchange_weak(
            expected, new_val, std::memory_order_release,
            std::memory_order_relaxed));
    }

    void lockShared() {
        uint_fast32_t count = 0;
        while (!tryLockShared()) {
            PAUSE();
            if (++count > 1000) std::this_thread::yield();
        }
    }

    bool tryLockShared() {
        RWTicket t, expected;
        expected.whole = ticket.load(std::memory_order_acquire);
        t.whole = expected.whole;
        expected.users = expected.read;
        ++t.read;
        ++t.users;
        return ticket.compare_exchange_weak(expected.whole, t.whole,
                                            std::memory_order_acquire);
    }

    void unlockShared() { fetch_add_write(1); }

   private:
    uint16_t fetch_add_read(uint16_t delta) {
        uint64_t expected = ticket.load(std::memory_order_relaxed);
        uint64_t new_val;
        RWTicket t;
        do {
            t.whole = expected;
            t.read += delta;
            new_val = t.whole;
        } while (!ticket.compare_exchange_weak(expected, new_val,
                                               std::memory_order_acquire));
        return static_cast<uint16_t>(t.read - delta);
    }

    uint16_t fetch_add_write(uint16_t delta) {
        uint64_t expected = ticket.load(std::memory_order_relaxed);
        uint64_t new_val;
        RWTicket t;
        do {
            t.whole = expected;
            t.write += delta;
            new_val = t.whole;
        } while (!ticket.compare_exchange_weak(expected, new_val,
                                               std::memory_order_release));
        return static_cast<uint16_t>(t.write - delta);
    }

   public:
    struct WriteGuard {
        WriteGuard(RWSpinlock &lock) : lock(lock) { lock.lock(); }

        WriteGuard(const WriteGuard &) = delete;

        WriteGuard &operator=(const WriteGuard &) = delete;

        ~WriteGuard() { lock.unlock(); }

        RWSpinlock &lock;
    };

    struct ReadGuard {
        ReadGuard(RWSpinlock &lock) : lock(lock) { lock.lockShared(); }

        ReadGuard(const ReadGuard &) = delete;

        ReadGuard &operator=(const ReadGuard &) = delete;

        ~ReadGuard() { lock.unlockShared(); }

        RWSpinlock &lock;
    };

   private:
    const static int64_t kExclusiveLock = INT64_MIN / 2;

    std::atomic<int64_t> lock_;
    uint64_t padding_[15];
};
}  // namespace tent
}  // namespace mooncake

#endif  // TENT_RW_SPINLOCK_H
