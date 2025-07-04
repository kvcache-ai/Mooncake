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

#ifndef CONCURRENCY_H
#define CONCURRENCY_H

#include <asio.hpp>
#include <boost/thread.hpp>

#include "v1/common/common.h"

namespace mooncake {
namespace v1 {
class RWSpinlock {
    union RWTicket {
        constexpr RWTicket() : whole(0) {}
        uint64_t whole;
        uint32_t readWrite;
        struct {
            uint16_t write;
            uint16_t read;
            uint16_t users;
        };
    } ticket;

   private:
    static void asm_volatile_memory() { asm volatile("" ::: "memory"); }

    template <class T>
    static T load_acquire(T *addr) {
        T t = *addr;
        asm_volatile_memory();
        return t;
    }

    template <class T>
    static void store_release(T *addr, T v) {
        asm_volatile_memory();
        *addr = v;
    }

   public:
    RWSpinlock() {}

    RWSpinlock(RWSpinlock const &) = delete;
    RWSpinlock &operator=(RWSpinlock const &) = delete;

    void lock() { writeLockNice(); }

    bool tryLock() {
        RWTicket t;
        uint64_t old = t.whole = load_acquire(&ticket.whole);
        if (t.users != t.write) return false;
        ++t.users;
        return __sync_bool_compare_and_swap(&ticket.whole, old, t.whole);
    }

    void writeLockAggressive() {
        uint32_t count = 0;
        uint16_t val = __sync_fetch_and_add(&ticket.users, 1);
        while (val != load_acquire(&ticket.write)) {
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
        uint16_t val = __sync_fetch_and_add(&ticket.read, 1);
        (void)val;
    }

    void unlock() {
        RWTicket t;
        t.whole = load_acquire(&ticket.whole);
        ++t.read;
        ++t.write;
        store_release(&ticket.readWrite, t.readWrite);
    }

    void lockShared() {
        uint_fast32_t count = 0;
        while (!tryLockShared()) {
            PAUSE();
            if (++count > 1000) std::this_thread::yield();
        }
    }

    bool tryLockShared() {
        RWTicket t, old;
        old.whole = t.whole = load_acquire(&ticket.whole);
        old.users = old.read;
        ++t.read;
        ++t.users;
        return __sync_bool_compare_and_swap(&ticket.whole, old.whole, t.whole);
    }

    void unlockShared() { __sync_fetch_and_add(&ticket.write, 1); }

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

class TicketLock {
   public:
    TicketLock() : next_ticket_(0), now_serving_(0) {}

    void lock() {
        int my_ticket = next_ticket_.fetch_add(1, std::memory_order_relaxed);
        while (now_serving_.load(std::memory_order_acquire) != my_ticket) {
            std::this_thread::yield();
        }
    }

    void unlock() { now_serving_.fetch_add(1, std::memory_order_release); }

   private:
    std::atomic<int> next_ticket_;
    std::atomic<int> now_serving_;
    uint64_t padding_[14];
};

class ThreadPool {
   public:
    static ThreadPool &Get() {
        static ThreadPool instance(4);
        return instance;
    }

   public:
    ThreadPool(size_t threadCount)
        : ioService_(),
          work_(asio::make_work_guard(ioService_)),
          stopped_(false) {
        for (size_t i = 0; i < threadCount; ++i) {
            threads_.create_thread(
                boost::bind(&asio::io_service::run, &ioService_));
        }
    }

    ~ThreadPool() { stop(); }

    void submit(std::function<void()> task) {
        ioService_.post(std::move(task));
    }

    void stop() {
        if (!stopped_) {
            stopped_ = true;
            ioService_.stop();
            threads_.join_all();
        }
    }

   private:
    asio::io_service ioService_;
    asio::executor_work_guard<asio::io_service::executor_type> work_;
    boost::thread_group threads_;
    bool stopped_;
};

}  // namespace v1
}  // namespace mooncake

#endif  // CONCURRENCY_H