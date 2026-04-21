/*
 * Copyright (c) 2023, Alibaba Group Holding Limited;
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#ifndef ASYNC_SIMPLE_USE_MODULES
#include <cassert>
#include <climits>

#include "async_simple/coro/ConditionVariable.h"
#include "async_simple/coro/Lazy.h"
#include "async_simple/coro/SpinLock.h"

#endif  // ASYNC_SIMPLE_USE_MODULES

namespace async_simple::coro {

template <typename Lock>
class SharedMutexBase {
    // Based on Howard Hinnant's reference implementation from N2406.

    // The high bit of state_ is the write-entered flag which is set to
    // indicate a writer has taken the lock or is queuing to take the lock.
    // The remaining bits are the count of reader locks.
    //
    // To take a reader lock, block on gate1_ while the write-entered flag is
    // set or the maximum number of reader locks is held, then increment the
    // reader lock count.
    // To release, decrement the count, then if the write-entered flag is set
    // and the count is zero then signal gate2_ to wake a queued writer,
    // otherwise if the maximum number of reader locks was held signal gate1_
    // to wake a reader.
    //
    // To take a writer lock, block on gate1_ while the write-entered flag is
    // set, then set the write-entered flag to start queueing, then block on
    // gate2_ while the number of reader locks is non-zero.
    // To release, unset the write-entered flag and signal gate1_ to wake all
    // blocked readers and writers.
    //
    // This means that when no reader locks are held readers and writers get
    // equal priority. When one or more reader locks is held a writer gets
    // priority and no more reader locks can be taken while the writer is
    // queued.

    // Only locked when accessing state_ or waiting on condition variables.
    Lock mut_;
    // Used to block while write-entered is set or reader count at maximum.
    ConditionVariable<Lock> gate1_;
    // Used to block queued writers while reader count is non-zero.
    ConditionVariable<Lock> gate2_;
    // The write-entered flag and reader count.
    unsigned state_;

    static constexpr unsigned write_entered_flag =
        1U << (sizeof(unsigned) * CHAR_BIT - 1);
    static constexpr unsigned max_readers = ~write_entered_flag;

    // Test whether the write-entered flag is set. mut_ must be locked.
    bool write_entered() const noexcept { return state_ & write_entered_flag; }

    // The number of reader locks currently held. mut_ must be locked.
    unsigned readers() const noexcept { return state_ & max_readers; }

public:
    template <typename... Args>
    SharedMutexBase(Args&&... args)
        : mut_(std::forward<Args>(args)...), state_(0) {}

    ~SharedMutexBase() { assert(state_ == 0); }

    SharedMutexBase(const SharedMutexBase&) = delete;
    SharedMutexBase& operator=(const SharedMutexBase&) = delete;

    // Exclusive ownership

    async_simple::coro::Lazy<> coLock() noexcept {
        auto scoper = co_await mut_.coScopedLock();
        // Wait until we can set the write-entered flag.
        if (write_entered()) {
            co_await gate1_.wait(mut_, [this] { return !write_entered(); });
        }
        state_ |= write_entered_flag;
        // Then wait until there are no more readers.
        if (readers() != 0) {
            co_await gate2_.wait(mut_, [this] { return readers() == 0; });
        }
    }

    bool tryLock() noexcept {
        if (!mut_.tryLock()) {
            return false;
        }
        bool allow_write = (state_ == 0);
        if (allow_write) {
            state_ = write_entered_flag;
        }
        mut_.unlock();
        return allow_write;
    }

    async_simple::coro::Lazy<void> unlock() noexcept {
        auto scoper = co_await mut_.coScopedLock();
        assert(write_entered());
        state_ = 0;
        // call notify_all() while mutex is held so that another thread can't
        // lock and unlock the mutex then destroy *this before we make the call.
        gate1_.notifyAll();
    }

    // Shared ownership

    async_simple::coro::Lazy<void> coLockShared() {
        auto scoper = co_await mut_.coScopedLock();
        if (state_ >= max_readers) {
            co_await gate1_.wait(mut_, [this] { return state_ < max_readers; });
        }
        ++state_;
    }

    bool tryLockShared() {
        if (!mut_.tryLock()) {
            return false;
        }
        bool allow_read = (state_ < max_readers);
        if (allow_read) {
            ++state_;
        }
        mut_.unlock();
        return allow_read;
    }

    async_simple::coro::Lazy<void> unlockShared() {
        auto scoper = co_await mut_.coScopedLock();
        assert(readers() > 0);
        auto prev = state_--;
        if (write_entered()) {
            // Wake the queued writer if there are no more readers.
            if (readers() == 0)
                gate2_.notifyOne();
            // No need to notify gate1_ because we give priority to the queued
            // writer, and that writer will eventually notify gate1_ after it
            // clears the write-entered flag.
        } else {
            // Wake any thread that was blocked on reader overflow.
            if (prev == max_readers)
                gate1_.notifyOne();
        }
    }
};
struct SharedMutex : public SharedMutexBase<SpinLock> {
    SharedMutex(int count = 128) : SharedMutexBase<SpinLock>(count) {}
};
}  // namespace async_simple::coro