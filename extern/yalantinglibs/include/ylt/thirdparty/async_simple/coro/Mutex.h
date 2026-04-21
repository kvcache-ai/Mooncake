///////////////////////////////////////////////////////////////////////////////
// Copyright (c) Lewis Baker
// Licenced under MIT license. See the following for detials.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.
///////////////////////////////////////////////////////////////////////////////
#ifndef ASYNC_SIMPLE_CORO__MUTEXH
#define ASYNC_SIMPLE_CORO__MUTEXH

#ifndef ASYNC_SIMPLE_USE_MODULES
#include <atomic>
#include <cassert>
#include <mutex>
#include "async_simple/experimental/coroutine.h"

#endif  // ASYNC_SIMPLE_USE_MODULES

namespace async_simple {
namespace coro {

class Mutex {
private:
    class ScopedLockAwaiter;
    class LockAwaiter;

public:
    /// Construct a new async mutex that is initially unlocked.
    Mutex() noexcept : _state(unlockedState()), _waiters(nullptr) {}

    Mutex(const Mutex&) = delete;
    Mutex(Mutex&&) = delete;
    Mutex& operator=(const Mutex&) = delete;
    Mutex& operator=(Mutex&&) = delete;

    ~Mutex() {
        // Check there are no waiters waiting to acquire the lock.
        assert(_state.load(std::memory_order_relaxed) == unlockedState() ||
               _state.load(std::memory_order_relaxed) == nullptr);
        assert(_waiters == nullptr);
    }

    /// Try to lock the mutex synchronously.
    ///
    /// Returns true if the lock was able to be acquired synchronously, false
    /// if the lock could not be acquired because it was already locked.
    ///
    /// If this method returns true then the caller is responsible for ensuring
    /// that unlock() is called to release the lock.
    bool tryLock() noexcept {
        void* oldValue = unlockedState();
        return _state.compare_exchange_strong(oldValue, nullptr,
                                              std::memory_order_acquire,
                                              std::memory_order_relaxed);
    }

    /// Lock the mutex asynchronously, returning an RAII object that will
    /// release the lock at the end of the scope.
    ///
    /// You must co_await the return value to wait until the lock is acquired.
    ///
    /// Chain a call to .via() to specify the executor to resume on when
    /// the lock is eventually acquired in the case that the lock could not be
    /// acquired synchronously. Note that the executor will be passed implicitly
    /// if awaiting from a Task or AsyncGenerator coroutine. The awaiting
    /// coroutine will continue without suspending if the lock could be acquired
    /// synchronously.
    [[nodiscard]] ScopedLockAwaiter coScopedLock() noexcept;

    /// Lock the mutex asynchronously.
    ///
    /// You must co_await the return value to wait until the lock is acquired.
    ///
    /// Chain a call to .via() to specify the executor to resume on when
    /// the lock is eventually acquired in the case that the lock could not be
    /// acquired synchronously. The awaiting coroutine will continue without
    /// suspending if the lock could be acquired synchronously.
    ///
    /// Once the 'co_await m.coLock()' operation completes, the awaiting
    /// coroutine is responsible for ensuring that .unlock() is called to
    /// release the lock.
    ///
    /// Consider using coScopedLock() instead to obtain a std::scoped_lock
    /// that handles releasing the lock at the end of the scope.
    [[nodiscard]] LockAwaiter coLock() noexcept;

    /// Unlock the mutex.
    ///
    /// If there are other coroutines waiting to lock the mutex then this will
    /// schedule the resumption of the next coroutine in the queue.
    void unlock() noexcept {
        assert(_state.load(std::memory_order_relaxed) != unlockedState());
        auto* waitersHead = _waiters;
        if (waitersHead == nullptr) {
            void* currentState = _state.load(std::memory_order_relaxed);
            if (currentState == nullptr) {
                // Looks like there are no waiters waiting to acquire the lock.
                // Try to unlock it - use a compare-exchange to decide the race
                // between unlocking the mutex and another thread enqueueing
                // another waiter.
                const bool releasedLock = _state.compare_exchange_strong(
                    currentState, unlockedState(), std::memory_order_release,
                    std::memory_order_relaxed);
                if (releasedLock) {
                    return;
                }
            }
            // There are some awaiters that have been newly queued.
            // Dequeue them and reverse their order from LIFO to FIFO.
            currentState = _state.exchange(nullptr, std::memory_order_acquire);
            assert(currentState != unlockedState());
            assert(currentState != nullptr);
            auto* waiter = static_cast<LockAwaiter*>(currentState);
            do {
                auto* temp = waiter->_next;
                waiter->_next = waitersHead;
                waitersHead = waiter;
                waiter = temp;
            } while (waiter != nullptr);
        }
        assert(waitersHead != nullptr);
        _waiters = waitersHead->_next;
        waitersHead->_awaitingCoroutine.resume();
    }

private:
    class LockAwaiter {
    public:
        explicit LockAwaiter(Mutex& mutex) noexcept : _mutex(mutex) {}

        bool await_ready() noexcept { return _mutex.tryLock(); }

        bool await_suspend(std::coroutine_handle<> awaitingCoroutine) noexcept {
            _awaitingCoroutine = awaitingCoroutine;
            return _mutex.lockAsyncImpl(this);
        }

        void await_resume() noexcept {}

        // FIXME: LockAwaiter should implement coAwait to avoid to fall in
        // ViaAsyncAwaiter.

    protected:
        Mutex& _mutex;

    private:
        friend Mutex;

        CoroHandle<> _awaitingCoroutine;
        LockAwaiter* _next;
    };

    class ScopedLockAwaiter : public LockAwaiter {
    public:
        using LockAwaiter::LockAwaiter;

        [[nodiscard]] std::unique_lock<Mutex> await_resume() noexcept {
            return std::unique_lock<Mutex>{_mutex, std::adopt_lock};
        }
    };

    // Special value for _state that indicates the mutex is not locked.
    void* unlockedState() noexcept { return this; }

    // Try to lock the mutex.
    //
    // Returns true if the lock could not be acquired synchronously and awaiting
    // coroutine should suspend. In this case the coroutine will be resumed
    // later once it acquires the mutex. Returns false if the lock was acquired
    // synchronously and the awaiting coroutine should continue without
    // suspending.
    bool lockAsyncImpl(LockAwaiter* awaiter) {
        void* oldValue = _state.load(std::memory_order_relaxed);
        while (true) {
            if (oldValue == unlockedState()) {
                // It looks like the mutex is currently unlocked.
                // Try to acquire it synchronously.
                void* newValue = nullptr;
                if (_state.compare_exchange_weak(oldValue, newValue,
                                                 std::memory_order_acquire,
                                                 std::memory_order_relaxed)) {
                    // Acquired synchronously, don't suspend.
                    return false;
                }
            } else {
                // It looks like the mutex is currently locked.
                // Try to queue this waiter to the list of waiters.
                void* newValue = awaiter;
                awaiter->_next = static_cast<LockAwaiter*>(oldValue);
                if (_state.compare_exchange_weak(oldValue, newValue,
                                                 std::memory_order_release,
                                                 std::memory_order_relaxed)) {
                    // Queued waiter successfully. Awaiting coroutine should
                    // suspend.
                    return true;
                }
            }
        }
    }

    // This contains either:
    // - this    => Not locked
    // - nullptr => Locked, no newly queued waiters (ie. empty list of waiters)
    // - other   => Pointer to first LockAwaiter* in a linked-list of newly
    //              queued awaiters in LIFO order.
    std::atomic<void*> _state;

    // Linked-list of waiters in FIFO order.
    // Only the current lock holder is allowed to access this member.
    LockAwaiter* _waiters;
};

inline Mutex::ScopedLockAwaiter Mutex::coScopedLock() noexcept {
    return ScopedLockAwaiter(*this);
}

inline Mutex::LockAwaiter Mutex::coLock() noexcept {
    return LockAwaiter(*this);
}

}  // namespace coro
}  // namespace async_simple

#endif
