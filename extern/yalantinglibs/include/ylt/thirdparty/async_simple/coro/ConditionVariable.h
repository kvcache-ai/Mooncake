/*
 * Copyright (c) 2022, Alibaba Group Holding Limited;
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
#ifndef ASYNC_SIMPLE_CORO_CONDITION_VARIABLE_H
#define ASYNC_SIMPLE_CORO_CONDITION_VARIABLE_H

#ifndef ASYNC_SIMPLE_USE_MODULES
#include <mutex>
#include "async_simple/coro/Lazy.h"

#endif  // ASYNC_SIMPLE_USE_MODULES

namespace async_simple {
namespace coro {

template <class Lock>
class ConditionVariableAwaiter;

template <class Lock>
class ConditionVariable {
public:
    ConditionVariable() noexcept {}
    ~ConditionVariable() {}

    ConditionVariable(const ConditionVariable&) = delete;
    ConditionVariable& operator=(const ConditionVariable&) = delete;

    void notify() noexcept { notifyAll(); }
    void notifyOne() noexcept;
    void notifyAll() noexcept;

    template <class Pred>
    Lazy<> wait(Lock& lock, Pred&& pred) noexcept;

private:
    void resumeWaiters(ConditionVariableAwaiter<Lock>* awaiters);

private:
    friend class ConditionVariableAwaiter<Lock>;
    std::atomic<ConditionVariableAwaiter<Lock>*> _awaiters = nullptr;
};

template <class Lock>
class ConditionVariableAwaiter {
public:
    ConditionVariableAwaiter(ConditionVariable<Lock>* cv, Lock& lock) noexcept
        : _cv(cv), _lock(lock) {}

    bool await_ready() const noexcept { return false; }

    void await_suspend(std::coroutine_handle<> continuation) noexcept {
        _continuation = continuation;
        std::unique_lock<Lock> lock{_lock, std::adopt_lock};
        auto awaitings = _cv->_awaiters.load(std::memory_order_relaxed);
        do {
            _next = awaitings;
        } while (!_cv->_awaiters.compare_exchange_weak(
            awaitings, this, std::memory_order_acquire,
            std::memory_order_relaxed));
    }
    void await_resume() const noexcept {}

public:
    ConditionVariable<Lock>* _cv;
    Lock& _lock;

private:
    friend class ConditionVariable<Lock>;
    ConditionVariableAwaiter<Lock>* _next = nullptr;
    std::coroutine_handle<> _continuation;
};

template <class Lock>
template <class Pred>
inline Lazy<> ConditionVariable<Lock>::wait(Lock& lock, Pred&& pred) noexcept {
    while (!pred()) {
        co_await ConditionVariableAwaiter<Lock>{this, lock};
        co_await lock.coLock();
    }
    co_return;
}

template <class Lock>
inline void ConditionVariable<Lock>::notifyAll() noexcept {
    auto awaitings = _awaiters.load(std::memory_order_relaxed);
    while (!_awaiters.compare_exchange_weak(awaitings, nullptr,
                                            std::memory_order_release,
                                            std::memory_order_relaxed))
        ;
    resumeWaiters(awaitings);
}

template <class Lock>
inline void ConditionVariable<Lock>::notifyOne() noexcept {
    auto awaitings = _awaiters.load(std::memory_order_relaxed);
    if (!awaitings) {
        return;
    }
    while (!_awaiters.compare_exchange_weak(awaitings, awaitings->_next,
                                            std::memory_order_release,
                                            std::memory_order_relaxed))
        ;
    awaitings->_next = nullptr;
    resumeWaiters(awaitings);
}

template <class Lock>
inline void ConditionVariable<Lock>::resumeWaiters(
    ConditionVariableAwaiter<Lock>* awaiters) {
    while (awaiters) {
        auto* prev = awaiters;
        awaiters = awaiters->_next;
        prev->_continuation.resume();
    }
}

template <>
class ConditionVariableAwaiter<void>;
template <>
class ConditionVariable<void> {
public:
    using pointer_type = void*;

public:
    ConditionVariable() noexcept {}
    ~ConditionVariable() {}

    ConditionVariable(const ConditionVariable&) = delete;
    ConditionVariable& operator=(const ConditionVariable&) = delete;

    void notify() noexcept;
    ConditionVariableAwaiter<void> wait() noexcept;
    void reset() noexcept;

private:
    void resumeWaiters(ConditionVariableAwaiter<void>* awaiters);

private:
    friend class ConditionVariableAwaiter<void>;
    std::atomic<pointer_type> _awaiters = nullptr;
};

template <>
class ConditionVariableAwaiter<void> {
public:
    using pointer_type = void*;

public:
    ConditionVariableAwaiter(ConditionVariable<void>* cv) noexcept : _cv(cv) {}

    bool await_ready() const noexcept {
        return static_cast<pointer_type>(_cv) ==
               _cv->_awaiters.load(std::memory_order_acquire);
    }
    bool await_suspend(std::coroutine_handle<> continuation) noexcept {
        _continuation = continuation;
        pointer_type awaitings = _cv->_awaiters.load(std::memory_order_acquire);
        do {
            if (awaitings == static_cast<pointer_type>(_cv)) {
                return false;
            }
            _next = static_cast<ConditionVariableAwaiter<void>*>(awaitings);
        } while (!_cv->_awaiters.compare_exchange_weak(
            awaitings, static_cast<pointer_type>(this),
            std::memory_order_release, std::memory_order_acquire));
        return true;
    }
    void await_resume() const noexcept {}

public:
    ConditionVariable<void>* _cv;

private:
    friend class ConditionVariable<void>;
    ConditionVariableAwaiter<void>* _next = nullptr;
    std::coroutine_handle<> _continuation;
};

inline ConditionVariableAwaiter<void> ConditionVariable<void>::wait() noexcept {
    return ConditionVariableAwaiter<void>{this};
}

inline void ConditionVariable<void>::notify() noexcept {
    pointer_type self = static_cast<pointer_type>(this);
    pointer_type awaitings =
        _awaiters.exchange(self, std::memory_order_acq_rel);
    if (awaitings != self) {
        resumeWaiters(static_cast<ConditionVariableAwaiter<void>*>(awaitings));
    }
}

inline void ConditionVariable<void>::reset() noexcept {
    pointer_type self = static_cast<pointer_type>(this);
    _awaiters.compare_exchange_strong(self, nullptr, std::memory_order_relaxed);
}

inline void ConditionVariable<void>::resumeWaiters(
    ConditionVariableAwaiter<void>* awaiters) {
    while (awaiters) {
        auto* prev = awaiters;
        awaiters = awaiters->_next;
        prev->_continuation.resume();
    }
}

using Notifier = ConditionVariable<void>;

}  // namespace coro
}  // namespace async_simple

#endif  // ASYNC_SIMPLE_CORO_CONDITION_VARIABLE_H
