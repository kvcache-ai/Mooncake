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
#ifndef ASYNC_SIMPLE_FUTURESTATE_H
#define ASYNC_SIMPLE_FUTURESTATE_H

#ifndef ASYNC_SIMPLE_USE_MODULES
#include <atomic>
#include <cassert>
#include <iostream>
#include <stdexcept>

#include "async_simple/Common.h"
#include "async_simple/Executor.h"
#include "async_simple/Try.h"
#include "async_simple/util/move_only_function.h"

#endif  // ASYNC_SIMPLE_USE_MODULES

namespace async_simple {

// Details about the State of Future/Promise,
// Users shouldn't care about it.
namespace detail {

enum class State : uint8_t {
    START = 0,
    ONLY_RESULT = 1 << 0,
    ONLY_CONTINUATION = 1 << 1,
    DONE = 1 << 5,
};

constexpr State operator|(State lhs, State rhs) {
    return State((uint8_t)lhs | (uint8_t)rhs);
}

constexpr State operator&(State lhs, State rhs) {
    return State((uint8_t)lhs & (uint8_t)rhs);
}

}  // namespace detail

// FutureState is a shared state between Future and Promise.
//
// This is the key component for Future/Promise. It guarantees
// the thread safety and call executor to schedule when necessary.
//
// Users should **never** use FutureState directly.
template <typename T>
class FutureState {
private:
    using Continuation = util::move_only_function<void(Try<T>&& value)>;

private:
    // A helper to help FutureState to count the references to guarantee
    // that the memory get released correctly.
    class ContinuationReference {
    public:
        ContinuationReference() = default;
        explicit ContinuationReference(FutureState<T>* fs) : _fs(fs) {
            attach();
        }
        ~ContinuationReference() { detach(); }

        ContinuationReference(const ContinuationReference& other)
            : _fs(other._fs) {
            attach();
        }
        ContinuationReference& operator=(const ContinuationReference& other) =
            delete;

        ContinuationReference(ContinuationReference&& other) : _fs(other._fs) {
            other._fs = nullptr;
        }

        ContinuationReference& operator=(ContinuationReference&& other) =
            delete;

        FutureState* getFutureState() const noexcept { return _fs; }

    private:
        void attach() {
            if (_fs) {
                _fs->attachOne();
                _fs->refContinuation();
            }
        }
        void detach() {
            if (_fs) {
                _fs->derefContinuation();
                _fs->detachOne();
            }
        }

    private:
        FutureState<T>* _fs = nullptr;
    };

public:
    FutureState()
        : _state(detail::State::START),
          _attached(0),
          _continuationRef(0),
          _executor(nullptr),
          _context(Executor::NULLCTX),
          _promiseRef(0),
          _forceSched(false) {}
    ~FutureState() {}

    FutureState(const FutureState&) = delete;
    FutureState& operator=(const FutureState&) = delete;

    FutureState(FutureState&& other) = delete;
    FutureState& operator=(FutureState&&) = delete;

public:
    bool hasResult() const noexcept {
        constexpr auto allow = detail::State::DONE | detail::State::ONLY_RESULT;
        auto state = _state.load(std::memory_order_acquire);
        return (state & allow) != detail::State();
    }

    bool hasContinuation() const noexcept {
        constexpr auto allow =
            detail::State::DONE | detail::State::ONLY_CONTINUATION;
        auto state = _state.load(std::memory_order_acquire);
        return (state & allow) != detail::State();
    }

    AS_INLINE void attachOne() {
        _attached.fetch_add(1, std::memory_order_relaxed);
    }
    AS_INLINE void detachOne() {
        auto old = _attached.fetch_sub(1, std::memory_order_acq_rel);
        assert(old >= 1u);
        if (old == 1) {
            delete this;
        }
    }
    AS_INLINE void attachPromise() {
        _promiseRef.fetch_add(1, std::memory_order_relaxed);
        attachOne();
    }
    AS_INLINE void detachPromise() {
        auto old = _promiseRef.fetch_sub(1, std::memory_order_acq_rel);
        assert(old >= 1u);
        if (!hasResult() && old == 1) {
            try {
                throw std::runtime_error("Promise is broken");
            } catch (...) {
                setResult(Try<T>(std::current_exception()));
            }
        }
        detachOne();
    }

public:
    Try<T>& getTry() noexcept { return _try_value; }
    const Try<T>& getTry() const noexcept { return _try_value; }

    void setExecutor(Executor* ex) { _executor = ex; }

    Executor* getExecutor() { return _executor; }

    void checkout() {
        if (_executor) {
            _context = _executor->checkout();
        }
    }
    void setForceSched(bool force = true) {
        if (!_executor && force) {
            std::cerr << "executor is nullptr, can not set force schedule "
                         "continaution\n";
            return;
        }
        _forceSched = force;
    }

public:
    // State Transfer:
    //  START: initial
    //  ONLY_RESULT: promise.setValue called
    //  ONLY_CONTINUATION: future.thenImpl called
    void setResult(Try<T>&& value) {
#if !defined(__GNUC__) || __GNUC__ < 12
        // GCC 12 issues a spurious uninitialized-var warning.
        // See details: https://gcc.gnu.org/bugzilla/show_bug.cgi?id=109448
        logicAssert(!hasResult(), "FutureState already has a result");
#endif
        _try_value = std::move(value);

        auto state = _state.load(std::memory_order_acquire);
        switch (state) {
            case detail::State::START:
                if (_state.compare_exchange_strong(state,
                                                   detail::State::ONLY_RESULT,
                                                   std::memory_order_release)) {
                    return;
                }
                // state has already transfered, fallthrough
                assert(_state.load(std::memory_order_relaxed) ==
                       detail::State::ONLY_CONTINUATION);
            case detail::State::ONLY_CONTINUATION:
                if (_state.compare_exchange_strong(state, detail::State::DONE,
                                                   std::memory_order_release)) {
                    scheduleContinuation(false);
                    return;
                }
            default:
                logicAssert(false, "State Transfer Error");
        }
    }

    template <typename F>
    void setContinuation(F&& func) {
        logicAssert(!hasContinuation(),
                    "FutureState already has a continuation");
        new (&_continuation)
            Continuation([func = std::move(func)](Try<T>&& v) mutable {
                func(std::forward<Try<T>>(v));
            });

        auto state = _state.load(std::memory_order_acquire);
        switch (state) {
            case detail::State::START:
                if (_state.compare_exchange_strong(
                        state, detail::State::ONLY_CONTINUATION,
                        std::memory_order_release)) {
                    return;
                }
                // state has already transferred, fallthrough
                assert(_state.load(std::memory_order_relaxed) ==
                       detail::State::ONLY_RESULT);
            case detail::State::ONLY_RESULT:
                if (_state.compare_exchange_strong(state, detail::State::DONE,
                                                   std::memory_order_release)) {
                    scheduleContinuation(true);
                    return;
                }
            default:
                logicAssert(false, "State Transfer Error");
        }
    }

    bool currentThreadInExecutor() const {
        if (!_executor) {
            return false;
        }
        return _executor->currentThreadInExecutor();
    }

private:
    void scheduleContinuation(bool triggerByContinuation) {
        logicAssert(
            _state.load(std::memory_order_relaxed) == detail::State::DONE,
            "FutureState is not DONE");
        if (!_forceSched && (!_executor || triggerByContinuation ||
                             currentThreadInExecutor())) {
            // execute inplace for better performance
            ContinuationReference guard(this);
            _continuation(std::move(_try_value));
        } else {
            ContinuationReference guard(this);
            ContinuationReference guardForException(this);
            try {
                bool ret;
                if (Executor::NULLCTX == _context) {
                    ret = _executor->schedule(
                        [fsRef = std::move(guard)]() mutable {
                            auto ref = std::move(fsRef);
                            auto fs = ref.getFutureState();
                            fs->_continuation(std::move(fs->_try_value));
                        });
                } else {
                    ScheduleOptions opts;
                    opts.prompt = !_forceSched;
                    // schedule continuation in the same context before
                    // checkout()
                    ret = _executor->checkin(
                        [fsRef = std::move(guard)]() mutable {
                            auto ref = std::move(fsRef);
                            auto fs = ref.getFutureState();
                            fs->_continuation(std::move(fs->_try_value));
                        },
                        _context, opts);
                }
                if (!ret) {
                    throw std::runtime_error(
                        "schedule continuation in executor failed");
                }
            } catch (std::exception& e) {
                // reschedule failed, execute inplace
                _continuation(std::move(_try_value));
            }
        }
    }

    void refContinuation() {
        _continuationRef.fetch_add(1, std::memory_order_relaxed);
    }
    void derefContinuation() {
        auto old = _continuationRef.fetch_sub(1, std::memory_order_relaxed);
        assert(old >= 1);
        if (old == 1) {
            _continuation.~Continuation();
        }
    }

private:
    std::atomic<detail::State> _state;
    std::atomic<uint8_t> _attached;
    std::atomic<uint8_t> _continuationRef;
    Try<T> _try_value;
    union {
        Continuation _continuation;
    };
    Executor* _executor;
    Executor::Context _context;
    std::atomic<std::size_t> _promiseRef;
    bool _forceSched;
};
}  // namespace async_simple

#endif
