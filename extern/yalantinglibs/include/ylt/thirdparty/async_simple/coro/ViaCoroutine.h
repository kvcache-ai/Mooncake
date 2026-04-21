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
/* This file implements the detail that how do we transfer
 * executor between coroutine and awaitables.
 *
 * The main detail here is what would we do if the awaitable
 * wouldn't accept an Executor. In this case, we would checkout
 * the current context first and when the awaited awitable got
 * ready we could checkin back. In this way, we simulate the process
 * that the awaitable get scheduled by the executor.
 */
#ifndef ASYNC_SIMPLE_CORO_VIA_COROUTINE_H
#define ASYNC_SIMPLE_CORO_VIA_COROUTINE_H

#ifndef ASYNC_SIMPLE_USE_MODULES
#include "async_simple/Executor.h"
#include "async_simple/coro/Traits.h"

#include <cassert>
#include <utility>

#endif  // ASYNC_SIMPLE_USE_MODULES

namespace async_simple {
namespace coro {

namespace detail {

class ViaCoroutine {
public:
    struct promise_type {
        struct FinalAwaiter;
        promise_type(Executor* ex) : _ex(ex), _ctx(Executor::NULLCTX) {}
        ViaCoroutine get_return_object() noexcept;
        void return_void() noexcept {}
        void unhandled_exception() const noexcept { assert(false); }

        std::suspend_always initial_suspend() const noexcept { return {}; }
        FinalAwaiter final_suspend() noexcept { return FinalAwaiter(_ctx); }

        struct FinalAwaiter {
            FinalAwaiter(Executor::Context ctx) : _ctx(ctx) {}
            bool await_ready() const noexcept { return false; }

            template <typename PromiseType>
            auto await_suspend(std::coroutine_handle<PromiseType> h) noexcept {
                auto& pr = h.promise();
                // promise will remain valid across final_suspend point
                if (pr._ex) {
                    pr._ex->checkin(pr._continuation, _ctx);
                } else {
                    pr._continuation.resume();
                }
            }
            void await_resume() const noexcept {}

            Executor::Context _ctx;
        };

        /// IMPORTANT: _continuation should be the first member due to the
        /// requirement of dbg script.
        std::coroutine_handle<> _continuation;
        Executor* _ex;
        Executor::Context _ctx;
    };

    ViaCoroutine(std::coroutine_handle<promise_type> coro) : _coro(coro) {}
    ~ViaCoroutine() {
        if (_coro) {
            _coro.destroy();
            _coro = nullptr;
        }
    }

    ViaCoroutine(const ViaCoroutine&) = delete;
    ViaCoroutine& operator=(const ViaCoroutine&) = delete;
    ViaCoroutine(ViaCoroutine&& other)
        : _coro(std::exchange(other._coro, nullptr)) {}

public:
    static ViaCoroutine create([[maybe_unused]] Executor* ex) { co_return; }

public:
    void checkin() {
        auto& pr = _coro.promise();
        if (pr._ex) {
            std::function<void()> func = []() {};
            pr._ex->checkin(func, pr._ctx);
        }
    }
    std::coroutine_handle<> getWrappedContinuation(
        std::coroutine_handle<> continuation) {
        // do not call this method on a moved ViaCoroutine,
        assert(_coro);
        auto& pr = _coro.promise();
        if (pr._ex) {
            pr._ctx = pr._ex->checkout();
        }
        pr._continuation = continuation;
        return _coro;
    }

private:
    std::coroutine_handle<promise_type> _coro;
};

inline ViaCoroutine ViaCoroutine::promise_type::get_return_object() noexcept {
    return ViaCoroutine(
        std::coroutine_handle<ViaCoroutine::promise_type>::from_promise(*this));
}

// used by co_await non-Lazy object
template <typename Awaiter>
struct [[nodiscard]] ViaAsyncAwaiter {
    template <typename Awaitable>
    ViaAsyncAwaiter(Executor * ex, Awaitable && awaitable)
        : _ex(ex),
          _awaiter(detail::getAwaiter(std::forward<Awaitable>(awaitable))),
          _viaCoroutine(ViaCoroutine::create(ex)) {}

    using HandleType = std::coroutine_handle<>;
    using AwaitSuspendResultType = decltype(
        std::declval<Awaiter&>().await_suspend(std::declval<HandleType>()));
    bool await_ready() { return _awaiter.await_ready(); }

    AwaitSuspendResultType await_suspend(HandleType continuation) {
        if constexpr (std::is_same_v<AwaitSuspendResultType, bool>) {
            bool should_suspend = _awaiter.await_suspend(
                _viaCoroutine.getWrappedContinuation(continuation));
            // TODO: if should_suspend is false, checkout/checkin should not be
            // called.
            if (should_suspend == false) {
                _viaCoroutine.checkin();
            }
            return should_suspend;
        } else {
            return _awaiter.await_suspend(
                _viaCoroutine.getWrappedContinuation(continuation));
        }
    }

    auto await_resume() { return _awaiter.await_resume(); }

    Executor* _ex;
    Awaiter _awaiter;
    ViaCoroutine _viaCoroutine;
};  // ViaAsyncAwaiter

// While co_await Awaitable in a Lazy coroutine body:
//  1. Awaitable has no "coAwait" method: a ViaAsyncAwaiter is created, current
// coroutine_handle will be wrapped into a ViaCoroutine. Reschedule will happen
// when resume from a ViaCoroutine, and the original continuation will be
// resumed in the same context before coro suspension. This usually happened
// between Lazy system and other hand-crafted Awaitables.
//  2. Awaitable has a "coAwait" method: coAwait will be called and an Awaiter
// should returned, then co_await Awaiter will performed. Lazy<T> has coAwait
// method, so co_await Lazy<T> will not lead to a reschedule.
//
// FIXME: In case awaitable is not a real awaitable, consider return
// ReadyAwaiter instead. It would be much cheaper in case we `co_await
// normal_function()`;
template <typename Awaitable>
inline auto coAwait(Executor* ex, Awaitable&& awaitable) {
    if constexpr (detail::HasCoAwaitMethod<Awaitable>) {
        return detail::getAwaiter(
            std::forward<Awaitable>(awaitable).coAwait(ex));
    } else {
        using AwaiterType =
            decltype(detail::getAwaiter(std::forward<Awaitable>(awaitable)));
        return ViaAsyncAwaiter<std::decay_t<AwaiterType>>(
            ex, std::forward<Awaitable>(awaitable));
    }
}

}  // namespace detail

}  // namespace coro
}  // namespace async_simple

#endif
