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

#ifndef ASYNC_SIMPLE_CORO_DISPATCH_H
#define ASYNC_SIMPLE_CORO_DISPATCH_H

#ifndef ASYNC_SIMPLE_USE_MODULES
#include "async_simple/Common.h"
#include "async_simple/Executor.h"
#include "async_simple/coro/Lazy.h"
#include "async_simple/experimental/coroutine.h"

#include <cassert>
#include <type_traits>

#endif  // ASYNC_SIMPLE_USE_MODULES

namespace async_simple {
namespace coro {

namespace detail {

// based on c++ coroutine common abi.
// it is not supported that the PromiseType's align is larger than the size of
// two function pointer.
inline CoroHandle<> GetContinuationFromHandle(CoroHandle<> h) {
    constexpr size_t promise_offset = 2 * sizeof(void*);
    char* ptr = static_cast<char*>(h.address());
    ptr = ptr + promise_offset;
    return CoroHandle<>::from_address(
        *static_cast<void**>(static_cast<void*>(ptr)));
}

inline void ChangeLaziessExecutorTo(CoroHandle<> h, Executor* ex) {
    while (true) {
        CoroHandle<> continuation = GetContinuationFromHandle(h);
        if (!continuation.address()) {
            break;
        }
        auto& promise =
            CoroHandle<LazyPromiseBase>::from_address(h.address()).promise();
        promise._executor = ex;
        h = continuation;
    }
}

class DispatchAwaiter {
public:
    explicit DispatchAwaiter(Executor* ex) noexcept : _ex(ex) {
        assert(_ex != nullptr);
    }

    bool await_ready() const noexcept { return false; }

    template <typename PromiseType>
    bool await_suspend(CoroHandle<PromiseType> h) {
        static_assert(std::is_base_of<LazyPromiseBase, PromiseType>::value,
                      "dispatch is only allowed to be called by Lazy");
        if (h.promise()._executor == _ex) {
            return false;
        }
        Executor* old_ex = h.promise()._executor;
        ChangeLaziessExecutorTo(h, _ex);
        bool succ = _ex->schedule(std::move(h));
        // cannot access *this after schedule.
        // If the scheduling fails, we must change the executor back to its
        // original value, as the user may catch exceptions and handle them
        // themselves, which can result in inconsistencies between the executor
        // recorded by Lazy and the actual executor running.
        if (succ == false)
            AS_UNLIKELY {
                ChangeLaziessExecutorTo(h, old_ex);
                throw std::runtime_error("dispatch to executor failed");
            }
        return true;
    }

    void await_resume() noexcept {}

private:
    Executor* _ex;
};

class DispatchAwaitable {
public:
    explicit DispatchAwaitable(Executor* ex) : _ex(ex) {}

    auto coAwait(Executor*) { return DispatchAwaiter(_ex); }

private:
    Executor* _ex;
};

}  // namespace detail

// schedule this Lazy to ex for execution
inline detail::DispatchAwaitable dispatch(Executor* ex) {
    logicAssert(ex != nullptr, "dispatch's param should not be nullptr");
    return detail::DispatchAwaitable(ex);
}

}  // namespace coro
}  // namespace async_simple

#endif
