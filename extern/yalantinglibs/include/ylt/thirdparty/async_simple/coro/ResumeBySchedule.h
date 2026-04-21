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
#ifndef ASYNC_RESUME_BY_SCHEDULE_H
#define ASYNC_RESUME_BY_SCHEDULE_H

#ifndef ASYNC_SIMPLE_USE_MODULES
#include "async_simple/Executor.h"
#include "async_simple/Future.h"
#include "async_simple/coro/Lazy.h"
#include "async_simple/experimental/coroutine.h"

#include <type_traits>
#include <utility>

#endif  // ASYNC_SIMPLE_USE_MODULES

namespace async_simple::coro {

namespace detail {

template <typename T>
class FutureResumeByScheduleAwaiter {
public:
    FutureResumeByScheduleAwaiter(Future<T>&& f) : _future(std::move(f)) {}

    bool await_ready() { return _future.hasResult(); }

    template <typename PromiseType>
    void await_suspend(CoroHandle<PromiseType> continuation) {
        static_assert(std::is_base_of_v<LazyPromiseBase, PromiseType>,
                      "FutureResumeByScheduleAwaiter is only allowed to be "
                      "called by Lazy");
        Executor* ex = continuation.promise()._executor;
        _future.setContinuation([continuation, ex](Try<T>&& t) mutable {
            if (ex != nullptr) {
                ex->schedule(continuation);
            } else {
                continuation.resume();
            }
        });
    }

    auto await_resume() { return std::move(_future.value()); }

private:
    Future<T> _future;
};

template <typename T>
class FutureResumeByScheduleAwaitable {
public:
    explicit FutureResumeByScheduleAwaitable(Future<T>&& f)
        : _future(std::move(f)) {}

    auto coAwait(Executor*) {
        return FutureResumeByScheduleAwaiter(std::move(_future));
    }

private:
    Future<T> _future;
};

}  // namespace detail

template <typename T>
inline auto ResumeBySchedule(Future<T>&& future) {
    return detail::FutureResumeByScheduleAwaitable<T>(std::move(future));
}

}  // namespace async_simple::coro

#endif
