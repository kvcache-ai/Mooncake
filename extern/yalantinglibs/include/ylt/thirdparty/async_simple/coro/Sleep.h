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
#ifndef ASYNC_SIMPLE_CORO_SLEEP_H
#define ASYNC_SIMPLE_CORO_SLEEP_H

#ifndef ASYNC_SIMPLE_USE_MODULES
#include <cstdint>
#include <system_error>
#include <thread>
#include "async_simple/Executor.h"
#include "async_simple/coro/Lazy.h"

#endif  // ASYNC_SIMPLE_USE_MODULES

namespace async_simple {
namespace coro {

// Returns an awaitable that would return after dur times.
//
// e.g. co_await sleep(100s);

template <typename Rep, typename Period>
Lazy<void> sleep(Executor* ex, std::chrono::duration<Rep, Period> dur,
                 uint64_t schedule_hint) {
    auto slot = co_await CurrentSlot{};
    co_await ex->after(std::chrono::duration_cast<Executor::Duration>(dur),
                       schedule_hint, slot);
}

template <typename Rep, typename Period>
Lazy<void> sleep(Executor* ex, std::chrono::duration<Rep, Period> dur) {
    return sleep(
        ex, dur,
        static_cast<uint64_t>(async_simple::Executor::Priority::DEFAULT));
}

template <typename Rep, typename Period>
Lazy<void> sleep(std::chrono::duration<Rep, Period> dur,
                 uint64_t schedule_hint) {
    auto ex = co_await CurrentExecutor();
    if (!ex) {
        std::this_thread::sleep_for(dur);
        co_return;
    }
    co_return co_await sleep(ex, dur, schedule_hint);
}

template <typename Rep, typename Period>
Lazy<void> sleep(std::chrono::duration<Rep, Period> dur) {
    return sleep(
        dur, static_cast<uint64_t>(async_simple::Executor::Priority::DEFAULT));
}

}  // namespace coro
}  // namespace async_simple

#endif
