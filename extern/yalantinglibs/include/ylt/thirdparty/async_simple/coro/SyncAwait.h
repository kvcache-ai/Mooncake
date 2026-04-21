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
#ifndef ASYNC_SIMPLE_CORO_SYNC_AWAIT_H
#define ASYNC_SIMPLE_CORO_SYNC_AWAIT_H

#ifndef ASYNC_SIMPLE_USE_MODULES
#include "async_simple/Common.h"
#include "async_simple/Executor.h"
#include "async_simple/Try.h"
#include "async_simple/util/Condition.h"

#endif  // ASYNC_SIMPLE_USE_MODULES

namespace async_simple {
namespace coro {

// Sync await on a coroutine, block until coro finished, coroutine result will
// be returned. Do not syncAwait in the same executor with Lazy, this may lead
// to a deadlock.
template <typename LazyType>
inline auto syncAwait(LazyType &&lazy) {
    auto executor = lazy.getExecutor();
    if (executor)
        logicAssert(!executor->currentThreadInExecutor(),
                    "do not sync await in the same executor with Lazy");

    util::Condition cond;
    using ValueType = typename std::decay_t<LazyType>::ValueType;

    Try<ValueType> value;
    std::move(std::forward<LazyType>(lazy))
        .start([&cond, &value](Try<ValueType> result) {
            value = std::move(result);
            cond.release();
        });
    cond.acquire();
    return std::move(value).value();
}

// A simple wrapper to ease the use.
template <typename LazyType>
inline auto syncAwait(LazyType &&lazy, Executor *ex) {
    return syncAwait(std::move(std::forward<LazyType>(lazy)).via(ex));
}

}  // namespace coro
}  // namespace async_simple

#endif
