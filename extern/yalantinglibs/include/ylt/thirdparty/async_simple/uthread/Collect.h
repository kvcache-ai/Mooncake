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
/* This file implements the collectAll interface. The collectAll interface would
 * await all functions in a specified range. It would create a uthread for every
 * function in the range and await for every uthread to complete.
 * uthread::collectAll should be called in a uthread only. Generally, when
 * uthread::collectAll is called, the current uthread would check out until all
 * of the corresponding awaited uthread finished. Then the current uthread would
 * check in.
 *
 * The available schedule policy includes Launch::Schedule and Launch::Current.
 * See Async.h for details.
 *
 * Example:
 * ```C++
 *  std::vector<Callable> v;
 *  make tasks concurrent execution.
 *  auto res1 = collectAll<Launch::Schedule>(v.begin(), v.end(), ex);
 *
 *  make tasks async execution in current thread.
 *  auto res2 = collectAll<Launch::Current>(v.begin(), v.end(), ex);
 * ```
 *
 * `F` is a C++ lambda function, the type of returned value `value `is
 * `std::vector<T>`, `T` is the return type of `F`. If `T` is `void`,
 * `collectAll` would return `async_simple::Unit`.
 */
#ifndef ASYNC_SIMPLE_UTHREAD_COLLECT_H
#define ASYNC_SIMPLE_UTHREAD_COLLECT_H

#ifndef ASYNC_SIMPLE_USE_MODULES
#include <type_traits>
#include "async_simple/Future.h"
#include "async_simple/uthread/Async.h"
#include "async_simple/uthread/Await.h"

#endif  // ASYNC_SIMPLE_USE_MODULES

namespace async_simple {
namespace uthread {

// TODO: Add Range version.
template <Launch Policy, std::input_iterator Iterator>
auto collectAll(Iterator first, Iterator last, Executor* ex) {
    assert(std::distance(first, last) >= 0);
    static_assert(Policy != Launch::Prompt,
                  "collectAll not support Prompt launch policy");

    using ValueType = std::invoke_result_t<
        typename std::iterator_traits<Iterator>::value_type>;
    constexpr bool IfReturnVoid = std::is_void_v<ValueType>;
    using ResultType =
        std::conditional_t<IfReturnVoid, void, std::vector<ValueType>>;

    struct Context {
#ifndef NDEBUG
        std::atomic<std::size_t> tasks;
#endif
        std::conditional_t<IfReturnVoid, bool, ResultType> result;
        Promise<ResultType> promise;

        Context(std::size_t n, Promise<ResultType>&& pr)
            :
#ifndef NDEBUG
            tasks(n),
#endif
            promise(pr) {
            if constexpr (!IfReturnVoid)
                result.resize(n);
        }

        ~Context() {
#ifndef NDEBUG
            assert(tasks == 0);
#endif
            if constexpr (IfReturnVoid)
                promise.setValue();
            else
                promise.setValue(std::move(result));
        }
    };

    return await<ResultType>(ex, [first, last,
                                  ex](Promise<ResultType>&& pr) mutable {
        auto n = static_cast<std::size_t>(std::distance(first, last));
        auto context = std::make_shared<Context>(n, std::move(pr));
        for (auto i = 0; first != last; ++i, ++first) {
            async<Policy>(
                [context, i, f = std::move(*first)]() mutable {
                    if constexpr (IfReturnVoid) {
                        f();
                        (void)i;
                    } else {
                        context->result[i] = std::move(f());
                    }
#ifndef NDEBUG
                    context->tasks.fetch_sub(1u, std::memory_order_acq_rel);
#endif
                },
                ex);
        }
    });
}

}  // namespace uthread
}  // namespace async_simple

#endif  // ASYNC_SIMPLE_UTHREAD_COLLECT_H
