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
#ifndef ASYNC_SIMPLE_COLLECT_H
#define ASYNC_SIMPLE_COLLECT_H

#ifndef ASYNC_SIMPLE_USE_MODULES
#include <iterator>
#include <vector>
#include "async_simple/Future.h"
#include "async_simple/Try.h"

#endif  // ASYNC_SIMPLE_USE_MODULES

namespace async_simple {

// collectAll - collect all the values for a range of futures.
//
// The arguments include a begin iterator and a end iterator.
// The arguments specifying a range for the futures to be collected.
//
// For a range of `Future<T>`, the return type of collectAll would
// be `Future<std::vector<Try<T>>>`. The length of the vector in the
// returned future is the same with the number of futures inputted.
// The `Try<T>` in each field reveals that if there is an exception
// happened during the execution for the Future.
//
// This is a non-blocking API. It wouldn't block the execution even
// if there are futures doesn't have a value. For each Future inputted,
// if it has a result, the result is forwarded to the corresponding fields
// of the returned future. If it wouldn't have a result, it would fulfill
// the corresponding field in the returned future once it has a result.
//
// Since the returned type is a future. So the user wants to get its value
// could use `get()` method synchronously or `then*()` method asynchronously.
template <std::input_iterator Iterator>
inline Future<std::vector<
    Try<typename std::iterator_traits<Iterator>::value_type::value_type>>>
collectAll(Iterator begin, Iterator end) {
    using T = typename std::iterator_traits<Iterator>::value_type::value_type;
    size_t n = std::distance(begin, end);

    bool allReady = true;
    for (auto iter = begin; iter != end; ++iter) {
        if (!iter->hasResult()) {
            allReady = false;
            break;
        }
    }
    if (allReady) {
        std::vector<Try<T>> results;
        results.reserve(n);
        for (auto iter = begin; iter != end; ++iter) {
            results.push_back(std::move(iter->result()));
        }
        return Future<std::vector<Try<T>>>(std::move(results));
    }

    Promise<std::vector<Try<T>>> promise;
    auto future = promise.getFuture();

    struct Context {
        Context(size_t n, Promise<std::vector<Try<T>>> p_)
            : results(n), p(std::move(p_)) {}
        ~Context() { p.setValue(std::move(results)); }
        std::vector<Try<T>> results;
        Promise<std::vector<Try<T>>> p;
    };

    auto ctx = std::make_shared<Context>(n, std::move(promise));
    for (size_t i = 0; i < n; ++i, ++begin) {
        if (begin->hasResult()) {
            ctx->results[i] = std::move(begin->result());
        } else {
            begin->setContinuation([ctx, i](Try<T>&& t) mutable {
                ctx->results[i] = std::move(t);
            });
        }
    }
    return future;
}

}  // namespace async_simple

#endif  // ASYNC_SIMPLE_COLLECT_H
