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
#ifndef ASYNC_SIMPLE_TRAITS_H
#define ASYNC_SIMPLE_TRAITS_H

#ifndef ASYNC_SIMPLE_USE_MODULES
#include "async_simple/Try.h"

#endif  // ASYNC_SIMPLE_USE_MODULES

namespace async_simple {

template <typename T>
class Future;

template <typename T>
struct IsFuture : std::false_type {
    using Inner = T;
};

template <typename T>
struct IsFuture<Future<T>> : std::true_type {
    using Inner = T;
};

template <typename T, typename F>
struct TryCallableResult {
    using Result = std::invoke_result_t<F, Try<T>&&>;
    using ReturnsFuture = IsFuture<Result>;
    static constexpr bool isTry = true;
};

template <typename T, typename F>
struct ValueCallableResult {
    using Result = std::invoke_result_t<F, T&&>;
    using ReturnsFuture = IsFuture<Result>;
    static constexpr bool isTry = false;
};

template <typename F>
struct ValueCallableResult<void, F> {
    using Result = std::invoke_result_t<F>;
    using ReturnsFuture = IsFuture<Result>;
    static constexpr bool isTry = false;
};

namespace detail {
template <typename T>
struct remove_cvref {
    using type =
        typename std::remove_cv<typename std::remove_reference<T>::type>::type;
};
template <typename T>
using remove_cvref_t = typename remove_cvref<T>::type;
}  // namespace detail

}  // namespace async_simple

#endif  // ASYNC_SIMPLE_TRAITS_H
