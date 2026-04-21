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
#ifndef ASYNC_SIMPLE_CORO_TRAITS_H
#define ASYNC_SIMPLE_CORO_TRAITS_H

#ifndef ASYNC_SIMPLE_USE_MODULES
#include <utility>

#endif  // ASYNC_SIMPLE_USE_MODULES

namespace async_simple {

namespace coro {

namespace detail {

template <class T>
concept HasCoAwaitMethod = requires(T&& awaitable) {
    std::forward<T>(awaitable).coAwait(nullptr);
};

template <class T>
concept HasMemberCoAwaitOperator = requires(T&& awaitable) {
    std::forward<T>(awaitable).operator co_await();
};

#ifdef _MSC_VER
// FIXME: MSVC compiler bug, See
// https://developercommunity.visualstudio.com/t/10160851
template <class, class = void>
struct HasGlobalCoAwaitOperatorHelp : std::false_type {};

template <class T>
struct HasGlobalCoAwaitOperatorHelp<
    T, std::void_t<decltype(operator co_await(std::declval<T>()))>>
    : std::true_type {};

template <class T>
concept HasGlobalCoAwaitOperator = HasGlobalCoAwaitOperatorHelp<T>::value;
#else
template <class T>
concept HasGlobalCoAwaitOperator = requires(T&& awaitable) {
    operator co_await(std::forward<T>(awaitable));
};
#endif

template <typename Awaitable>
auto getAwaiter(Awaitable&& awaitable) {
    if constexpr (HasMemberCoAwaitOperator<Awaitable>)
        return std::forward<Awaitable>(awaitable).operator co_await();
    else if constexpr (HasGlobalCoAwaitOperator<Awaitable>)
        return operator co_await(std::forward<Awaitable>(awaitable));
    else
        return std::forward<Awaitable>(awaitable);
}

}  // namespace detail

}  // namespace coro
}  // namespace async_simple

#endif
