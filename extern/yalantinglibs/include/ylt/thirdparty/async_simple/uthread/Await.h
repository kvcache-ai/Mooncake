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
/* This file implements await interface. This should be used in
 * a uthread stackful coroutine to await a stackless coroutine.
 * The key point here is that the function to call await doesn't
 * want to be a stackful coroutine since if a function wants to await
 * the value of a coroutine by a co_await operator, the function itself must
 * be a stackless coroutine too. In the case the function itself doesn't want to
 * be a stackless coroutine, it could use the await interface.
 */
#ifndef ASYNC_SIMPLE_UTHREAD_AWAIT_H
#define ASYNC_SIMPLE_UTHREAD_AWAIT_H

#ifndef ASYNC_SIMPLE_USE_MODULES
#include <type_traits>
#include "async_simple/Future.h"
#include "async_simple/coro/Lazy.h"
#include "async_simple/uthread/internal/thread_impl.h"

#endif  // ASYNC_SIMPLE_USE_MODULES

namespace async_simple {
namespace uthread {

// Use to async get future value in uthread context.
// Invoke await will not block current thread.
// The current uthread will be suspend until promise.setValue() be called.
template <class T>
T await(Future<T>&& fut) {
    logicAssert(fut.valid(), "Future is broken");
    if (fut.hasResult()) {
        return fut.value();
    }
    auto executor = fut.getExecutor();
    logicAssert(executor, "Future not has Executor");
    logicAssert(executor->currentThreadInExecutor(),
                "await invoked not in Executor");
    Promise<T> p;
    auto f = p.getFuture().via(executor);
    p.forceSched().checkout();

    auto ctx = uthread::internal::thread_impl::get();
    f.setContinuation(
        [ctx](auto&&) { uthread::internal::thread_impl::switch_in(ctx); });

    std::move(fut).thenTry(
        [p = std::move(p)](Try<T>&& t) mutable { p.setValue(std::move(t)); });
    do {
        uthread::internal::thread_impl::switch_out(ctx);
        assert(f.hasResult());
    } while (!f.hasResult());
    return f.value();
}

// This await interface focus on await function of an object.
// Here is an example:
// ```C++
//  class Foo {
//  public:
//     lazy<T> bar(Ts&&...) {}
//  };
//  Foo f;
//  await(ex, &Foo::bar, &f, Ts&&...);
// ```
// ```C++
//  lazy<T> foo(Ts&&...);
//  await(ex, foo, Ts&&...);
//  auto lambda = [](Ts&&...) -> lazy<T> {};
//  await(ex, lambda, Ts&&...);
// ```
template <class Fn, class... Args>
decltype(auto) await(Executor* ex, Fn&& fn, Args&&... args) requires
    std::is_invocable_v<Fn&&, Args&&...> {
    using ValueType = typename std::invoke_result_t<Fn&&, Args&&...>::ValueType;
    Promise<ValueType> p;
    auto f = p.getFuture().via(ex);
    auto lazy =
        [p = std::move(p)]<typename... Ts>(Ts&&... ts) mutable -> coro::Lazy<> {
        if constexpr (std::is_void_v<ValueType>) {
            co_await std::invoke(std::forward<Ts>(ts)...);
            p.setValue();
        } else {
            p.setValue(co_await std::invoke(std::forward<Ts>(ts)...));
        }
        co_return;
    };
    lazy(std::forward<Fn>(fn), std::forward<Args>(args)...)
        .directlyStart([](auto&&) {}, ex);
    return await(std::move(f));
}

// This await interface is special. It would accept the function who receive an
// argument whose type is `Promise<T>&&`. The usage for this interface is
// limited. The example includes:
//
// ```C++
//  void foo(Promise<T>&&);
//  await<T>(ex, foo);
//  auto lambda = [](Promise<T>&&) {};
//  await<T>(ex, lambda);
// ```
template <class T, class Fn>
T await(Executor* ex, Fn&& fn) {
    static_assert(std::is_invocable<decltype(fn), Promise<T>>::value,
                  "Callable of await is not support, eg: Callable(Promise<T>)");
    Promise<T> p;
    auto f = p.getFuture().via(ex);
    p.forceSched().checkout();
    fn(std::move(p));
    return await(std::move(f));
}

}  // namespace uthread
}  // namespace async_simple

#endif  // ASYNC_SIMPLE_UTHREAD_AWAIT_H
