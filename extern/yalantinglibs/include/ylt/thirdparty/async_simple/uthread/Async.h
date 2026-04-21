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
/* This file implements async interface. The call to async
 * would create a uthread stackful coroutine and the corresponding
 * root function in the uthread stackful coroutine.
 *
 * Here are 3 modes available when creating a uthread.
 * - Launch::Prompt. This mode would create a uthread and return the uthread to
 * the caller.
 *   The caller could set a callback by `Uthread::join` API. `Uthread::join` API
 *   would call the callback after the root function ends.
 * - Launch::Schedule. In this mode, it depends on the exeuctor to schedule the
 * task to create
 *   the uthread. So in this mode, the executor passed in shouldn't be nullptr.
 *   In this mode, the user could choose to offer a callback.
 * - Launch::Current. This mode would create a uthread and detach it. So the
 * user wouldn't get the
 *   uthread instance. So that the user couldn't set a callback nor know whether
 *   or not if the uthread finished.
 */
#ifndef ASYNC_SIMPLE_UTHREAD_ASYNC_H
#define ASYNC_SIMPLE_UTHREAD_ASYNC_H

#ifndef ASYNC_SIMPLE_USE_MODULES
#include <memory>
#include <type_traits>
#include <tuple>
#include "async_simple/uthread/Uthread.h"

#endif  // ASYNC_SIMPLE_USE_MODULES

namespace async_simple {
namespace uthread {

enum class Launch {
    Prompt,
    Schedule,
    Current,
};

template <Launch policy, class F>
requires(policy == Launch::Prompt) inline Uthread async(F&& f, Executor* ex) {
    return Uthread(Attribute{ex}, std::forward<F>(f));
}

template <Launch policy, class F>
requires(policy == Launch::Schedule) inline void async(F&& f, Executor* ex) {
    if (!ex)
        AS_UNLIKELY { return; }
    ex->schedule([f = std::move(f), ex]() {
        Uthread uth(Attribute{ex}, std::move(f));
        uth.detach();
    });
}

// schedule async task, set a callback
template <Launch policy, class F, class C>
requires(policy == Launch::Schedule) inline void async(F&& f, C&& c,
                                                       Executor* ex) {
    if (!ex)
        AS_UNLIKELY { return; }
    ex->schedule([f = std::move(f), c = std::move(c), ex]() {
        Uthread uth(Attribute{ex}, std::move(f));
        uth.join(std::move(c));
    });
}

template <Launch policy, class F>
requires(policy == Launch::Current) inline void async(F&& f, Executor* ex) {
    Uthread uth(Attribute{ex}, std::forward<F>(f));
    uth.detach();
}

template <class F, class... Args,
          typename R = std::invoke_result_t<F&&, Args&&...>>
inline Future<R> async(Launch policy, Attribute attr, F&& f, Args&&... args) {
    if (policy == Launch::Schedule) {
        if (!attr.ex)
            AS_UNLIKELY {
                // TODO log
                assert(false);
            }
    }
    Promise<R> p;
    auto rc = p.getFuture().via(attr.ex);
    auto proc = [p = std::move(p), ex = attr.ex, f = std::forward<F>(f),
                 args =
                     std::make_tuple(std::forward<Args>(args)...)]() mutable {
        if (ex) {
            p.forceSched().checkout();
        }
        if constexpr (std::is_void_v<R>) {
            std::apply(f, std::move(args));
            p.setValue();
        } else {
            p.setValue(std::apply(f, std::move(args)));
        }
    };
    if (policy == Launch::Schedule) {
        attr.ex->schedule([fn = std::move(proc), attr]() {
            Uthread(attr, std::move(fn)).detach();
        });
    } else if (policy == Launch::Current) {
        Uthread(attr, std::move(proc)).detach();
    } else {
        // TODO log
        assert(false);
    }

    return rc;
}

}  // namespace uthread

}  // namespace async_simple

#endif  // ASYNC_SIMPLE_UTHREAD_ASYNC_H
