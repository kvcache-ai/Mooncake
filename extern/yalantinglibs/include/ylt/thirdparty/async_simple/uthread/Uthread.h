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
#ifndef ASYNC_SIMPLE_UTHREAD_UTHREAD_H
#define ASYNC_SIMPLE_UTHREAD_UTHREAD_H

#ifndef ASYNC_SIMPLE_USE_MODULES
#include <memory>
#include "async_simple/uthread/internal/thread.h"

#endif  // ASYNC_SIMPLE_USE_MODULES

namespace async_simple {
namespace uthread {

struct Attribute {
    Executor* ex;
    size_t stack_size = 0;
};

// A Uthread is a stackful coroutine which would checkin/checkout based on
// context switching. A user shouldn't use Uthread directly. He should use
// async/await instead. See Async.h/Await.h for details.
//
// When a user gets a uthread returned from async. He could use `Uthread::join`
// to set a callback for that uthread. The callback would be called when the
// uthread finished.
//
// The implementation for Uthread is extracted from Boost. See
// uthread/internal/*.S for details.
class Uthread {
public:
    Uthread() = default;
    template <class Func>
    Uthread(Attribute attr, Func&& func) : _attr(std::move(attr)) {
        _ctx = std::make_unique<internal::thread_context>(std::move(func),
                                                          _attr.stack_size);
    }
    ~Uthread() = default;
    Uthread(Uthread&& x) noexcept = default;
    Uthread& operator=(Uthread&& x) noexcept = default;

public:
    template <class Callback>
    bool join(Callback&& callback) {
        if (!_ctx || _ctx->joined_) {
            return false;
        }
        _ctx->joined_ = true;
        auto f = _ctx->done_.getFuture().via(_attr.ex);
        if (f.hasResult()) {
            callback();
            return true;
        }
        if (!_attr.ex) {
            // we can not delay the uthread life without executor.
            // so, if the user do not hold the uthread in outside,
            // the user can not do switch in again.
            std::move(f).setContinuation(
                [callback = std::move(callback)](auto&&) { callback(); });
        } else {
            _ctx->done_.forceSched().checkout();
            std::move(f).setContinuation(
                [callback = std::move(callback),
                 // hold on the life of uthread.
                 // user never care about the uthread's destruct.
                 self = std::move(*this)](auto&&) { callback(); });
        }
        return true;
    }
    void detach() {
        join([]() {});
    }

private:
    Attribute _attr;
    std::unique_ptr<internal::thread_context> _ctx;
};

}  // namespace uthread
}  // namespace async_simple

#endif  // ASYNC_SIMPLE_UTHREAD_UTHREAD_H