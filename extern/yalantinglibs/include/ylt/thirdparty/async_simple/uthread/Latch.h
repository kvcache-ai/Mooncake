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
#ifndef ASYNC_SIMPLE_UTHREAD_LATCH_H
#define ASYNC_SIMPLE_UTHREAD_LATCH_H

#ifndef ASYNC_SIMPLE_USE_MODULES
#include <type_traits>
#include "async_simple/Future.h"
#include "async_simple/Promise.h"
#include "async_simple/uthread/Await.h"

#endif  // ASYNC_SIMPLE_USE_MODULES

namespace async_simple {
namespace uthread {

// The semantics of uthread::Latch is similar to std::latch. The design of
// uthread::Latch also mimics the design of std::latch. The uthread::Latch is
// used to synchronize different uthread stackful coroutine. The uthread
// stackful coroutine who is awaiting a uthread::Latch would be suspended until
// the counter in the awaited uthread::Latch counted to zero.
//
// Example:
//
// ```C++
// Latch latch(2);
// latch.await();
//      // In another uthread
//      latch.downCount();
//      // In another uthread
//      latch.downCount();
// ```
class Latch {
public:
    explicit Latch(std::size_t count) : _count(count), _skip(!count) {}
    Latch(const Latch&) = delete;
    Latch(Latch&&) = delete;

    ~Latch() {}

public:
    void downCount(std::size_t n = 1) {
        if (_skip) {
            return;
        }
        auto lastCount = _count.fetch_sub(n, std::memory_order_acq_rel);
        if (lastCount == 1u) {
            _promise.setValue(true);
        }
    }
    void await(Executor* ex) {
        if (_skip) {
            return;
        }
        uthread::await(_promise.getFuture().via(ex));
    }
    std::size_t currentCount() const {
        return _count.load(std::memory_order_acquire);
    }

private:
    Promise<bool> _promise;
    std::atomic<std::size_t> _count;
    bool _skip;
};

}  // namespace uthread
}  // namespace async_simple

#endif  // ASYNC_SIMPLE_UTHREAD_LATCH_H
