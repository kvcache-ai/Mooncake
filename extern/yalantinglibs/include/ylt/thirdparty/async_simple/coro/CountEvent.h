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
#ifndef ASYNC_SIMPLE_CORO_EVENT_H
#define ASYNC_SIMPLE_CORO_EVENT_H

#ifndef ASYNC_SIMPLE_USE_MODULES
#include <atomic>
#include <cstddef>
#include <utility>
#include "async_simple/experimental/coroutine.h"

#endif  // ASYNC_SIMPLE_USE_MODULES

namespace async_simple {

namespace coro {

namespace detail {

// CountEvent is a count-down event.
// The last 'down' will resume the awaiting coroutine on this event.
class CountEvent {
public:
    CountEvent(size_t count) : _count(count) {}
    CountEvent(const CountEvent&) = delete;
    CountEvent(CountEvent&& other)
        : _count(other._count.exchange(0, std::memory_order_relaxed)),
          _awaitingCoro(std::exchange(other._awaitingCoro, nullptr)) {}

    [[nodiscard]] CoroHandle<> down(size_t n = 1) {
        std::size_t oldCount;
        return down(oldCount, n);
    }
    [[nodiscard]] CoroHandle<> down(size_t& oldCount, std::size_t n) {
        // read acquire and write release, _awaitingCoro store can not be
        // reordered after this barrier
        oldCount = _count.fetch_sub(n, std::memory_order_acq_rel);
        if (oldCount == 1) {
            auto awaitingCoro = _awaitingCoro;
            _awaitingCoro = nullptr;
            return awaitingCoro;
        } else {
            return nullptr;
            // return nullptr instead of noop_coroutine could save one time
            // for accessing the memory.
            // return std::noop_coroutine();
        }
    }
    [[nodiscard]] size_t downCount(size_t n = 1) {
        // read acquire and write release
        return _count.fetch_sub(n, std::memory_order_acq_rel);
    }

    void setAwaitingCoro(CoroHandle<> h) { _awaitingCoro = h; }

private:
    std::atomic<size_t> _count;
    CoroHandle<> _awaitingCoro;
};

}  // namespace detail

}  // namespace coro
}  // namespace async_simple

#endif
