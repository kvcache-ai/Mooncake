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
/* This file implements a simple condition variable. This is used as a
 * low level component in async_simple. Users shouldn't use this directly.
 */
#ifndef ASYNC_SIMPLE_UTIL_CONDITION_H
#define ASYNC_SIMPLE_UTIL_CONDITION_H

#ifndef ASYNC_SIMPLE_USE_MODULES
#if __has_include(<semaphore>)
#include <semaphore>
#else
#include <atomic>
#include <condition_variable>
#include <mutex>
#endif

#endif  // ASYNC_SIMPLE_USE_MODULES

namespace async_simple {
namespace util {

#if __has_include(<semaphore>)
class Condition : public std::binary_semaphore {
public:
    explicit Condition(ptrdiff_t num = 0) : std::binary_semaphore(num) {}
};

#else

class Condition {
public:
    void release() {
        std::lock_guard lock(_mutex);
        ++_count;
        _condition.notify_one();
    }

    void acquire() {
        std::unique_lock lock(_mutex);
        _condition.wait(lock, [this] { return _count > 0; });
        --_count;
    }

private:
    std::mutex _mutex;
    std::condition_variable _condition;
    size_t _count = 0;
};

#endif

}  // namespace util
}  // namespace async_simple

#endif
