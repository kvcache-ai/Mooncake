
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
#ifndef ASYNC_SIMPLE_LOCALSTATE_H
#define ASYNC_SIMPLE_LOCALSTATE_H

#ifndef ASYNC_SIMPLE_USE_MODULES
#include <functional>
#include <utility>

#include "async_simple/Executor.h"
#include "async_simple/Try.h"

#endif  // ASYNC_SIMPLE_USE_MODULES

namespace async_simple {

// A component of Future/Promise. LocalState is owned by
// Future only. LocalState should be valid after
// Future and Promise disconnects.
//
// Users should never use LocalState directly.
template <typename T>
class LocalState {
private:
    using Continuation = std::function<void(Try<T>&& value)>;

public:
    LocalState() : _executor(nullptr) {}
    LocalState(T&& v) : _try_value(std::forward<T>(v)), _executor(nullptr) {}
    LocalState(Try<T>&& t) : _try_value(std::move(t)), _executor(nullptr) {}

    ~LocalState() {}

    LocalState(const LocalState&) = delete;
    LocalState& operator=(const LocalState&) = delete;

    LocalState(LocalState&& other)
        : _try_value(std::move(other._try_value)),
          _executor(std::exchange(other._executor, nullptr)) {}
    LocalState& operator=(LocalState&& other) {
        if (this != &other) {
            std::swap(_try_value, other._try_value);
            std::swap(_executor, other._executor);
        }
        return *this;
    }

public:
    bool hasResult() const noexcept { return _try_value.available(); }

public:
    Try<T>& getTry() noexcept { return _try_value; }
    const Try<T>& getTry() const noexcept { return _try_value; }

    void setExecutor(Executor* ex) { _executor = ex; }

    Executor* getExecutor() { return _executor; }

    bool currentThreadInExecutor() const {
        if (!_executor) {
            return false;
        }
        return _executor->currentThreadInExecutor();
    }

    template <typename F>
    void setContinuation(F&& f) {
        assert(_try_value.available());
        std::forward<F>(f)(std::move(_try_value));
    }

private:
    Try<T> _try_value;
    Executor* _executor;
};
}  // namespace async_simple

#endif
