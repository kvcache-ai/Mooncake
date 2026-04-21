/*
 * Copyright (c) Facebook, Inc. and its affiliates.
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
#ifndef ASYNC_SIMPLE_TRY_H
#define ASYNC_SIMPLE_TRY_H

#ifndef ASYNC_SIMPLE_USE_MODULES
#include <cassert>
#include <exception>
#include <functional>
#include <utility>
#include <variant>
#include "async_simple/Common.h"
#include "async_simple/Unit.h"

#endif  // ASYNC_SIMPLE_USE_MODULES

namespace async_simple {

// Forward declaration
template <typename T>
class Try;

template <>
class Try<void>;
// Try<T> contains either an instance of T, an exception, or nothing.
// Try<T>::value() will return the instance.
// If exception or nothing inside, Try<T>::value() would throw an exception.
//
// You can create a Try<T> by:
// 1. default constructor: contains nothing.
// 2. construct from exception_ptr.
// 3. construct from value T.
// 4. moved from another Try<T> instance.
template <typename T>
class Try {
public:
    Try() = default;
    ~Try() = default;

    Try(Try<T>&& other) = default;
    template <typename T2 = T>
    Try(std::enable_if_t<std::is_same<Unit, T2>::value, const Try<void>&>
            other) {
        if (other.hasError()) {
            _value.template emplace<std::exception_ptr>(other._error);
        } else {
            _value.template emplace<T>();
        }
    }
    Try& operator=(Try<T>&& other) = default;
    Try& operator=(std::exception_ptr error) {
        if (std::holds_alternative<std::exception_ptr>(_value) &&
            std::get<std::exception_ptr>(_value) == error) {
            return *this;
        }

        _value.template emplace<std::exception_ptr>(error);
        return *this;
    }

    template <class... U>
    Try(U&&... value) requires std::is_constructible_v<T, U...>
        : _value(std::in_place_type<T>, std::forward<U>(value)...) {}

    Try(std::exception_ptr error) : _value(error) {}

private:
    Try(const Try&) = delete;
    Try& operator=(const Try&) = delete;

public:
    constexpr bool available() const noexcept {
        return !std::holds_alternative<std::monostate>(_value);
    }
    constexpr bool hasError() const noexcept {
        return std::holds_alternative<std::exception_ptr>(_value);
    }
    const T& value() const& {
        checkHasTry();
        return std::get<T>(_value);
    }
    T& value() & {
        checkHasTry();
        return std::get<T>(_value);
    }
    T&& value() && {
        checkHasTry();
        return std::move(std::get<T>(_value));
    }
    const T&& value() const&& {
        checkHasTry();
        return std::move(std::get<T>(_value));
    }

    template <class... Args>
    T& emplace(Args&&... args) {
        return _value.template emplace<T>(std::forward<Args>(args)...);
    }

    void setException(std::exception_ptr error) {
        if (std::holds_alternative<std::exception_ptr>(_value) &&
            std::get<std::exception_ptr>(_value) == error) {
            return;
        }
        _value.template emplace<std::exception_ptr>(error);
    }
    std::exception_ptr getException() const {
        logicAssert(std::holds_alternative<std::exception_ptr>(_value),
                    "Try object do not has an error");
        return std::get<std::exception_ptr>(_value);
    }

    operator Try<void>() const;

private:
    AS_INLINE void checkHasTry() const {
        if (std::holds_alternative<T>(_value))
            AS_LIKELY { return; }
        else if (std::holds_alternative<std::exception_ptr>(_value)) {
            std::rethrow_exception(std::get<std::exception_ptr>(_value));
        } else if (std::holds_alternative<std::monostate>(_value)) {
            throw std::logic_error("Try object is empty");
        } else {
            assert(false);
        }
    }

private:
    std::variant<std::monostate, T, std::exception_ptr> _value;

private:
    friend Try<Unit>;
};

template <>
class Try<void> {
public:
    Try() {}
    Try(std::exception_ptr error) : _error(error) {}

    Try& operator=(std::exception_ptr error) {
        _error = error;
        return *this;
    }

public:
    Try(Try&& other) : _error(std::move(other._error)) {}
    Try& operator=(Try&& other) {
        if (this != &other) {
            std::swap(_error, other._error);
        }
        return *this;
    }

public:
    void value() {
        if (_error) {
            std::rethrow_exception(_error);
        }
    }

    bool hasError() const { return _error.operator bool(); }

    void setException(std::exception_ptr error) { _error = error; }
    std::exception_ptr getException() const { return _error; }

private:
    std::exception_ptr _error;

private:
    friend Try<Unit>;
};

template <class T>
Try<T>::operator Try<void>() const {
    if (hasError()) {
        return Try<void>(getException());
    }
    return Try<void>();
}

template <class T>
Try(T) -> Try<T>;

template <typename F, typename... Args>
auto makeTryCall(F&& f, Args&&... args) {
    using T = std::invoke_result_t<F, Args...>;
    try {
        if constexpr (std::is_void_v<T>) {
            std::invoke(std::forward<F>(f), std::forward<Args>(args)...);
            return Try<void>();
        } else {
            return Try<T>(
                std::invoke(std::forward<F>(f), std::forward<Args>(args)...));
        }
    } catch (...) {
        return Try<T>(std::current_exception());
    }
}

}  // namespace async_simple

#endif  // ASYNC_SIMPLE_TRY_H
