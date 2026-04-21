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
#ifndef ASYNC_SIMPLE_PROMISE_H
#define ASYNC_SIMPLE_PROMISE_H

#ifndef ASYNC_SIMPLE_USE_MODULES
#include <exception>
#include "async_simple/Common.h"
#include "async_simple/Future.h"

#endif  // ASYNC_SIMPLE_USE_MODULES

namespace async_simple {

template <typename T>
class Future;

// The well-known Future/Promise pair mimics a producer/consumer pair.
// The Promise stands for the producer-side.
//
// We could get a Future from the Promise by calling getFuture(). And
// set value by calling setValue(). In case we need to set exception,
// we could call setException().
template <typename T>
class Promise {
public:
    using value_type = std::conditional_t<std::is_void_v<T>, Unit, T>;
    Promise() : _sharedState(new FutureState<value_type>()), _hasFuture(false) {
        _sharedState->attachPromise();
    }
    ~Promise() {
        if (_sharedState) {
            _sharedState->detachPromise();
        }
    }

    Promise(const Promise& other) {
        _sharedState = other._sharedState;
        _hasFuture = other._hasFuture;
        _sharedState->attachPromise();
    }
    Promise& operator=(const Promise& other) {
        if (this == &other) {
            return *this;
        }
        this->~Promise();
        _sharedState = other._sharedState;
        _hasFuture = other._hasFuture;
        _sharedState->attachPromise();
        return *this;
    }

    Promise(Promise<T>&& other)
        : _sharedState(other._sharedState), _hasFuture(other._hasFuture) {
        other._sharedState = nullptr;
        other._hasFuture = false;
    }
    Promise& operator=(Promise<T>&& other) {
        std::swap(_sharedState, other._sharedState);
        std::swap(_hasFuture, other._hasFuture);
        return *this;
    }

public:
    Future<T> getFuture() {
        logicAssert(valid(), "Promise is broken");
        logicAssert(!_hasFuture, "Promise already has a future");
        _hasFuture = true;
        return Future<T>(_sharedState);
    }
    bool valid() const { return _sharedState != nullptr; }
    // make the continuation back to origin context
    Promise& checkout() {
        if (_sharedState) {
            _sharedState->checkout();
        }
        return *this;
    }
    Promise& forceSched() {
        if (_sharedState) {
            _sharedState->setForceSched();
        }
        return *this;
    }

public:
    void setException(std::exception_ptr error) {
        logicAssert(valid(), "Promise is broken");
        _sharedState->setResult(Try<value_type>(error));
    }
    void setValue(value_type&& v) requires(!std::is_void_v<T>) {
        logicAssert(valid(), "Promise is broken");
        _sharedState->setResult(Try<value_type>(std::forward<T>(v)));
    }
    void setValue(Try<value_type>&& t) {
        logicAssert(valid(), "Promise is broken");
        _sharedState->setResult(std::move(t));
    }

    void setValue() requires(std::is_void_v<T>) {
        logicAssert(valid(), "Promise is broken");
        _sharedState->setResult(Try<value_type>(Unit()));
    }

private:
    FutureState<value_type>* _sharedState = nullptr;
    bool _hasFuture = false;
};

}  // namespace async_simple

#endif  // ASYNC_SIMPLE_PROMISE_H
