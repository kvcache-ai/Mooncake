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
#ifndef ASYNC_SIMPLE_CORO_COLLECT_H
#define ASYNC_SIMPLE_CORO_COLLECT_H

#ifndef ASYNC_SIMPLE_USE_MODULES
#include <array>
#include <exception>
#include <memory>
#include <system_error>
#include <type_traits>
#include <utility>
#include <variant>
#include <vector>
#include "async_simple/Common.h"
#include "async_simple/Signal.h"
#include "async_simple/Try.h"
#include "async_simple/Unit.h"
#include "async_simple/coro/CountEvent.h"
#include "async_simple/coro/Lazy.h"
#include "async_simple/coro/LazyLocalBase.h"
#include "async_simple/experimental/coroutine.h"

#endif  // ASYNC_SIMPLE_USE_MODULES

namespace async_simple {
namespace coro {

// collectAll types
// auto [x, y] = co_await collectAll(IntLazy, FloatLazy);
// auto [x, y] = co_await collectAllPara(IntLazy, FloatLazy);
// std::vector<Try<int>> = co_await collectAll(std::vector<intLazy>);
// std::vector<Try<int>> = co_await collectAllPara(std::vector<intLazy>);
// std::vector<Try<int>> = co_await collectAllWindowed(maxConcurrency, yield,
// std::vector<intLazy>); std::vector<Try<int>> = co_await
// collectAllWindowedPara(maxConcurrency, yield, std::vector<intLazy>);

namespace detail {

template <typename T>
struct CollectAnyResult {
    CollectAnyResult() : _idx(static_cast<size_t>(-1)), _value() {}
    CollectAnyResult(size_t idx, std::add_rvalue_reference_t<T> value)
        : _idx(idx), _value(std::move(value)) {}

    CollectAnyResult(const CollectAnyResult&) = delete;
    CollectAnyResult& operator=(const CollectAnyResult&) = delete;
    CollectAnyResult& operator=(CollectAnyResult&& other) {
        _idx = std::move(other._idx);
        _value = std::move(other._value);
        other._idx = static_cast<size_t>(-1);
        return *this;
    }
    CollectAnyResult(CollectAnyResult&& other)
        : _idx(std::move(other._idx)), _value(std::move(other._value)) {
        other._idx = static_cast<size_t>(-1);
    }

    size_t _idx;
    Try<T> _value;

    size_t index() const { return _idx; }

    bool hasError() const { return _value.hasError(); }
    // Require hasError() == true. Otherwise it is UB to call
    // this method.
    std::exception_ptr getException() const { return _value.getException(); }

    // Require hasError() == false. Otherwise it is UB to call
    // value() method.
#if __cpp_explicit_this_parameter >= 202110L
    template <class Self>
    auto&& value(this Self&& self) {
        return std::forward<Self>(self)._value.value();
    }
#else
    const T& value() const& { return _value.value(); }
    T& value() & { return _value.value(); }
    T&& value() && { return std::move(_value).value(); }
    const T&& value() const&& { return std::move(_value).value(); }
#endif
};

template <>
struct CollectAnyResult<void> {
    CollectAnyResult() : _idx(static_cast<size_t>(-1)), _value() {}
    CollectAnyResult(size_t idx) : _idx(idx) {}

    CollectAnyResult(const CollectAnyResult&) = delete;
    CollectAnyResult& operator=(const CollectAnyResult&) = delete;
    CollectAnyResult& operator=(CollectAnyResult&& other) {
        _idx = std::move(other._idx);
        _value = std::move(other._value);
        other._idx = static_cast<size_t>(-1);
        return *this;
    }
    CollectAnyResult(CollectAnyResult&& other)
        : _idx(std::move(other._idx)), _value(std::move(other._value)) {
        other._idx = static_cast<size_t>(-1);
    }

    size_t _idx;
    Try<void> _value;

    size_t index() const { return _idx; }

    bool hasError() const { return _value.hasError(); }
    // Require hasError() == true. Otherwise it is UB to call
    // this method.
    std::exception_ptr getException() const { return _value.getException(); }
};

template <typename LazyType, typename InAlloc>
struct CollectAnyAwaiter {
    using ValueType = typename LazyType::ValueType;
    using ResultType = CollectAnyResult<ValueType>;

    CollectAnyAwaiter(Slot* slot, SignalType SignalType,
                      std::vector<LazyType, InAlloc>&& input)
        : _slot(slot),
          _SignalType(SignalType),
          _result(nullptr),
          _input(std::move(input)) {}

    CollectAnyAwaiter(const CollectAnyAwaiter&) = delete;
    CollectAnyAwaiter& operator=(const CollectAnyAwaiter&) = delete;
    CollectAnyAwaiter(CollectAnyAwaiter&& other)
        : _slot(other._slot),
          _SignalType(other._SignalType),
          _result(std::move(other._result)),
          _input(std::move(other._input)) {}

    bool await_ready() const noexcept {
        return _input.empty() || signalHelper{Terminate}.hasCanceled(_slot);
    }

    bool await_suspend(std::coroutine_handle<> continuation) {
        auto promise_type =
            std::coroutine_handle<LazyPromiseBase>::from_address(
                continuation.address())
                .promise();

        auto executor = promise_type._executor;

        // Make local copies to shared_ptr to avoid deleting objects too early
        // if coroutine resume before this function.
        auto input = std::move(_input);
        auto event = std::make_shared<detail::CountEvent>(input.size() + 1);
        auto signal = Signal::create();
        if (_slot)
            _slot->chainedSignal(signal.get());
        if (!signalHelper{Terminate}.tryEmplace(
                _slot, [c = continuation, e = event, size = input.size()](
                           SignalType type, Signal*) mutable {
                    auto count = e->downCount();
                    if (count == size + 1) {
                        c.resume();
                    }
                })) {  // has canceled
            return false;
        }

        for (size_t i = 0; i < input.size(); ++i) {
            if (!input[i]._coro.promise()._executor) {
                input[i]._coro.promise()._executor = executor;
            }
            std::unique_ptr<LazyLocalBase> local;
            local = std::make_unique<LazyLocalBase>(signal.get());
            input[i]._coro.promise()._lazy_local = local.get();
            input[i].start(
                [this, i, size = input.size(), c = continuation, e = event,
                 local = std::move(local)](Try<ValueType>&& result) mutable {
                    assert(e != nullptr);
                    auto count = e->downCount();
                    // n+1: n coro + 1 cancel handler
                    if (count == size + 1) {
                        _result = std::make_unique<ResultType>();
                        _result->_idx = i;
                        _result->_value = std::move(result);
                        if (auto ptr = local->getSlot(); ptr) {
                            ptr->signal()->emits(_SignalType);
                        }
                        c.resume();
                    }
                });
        }  // end for
        return true;
    }
    auto await_resume() {
        signalHelper{Terminate}.checkHasCanceled(
            _slot, "async_simple::CollectAny is canceled!");
        if (_result == nullptr) {
            return ResultType{};
        }
        return std::move(*_result);
    }

    Slot* _slot;
    SignalType _SignalType;
    std::unique_ptr<ResultType> _result;
    std::vector<LazyType, InAlloc> _input;
};

template <template <typename> typename LazyType, typename... Ts>
struct CollectAnyVariadicAwaiter {
    using ResultType = std::variant<Try<Ts>...>;
    using InputType = std::tuple<LazyType<Ts>...>;

    CollectAnyVariadicAwaiter(Slot* slot, SignalType SignalType,
                              LazyType<Ts>&&... inputs)
        : _slot(slot),
          _SignalType(SignalType),
          _input(std::make_unique<InputType>(std::move(inputs)...)),
          _result(nullptr) {}

    CollectAnyVariadicAwaiter(Slot* slot, SignalType SignalType,
                              InputType&& inputs)
        : _slot(slot),
          _SignalType(SignalType),
          _input(std::make_unique<InputType>(std::move(inputs))),
          _result(nullptr) {}

    CollectAnyVariadicAwaiter(const CollectAnyVariadicAwaiter&) = delete;
    CollectAnyVariadicAwaiter& operator=(const CollectAnyVariadicAwaiter&) =
        delete;
    CollectAnyVariadicAwaiter(CollectAnyVariadicAwaiter&& other)
        : _slot(other._slot),
          _SignalType(other._SignalType),
          _input(std::move(other._input)),
          _result(std::move(other._result)) {}

    bool await_ready() const noexcept {
        return signalHelper{Terminate}.hasCanceled(_slot);
    }

    template <size_t... index>
    bool await_suspend_impl(std::index_sequence<index...>,
                            std::coroutine_handle<> continuation) {
        auto promise_type =
            std::coroutine_handle<LazyPromiseBase>::from_address(
                continuation.address())
                .promise();
        auto executor = promise_type._executor;

        // Make local copies to avoid deleting objects too early
        // if coroutine resume before this function.
        auto input = std::move(_input);

        auto event = std::make_shared<detail::CountEvent>(
            std::tuple_size<InputType>() + 1);
        auto signal = Signal::create();
        if (_slot)
            _slot->chainedSignal(signal.get());
        if (!signalHelper{Terminate}.tryEmplace(
                _slot, [c = continuation, e = event](SignalType type,
                                                     Signal*) mutable {
                    auto count = e->downCount();
                    if (count == std::tuple_size<InputType>() + 1) {
                        c.resume();
                    }
                })) {  // has canceled
            return false;
        }
        (
            [&]() {
                auto& element = std::get<index>(*input);
                if (!element._coro.promise()._executor) {
                    element._coro.promise()._executor = executor;
                }
                std::unique_ptr<LazyLocalBase> local;
                local = std::make_unique<LazyLocalBase>(signal.get());
                element._coro.promise()._lazy_local = local.get();
                element.start(
                    [this, c = continuation, e = event,
                     local = std::move(local)](
                        std::variant_alternative_t<index, ResultType>&&
                            res) mutable {
                        auto count = e->downCount();
                        // n+1: n coro + 1 cancel handler
                        if (count == std::tuple_size<InputType>() + 1) {
                            _result = std::make_unique<ResultType>(
                                std::in_place_index_t<index>(), std::move(res));
                            if (auto ptr = local->getSlot(); ptr) {
                                ptr->signal()->emits(_SignalType);
                            }
                            c.resume();
                        }
                    });
            }(),
            ...);
        return true;
    }

    bool await_suspend(std::coroutine_handle<> continuation) {
        return await_suspend_impl(std::make_index_sequence<sizeof...(Ts)>{},
                                  std::move(continuation));
    }

    auto await_resume() {
        signalHelper{Terminate}.checkHasCanceled(
            _slot, "async_simple::CollectAny is canceled!");
        return std::move(*_result);
    }

    async_simple::Slot* _slot;
    SignalType _SignalType;
    std::unique_ptr<std::tuple<LazyType<Ts>...>> _input;
    std::unique_ptr<ResultType> _result;
};

template <typename T, typename InAlloc>
struct SimpleCollectAnyAwaitable {
    using ValueType = T;
    using LazyType = Lazy<T>;
    using VectorType = std::vector<LazyType, InAlloc>;

    async_simple::Slot* _slot;
    SignalType _SignalType;
    VectorType _input;

    SimpleCollectAnyAwaitable(async_simple::Slot* slot, SignalType SignalType,
                              std::vector<LazyType, InAlloc>&& input)
        : _slot(slot), _SignalType(SignalType), _input(std::move(input)) {}

    auto coAwait(Executor* ex) {
        return CollectAnyAwaiter<LazyType, InAlloc>(_slot, _SignalType,
                                                    std::move(_input));
    }
};

template <template <typename> typename LazyType, typename... Ts>
struct SimpleCollectAnyVariadicAwaiter {
    using InputType = std::tuple<LazyType<Ts>...>;

    Slot* _slot;
    SignalType _SignalType;
    InputType _inputs;

    SimpleCollectAnyVariadicAwaiter(Slot* slot, SignalType SignalType,
                                    LazyType<Ts>&&... inputs)
        : _slot(slot), _SignalType(SignalType), _inputs(std::move(inputs)...) {}

    auto coAwait(Executor* ex) {
        return CollectAnyVariadicAwaiter<LazyType, Ts...>(_slot, _SignalType,
                                                          std::move(_inputs));
    }
};

template <class Container, typename OAlloc, bool Para = false>
struct CollectAllAwaiter {
    using ValueType = typename Container::value_type::ValueType;

    CollectAllAwaiter(Slot* slot, SignalType SignalType, Container&& input,
                      OAlloc outAlloc)
        : _slot(slot),
          _SignalType(SignalType),
          _input(std::move(input)),
          _output(outAlloc),
          _event(_input.size()) {
        _output.resize(_input.size());
    }
    CollectAllAwaiter(CollectAllAwaiter&& other) = default;

    CollectAllAwaiter(const CollectAllAwaiter&) = delete;
    CollectAllAwaiter& operator=(const CollectAllAwaiter&) = delete;

    inline bool await_ready() const noexcept { return _input.empty(); }
    inline void await_suspend(std::coroutine_handle<> continuation) {
        auto promise_type =
            std::coroutine_handle<LazyPromiseBase>::from_address(
                continuation.address())
                .promise();
        _signal = Signal::create();
        if (_slot)
            _slot->chainedSignal(_signal.get());

        auto executor = promise_type._executor;

        _event.setAwaitingCoro(continuation);
        auto size = _input.size();
        for (size_t i = 0; i < size; ++i) {
            auto& exec = _input[i]._coro.promise()._executor;
            if (exec == nullptr) {
                exec = executor;
            }
            std::unique_ptr<LazyLocalBase> local;
            local = std::make_unique<LazyLocalBase>(_signal.get());
            _input[i]._coro.promise()._lazy_local = local.get();
            auto&& func = [this, i, local = std::move(local)]() mutable {
                _input[i].start([this, i, local = std::move(local)](
                                    Try<ValueType>&& result) {
                    _output[i] = std::move(result);
                    std::size_t oldCount;
                    auto size = _input.size();
                    auto signal = _signal;
                    auto signalType = _SignalType;
                    auto awaitingCoro = _event.down(oldCount, 1);
                    if (oldCount == size) {
                        signal->emits(signalType);
                    }
                    if (awaitingCoro) {
                        awaitingCoro.resume();
                    }
                });
            };
            if (Para == true && _input.size() > 1) {
                if (exec != nullptr)
                    AS_LIKELY {
                        exec->schedule_move_only(std::move(func));
                        continue;
                    }
            }
            func();
        }
    }
    inline auto await_resume() { return std::move(_output); }

    Slot* _slot;
    SignalType _SignalType;
    Container _input;
    std::vector<Try<ValueType>, OAlloc> _output;
    detail::CountEvent _event;
    std::shared_ptr<Signal> _signal;
};  // CollectAllAwaiter

template <class Container, typename OAlloc, bool Para = false>
struct SimpleCollectAllAwaitable {
    Slot* _slot;
    SignalType _SignalType;
    Container _input;
    OAlloc _out_alloc;

    SimpleCollectAllAwaitable(Slot* slot, SignalType SignalType,
                              Container&& input, OAlloc out_alloc)
        : _slot(slot),
          _SignalType(SignalType),
          _input(std::move(input)),
          _out_alloc(out_alloc) {}

    auto coAwait(Executor* ex) {
        return CollectAllAwaiter<Container, OAlloc, Para>(
            _slot, _SignalType, std::move(_input), _out_alloc);
    }
};  // SimpleCollectAllAwaitable

}  // namespace detail

namespace detail {

template <typename T>
struct is_lazy : std::false_type {};

template <typename T>
struct is_lazy<Lazy<T>> : std::true_type {};

template <bool Para, class Container,
          typename T = typename Container::value_type::ValueType,
          typename OAlloc = std::allocator<Try<T>>>
inline auto collectAllImpl(Slot* slot, SignalType SignalType, Container input,
                           OAlloc out_alloc = OAlloc()) {
    using LazyType = typename Container::value_type;
    using AT = std::conditional_t<
        is_lazy<LazyType>::value,
        detail::SimpleCollectAllAwaitable<Container, OAlloc, Para>,
        detail::CollectAllAwaiter<Container, OAlloc, Para>>;
    return AT(slot, SignalType, std::move(input), out_alloc);
}

template <bool Para, class Container,
          typename T = typename Container::value_type::ValueType,
          typename OAlloc = std::allocator<Try<T>>>
inline auto collectAllWindowedImpl(Slot* slot, SignalType SignalType,
                                   size_t maxConcurrency,
                                   bool yield /*yield between two batchs*/,
                                   Container input, OAlloc out_alloc = OAlloc())
    -> Lazy<std::vector<Try<T>, OAlloc>> {
    using LazyType = typename Container::value_type;
    using AT = std::conditional_t<
        is_lazy<LazyType>::value,
        detail::SimpleCollectAllAwaitable<Container, OAlloc, Para>,
        detail::CollectAllAwaiter<Container, OAlloc, Para>>;
    std::vector<Try<T>, OAlloc> output(out_alloc);
    output.reserve(input.size());
    size_t input_size = input.size();
    // maxConcurrency == 0;
    // input_size <= maxConcurrency size;
    // act just like CollectAll.
    if (maxConcurrency == 0 || input_size <= maxConcurrency) {
        co_return co_await AT(slot, SignalType, std::move(input), out_alloc);
    }
    size_t start = 0;
    while (start < input_size) {
        size_t end = (std::min)(
            input_size,
            start + maxConcurrency);  // Avoid to conflict with Windows macros.
        std::vector<LazyType> tmp_group(
            std::make_move_iterator(input.begin() + start),
            std::make_move_iterator(input.begin() + end));
        start = end;
        for (auto& t :
             co_await AT(slot, SignalType, std::move(tmp_group), out_alloc)) {
            output.push_back(std::move(t));
        }
        if (yield) {
            co_await Yield{};
        }
    }
    co_return std::move(output);
}

// variadic collectAll

template <bool Para, template <typename> typename LazyType, typename... Ts>
struct CollectAllVariadicAwaiter {
    using ResultType = std::tuple<Try<Ts>...>;
    using InputType = std::tuple<LazyType<Ts>...>;

    CollectAllVariadicAwaiter(Slot* slot, SignalType SignalType,
                              LazyType<Ts>&&... inputs)
        : _slot(slot),
          _SignalType(SignalType),
          _inputs(std::move(inputs)...),
          _event(sizeof...(Ts)) {}
    CollectAllVariadicAwaiter(Slot* slot, SignalType SignalType,
                              InputType&& inputs)
        : _slot(slot),
          _SignalType(SignalType),
          _inputs(std::move(inputs)),
          _event(sizeof...(Ts)) {}

    CollectAllVariadicAwaiter(const CollectAllVariadicAwaiter&) = delete;
    CollectAllVariadicAwaiter& operator=(const CollectAllVariadicAwaiter&) =
        delete;
    CollectAllVariadicAwaiter(CollectAllVariadicAwaiter&&) = default;

    constexpr bool await_ready() const noexcept { return false; }

    template <size_t... index>
    void await_suspend_impl(std::index_sequence<index...>,
                            std::coroutine_handle<> continuation) {
        _signal = Signal::create();
        if (_slot)
            _slot->chainedSignal(_signal.get());

        auto promise_type =
            std::coroutine_handle<LazyPromiseBase>::from_address(
                continuation.address())
                .promise();

        auto executor = promise_type._executor;

        _event.setAwaitingCoro(continuation);

        // fold expression
        (
            [executor, this](auto& lazy, auto& result) {
                auto& exec = lazy._coro.promise()._executor;
                if (exec == nullptr) {
                    exec = executor;
                }
                std::unique_ptr<LazyLocalBase> local;
                local = std::make_unique<LazyLocalBase>(_signal.get());
                lazy._coro.promise()._lazy_local = local.get();
                auto func = [&, local = std::move(local)]() mutable {
                    lazy.start([&, local = std::move(local), this](auto&& res) {
                        result = std::move(res);
                        std::size_t oldCount;
                        auto signal = _signal;
                        auto signalType = _SignalType;
                        auto awaitingCoro = _event.down(oldCount, 1);
                        if (oldCount == sizeof...(Ts)) {
                            signal->emits(signalType);
                        }
                        if (awaitingCoro) {
                            awaitingCoro.resume();
                        }
                    });
                };

                if constexpr (Para == true && sizeof...(Ts) > 1) {
                    if (exec != nullptr)
                        AS_LIKELY { exec->schedule_move_only(std::move(func)); }
                    else
                        AS_UNLIKELY { func(); }
                } else {
                    func();
                }
            }(std::get<index>(_inputs), std::get<index>(_results)),
            ...);
    }

    void await_suspend(std::coroutine_handle<> continuation) {
        await_suspend_impl(std::make_index_sequence<sizeof...(Ts)>{},
                           std::move(continuation));
    }

    auto await_resume() { return std::move(_results); }

    Slot* _slot;
    SignalType _SignalType;
    InputType _inputs;
    ResultType _results;
    detail::CountEvent _event;
    std::shared_ptr<Signal> _signal;
};

template <bool Para, template <typename> typename LazyType, typename... Ts>
struct SimpleCollectAllVariadicAwaiter {
    using InputType = std::tuple<LazyType<Ts>...>;

    SimpleCollectAllVariadicAwaiter(Slot* slot, SignalType SignalType,
                                    LazyType<Ts>&&... inputs)
        : _slot(slot), _SignalType(SignalType), _input(std::move(inputs)...) {}

    auto coAwait(Executor* ex) {
        return CollectAllVariadicAwaiter<Para, LazyType, Ts...>(
            _slot, _SignalType, std::move(_input));
    }

    Slot* _slot;
    SignalType _SignalType;
    InputType _input;
};

template <bool Para, template <typename> typename LazyType, typename... Ts>
inline auto collectAllVariadicImpl(Slot* slot, SignalType SignalType,
                                   LazyType<Ts>&&... awaitables) {
    static_assert(sizeof...(Ts) > 0);
    using AT = std::conditional_t<
        is_lazy<LazyType<void>>::value,
        SimpleCollectAllVariadicAwaiter<Para, LazyType, Ts...>,
        CollectAllVariadicAwaiter<Para, LazyType, Ts...>>;
    return AT(slot, SignalType, std::move(awaitables)...);
}

// collectAny
template <typename T, template <typename> typename LazyType,
          typename IAlloc = std::allocator<LazyType<T>>>
inline auto collectAnyImpl(Slot* slot, SignalType SignalType,
                           std::vector<LazyType<T>, IAlloc> input) {
    using AT =
        std::conditional_t<std::is_same_v<LazyType<T>, Lazy<T>>,
                           detail::SimpleCollectAnyAwaitable<T, IAlloc>,
                           detail::CollectAnyAwaiter<LazyType<T>, IAlloc>>;
    return AT(slot, SignalType, std::move(input));
}

// collectAnyVariadic
template <template <typename> typename LazyType, typename... Ts>
inline auto CollectAnyVariadicImpl(Slot* slot, SignalType SignalType,
                                   LazyType<Ts>&&... inputs) {
    using AT =
        std::conditional_t<is_lazy<LazyType<void>>::value,
                           SimpleCollectAnyVariadicAwaiter<LazyType, Ts...>,
                           CollectAnyVariadicAwaiter<LazyType, Ts...>>;
    return AT(slot, SignalType, std::move(inputs)...);
}

}  // namespace detail

template <SignalType SignalType = SignalType::None, typename T,
          template <typename> typename LazyType,
          typename IAlloc = std::allocator<LazyType<T>>>
inline Lazy<detail::CollectAnyResult<typename LazyType<T>::ValueType>>
collectAny(std::vector<LazyType<T>, IAlloc>&& input) {
    auto slot = co_await CurrentSlot{};
    co_return co_await detail::collectAnyImpl(slot, SignalType,
                                              std::move(input));
}

template <SignalType SignalType = SignalType::None,
          template <typename> typename LazyType, typename... Ts>
inline Lazy<std::variant<Try<Ts>...>> collectAny(LazyType<Ts>... awaitables) {
    static_assert(sizeof...(Ts), "collectAny need at least one param!");
    auto slot = co_await CurrentSlot{};
    co_return co_await detail::CollectAnyVariadicImpl(slot, SignalType,
                                                      std::move(awaitables)...);
}

// The collectAll() function can be used to co_await on a vector of LazyType
// tasks in **one thread**,and producing a vector of Try values containing each
// of the results.
template <SignalType SignalType = SignalType::None, typename T,
          template <typename> typename LazyType,
          typename IAlloc = std::allocator<LazyType<T>>,
          typename OAlloc = std::allocator<Try<T>>>
inline Lazy<std::vector<Try<typename LazyType<T>::ValueType>, OAlloc>>
collectAll(std::vector<LazyType<T>, IAlloc>&& input,
           OAlloc out_alloc = OAlloc()) {
    auto slot = co_await CurrentSlot{};
    co_return co_await detail::collectAllImpl<false>(
        slot, SignalType, std::move(input), out_alloc);
}

// Like the collectAll() function above, The collectAllPara() function can be
// used to concurrently co_await on a vector LazyType tasks in executor,and
// producing a vector of Try values containing each of the results.
template <SignalType SignalType = SignalType::None, typename T,
          template <typename> typename LazyType,
          typename IAlloc = std::allocator<LazyType<T>>,
          typename OAlloc = std::allocator<Try<T>>>
inline Lazy<std::vector<Try<typename LazyType<T>::ValueType>, OAlloc>>
collectAllPara(std::vector<LazyType<T>, IAlloc>&& input,
               OAlloc out_alloc = OAlloc()) {
    auto slot = co_await CurrentSlot{};
    co_return co_await detail::collectAllImpl<true>(
        slot, SignalType, std::move(input), out_alloc);
}

// This collectAll function can be used to co_await on some different kinds of
// LazyType tasks in one thread,and producing a tuple of Try values containing
// each of the results.
template <SignalType SignalType = SignalType::None,
          template <typename> typename LazyType, typename... Ts>
// The temporary object's life-time which binding to reference(inputs) won't
// be extended to next time of coroutine resume. Just Copy inputs to avoid
// crash.
inline Lazy<std::tuple<Try<typename LazyType<Ts>::ValueType>...>> collectAll(
    LazyType<Ts>... inputs) {
    static_assert(sizeof...(Ts), "collectAll need at least one param!");
    auto slot = co_await CurrentSlot{};
    co_return co_await detail::collectAllVariadicImpl<false>(
        slot, SignalType, std::move(inputs)...);
}

// Like the collectAll() function above, This collectAllPara() function can be
// used to concurrently co_await on some different kinds of LazyType tasks in
// executor,and producing a tuple of Try values containing each of the results.
template <SignalType SignalType = SignalType::None,
          template <typename> typename LazyType, typename... Ts>
inline Lazy<std::tuple<Try<typename LazyType<Ts>::ValueType>...>>
collectAllPara(LazyType<Ts>... inputs) {
    static_assert(sizeof...(Ts), "collectAllPara need at least one param!");
    auto slot = co_await CurrentSlot{};
    co_return co_await detail::collectAllVariadicImpl<true>(
        slot, SignalType, std::move(inputs)...);
}

// Await each of the input LazyType tasks in the vector, allowing at most
// 'maxConcurrency' of these input tasks to be awaited in one thread. yield is
// true: yield collectAllWindowedPara from thread when a 'maxConcurrency' of
// tasks is done.
template <SignalType SignalType = SignalType::None, typename T,
          template <typename> typename LazyType,
          typename IAlloc = std::allocator<LazyType<T>>,
          typename OAlloc = std::allocator<Try<T>>>
inline Lazy<std::vector<Try<typename LazyType<T>::ValueType>, OAlloc>>
collectAllWindowed(size_t maxConcurrency,
                   bool yield /*yield between two batchs*/,
                   std::vector<LazyType<T>, IAlloc>&& input,
                   OAlloc out_alloc = OAlloc()) {
    auto slot = co_await CurrentSlot{};
    co_return co_await detail::collectAllWindowedImpl<true>(
        slot, SignalType, maxConcurrency, yield, std::move(input), out_alloc);
}

// Await each of the input LazyType tasks in the vector, allowing at most
// 'maxConcurrency' of these input tasks to be concurrently awaited at any one
// point in time.
// yield is true: yield collectAllWindowedPara from thread when a
// 'maxConcurrency' of tasks is done.
template <SignalType SignalType = SignalType::None, typename T,
          template <typename> typename LazyType,
          typename IAlloc = std::allocator<LazyType<T>>,
          typename OAlloc = std::allocator<Try<T>>>
inline Lazy<std::vector<Try<typename LazyType<T>::ValueType>, OAlloc>>
collectAllWindowedPara(size_t maxConcurrency,
                       bool yield /*yield between two batchs*/,
                       std::vector<LazyType<T>, IAlloc>&& input,
                       OAlloc out_alloc = OAlloc()) {
    auto slot = co_await CurrentSlot{};
    co_return co_await detail::collectAllWindowedImpl<false>(
        slot, SignalType, maxConcurrency, yield, std::move(input), out_alloc);
}

}  // namespace coro
}  // namespace async_simple

#endif
