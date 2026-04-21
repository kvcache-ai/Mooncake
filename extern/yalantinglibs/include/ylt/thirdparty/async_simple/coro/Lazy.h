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
#ifndef ASYNC_SIMPLE_CORO_LAZY_H
#define ASYNC_SIMPLE_CORO_LAZY_H


#ifndef ASYNC_SIMPLE_USE_MODULES

#include <cstddef>
#include <system_error>
#include <cstdio>
#include <exception>
#include <memory>
#include <type_traits>
#include <utility>
#include <variant>
#include "async_simple/Common.h"
#include "async_simple/Executor.h"
#include "async_simple/Signal.h"
#include "async_simple/Try.h"
#include "async_simple/coro/DetachedCoroutine.h"
#include "async_simple/coro/LazyLocalBase.h"
#include "async_simple/coro/PromiseAllocator.h"
#include "async_simple/coro/ViaCoroutine.h"
#include "async_simple/experimental/coroutine.h"

#endif  // ASYNC_SIMPLE_USE_MODULES

namespace async_simple {

class Executor;

namespace coro {

template <typename T>
class Lazy;

// In the middle of the execution of one coroutine, if we want to give out the
// rights to execute back to the executor, to make it schedule other tasks to
// execute, we could write:
//
// ```C++
//  co_await Yield{};
// ```
//
// This would suspend the executing coroutine.
struct Yield {};

template <typename T = LazyLocalBase>
struct CurrentLazyLocals {};

// co_await CurrentSlot{} could return the point to current
// Slot. Return nullptr if lazy don't binding to signal.
struct CurrentSlot {};

// co_await ForbidSignal{} could forbid Signal in lazy. After
// calling this, call `co_await CurrentSlot{}` will return nullptr.
struct ForbidSignal {};

namespace detail {
template <class, typename OAlloc, bool Para>
struct CollectAllAwaiter;

template <bool Para, template <typename> typename LazyType, typename... Ts>
struct CollectAllVariadicAwaiter;

template <typename LazyType, typename IAlloc>
struct CollectAnyAwaiter;

template <template <typename> typename LazyType, typename... Ts>
struct CollectAnyVariadicAwaiter;

template <typename... Ts>
struct CollectAnyVariadicPairAwaiter;

}  // namespace detail

namespace detail {

class LazyPromiseBase : public PromiseAllocator<void, true> {
public:
    // Resume the caller waiting to the current coroutine. Note that we need
    // destroy the frame for the current coroutine explicitly. Since after
    // FinalAwaiter, The current coroutine should be suspended and never to
    // resume. So that we couldn't expect it to release it self any more.
    struct FinalAwaiter {
        bool await_ready() const noexcept { return false; }
        template <typename PromiseType>
        auto await_suspend(std::coroutine_handle<PromiseType> h) noexcept {
            static_assert(
                std::is_base_of<LazyPromiseBase, PromiseType>::value,
                "the final awaiter is only allowed to be called by Lazy");

            return h.promise()._continuation;
        }
        void await_resume() noexcept {}
    };

    struct YieldAwaiter {
        YieldAwaiter(Executor* executor, Slot* slot)
            : _executor(executor), _slot(slot) {}
        bool await_ready() const noexcept {
            return signalHelper{Terminate}.hasCanceled(_slot);
        }
        template <typename PromiseType>
        void await_suspend(std::coroutine_handle<PromiseType> handle) {
            static_assert(
                std::is_base_of<LazyPromiseBase, PromiseType>::value,
                "'co_await Yield' is only allowed to be called by Lazy");

            logicAssert(_executor,
                        "Yielding is only meaningful with an executor!");
            // schedule_info is YIELD here, which avoid executor always
            // run handle immediately when other works are waiting, which may
            // cause deadlock.
            _executor->schedule(
                std::move(handle),
                static_cast<uint64_t>(Executor::Priority::YIELD));
        }
        void await_resume() const {
            signalHelper{Terminate}.checkHasCanceled(
                _slot, "async_simple::Yield/SpinLock is canceled!");
        }

    private:
        Executor* _executor;
        Slot* _slot;
    };

public:
    LazyPromiseBase() noexcept : _executor(nullptr), _lazy_local(nullptr) {}
    // Lazily started, coroutine will not execute until first resume() is called
    std::suspend_always initial_suspend() noexcept { return {}; }
    FinalAwaiter final_suspend() noexcept { return {}; }

    template <typename Awaitable>
    auto await_transform(Awaitable&& awaitable) {
        // See CoAwait.h for details.
        return detail::coAwait(_executor, std::forward<Awaitable>(awaitable));
    }

    auto await_transform(CurrentExecutor) {
        return ReadyAwaiter<Executor*>(_executor);
    }

    template <typename T>
    auto await_transform(CurrentLazyLocals<T>) {
        return ReadyAwaiter<T*>(_lazy_local ? dynamicCast<T>(_lazy_local)
                                            : nullptr);
    }

    auto await_transform(CurrentSlot) {
        return ReadyAwaiter<Slot*>(_lazy_local ? _lazy_local->getSlot()
                                               : nullptr);
    }

    auto await_transform(ForbidSignal) {
        if (_lazy_local) {
            _lazy_local->forbidSignal();
        }
        return ReadyAwaiter<void>();
    }

    auto await_transform(Yield) {
        return YieldAwaiter(_executor,
                            _lazy_local ? _lazy_local->getSlot() : nullptr);
    }

    /// IMPORTANT: _continuation should be the first member due to the
    /// requirement of dbg script.
    std::coroutine_handle<> _continuation;
    Executor* _executor;
    LazyLocalBase* _lazy_local;
};

template <typename T>
class LazyPromise : public LazyPromiseBase {
public:
#if defined(__clang__)
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wgnu-alignof-expression"
    static_assert(alignof(T) <= alignof(std::max_align_t),
                  "async_simple doesn't allow Lazy with over aligned object");
#endif

    LazyPromise() noexcept {}
    ~LazyPromise() noexcept {}

    Lazy<T> get_return_object() noexcept;

    static Lazy<T> get_return_object_on_allocation_failure() noexcept;

    template <typename V>
    void return_value(V&& value) noexcept(
        std::is_nothrow_constructible_v<T, V&&>) requires
        std::is_convertible_v<V&&, T> {
        _value.template emplace<T>(std::forward<V>(value));
    }
    void unhandled_exception() noexcept {
        _value.template emplace<std::exception_ptr>(std::current_exception());
    }

public:
    T& result() & {
        if (std::holds_alternative<std::exception_ptr>(_value))
            AS_UNLIKELY {
                std::rethrow_exception(std::get<std::exception_ptr>(_value));
            }
        assert(std::holds_alternative<T>(_value));
        return std::get<T>(_value);
    }
    T&& result() && {
        if (std::holds_alternative<std::exception_ptr>(_value))
            AS_UNLIKELY {
                std::rethrow_exception(std::get<std::exception_ptr>(_value));
            }
        assert(std::holds_alternative<T>(_value));
        return std::move(std::get<T>(_value));
    }

    Try<T> tryResult() noexcept {
        if (std::holds_alternative<std::exception_ptr>(_value))
            AS_UNLIKELY { return Try<T>(std::get<std::exception_ptr>(_value)); }
        else {
            assert(std::holds_alternative<T>(_value));
            return Try<T>(std::move(std::get<T>(_value)));
        }
    }

    std::variant<std::monostate, T, std::exception_ptr> _value;
};

template <>
class LazyPromise<void> : public LazyPromiseBase {
public:
    LazyPromise() noexcept {}
    ~LazyPromise() noexcept {}

    Lazy<void> get_return_object() noexcept;
    static Lazy<void> get_return_object_on_allocation_failure() noexcept;

    void return_void() noexcept {}
    void unhandled_exception() noexcept {
        _exception = std::current_exception();
    }

    void result() {
        if (_exception != nullptr)
            AS_UNLIKELY { std::rethrow_exception(_exception); }
    }
    Try<void> tryResult() noexcept { return Try<void>(_exception); }

public:
    std::exception_ptr _exception{nullptr};
};

}  // namespace detail

template <typename T>
class RescheduleLazy;

namespace detail {

template <typename T>
struct LazyAwaiterBase {
    using Handle = CoroHandle<detail::LazyPromise<T>>;
    Handle _handle;

    LazyAwaiterBase(LazyAwaiterBase& other) = delete;
    LazyAwaiterBase& operator=(LazyAwaiterBase& other) = delete;

    LazyAwaiterBase(LazyAwaiterBase&& other)
        : _handle(std::exchange(other._handle, nullptr)) {}

    LazyAwaiterBase& operator=(LazyAwaiterBase&& other) {
        std::swap(_handle, other._handle);
        return *this;
    }

    LazyAwaiterBase(Handle coro) : _handle(coro) {}
    ~LazyAwaiterBase() {
        if (_handle) {
            _handle.destroy();
            _handle = nullptr;
        }
    }

    bool await_ready() const noexcept { return false; }

    auto awaitResume() {
        if constexpr (std::is_void_v<T>) {
            _handle.promise().result();
            // We need to destroy the handle expclictly since the awaited
            // coroutine after symmetric transfer couldn't release it self any
            // more.
            _handle.destroy();
            _handle = nullptr;
        } else {
            auto r = std::move(_handle.promise()).result();
            _handle.destroy();
            _handle = nullptr;
            return r;
        }
    }

    Try<T> awaitResumeTry() noexcept {
        Try<T> ret = _handle.promise().tryResult();
        _handle.destroy();
        _handle = nullptr;
        return ret;
    }
};

template <typename T, typename CB>
concept isLazyCallback = std::is_invocable_v<CB, Try<T>>;

template <typename T, bool reschedule>
class LazyBase {
public:
    using promise_type = detail::LazyPromise<T>;
    using Handle = CoroHandle<promise_type>;
    using ValueType = T;

    struct AwaiterBase : public detail::LazyAwaiterBase<T> {
        using Base = detail::LazyAwaiterBase<T>;
        AwaiterBase(Handle coro) : Base(coro) {}

        template <typename PromiseType>
        AS_INLINE auto await_suspend(std::coroutine_handle<PromiseType>
                                         continuation) noexcept(!reschedule) {
            static_assert(
                std::is_base_of<LazyPromiseBase, PromiseType>::value ||
                    std::is_same_v<detail::DetachedCoroutine::promise_type,
                                   PromiseType>,
                "'co_await Lazy' is only allowed to be called by Lazy or "
                "DetachedCoroutine");
            // current coro started, caller becomes my continuation
            this->_handle.promise()._continuation = continuation;
            if constexpr (std::is_base_of<LazyPromiseBase,
                                          PromiseType>::value) {
                LazyLocalBase*& local = this->_handle.promise()._lazy_local;
                if (local == nullptr)
                    local = continuation.promise()._lazy_local;
            }
            return awaitSuspendImpl();
        }

    private:
        auto awaitSuspendImpl() noexcept(!reschedule) {
            if constexpr (reschedule) {
                // executor schedule performed
                auto& pr = this->_handle.promise();
                logicAssert(pr._executor, "RescheduleLazy need executor");
                pr._executor->schedule(this->_handle);
            } else {
                return this->_handle;
            }
        }
    };

    struct TryAwaiter : public AwaiterBase {
        TryAwaiter(Handle coro) : AwaiterBase(coro) {}
        AS_INLINE Try<T> await_resume() noexcept {
            return AwaiterBase::awaitResumeTry();
        };

        auto coAwait(Executor* ex) {
            if constexpr (reschedule) {
                logicAssert(false,
                            "RescheduleLazy should be only allowed in "
                            "DetachedCoroutine");
            }
            // derived lazy inherits executor
            this->_handle.promise()._executor = ex;
            return std::move(*this);
        }
    };

    struct ValueAwaiter : public AwaiterBase {
        ValueAwaiter(Handle coro) : AwaiterBase(coro) {}
        AS_INLINE T await_resume() { return AwaiterBase::awaitResume(); }
    };

    ~LazyBase() {
        if (_coro) {
            _coro.destroy();
            _coro = nullptr;
        }
    };
    explicit LazyBase(Handle coro) noexcept : _coro(coro) {}
    LazyBase(LazyBase&& other) noexcept : _coro(std::move(other._coro)) {
        other._coro = nullptr;
    }

    LazyBase(const LazyBase&) = delete;
    LazyBase& operator=(const LazyBase&) = delete;

    Executor* getExecutor() { return _coro.promise()._executor; }

    template <typename F>
    void start(F&& callback) requires(isLazyCallback<T, F>) {
        logicAssert(this->_coro.operator bool(),
                    "Lazy do not have a coroutine_handle "
                    "Maybe the allocation failed or you're using a used Lazy");

        // callback should take a single Try<T> as parameter, return value will
        // be ignored. a detached coroutine will not suspend at initial/final
        // suspend point.
        auto launchCoro = [](LazyBase lazy,
                             std::decay_t<F> cb) -> detail::DetachedCoroutine {
            cb(co_await lazy.coAwaitTry());
        };
        [[maybe_unused]] auto detached =
            launchCoro(std::move(*this), std::forward<F>(callback));
    }

    bool isReady() const { return !_coro || _coro.done(); }

    auto operator co_await() {
        return ValueAwaiter(std::exchange(_coro, nullptr));
    }

    auto coAwaitTry() { return TryAwaiter(std::exchange(_coro, nullptr)); }

protected:
    Handle _coro;

    template <class, typename OAlloc, bool Para>
    friend struct detail::CollectAllAwaiter;

    template <bool, template <typename> typename, typename...>
    friend struct detail::CollectAllVariadicAwaiter;

    template <typename LazyType, typename IAlloc>
    friend struct detail::CollectAnyAwaiter;

    template <template <typename> typename LazyType, typename... Ts>
    friend struct detail::CollectAnyVariadicAwaiter;

    template <typename... Ts>
    friend struct detail::CollectAnyVariadicPairAwaiter;
};

}  // namespace detail

// Lazy is a coroutine task which would be executed lazily.
// The user who wants to use Lazy should declare a function whose return type
// is Lazy<T>. T is the type you want the function to return originally.
// And if the function doesn't want to return any thing, use Lazy<>.
//
// Then in the function, use co_return instead of return. And use co_await to
// wait things you want to wait. For example:
//
// ```C++
//  // Return 43 after 10s.
//  Lazy<int> foo() {
//     co_await sleep(10s);
//     co_return 43;
// }
// ```
//
// To get the value wrapped in Lazy, we could co_await it like:
//
// ```C++
//  Lazy<int> bar() {
//      // This would return the value foo returned.
//      co_return co_await foo();
// }
// ```
//
// If we don't want the caller to be a coroutine too, we could use Lazy::start
// to get the value asynchronously.
//
// ```C++
// void foo_use() {
//     foo().start([](Try<int> &&value){
//         std::cout << "foo: " << value.value() << "\n";
//     });
// }
// ```
//
// When the foo gets its value, the value would be passed to the lambda in
// Lazy::start().
//
// If the user wants to get the value synchronously, he could use
// async_simple::coro::syncAwait.
//
// ```C++
// void foo_use2() {
//     auto val = async_simple::coro::syncAwait(foo());
//     std::cout << "foo: " << val << "\n";
// }
// ```
//
// There is no executor instance in a Lazy. To specify an executor to schedule
// the execution of the Lazy and corresponding Lazy tasks inside, user could use
// `Lazy::via` to assign an executor for this Lazy. `Lazy::via` would return a
// RescheduleLazy. User should use the returned RescheduleLazy directly. The
// Lazy which called `via()` shouldn't be used any more.
//
// If Lazy is co_awaited directly, sysmmetric transfer would happend. That is,
// the stack frame for current caller would be released and the lazy task would
// be resumed directly. So the user needn't to worry about the stack overflow.
//
// The co_awaited Lazy shouldn't be accessed any more.
//
// When a Lazy is co_awaited, if there is any exception happened during the
// process, the co_awaited expression would throw the exception happened. If the
// user does't want the co_await expression to throw an exception, he could use
// `Lazy::coAwaitTry`. For example:
//
//  ```C++
//      Try<int> res = co_await foo().coAwaitTry();
//      if (res.hasError())
//          std::cout << "Error happend.\n";
//      else
//          std::cout << "We could get the value: " << res.value() << "\n";
// ```
//
// If any awaitable wants to derive the executor instance from its caller, it
// should implement `coAwait(Executor*)` member method. Then the caller would
// pass its executor instance to the awaitable.

template <typename T>
concept isDerivedFromLazyLocal = std::is_base_of_v<LazyLocalBase, T> &&
    (std::is_same_v<LazyLocalBase, T> || requires(const T* base) {
        std::is_same_v<decltype(T::classof(base)), bool>;
    });

namespace detail {
inline void moveSlotFromContinuation(LazyLocalBase* nowLocal,
                                     LazyLocalBase* preLocal) {
    if (preLocal) {
        // We only allow continuation has a local with LazyLocalBase type, which
        // is designed for calling collectAll/collectAny with lazy has local
        // variable.
        if (preLocal->getTypeTag() == nullptr && preLocal->getSlot() &&
            nowLocal->getSlot() == nullptr) {
            nowLocal->_slot = std::move(preLocal->_slot);
        } else {
            logicAssert(false, "we dont allowed set lazy local twice");
        }
    }
}
}  // namespace detail

template <typename T = void>
class [[nodiscard]] CORO_ONLY_DESTROY_WHEN_DONE ELIDEABLE_AFTER_AWAIT Lazy
    : public detail::LazyBase<T, /*reschedule=*/false> {
    using Base = detail::LazyBase<T, false>;
    template <isDerivedFromLazyLocal LazyLocal>
    static Lazy<T> setLazyLocalImpl(Lazy<T> self, LazyLocal local) {
        detail::moveSlotFromContinuation(&local, co_await CurrentLazyLocals{});
        self._coro.promise()._lazy_local = &local;
        co_return co_await std::move(self);
    }
    template <isDerivedFromLazyLocal LazyLocal>
    static Lazy<T> setLazyLocalImpl(Lazy<T> self,
                                    std::unique_ptr<LazyLocal> local) {
        detail::moveSlotFromContinuation(local.get(),
                                         co_await CurrentLazyLocals{});
        self._coro.promise()._lazy_local = local.get();
        co_return co_await std::move(self);
    }

    template <isDerivedFromLazyLocal LazyLocal>
    static Lazy<T> setLazyLocalImpl(Lazy<T> self,
                                    std::shared_ptr<LazyLocal> local) {
        detail::moveSlotFromContinuation(local.get(),
                                         co_await CurrentLazyLocals{});
        self._coro.promise()._lazy_local = local.get();
        co_return co_await std::move(self);
    }

public:
    using Base::Base;

    // Bind an executor to a Lazy, and convert it to RescheduleLazy.
    // You can only call via on rvalue, i.e. a Lazy is not accessible after
    // via() called.
    RescheduleLazy<T> via(Executor* ex) && {
        logicAssert(this->_coro.operator bool(),
                    "Lazy do not have a coroutine_handle "
                    "Maybe the allocation failed or you're using a used Lazy");
        this->_coro.promise()._executor = ex;
        return RescheduleLazy<T>(std::exchange(this->_coro, nullptr));
    }

    // Bind an executor only. Don't re-schedule.
    //
    // This function is deprecated, please use start(cb,ex) instead of setEx.
    [[deprecated]] Lazy<T> setEx(Executor* ex) && {
        logicAssert(this->_coro.operator bool(),
                    "Lazy do not have a coroutine_handle "
                    "Maybe the allocation failed or you're using a used Lazy");
        this->_coro.promise()._executor = ex;
        return Lazy<T>(std::exchange(this->_coro, nullptr));
    }

    template <isDerivedFromLazyLocal LazyLocal = LazyLocalBase>
    Lazy<T> setLazyLocal(std::unique_ptr<LazyLocal> base) && {
        logicAssert(this->_coro.operator bool(),
                    "Lazy do not have a coroutine_handle "
                    "Maybe the allocation failed or you're using a used Lazy");
        return setLazyLocalImpl(std::move(*this), std::move(base));
    }

    template <isDerivedFromLazyLocal LazyLocal = LazyLocalBase>
    Lazy<T> setLazyLocal(std::shared_ptr<LazyLocal> base) && {
        logicAssert(this->_coro.operator bool(),
                    "Lazy do not have a coroutine_handle "
                    "Maybe the allocation failed or you're using a used Lazy");
        return setLazyLocalImpl(std::move(*this), std::move(base));
    }

    template <isDerivedFromLazyLocal LazyLocal = LazyLocalBase,
              typename... Args>
    Lazy<T> setLazyLocal(Args&&... args) && {
        logicAssert(this->_coro.operator bool(),
                    "Lazy do not have a coroutine_handle "
                    "Maybe the allocation failed or you're using a used Lazy");
        if constexpr (std::is_move_constructible_v<LazyLocal>) {
            return setLazyLocalImpl<LazyLocal>(
                std::move(*this), LazyLocal{std::forward<Args>(args)...});
        } else {
            return setLazyLocalImpl<LazyLocal>(
                std::move(*this),
                std::make_unique<LazyLocal>(std::forward<Args>(args)...));
        }
    }

    using Base::start;

    // Bind an executor and start coroutine without scheduling immediately.
    template <typename F>
    void directlyStart(F&& callback, Executor* executor) requires(
        detail::isLazyCallback<T, F>) {
        this->_coro.promise()._executor = executor;
        return start(std::forward<F>(callback));
    }

    auto coAwait(Executor* ex) {
        logicAssert(this->_coro.operator bool(),
                    "Lazy do not have a coroutine_handle "
                    "Maybe the allocation failed or you're using a used Lazy");

        // derived lazy inherits executor
        this->_coro.promise()._executor = ex;
        return typename Base::ValueAwaiter(std::exchange(this->_coro, nullptr));
    }

private:
    friend class RescheduleLazy<T>;
};

// dispatch a lazy to executor, dont reschedule immediately

// A RescheduleLazy is a Lazy with an executor. The executor of a RescheduleLazy
// wouldn't/shouldn't be nullptr. So we needn't check it.
//
// The user couldn't/shouldn't declare a coroutine function whose return type is
// RescheduleLazy. The user should get a RescheduleLazy by a call to
// `Lazy::via(Executor)` only.
//
// Different from Lazy, when a RescheduleLazy is co_awaited/started/syncAwaited,
// the RescheduleLazy wouldn't be executed immediately. The RescheduleLazy would
// submit a task to resume the corresponding Lazy task to the executor. Then the
// executor would execute the Lazy task later.
template <typename T = void>
class [[nodiscard]] RescheduleLazy
    : public detail::LazyBase<T, /*reschedule=*/true> {
    using Base = detail::LazyBase<T, true>;

public:
    void detach() {
        this->start([](auto&& t) {
            if (t.hasError()) {
                std::rethrow_exception(t.getException());
            }
        });
    }

    [[deprecated(
        "RescheduleLazy should be only allowed in DetachedCoroutine")]] auto
    operator co_await() {
        return Base::operator co_await();
    }

private:
    using Base::Base;
};

template <typename T>
inline Lazy<T> detail::LazyPromise<T>::get_return_object() noexcept {
    return Lazy<T>(Lazy<T>::Handle::from_promise(*this));
}

inline Lazy<void> detail::LazyPromise<void>::get_return_object() noexcept {
    return Lazy<void>(Lazy<void>::Handle::from_promise(*this));
}

/// Why do we want to introduce `get_return_object_on_allocation_failure()`?
/// Since a coroutine will be roughly converted to:
///
/// ```C++
/// void *frame_addr = ::operator new(required size);
/// __promise_ = new (frame_addr) __promise_type(...);
/// __return_object_ = __promise_.get_return_object();
/// co_await __promise_.initial_suspend();
/// try {
///     function-body
/// } catch (...) {
///     __promise_.unhandled_exception();
/// }
/// co_await __promise_.final_suspend();
/// ```
///
/// Then we can find that the coroutine should be nounwind (noexcept) naturally
/// if the constructor of the promise_type, the get_return_object() function,
/// the initial_suspend, the unhandled_exception(), the final_suspend and the
/// allocation function is noexcept.
///
/// For the specific coroutine type, Lazy, all the above except the allocation
/// function is noexcept. So that we can make every Lazy function noexcept
/// naturally if we make the allocation function nothrow. This is the reason why
/// we want to introduce `get_return_object_on_allocation_failure()` to Lazy.
///
/// Note that the optimization may not work in some platforms due the ABI
/// limitations. Since they need to consider the case that the destructor of an
/// exception can throw exceptions.
template <typename T>
inline Lazy<T>
detail::LazyPromise<T>::get_return_object_on_allocation_failure() noexcept {
    return Lazy<T>(typename Lazy<T>::Handle(nullptr));
}

inline Lazy<void>
detail::LazyPromise<void>::get_return_object_on_allocation_failure() noexcept {
    return Lazy<void>(Lazy<void>::Handle(nullptr));
}
}  // namespace coro
}  // namespace async_simple

#endif
