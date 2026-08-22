#pragma once

#include <memory>

#include <glog/logging.h>
#include <async_simple/Executor.h>
#include <async_simple/Future.h>
#include <async_simple/Promise.h>
#include <async_simple/coro/FutureAwaiter.h>
#include <async_simple/coro/Lazy.h>
#include <async_simple/coro/SyncAwait.h>
#include <ylt/coro_io/coro_io.hpp>
#include <ylt/util/tl/expected.hpp>

#include "types.h"

namespace mooncake {

// Await a Future and resume off the completing thread.
// Prefer CurrentExecutor; if unbound (e.g. syncAwait without via), use
// caller-provided fallback, then coro_io's global executor.
template <typename V>
inline async_simple::coro::Lazy<tl::expected<V, ErrorCode>> AwaitExpectedFuture(
    async_simple::Future<tl::expected<V, ErrorCode>> fut,
    async_simple::Executor* fallback_ex = nullptr) {
    async_simple::Executor* ex = co_await async_simple::CurrentExecutor{};
    if (ex == nullptr) {
        ex = fallback_ex != nullptr ? fallback_ex
                                    : coro_io::get_global_executor();
        LOG(WARNING) << "AwaitExpectedFuture: no CurrentExecutor; resuming via "
                     << (fallback_ex != nullptr
                             ? "caller fallback executor"
                             : "coro_io::get_global_executor()");
    }
    try {
        co_return co_await std::move(fut).via(ex);
    } catch (const std::exception& e) {
        LOG(ERROR) << "AwaitExpectedFuture exception: " << e.what();
        co_return tl::unexpected(ErrorCode::INTERNAL_ERROR);
    } catch (...) {
        LOG(ERROR) << "AwaitExpectedFuture unknown exception";
        co_return tl::unexpected(ErrorCode::INTERNAL_ERROR);
    }
}

// ============================================================================
// TaskHandle<V> — abstract base for pending operations.
// Wait() returns tl::expected<V, ErrorCode>, where ErrorCode is the error type
// and V is the value type on success.
// WaitAsync() is the coroutine-friendly entry; default falls back to Wait()
// (sync block on the coroutine's current thread). Subclasses backed by an
// asynchronously-completed future (see FutureHandle) override it for true
// suspension.
// ============================================================================

template <typename V>
class TaskHandle {
   public:
    virtual ~TaskHandle() = default;
    virtual tl::expected<V, ErrorCode> Wait() = 0;
    virtual async_simple::coro::Lazy<tl::expected<V, ErrorCode>> WaitAsync() {
        co_return Wait();
    }
};

template <typename V>
class ImmediateHandle : public TaskHandle<V> {
   public:
    tl::expected<V, ErrorCode> Wait() override { return {}; }

    static std::unique_ptr<ImmediateHandle<V>> Create() {
        return std::make_unique<ImmediateHandle<V>>();
    }
};

template <typename V>
class CallableTaskHandle : public TaskHandle<V> {
   public:
    template <typename F>
    explicit CallableTaskHandle(F&& fn)
        : impl_(
              std::make_unique<Wrapper<std::decay_t<F>>>(std::forward<F>(fn))) {
    }

    tl::expected<V, ErrorCode> Wait() override { return impl_->invoke(); }

    template <typename F>
    static std::unique_ptr<CallableTaskHandle<V>> Create(F&& fn) {
        return std::make_unique<CallableTaskHandle<V>>(std::forward<F>(fn));
    }

   private:
    struct Impl {
        virtual ~Impl() = default;
        virtual tl::expected<V, ErrorCode> invoke() = 0;
    };

    template <typename F>
    struct Wrapper final : Impl {
        explicit Wrapper(F&& f) : fn(std::move(f)) {}
        tl::expected<V, ErrorCode> invoke() override { return fn(); }

        F fn;
    };

    std::unique_ptr<Impl> impl_;
};

template <typename V>
class FutureHandle : public TaskHandle<V> {
   public:
    FutureHandle(std::shared_ptr<void> request_storage,
                 async_simple::Future<tl::expected<V, ErrorCode>> future)
        : request_storage_(std::move(request_storage)),
          future_(std::move(future)) {}

    // Block on the Future via CV. Do not syncAwait(WaitAsync()): that can
    // resume on the completing thread and run caller continuation there.
    tl::expected<V, ErrorCode> Wait() override {
        try {
            return std::move(future_).get();
        } catch (const std::exception& e) {
            LOG(ERROR) << "FutureHandle::Wait exception: " << e.what();
            return tl::unexpected(ErrorCode::INTERNAL_ERROR);
        } catch (...) {
            LOG(ERROR) << "FutureHandle::Wait unknown exception";
            return tl::unexpected(ErrorCode::INTERNAL_ERROR);
        }
    }

    async_simple::coro::Lazy<tl::expected<V, ErrorCode>> WaitAsync() override {
        co_return co_await AwaitExpectedFuture(std::move(future_));
    }

    template <typename T>
    static std::unique_ptr<FutureHandle<V>> Create(
        std::shared_ptr<T> request_storage,
        async_simple::Future<tl::expected<V, ErrorCode>> future) {
        return std::make_unique<FutureHandle<V>>(std::move(request_storage),
                                                 std::move(future));
    }

   private:
    std::shared_ptr<void> request_storage_;
    async_simple::Future<tl::expected<V, ErrorCode>> future_;
};

// Future plus a continuation after it completes. Wait() uses Future::get();
// WaitAsync() resumes off the completing thread via AwaitExpectedFuture.
template <typename V, typename Then>
class FutureThenHandle final : public TaskHandle<V> {
   public:
    FutureThenHandle(async_simple::Future<tl::expected<V, ErrorCode>> future,
                     Then then)
        : future_(std::move(future)), then_(std::move(then)) {}

    tl::expected<V, ErrorCode> Wait() override { return then_(GetFuture()); }

    async_simple::coro::Lazy<tl::expected<V, ErrorCode>> WaitAsync() override {
        co_return then_(co_await AwaitExpectedFuture(std::move(future_)));
    }

   private:
    tl::expected<V, ErrorCode> GetFuture() {
        try {
            return std::move(future_).get();
        } catch (const std::exception& e) {
            LOG(ERROR) << "FutureThenHandle::Wait exception: " << e.what();
            return tl::unexpected(ErrorCode::INTERNAL_ERROR);
        } catch (...) {
            LOG(ERROR) << "FutureThenHandle::Wait unknown exception";
            return tl::unexpected(ErrorCode::INTERNAL_ERROR);
        }
    }

    async_simple::Future<tl::expected<V, ErrorCode>> future_;
    Then then_;
};

template <typename V, typename Then>
std::unique_ptr<FutureThenHandle<V, std::decay_t<Then>>> MakeFutureThenHandle(
    async_simple::Future<tl::expected<V, ErrorCode>> future, Then&& then) {
    return std::make_unique<FutureThenHandle<V, std::decay_t<Then>>>(
        std::move(future), std::forward<Then>(then));
}

}  // namespace mooncake
