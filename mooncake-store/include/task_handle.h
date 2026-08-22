#pragma once

#include <memory>

#include <glog/logging.h>
#include <async_simple/Future.h>
#include <async_simple/Promise.h>
#include <async_simple/Executor.h>
#include <async_simple/coro/CurrentExecutor.h>
#include <async_simple/coro/FutureAwaiter.h>
#include <async_simple/coro/Lazy.h>
#include <async_simple/coro/SyncAwait.h>
#include <ylt/util/tl/expected.hpp>

#include "types.h"

namespace mooncake {

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

    // Block the caller on the Future itself. Do not syncAwait(WaitAsync()):
    // that would resume TE-poll completions on the poll worker and run any
    // caller-visible continuation there. Future::get() waits via CV without
    // executing business logic on the completer thread.
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

    // When the Lazy has an executor (via CurrentExecutor), bind the Future so
    // TE-poll setValue resumes the coroutine on that executor, not on the poll
    // worker.
    async_simple::coro::Lazy<tl::expected<V, ErrorCode>> WaitAsync() override {
        async_simple::Executor* ex = co_await async_simple::coro::CurrentExecutor{};
        try {
            if (ex != nullptr) {
                co_return co_await std::move(future_).via(ex);
            }
            co_return co_await std::move(future_);
        } catch (const std::exception& e) {
            LOG(ERROR) << "FutureHandle::WaitAsync exception: " << e.what();
            co_return tl::unexpected(ErrorCode::INTERNAL_ERROR);
        } catch (...) {
            LOG(ERROR) << "FutureHandle::WaitAsync unknown exception";
            co_return tl::unexpected(ErrorCode::INTERNAL_ERROR);
        }
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

}  // namespace mooncake
