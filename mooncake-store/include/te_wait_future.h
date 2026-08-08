#pragma once

#include <glog/logging.h>
#include <async_simple/Executor.h>
#include <async_simple/Future.h>
#include <async_simple/coro/CurrentExecutor.h>
#include <async_simple/coro/FutureAwaiter.h>
#include <async_simple/coro/Lazy.h>
#include <ylt/coro_io/coro_io.hpp>
#include <ylt/util/tl/expected.hpp>

#include "types.h"

namespace mooncake {

// Await a TE-poll Future and always resume off the poll worker.
// Prefer the Lazy's CurrentExecutor; if none is bound (e.g. syncAwait without
// via), fall back to coro_io's global executor so copy/commit/RPC never continue
// on te_poll threads.
template <typename V>
inline async_simple::coro::Lazy<tl::expected<V, ErrorCode>>
AwaitTeExpectedFuture(
    async_simple::Future<tl::expected<V, ErrorCode>> fut) {
    async_simple::Executor* ex = co_await async_simple::coro::CurrentExecutor{};
    if (ex == nullptr) {
        ex = coro_io::get_global_executor();
        LOG(WARNING) << "AwaitTeExpectedFuture: no CurrentExecutor; "
                        "resuming via coro_io::get_global_executor()";
    }
    try {
        co_return co_await std::move(fut).via(ex);
    } catch (const std::exception& e) {
        LOG(ERROR) << "TE wait future exception: " << e.what();
        co_return tl::unexpected(ErrorCode::INTERNAL_ERROR);
    } catch (...) {
        LOG(ERROR) << "TE wait future unknown exception";
        co_return tl::unexpected(ErrorCode::INTERNAL_ERROR);
    }
}

}  // namespace mooncake
