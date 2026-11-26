// Bounded-concurrency execution of independent work items on a persistent
// thread pool.
//
// Several batch paths group their work by some key -- offload reads by
// transport endpoint, offload writes by bucket -- and then walk the groups one
// at a time. Each group is independent, so the walk serializes latencies that
// could overlap. ParallelExecute runs the groups on a pool the caller owns and
// hands back one result per item, in input order, so the caller's existing
// merge loop is unchanged.
//
// This is deliberately not a "spawn a thread per round" helper: the read path
// runs per request, where thread creation would eat the latency the fan-out is
// meant to save.

#pragma once

#include <glog/logging.h>
#include <ylt/util/tl/expected.hpp>

#include <cstddef>
#include <exception>
#include <future>
#include <type_traits>
#include <utility>
#include <vector>

#include "thread_pool.h"
#include "types.h"

namespace mooncake {

/**
 * @brief Builds the result reported for a task that threw.
 *
 * Specialize this for result types other than ErrorCode and
 * tl::expected<..., ErrorCode>.
 */
template <typename R>
struct ParallelExecuteError;

template <>
struct ParallelExecuteError<ErrorCode> {
    static ErrorCode Make(ErrorCode error) { return error; }
};

template <typename V>
struct ParallelExecuteError<tl::expected<V, ErrorCode>> {
    static tl::expected<V, ErrorCode> Make(ErrorCode error) {
        return tl::make_unexpected(error);
    }
};

namespace detail {

// Never propagates: an exception escaping a pool worker would terminate the
// process, and one escaping a dispatched task would leave its promise unset
// and hang the caller on future::get().
template <typename R, typename F, typename T>
R InvokeParallelTask(F &fn, const T &item) {
    try {
        return fn(item);
    } catch (const std::exception &e) {
        LOG(ERROR) << "ParallelExecute task threw: " << e.what();
    } catch (...) {
        LOG(ERROR) << "ParallelExecute task threw an unknown exception";
    }
    return ParallelExecuteError<R>::Make(ErrorCode::INTERNAL_ERROR);
}

}  // namespace detail

/**
 * @brief Runs `fn` over every item concurrently on `pool` and joins.
 *
 * All but the last item are dispatched to the pool; the caller thread runs the
 * last one inline, so a single item never touches the pool and progress is
 * guaranteed even when every worker is busy. If `fn` throws, that item's
 * result is built by ParallelExecuteError<R> from ErrorCode::INTERNAL_ERROR
 * and the other items are unaffected. A stopped pool degrades to running every
 * item on the caller thread rather than failing.
 *
 * Thread-safety: `fn` runs concurrently on pool workers and on the caller
 * thread, so it must be safe to call concurrently. A task must never submit to
 * `pool` and wait on the result -- that deadlocks once the workers are all
 * blocked in such a wait.
 *
 * @param items Work items; each is passed to `fn` exactly once.
 * @param fn Per-item work, invoked as `fn(item)`.
 * @param pool Persistent worker pool; borrowed for the duration of the call.
 * @return One result per item, in input order: result[i] belongs to items[i].
 */
template <typename T, typename F,
          typename R = std::invoke_result_t<F &, const T &>>
std::vector<R> ParallelExecute(const std::vector<T> &items, F &&fn,
                               ThreadPool &pool) {
    std::vector<R> results;
    if (items.empty()) {
        return results;
    }

    const size_t dispatched = items.size() - 1;
    std::vector<std::promise<R>> promises(dispatched);
    std::vector<std::future<R>> futures;
    futures.reserve(dispatched);
    for (auto &promise : promises) {
        futures.push_back(promise.get_future());
    }

    for (size_t i = 0; i < dispatched; ++i) {
        // Captured by reference: the caller blocks on futures below, so items,
        // fn and promises all outlive every dispatched task.
        auto task = [&fn, &items, &promises, i]() {
            promises[i].set_value(detail::InvokeParallelTask<R>(fn, items[i]));
        };
        try {
            pool.enqueue(task);
        } catch (const std::exception &e) {
            LOG(WARNING) << "ParallelExecute running item " << i
                         << " inline, enqueue failed: " << e.what();
            task();
        }
    }

    R last = detail::InvokeParallelTask<R>(fn, items.back());

    results.reserve(items.size());
    for (auto &future : futures) {
        results.push_back(future.get());
    }
    results.push_back(std::move(last));
    return results;
}

}  // namespace mooncake
