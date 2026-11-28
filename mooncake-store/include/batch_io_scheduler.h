#pragma once

#include <chrono>
#include <cstddef>
#include <functional>
#include <memory>
#include <string_view>

#include <ylt/util/tl/expected.hpp>

#include "types.h"

namespace mooncake {

// Runs caller-defined tasks on a fixed set of resident worker threads.
// Callers accumulate tasks in a batch with PrepareTasks, publish and wait
// for the whole batch with SubmitAndWait, and keep all data referenced by
// their callbacks valid until SubmitAndWait returns.
class BatchIoScheduler {
   public:
    // Executes one caller-defined task index. A task belongs to the batch
    // passed to the PrepareTasks call that declared it; task indices are
    // local to that call. An accepted task is executed at most once; a
    // cancelled task is not executed. Tasks may run concurrently and out of
    // order, including tasks from the same PendingBatch. Exceptions are
    // converted to INTERNAL_ERROR.
    using Execute =
        std::function<tl::expected<void, ErrorCode>(size_t task_index)>;

    // Finalizes one nonempty PrepareTasks call after all its tasks
    // terminate; the argument is their first task error, or success. Runs
    // once for each accepted, published group, including a group cancelled
    // after publication. Its errors and exceptions join the batch result
    // (exceptions become INTERNAL_ERROR).
    using Complete = std::function<tl::expected<void, ErrorCode>(
        tl::expected<void, ErrorCode>)>;

    // Decides whether a task may start. Returning false defers the task for
    // a retry after the configured backoff. Exceptions fail the task with
    // INTERNAL_ERROR. A task whose check has started is not interrupted.
    using Ready = std::function<bool()>;

    // Starts the worker threads and returns the scheduler.
    //
    // - `workers` (in): number of resident worker threads; must be nonzero.
    // - `ready` (in, optional): consulted before a task starts; returning
    //   false defers the task for a retry after `backoff`. Empty means tasks
    //   start immediately.
    // - `backoff` (in): how long a deferred task waits before its next
    //   readiness check; must be positive.
    // - `worker_name_prefix` (in): worker threads are named with this prefix
    //   followed by their zero-based index, truncated to platform limits.
    //
    // Returns INVALID_PARAMS for zero workers or a non-positive backoff, and
    // INTERNAL_ERROR if allocation or thread startup fails.
    static tl::expected<std::unique_ptr<BatchIoScheduler>, ErrorCode> Create(
        size_t workers, Ready ready = {},
        std::chrono::milliseconds backoff = std::chrono::milliseconds(300),
        std::string_view worker_name_prefix = "mc-io-");

    // Stops the scheduler and joins its workers. The scheduler must outlive all
    // SubmitAndWait calls using it.
    ~BatchIoScheduler();
    BatchIoScheduler(const BatchIoScheduler&) = delete;
    BatchIoScheduler& operator=(const BatchIoScheduler&) = delete;

    // A batch is a batch of tasks. Owns tasks that have not yet been
    // published. A default-constructed or consumed batch is empty and may be
    // reused. Do not access one PendingBatch concurrently.
    class PendingBatch {
       public:
        PendingBatch();
        // Discards unpublished tasks without running Execute or Complete.
        ~PendingBatch();
        PendingBatch(const PendingBatch&) = delete;
        PendingBatch& operator=(const PendingBatch&) = delete;

       private:
        friend class BatchIoScheduler;
        struct State;
        // Null until the first PrepareTasks call; moved out by SubmitAndWait.
        std::unique_ptr<State> state_;
    };

    // Prepares `task_count` tasks and appends them to the batch; the tasks
    // run when the batch is submitted with SubmitAndWait.
    //
    // - `batch` (in-out): receives the tasks. After a preparation error it
    //   reports that error until SubmitAndWait consumes it.
    // - `task_count` (in): number of tasks to prepare; task `i` receives
    //   index `i`, local to this call. Zero appends nothing.
    // - `affinity_key` (in): a stable identifier of the work source, such as
    //   the bucket file's inode. Equal keys always map to the same worker;
    //   nothing else about the mapping is guaranteed or required.
    // - `execute` (in): runs one task, called with the task's index, at most
    //   once per accepted task.
    // - `complete` (in, optional): runs once after all of this call's tasks
    //   terminate, with their first error. With `complete`, these tasks fail
    //   independently of the rest of the batch; without it, a task error
    //   cancels the batch's other queued tasks.
    //
    // Returns INVALID_PARAMS if `execute` is empty, NO_AVAILABLE_HANDLE on
    // allocation failure, or INTERNAL_ERROR for another preparation failure.
    tl::expected<void, ErrorCode> PrepareTasks(PendingBatch& batch,
                                               size_t task_count,
                                               uint64_t affinity_key,
                                               const Execute& execute,
                                               const Complete& complete = {});

    // Submits the batch: publishes all its prepared tasks and blocks until
    // every task has terminated and all started Execute and Complete
    // callbacks have finished. Returns the first task error, or success when
    // every task and finalizer succeeds.
    //
    // - `batch` (in-out): the batch to submit; it is empty and reusable when
    //   this returns, in every case.
    //
    // An empty batch succeeds without running callbacks. If preparation
    // reported an error, that error is returned and nothing is published. A
    // nonempty batch submitted after Shutdown returns
    // UNAVAILABLE_IN_CURRENT_STATUS without running callbacks. On a task
    // error, ungrouped tasks of this batch are cancelled; a group error
    // cancels only that group's queued tasks.
    tl::expected<void, ErrorCode> SubmitAndWait(PendingBatch& batch);

    // Stops the scheduler: cancels queued tasks without running Execute,
    // lets running callbacks finish, runs Complete for groups already
    // published, and joins the workers.
    //
    // Takes no parameters. Safe to call repeatedly and concurrently with
    // SubmitAndWait; an in-progress backoff sleep or running callback is not
    // interrupted.
    void Shutdown();

   private:
    class Impl;
    explicit BatchIoScheduler(std::unique_ptr<Impl> impl) noexcept;
    std::unique_ptr<Impl> impl_;
};

}  // namespace mooncake
