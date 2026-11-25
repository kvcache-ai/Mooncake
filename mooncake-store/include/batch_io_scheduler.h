#pragma once

#include <chrono>
#include <cstddef>
#include <functional>
#include <memory>
#include <optional>
#include <string_view>

#include <ylt/util/tl/expected.hpp>

#include "types.h"

namespace mooncake {

// Owns resident workers, each with a private task queue. A task runs on the
// worker selected by its caller-supplied affinity key, so all tasks prepared
// from one source (for example, one file) land on one worker and that worker
// owns the source's state. Each SubmitAndWait call publishes a set of prepared
// tasks, blocks until that set has terminated, and returns its first reported
// error. Callers keep all data referenced by their callbacks valid until
// SubmitAndWait returns.
class BatchIoScheduler {
   public:
    // Executes one caller-defined task index. An accepted task is executed at
    // most once; a cancelled task is not executed. Tasks may run concurrently
    // and out of order, including tasks from the same PendingBatch. Exceptions
    // are converted to INTERNAL_ERROR.
    using Execute =
        std::function<tl::expected<void, ErrorCode>(size_t task_index)>;

    // Finalizes one nonempty PrepareTasks call after all its tasks terminate.
    // The argument is its first task error, or success. This callback runs once
    // for each accepted, published group, including a group cancelled after
    // publication. It runs outside scheduler locks; its errors and exceptions
    // join the batch result (exceptions become INTERNAL_ERROR).
    using Complete = std::function<tl::expected<void, ErrorCode>(
        tl::expected<void, ErrorCode>)>;

    // Gates task claiming without holding scheduler locks. Returning false
    // defers the task and rechecks after the configured backoff. Exceptions
    // fail the task with INTERNAL_ERROR. Already running callbacks are not
    // interrupted.
    using Ready = std::function<bool()>;

    // Starts the worker threads. An empty ready callback accepts tasks
    // immediately. Workers are named with worker_name_prefix followed by their
    // zero-based index, truncated to platform limits.
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

    // Owns tasks that have not yet been published. A default-constructed or
    // consumed batch is empty and may be reused. Do not access one PendingBatch
    // concurrently.
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
        std::unique_ptr<State> state_;
        std::optional<ErrorCode> error_;
    };

    // Appends task_count unpublished tasks to the queue of the worker selected
    // by affinity_key % worker count. Callers pass a stable identity of the
    // work (for this design, the bucket file's inode), so one source's tasks
    // all run on one worker and that worker owns the source's state. Task i
    // receives index i; indices are local to this call. No callback runs
    // during preparation. task_count == 0 appends nothing and never runs
    // complete; execute must still be nonempty.
    //
    // A nonempty complete creates an independent failure group for this call.
    // Without complete, the tasks share the batch-wide cancellation domain.
    // Returns INVALID_PARAMS for an empty execute, NO_AVAILABLE_HANDLE on
    // allocation failure, or INTERNAL_ERROR for another preparation exception.
    // Any error invalidates all tasks accumulated in this PendingBatch; later
    // preparations return the same error until SubmitAndWait consumes it.
    tl::expected<void, ErrorCode> PrepareTasks(PendingBatch& batch,
                                               size_t task_count,
                                               uint64_t affinity_key,
                                               const Execute& execute,
                                               const Complete& complete = {});

    // Consumes the PendingBatch and publishes all its prepared tasks to their
    // worker queues without further allocation. Preparation errors are
    // returned without publication or completion callbacks. An empty batch
    // succeeds; a nonempty batch submitted after Shutdown returns
    // UNAVAILABLE_IN_CURRENT_STATUS without running callbacks. The consumed
    // batch is empty and reusable in every case.
    //
    // For an accepted publication, waits until every task has terminated and
    // all started Execute and Complete callbacks have finished. Ungrouped task
    // errors cancel queued ungrouped tasks in this batch. A group error cancels
    // only that group's queued tasks; other groups continue. Returns the first
    // reported task error, or success when every task and finalizer succeeds.
    tl::expected<void, ErrorCode> SubmitAndWait(PendingBatch& batch);

    // Stops accepting nonempty publications, cancels queued tasks without
    // running Execute, lets running callbacks finish, runs Complete for groups
    // already published, and joins the workers. Safe to call repeatedly and
    // concurrently with SubmitAndWait. Does not interrupt an in-progress
    // backoff sleep or a running callback.
    void Shutdown();

   private:
    class Impl;
    explicit BatchIoScheduler(std::unique_ptr<Impl> impl) noexcept;
    std::unique_ptr<Impl> impl_;
};

}  // namespace mooncake
