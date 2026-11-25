#pragma once

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <optional>
#include <span>
#include <string_view>

#include <ylt/util/tl/expected.hpp>

#include "types.h"

namespace mooncake {

// Executes batches synchronously. Callers keep referenced plans and buffers
// valid until SubmitAndWait returns. Callback failures are returned as
// ErrorCode.
class BatchIoScheduler {
   public:
    using Execute = std::function<tl::expected<void, ErrorCode>(
        size_t slice_idx_begin, size_t slice_idx_end)>;

    using Complete = std::function<tl::expected<void, ErrorCode>(
        tl::expected<void, ErrorCode>)>;
    using Ready = std::function<bool()>;

    static constexpr size_t kWriteSqeBytes = 4ULL << 20;
    struct WriteRequest {
        int fd;
        const void* data;
        size_t length;
        int64_t offset;
        std::function<void(int)> report_error;
    };
    // The scratch span belongs to this SQE until its CQE is collected.
    // An empty request means the callback already did the I/O synchronously.
    using PrepareWrite =
        std::function<tl::expected<std::optional<WriteRequest>, ErrorCode>(
            size_t index, std::span<char> scratch)>;
    // Initialize one file lazily and return the inode used for worker affinity.
    using PrepareWriteFile = std::function<tl::expected<uint64_t, ErrorCode>()>;

    // Create a scheduler with workers > 0 concurrent workers.
    // Returns INVALID_PARAMS for zero workers, INTERNAL_ERROR on startup
    // failure.
    static tl::expected<std::unique_ptr<BatchIoScheduler>, ErrorCode> Create(
        size_t workers, Ready ready = {},
        std::chrono::milliseconds backoff = std::chrono::milliseconds(300),
        std::string_view worker_name_prefix = "mc-io-");
    ~BatchIoScheduler();
    BatchIoScheduler(const BatchIoScheduler&) = delete;
    BatchIoScheduler& operator=(const BatchIoScheduler&) = delete;

    // Private, unpublished tasks. Destroying this object cancels them without
    // executing callbacks. Do not access the same batch concurrently.
    class PendingBatch {
       public:
        PendingBatch();
        ~PendingBatch();
        PendingBatch(const PendingBatch&) = delete;
        PendingBatch& operator=(const PendingBatch&) = delete;

       private:
        friend class BatchIoScheduler;
        struct State;
        std::unique_ptr<State> state_;
        std::optional<ErrorCode> error_;
    };

    // Split one file's logical Slice plans into groups of sub_batch_size (>=
    // 4). A final group of at most floor(sub_batch_size / 4) Slices joins the
    // previous group. Append directly to unpublished task nodes.
    // No callback runs here. Any error invalidates the entire pending batch.
    tl::expected<void, ErrorCode> PrepareSubBatches(
        PendingBatch& batch, size_t slice_count, size_t sub_batch_size,
        const Execute& execute, const Complete& complete = {});
    // Split one file's logical Slice plans into groups of whole Slices of at
    // most sub_batch_bytes, and never more than max_slices Slices. A Slice
    // is never split, so one larger than sub_batch_bytes stands alone or
    // ends its group. A final group of at most floor(sub_batch_bytes / 4)
    // bytes joins the previous group. Append directly to unpublished task
    // nodes. No callback runs here. Any error invalidates the entire pending
    // batch.
    tl::expected<void, ErrorCode> PrepareSubBatchesByBytes(
        PendingBatch& batch, std::span<const size_t> slice_sizes,
        size_t sub_batch_bytes, size_t max_slices, const Execute& execute,
        const Complete& complete = {});
    // Prepare one task for each index in [0, task_count). Each task receives
    // [index, index + 1), allowing callers to pre-partition work by bytes or
    // submission capabilities instead of Slice count.
    tl::expected<void, ErrorCode> PrepareSubBatchTasks(
        PendingBatch& batch, size_t task_count, const Execute& execute,
        const Complete& complete = {});
    // Queue a file's writes as one completion group. The first worker lazily
    // initializes the file, then moves all its writes to the queue selected by
    // XXH3(inode). Workers refill their io_uring independently of file
    // boundaries, and file completion remains independent of the whole batch.
    tl::expected<void, ErrorCode> PrepareWriteTasks(
        PendingBatch& batch, size_t task_count,
        const PrepareWriteFile& prepare_file, const PrepareWrite& prepare,
        const Complete& complete);
    // With complete, each nonempty preparation is an independent failure group.
    // Its callback runs once, after all group tasks terminate (including
    // skipped tasks), outside scheduler locks. A group failure does not cancel
    // other groups. Unpublished/rejected batches never run completion
    // callbacks.

    // Publish all prepared tasks without allocating, then wait for every
    // started callback to finish. Consumes the batch, including on error.
    // Callbacks may execute concurrently and out of order; referenced plans
    // and buffers must remain valid until this returns. Without groups, first
    // callback errors cancel the batch's queued tasks; callback exceptions
    // return INTERNAL_ERROR.
    tl::expected<void, ErrorCode> SubmitAndWait(PendingBatch& batch);

    // Stop accepting work, cancel queued work, and join running callbacks.
    // Safe to call repeatedly; callers must still ensure
    // the scheduler object outlives every SubmitAndWait invocation.
    void Shutdown();

   private:
    // Slice-index range [slice_idx_begin, slice_idx_end) within one file's
    // plan list for one sub batch.
    // Each index selects one whole Slice. This is not a buffer address range;
    // the selected Slices may point to separate, noncontiguous buffers.
    struct SubBatch {
        size_t slice_idx_begin;
        size_t slice_idx_end;
        Execute execute{};
        PrepareWrite prepare_write{};
    };

    tl::expected<void, ErrorCode> PrepareIndexedTasks(
        PendingBatch& batch, size_t task_count, const Execute& execute,
        const PrepareWriteFile& prepare_file, const PrepareWrite& prepare_write,
        const Complete& complete);

    // Append an already prepared range without publishing it.
    tl::expected<void, ErrorCode> AddSubBatch(PendingBatch& batch,
                                              SubBatch&& sub_batch);

    class Impl;
    explicit BatchIoScheduler(std::unique_ptr<Impl> impl) noexcept;
    std::unique_ptr<Impl> impl_;
};

}  // namespace mooncake
