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
    tl::expected<void, ErrorCode> PrepareSubBatches(PendingBatch& batch,
                                                    size_t slice_count,
                                                    size_t sub_batch_size,
                                                    const Execute& execute,
                                                    const Complete& complete = {});
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
    };

    // Append an already prepared range without publishing it.
    tl::expected<void, ErrorCode> AddSubBatch(PendingBatch& batch,
                                              SubBatch&& sub_batch);

    class Impl;
    explicit BatchIoScheduler(std::unique_ptr<Impl> impl) noexcept;
    std::unique_ptr<Impl> impl_;
};

}  // namespace mooncake
