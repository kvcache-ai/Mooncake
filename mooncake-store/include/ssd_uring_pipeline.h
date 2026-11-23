// Copyright 2026 KVCache.AI
// Licensed under the Apache License, Version 2.0.
#pragma once
#include "ssd_pipeline_io.h"

namespace mooncake::ssd_offload {
// Persistent ordinary-READ workers. Each worker alone owns its ring; accepted
// operations retain their source and buffer until the actual CQE is consumed.
class UringReadExecutor {
   public:
    using Completion = std::function<void(ReadResult)>;
    // Returns empty when unsupported or initialization fails. Limits match the
    // streaming configuration: 1..64 workers and 1..65536 outstanding reads.
    static std::shared_ptr<UringReadExecutor> Create(
        size_t workers, size_t max_outstanding) noexcept;
    ~UringReadExecutor();
    UringReadExecutor(const UringReadExecutor&) = delete;
    UringReadExecutor& operator=(const UringReadExecutor&) = delete;
    // fd must be source->native_read_fd(). false accepts nothing and never
    // calls completion. true retains the borrowed buffer until completion,
    // including after cancellation, submission errors or Shutdown(). Callback
    // exceptions are contained; credit includes the callback's destruction.
    bool TrySubmit(std::shared_ptr<ReadSource> source, int fd,
                   std::span<std::byte> buffer, uint64_t file_offset,
                   Completion completion) noexcept;
    // Wait for admission on an external scheduler thread.
    bool WaitSubmit(std::shared_ptr<ReadSource>, int, std::span<std::byte>,
                    uint64_t, Completion) noexcept;
    // Stops admission and drains before joining. Call externally, never from
    // a completion callback. An unprovable kernel drain can block indefinitely.
    void Shutdown();
    size_t outstanding() const;
    size_t worker_count() const noexcept;

   private:
    bool Submit(std::shared_ptr<ReadSource>, int, std::span<std::byte>,
                uint64_t, Completion, bool wait) noexcept;
    struct Impl;
    explicit UringReadExecutor(std::unique_ptr<Impl>);
    std::unique_ptr<Impl> impl_;
};

class UringPipelineDriver final : public EventPipelineDriver {
   public:
    struct Creation {
        Error error{Error::kNotSupported};
        std::unique_ptr<UringPipelineDriver> driver;
    };
    struct Options {
        // Opt-in borrows every supplied slot mapping and its backing memory
        // until driver destruction, including idle periods and RebindSource.
        // Callers must not unmap, replace or release any slot sooner. The
        // default OFF accepted-read/CQE borrowing contract remains unchanged;
        // the driver does not acquire ownership of the staging arena.
        bool fixed_buffers{false};
        // Ordinary READ may use shared workers instead of a request-local
        // ring. Fixed buffers always retain their request-local registration.
        std::shared_ptr<UringReadExecutor> reader{};
    };
    // Reads borrow the supplied buffers until their CQEs are consumed. Native
    // fds remain owned by retained sources. By default the request scheduler
    // owns a local ring; Options.reader can execute ordinary READ on persistent
    // workers. A fixed-buffer table always belongs to the request-local ring.
    static Creation Create(std::shared_ptr<ReadSource>,
                           std::shared_ptr<SourceDrainedWriteTarget>,
                           BoundedExecutor& writes,
                           std::vector<SlotBuffer> buffers);
    // One immutable source index per global chunk, for a bounded batch.
    static Creation Create(std::vector<std::shared_ptr<ReadSource>>,
                           std::vector<size_t> chunk_sources,
                           std::shared_ptr<SourceDrainedWriteTarget>,
                           BoundedExecutor& writes,
                           std::vector<SlotBuffer> buffers);
    // Old overloads keep their symbols and default to ordinary READ.
    static Creation Create(std::shared_ptr<ReadSource>,
                           std::shared_ptr<SourceDrainedWriteTarget>,
                           BoundedExecutor&, std::vector<SlotBuffer>, Options);
    static Creation Create(std::vector<std::shared_ptr<ReadSource>>,
                           std::vector<size_t> chunk_sources,
                           std::shared_ptr<SourceDrainedWriteTarget>,
                           BoundedExecutor&, std::vector<SlotBuffer>, Options);
    ~UringPipelineDriver() override;
    // Reuse this driver for another source. The scheduler
    // must first drain the preceding ReadPipeline and deliver its completions.
    // Pending SQEs, unread events or write callback tails reject the change.
    // The destination target and staging-buffer ownership remain unchanged.
    // Drivers with a global chunk/source mapping cannot be rebound.
    Error RebindSource(std::shared_ptr<ReadSource>) noexcept;
    bool SubmitRead(const SlotToken&, const ReadChunk&,
                    SlotBuffer) noexcept override;
    bool SubmitWrite(const SlotToken&, const ReadChunk&,
                     SlotBuffer) noexcept override;
    void RequestCancel(const SlotToken&, SlotStage) noexcept override;
    bool WaitForEvent(IoCompletion&, std::chrono::milliseconds) override;

   private:
    static Creation CreateImpl(std::vector<std::shared_ptr<ReadSource>>,
                               std::vector<size_t> chunk_sources, bool mapped,
                               std::shared_ptr<SourceDrainedWriteTarget>,
                               BoundedExecutor&, std::vector<SlotBuffer>,
                               Options);
    struct Impl;
    explicit UringPipelineDriver(std::unique_ptr<Impl>);
    std::unique_ptr<Impl> impl_;
};
}  // namespace mooncake::ssd_offload
