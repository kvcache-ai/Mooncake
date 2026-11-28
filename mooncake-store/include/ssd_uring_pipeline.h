// Copyright 2026 KVCache.AI
// Licensed under the Apache License, Version 2.0.
#pragma once

#include <chrono>
#include <functional>
#include <memory>
#include <span>
#include <vector>

#include "ssd_pipeline_io.h"

namespace mooncake::ssd_offload {

// Persistent ordinary-READ io_uring worker pool. Each worker owns one ring;
// accepted operations retain their source and buffer until the CQE is consumed.
class UringReadExecutor {
   public:
    using Completion = std::function<void(ReadResult)>;
    // Returns empty when unsupported or initialization fails. Limits match
    // StreamingReadConfig: 1..64 workers and 1..65536 outstanding reads.
    static std::shared_ptr<UringReadExecutor> Create(
        size_t workers, size_t max_outstanding) noexcept;
    ~UringReadExecutor();
    UringReadExecutor(const UringReadExecutor&) = delete;
    UringReadExecutor& operator=(const UringReadExecutor&) = delete;

    // Waits for admission credit on an external scheduler thread. fd must equal
    // source->native_read_fd(). false accepts nothing and never invokes
    // completion. true retains the source and borrowed buffer until completion,
    // including across Shutdown(). Callback exceptions are contained; credit
    // covers completion callback destruction.
    bool WaitSubmit(std::shared_ptr<ReadSource> source, int fd,
                    std::span<std::byte> buffer, uint64_t file_offset,
                    Completion completion) noexcept;
    // Stops admission and drains accepted reads before joining workers. Call
    // externally, never from a completion callback.
    void Shutdown();
    bool healthy() const noexcept;

   private:
    struct Impl;
    explicit UringReadExecutor(std::unique_ptr<Impl>);
    std::unique_ptr<Impl> impl_;
};

class UringPipelineDriver final : public PipelineDriver {
   public:
    struct Creation {
        Error error{Error::kNotSupported};
        std::unique_ptr<UringPipelineDriver> driver;
    };
    // Executes reads on the shared UringReadExecutor and copies through writes
    // (or inline on the scheduler thread when writes == nullptr).
    // chunk_sources maps each chunk index to an index in sources.
    static Creation Create(std::vector<std::shared_ptr<ReadSource>> sources,
                           std::vector<size_t> chunk_sources,
                           std::shared_ptr<WriteTarget> target,
                           std::shared_ptr<UringReadExecutor> reader,
                           BoundedExecutor* writes,
                           std::vector<SlotBuffer> buffers);
    static Creation Create(std::vector<std::shared_ptr<ReadSource>> sources,
                           std::vector<size_t> chunk_sources,
                           std::shared_ptr<WriteTarget> target,
                           std::shared_ptr<UringReadExecutor> reader,
                           BoundedExecutor& writes,
                           std::vector<SlotBuffer> buffers) {
        return Create(std::move(sources), std::move(chunk_sources),
                      std::move(target), std::move(reader), &writes,
                      std::move(buffers));
    }
    ~UringPipelineDriver() override;
    bool SubmitRead(const SlotToken&, const ReadChunk&,
                    SlotBuffer) noexcept override;
    bool SubmitWrite(const SlotToken&, const ReadChunk&,
                     SlotBuffer) noexcept override;
    bool WaitForEvent(IoCompletion&, std::chrono::milliseconds) override;

   private:
    struct Impl;
    explicit UringPipelineDriver(std::unique_ptr<Impl>);
    std::unique_ptr<Impl> impl_;
};

}  // namespace mooncake::ssd_offload
