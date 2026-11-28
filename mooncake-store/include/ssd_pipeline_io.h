// Copyright 2026 KVCache.AI
// Licensed under the Apache License, Version 2.0.
#pragma once

#include <chrono>
#include <functional>
#include <memory>
#include <vector>

#include "ssd_read_source.h"

namespace mooncake::ssd_offload {

class BoundedExecutor {
   public:
    BoundedExecutor(size_t workers, size_t max_outstanding);
    ~BoundedExecutor();
    BoundedExecutor(const BoundedExecutor&) = delete;
    BoundedExecutor& operator=(const BoundedExecutor&) = delete;
    // Non-blocking submission; returns false immediately if stopping or full.
    bool TrySubmit(std::function<void()> task) noexcept;
    // Wait for credit on the scheduler thread. Workers must never call this
    // method on their own executor.
    bool WaitSubmit(std::function<void()> task) noexcept;
    // Call from the owning thread; drains tasks before returning.
    void Shutdown();

   private:
    struct Impl;
    std::unique_ptr<Impl> impl_;
};

class WriteTarget {
   public:
    virtual ~WriteTarget() = default;
    // Copies the chunk's fragments out of the staging slot synchronously.
    virtual Error Write(const ReadChunk&, SlotBuffer) noexcept = 0;
};

class ThreadedPipelineDriver final : public PipelineDriver {
   public:
    // One immutable source index per global chunk. Every source and mapping
    // entry is validated before work can be accepted. Passing writes == nullptr
    // runs WriteTarget::Write inline on the scheduler thread (ideal for pure
    // CPU memcpy targets without spawning an extra write thread pool).
    ThreadedPipelineDriver(std::vector<std::shared_ptr<ReadSource>> sources,
                           std::vector<size_t> chunk_sources,
                           std::shared_ptr<WriteTarget> target,
                           BoundedExecutor& reads, BoundedExecutor* writes,
                           size_t slot_count);
    ThreadedPipelineDriver(std::vector<std::shared_ptr<ReadSource>> sources,
                           std::vector<size_t> chunk_sources,
                           std::shared_ptr<WriteTarget> target,
                           BoundedExecutor& reads, BoundedExecutor& writes,
                           size_t slot_count)
        : ThreadedPipelineDriver(std::move(sources), std::move(chunk_sources),
                                 std::move(target), reads, &writes,
                                 slot_count) {}
    ~ThreadedPipelineDriver() override;
    bool SubmitRead(const SlotToken&, const ReadChunk&,
                    SlotBuffer) noexcept override;
    bool SubmitWrite(const SlotToken&, const ReadChunk&,
                     SlotBuffer) noexcept override;
    bool WaitForEvent(IoCompletion&,
                      std::chrono::milliseconds timeout) override;

   private:
    struct Impl;
    std::unique_ptr<Impl> impl_;
};

}  // namespace mooncake::ssd_offload
