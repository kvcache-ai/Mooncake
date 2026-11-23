// Copyright 2026 KVCache.AI
// Licensed under the Apache License, Version 2.0.
#pragma once

#include <chrono>
#include <functional>
#include <memory>

#include "ssd_read_source.h"

namespace mooncake::ssd_offload {

class BoundedExecutor {
   public:
    BoundedExecutor(size_t workers, size_t max_outstanding);
    ~BoundedExecutor();
    BoundedExecutor(const BoundedExecutor&) = delete;
    BoundedExecutor& operator=(const BoundedExecutor&) = delete;
    // Counts both queued and running tasks. false accepts no work.
    bool TrySubmit(std::function<void()> task) noexcept;
    // Wait for credit on the scheduler thread. Workers must never call this
    // method on their own executor.
    bool WaitSubmit(std::function<void()> task) noexcept;
    // Call from the owning thread; drains tasks before returning.
    void Shutdown();
    size_t outstanding() const;

   private:
    struct Impl;
    std::unique_ptr<Impl> impl_;
};

class SourceDrainedWriteTarget {
   public:
    virtual ~SourceDrainedWriteTarget() = default;
    // On return, no transport can still read the staging slot. Remote
    // destination drain is a separate proof, queried after all calls return.
    virtual Error WriteAndDrain(const ReadChunk&, SlotBuffer) noexcept = 0;
    virtual bool DestinationsDrained() const noexcept = 0;
};

class DrainedWriteTarget : public SourceDrainedWriteTarget {
   public:
    virtual ~DrainedWriteTarget() = default;
    // On return, no device can still read the slot or write the destination.
    // A transport software timeout alone cannot satisfy this contract.
    virtual Error WriteAndDrain(const ReadChunk&, SlotBuffer) noexcept = 0;
    bool DestinationsDrained() const noexcept final { return true; }
};

struct IoCompletion {
    SlotToken token{};
    SlotStage stage{SlotStage::kFree};
    size_t bytes{0};
    Error result{Error::kOk};
};

class EventPipelineDriver : public PipelineDriver {
   public:
    virtual bool WaitForEvent(IoCompletion&, std::chrono::milliseconds) = 0;
};
class ThreadedPipelineDriver final : public EventPipelineDriver {
   public:
    ThreadedPipelineDriver(std::shared_ptr<ReadSource> source,
                           std::shared_ptr<SourceDrainedWriteTarget> target,
                           BoundedExecutor& reads, BoundedExecutor& writes,
                           size_t slot_count);
    // One immutable source index per global chunk. Every source and mapping
    // entry is validated before work can be accepted. Both vectors must be
    // nonempty; the single-source overload keeps its unrestricted chunk IDs.
    ThreadedPipelineDriver(std::vector<std::shared_ptr<ReadSource>> sources,
                           std::vector<size_t> chunk_sources,
                           std::shared_ptr<SourceDrainedWriteTarget> target,
                           BoundedExecutor& reads, BoundedExecutor& writes,
                           size_t slot_count);
    ~ThreadedPipelineDriver() override;
    bool SubmitRead(const SlotToken&, const ReadChunk&,
                    SlotBuffer) noexcept override;
    bool SubmitWrite(const SlotToken&, const ReadChunk&,
                     SlotBuffer) noexcept override;
    void RequestCancel(const SlotToken&, SlotStage) noexcept override;
    // The scheduler consumes events here, then calls ReadDrained/WriteDrained.
    // At most one running/ready operation is stored per slot.
    bool WaitForEvent(IoCompletion&,
                      std::chrono::milliseconds timeout) override;

   private:
    struct Impl;
    std::unique_ptr<Impl> impl_;
};

}  // namespace mooncake::ssd_offload
