// Copyright 2026 KVCache.AI
// Licensed under the Apache License, Version 2.0.
#pragma once
#include "ssd_read_control.h"
#include "ssd_pipeline_io.h"
#include <functional>
namespace mooncake::ssd_offload {
class SourceDrainedWriteTarget : public WriteTarget {
   public:
    ~SourceDrainedWriteTarget() override = default;
    // WriteAndDrain guarantees the staging slot is no longer read once it
    // returns (including on error). DestinationsDrained reports whether every
    // accepted destination write has also drained on the receiver.
    virtual Error WriteAndDrain(const ReadChunk&, SlotBuffer) noexcept = 0;
    virtual bool DestinationsDrained() const noexcept = 0;
    Error Write(const ReadChunk& chunk,
                SlotBuffer buffer) noexcept final override {
        return WriteAndDrain(chunk, buffer);
    }
};
struct SsdOwnerCallbacks {
    using TargetFactory =
        std::function<std::shared_ptr<SourceDrainedWriteTarget>(
            std::vector<ResolvedDestination>)>;
    // nullopt means a complete preflight rejection with NO accepted writes.
    // A reader must drain accepted staging accesses even on exceptions; it
    // retains its backend/arena through return. FileStorage satisfies this
    // contract. Factory creation also accepts no writes.
    using Reader = std::function<std::optional<SsdReadResultV1>(
        const SsdReadRequestV1&, std::shared_ptr<SourceDrainedWriteTarget>,
        std::chrono::steady_clock::time_point)>;
};
}  // namespace mooncake::ssd_offload
