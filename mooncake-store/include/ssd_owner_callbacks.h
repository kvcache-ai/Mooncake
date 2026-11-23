// Copyright 2026 KVCache.AI
// Licensed under the Apache License, Version 2.0.
#pragma once
#include "ssd_read_control.h"
#include "ssd_pipeline_io.h"
#include <functional>
namespace mooncake::ssd_offload {
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
