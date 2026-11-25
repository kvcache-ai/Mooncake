// Copyright 2026 KVCache.AI
// Licensed under the Apache License, Version 2.0.
#pragma once

#include "ssd_destination_registry.h"
#include "ssd_owner_callbacks.h"
#include "ssd_pipeline_io.h"

#include <cstddef>
#include <memory>
#include <vector>

namespace mooncake {
class TransferEngine;
namespace ssd_offload {

struct SsdTransferTargetPreparation {
    DestinationError error{DestinationError::kNotSupported};
    std::shared_ptr<SourceDrainedWriteTarget> target;
};

// Trusted Owner setup only: views come from exact requester grant resolution.
// Classic host TCP v2 only, using explicit
// TransferEngine::submitAcknowledgedTcpWrite. Accepted writes wait for terminal
// batch completion before releasing source buffers; any failed write clears
// DestinationsDrained().
SsdTransferTargetPreparation PrepareSsdTcpWriteTarget(
    std::shared_ptr<TransferEngine> engine,
    std::vector<ResolvedDestination> views, size_t max_fragments = 65536);

}  // namespace ssd_offload
}  // namespace mooncake
