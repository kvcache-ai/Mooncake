// Copyright 2026 KVCache.AI
// Licensed under the Apache License, Version 2.0.
#pragma once
#include "ssd_destination_registry.h"
#include "ssd_pipeline_io.h"

namespace mooncake {
class TransferEngine;
namespace ssd_offload {
struct SsdTransferTargetPreparation {
    DestinationError error{DestinationError::kNotSupported};
    std::shared_ptr<SourceDrainedWriteTarget> target;
};
// Trusted Owner setup only: views come from exact requester grant resolution.
// Initially classic host TCP v2 only. Accepted writes retain their operation
// until local physical drain; a failed write never proves destination drain.
SsdTransferTargetPreparation PrepareSsdTcpWriteTarget(
    std::shared_ptr<TransferEngine>, std::vector<ResolvedDestination>,
    size_t max_fragments = 65536);
// Classic RC RDMA host WRITE. Successful completion proves destination writes
// finished; failures never fabricate a remote drain. Existing TE READ uses the
// same registered buffers/scatter completion contract for legacy/fallback.
SsdTransferTargetPreparation PrepareSsdRdmaWriteTarget(
    std::shared_ptr<TransferEngine>, std::vector<ResolvedDestination>,
    size_t max_fragments = 65536);
}  // namespace ssd_offload
}  // namespace mooncake
