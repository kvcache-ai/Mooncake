#pragma once

#include <memory>

#include "nof/dma_buffer_allocator.h"
#include "nof/nvmeof_initiator.h"
#include "nvme_kv/executor.h"

namespace mooncake {

// Test seam for the transport-independent SPDK executor scheduler. Production
// construction remains behind CreateNvmeKvSpdkExecutor in executor.h.
NvmeKvExecutorResult CreateNvmeKvSpdkExecutorForTesting(
    std::shared_ptr<NVMeoFInitiator> initiator,
    std::shared_ptr<DmaBufferAllocator> allocator, NofSegmentHandle* segment,
    NvmeKvCommandExecutor::Capabilities capabilities);

}  // namespace mooncake
