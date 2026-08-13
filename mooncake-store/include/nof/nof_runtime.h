#pragma once

#include <memory>

#include "nof/dma_buffer_allocator.h"
#include "nof/nvmeof_initiator.h"

namespace mooncake {

struct NofRuntime {
    // nullptr uniformly means "NoF is unavailable in this process"
    // (non-USE_NOF build, or factory-time init failure of a future
    // eager-init initiator).
    std::shared_ptr<NVMeoFInitiator> initiator;
    // Never nullptr: SystemDmaAllocator when no DMA-specialized allocator
    // exists.
    std::shared_ptr<DmaBufferAllocator> dma_allocator;
};

// The ONLY #ifdef USE_NOF-gated definition site that names SPDK types.
// Compiled unconditionally; in non-USE_NOF builds returns
// {nullptr, SystemDmaAllocator}.
//
// Returned objects are ready to use: no caller ever invokes a setup method.
// The SPDK environment lifecycle is owned inside the implementation (a
// shared, refcounted env guard) and acquired lazily on first use.
NofRuntime CreateNofRuntime();

// For the Python-ABI exception site (hugepage_memory_alloc/free) only:
// creates just the DMA allocator, without constructing an initiator the
// caller would throw away. Returns nullptr when NoF is unavailable, which
// preserves hugepage_memory_alloc's historical "nullptr = unavailable"
// signal.
std::shared_ptr<DmaBufferAllocator> CreateDefaultDmaAllocator();

}  // namespace mooncake
