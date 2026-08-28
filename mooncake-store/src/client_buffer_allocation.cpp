#include "utils.h"

#include "config.h"
#include "ub_allocator.h"

#include <cstdlib>
#include <glog/logging.h>

#ifdef USE_CUDA
#include <cuda_runtime.h>
#endif
#ifdef USE_INTRA_NVLINK
#include "gpu_vendor/intra_nvlink.h"
#endif
#if defined(USE_ASCEND_DIRECT) || defined(USE_UBSHMEM)
#include "ascend_allocator.h"
#endif
#if defined(USE_SUNRISE)
#include "sunrise_allocator.h"
#endif

#include "nof/dma_buffer_allocator.h"

namespace mooncake {

#ifdef USE_VRAM_SEGMENT
tl::expected<void *, std::string> allocate_vram_memory(
    size_t total_size, const std::string &protocol) {
    cudaError_t res;
    int device;
    void *ptr = nullptr;
    res = cudaGetDevice(&device);
    if (res != cudaSuccess) {
        LOG(ERROR) << "VRAM Segment cudaGetDevice failed.";
        return tl::make_unexpected("VRAM Segment cudaGetDevice failed.");
    }
    if (protocol == "nvlink_intra") {
#ifdef USE_INTRA_NVLINK
        ptr = allocateFabricMemory_intra(total_size);
        return ptr;
#else
        LOG(ERROR) << "Protocol nvlink_intra need USE_INTRA_NVLINK=ON. Please "
                      "rebuild mooncake from source.";
        return tl::make_unexpected("Protocol not supported");
#endif
    }
    res = cudaMalloc((void **)&ptr, total_size);
    if (res != cudaSuccess) {
        LOG(ERROR) << "VRAM Segment cudaMalloc failed.";
        return tl::make_unexpected("VRAM Segment cudaMalloc failed.");
    }
    return ptr;
}
#endif

void *allocate_buffer_allocator_memory(size_t total_size,
                                       const std::string &protocol,
                                       size_t alignment,
                                       DmaBufferAllocator *dma_allocator) {
    const size_t default_alignment = facebook::cachelib::Slab::kSize;
    // Ensure total_size is a multiple of alignment
    if (alignment == default_alignment && total_size < alignment) {
        LOG(ERROR) << "Total size must be at least " << alignment;
        return nullptr;
    }
#if defined(USE_ASCEND_DIRECT) || defined(USE_UBSHMEM)
    if (protocol == "ascend" || protocol == "ubshmem") {
        return ascend_allocate_memory(total_size, protocol);
    }
#endif
#if defined(USE_SUNRISE)
    if (protocol == "sunrise_link") {
        return sunrise_allocate_memory(
            total_size, alignment,
            mooncake::globalConfig().sunrise_use_device_mem);
    }
#endif
#if defined(USE_UB)
    if (protocol == "ub") {
        return mooncake::ub_allocate_memory(alignment, total_size);
    }
#endif
    // A DMA-specialized allocator (SPDK hugepage pool) takes precedence over
    // the generic fallback; nullptr keeps the historical path below.
    if (dma_allocator && total_size > 0) {
        return dma_allocator->Alloc(total_size, alignment, -1);
    }
#ifdef USE_VRAM_SEGMENT
    auto ret = allocate_vram_memory(total_size, protocol);
    if (!ret) {
        LOG(ERROR) << ret.error();
        return nullptr;
    }
    return *ret;
#endif
    // Allocate aligned memory
    return aligned_alloc(alignment, total_size);
}

void free_memory(const std::string &protocol, void *ptr,
                 DmaBufferAllocator *dma_allocator) {
#if defined(USE_ASCEND_DIRECT) || defined(USE_UBSHMEM)
    if (protocol == "ascend" || protocol == "ubshmem") {
        return ascend_free_memory(protocol, ptr);
    }
#endif
#if defined(USE_SUNRISE)
    if (protocol == "sunrise_link") {
        return sunrise_free_memory(ptr);
    }
#endif
#if defined(USE_UB)
    if (protocol == "ub") {
        mooncake::ub_free_memory(ptr);
        return;
    }
#endif
    // Mirror allocate_buffer_allocator_memory(): a buffer taken from a
    // DMA-specialized allocator (spdk_zmalloc) must be released by that same
    // allocator (spdk_free), never glibc free(). The mirror contract is now
    // guaranteed by the allocator object identity.
    if (dma_allocator) {
        dma_allocator->Free(ptr);
        return;
    }
#ifdef USE_VRAM_SEGMENT
#ifdef USE_INTRA_NVLINK
    freeFabricMemory_intra(ptr);
#else
    cudaFree(ptr);
#endif
    return;
#endif
    free(ptr);
}

}  // namespace mooncake
