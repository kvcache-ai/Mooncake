#include "common/client_buffer_allocation.h"

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
#ifdef USE_NOF
#include "spdk/spdk_wrapper.h"
#endif

namespace mooncake {
namespace {

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

}  // namespace

void *allocate_buffer_allocator_memory(size_t total_size,
                                       const std::string &protocol,
                                       size_t alignment, bool use_spdk_dma) {
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
#ifdef USE_NOF
    if (use_spdk_dma && total_size > 0) {
        return mooncake::SpdkWrapper::GetInstance().Alloc(total_size, alignment,
                                                          -1);
    }
#endif
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

void free_memory(const std::string &protocol, void *ptr, bool use_spdk_dma) {
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
#ifdef USE_NOF
    // Mirror allocate_buffer_allocator_memory(): a buffer taken from the SPDK
    // hugepage pool (spdk_zmalloc) must be released with spdk_free, not glibc
    // free(), which would abort with "free(): invalid pointer".
    if (use_spdk_dma) {
        mooncake::SpdkWrapper::GetInstance().Free(ptr);
        return;
    }
#endif
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
