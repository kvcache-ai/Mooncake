#include "common/client_buffer_allocation.h"

#include "config.h"
#include "config/hugepage_config.h"
#include "host_memory_affinity.h"
#include "integer_parser.h"
#include "ub_allocator.h"

#include <cstdlib>
#include <sys/mman.h>
#include <thread>
#include <unistd.h>
#include <glog/logging.h>

#ifdef USE_CUDA
#include <cuda_runtime.h>
#endif
#ifdef USE_INTRA_NVLINK
#include "gpu_vendor/intra_nvlink.h"
#endif
#if defined(USE_ASCEND_DIRECT) || defined(USE_UBSHMEM)
#include "ascend_allocator.h"
#elif defined(USE_MEMFABRIC)
#include "memfabric_allocator.h"
#endif
#if defined(USE_SUNRISE)
#include "sunrise_allocator.h"
#endif

#include "nof/dma_buffer_allocator.h"

namespace mooncake {
namespace {

// Only called for ordinary allocator-owned host memory whose RDMA and optional
// pinned registrations have already been released. Keep partial edge pages
// intact: they may also contain allocator bookkeeping or another allocation.
void discard_owned_pages(void *ptr, size_t total_size) {
    if (!ptr || total_size < (16ull << 30)) return;
    const char *setting = std::getenv("MC_STORE_FREE_THREADS");
    if (!setting) return;
    const auto threads = TryParseInteger<size_t>(setting);
    if (!threads || *threads > 64) {
        LOG(WARNING) << "Invalid MC_STORE_FREE_THREADS value: " << setting
                     << ", expected 0..64";
        return;
    }
    if (*threads < 2) return;
    const long page_size = sysconf(_SC_PAGESIZE);
    if (page_size <= 0) return;
    const size_t page = static_cast<size_t>(page_size);
    const uintptr_t base = reinterpret_cast<uintptr_t>(ptr);
    const size_t skip = (page - base % page) % page;
    const size_t pages = (total_size - skip) / page;
    const size_t count = std::min(*threads, pages);
    if (count < 2) return;
    const size_t per_worker = pages / count + (pages % count != 0);
    const HostMemoryAffinityPlan affinity(
        globalConfig().host_mem_numa_affinity);
    auto discard = [&](size_t offset) {
        size_t length = std::min(per_worker, pages - offset) * page;
        ScopedHostMemoryAffinity bound(affinity.cpuForPartition(
            offset / per_worker, count,
            static_cast<char *>(ptr) + skip + offset * page + length / 2));
        if (madvise(static_cast<char *>(ptr) + skip + offset * page, length,
                    MADV_DONTNEED) != 0) {
            PLOG(WARNING) << "Parallel Store page discard failed";
        }
    };
    std::vector<std::jthread> workers;
    workers.reserve(count - 1);
    for (size_t offset = per_worker; offset < pages; offset += per_worker) {
        try {
            workers.emplace_back(discard, offset);
        } catch (const std::system_error &error) {
            LOG(WARNING) << "Store page discard worker unavailable: "
                         << error.what();
            discard(offset);
        }
    }
    discard(0);
}

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

size_t get_hugepage_size_from_env(unsigned int *out_flags, bool use_memfd) {
    const HugepageConfig config = HugepageConfig::FromEnvironment();
    if (!config.enabled) {
        return 0;
    }

    const size_t size = config.page_size;
    if (out_flags == nullptr) {
        return size;
    }
#ifdef __linux__
    if (use_memfd) {
        *out_flags |= MFD_HUGETLB;
        *out_flags |= size == SZ_2MB     ? MFD_HUGE_2MB
                      : size == SZ_512MB ? MFD_HUGE_512MB
                                         : MFD_HUGE_1GB;
    } else {
        *out_flags |= MAP_HUGETLB;
        *out_flags |= size == SZ_2MB     ? MAP_HUGE_2MB
                      : size == SZ_512MB ? MAP_HUGE_512MB
                                         : MAP_HUGE_1GB;
    }
    LOG(INFO) << "Using hugepage size: "
              << (size == SZ_2MB     ? "2MB"
                  : size == SZ_512MB ? "512MB"
                                     : "1GB");
#else
    (void)use_memfd;
    LOG(WARNING) << "Hugepage flags are not supported on this platform; "
                    "ignoring out_flags parameter.";
#endif
    return size;
}

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
#elif defined(USE_MEMFABRIC)
    if (protocol == "ascend") {
        return memfabric_allocate_memory(total_size, protocol);
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
    free_memory(protocol, ptr, dma_allocator, 0);
}

void free_memory(const std::string &protocol, void *ptr,
                 DmaBufferAllocator *dma_allocator, size_t total_size) {
#if defined(USE_ASCEND_DIRECT) || defined(USE_UBSHMEM)
    if (protocol == "ascend" || protocol == "ubshmem") {
        return ascend_free_memory(protocol, ptr);
    }
#elif defined(USE_MEMFABRIC)
    if (protocol == "ascend") {
        return memfabric_free_memory(protocol, ptr);
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
    discard_owned_pages(ptr, total_size);
    free(ptr);
}

}  // namespace mooncake
