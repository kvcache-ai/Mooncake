#pragma once

#include <cstddef>
#include <cstdlib>
#include <string>
#include <vector>

#ifdef __linux__
#include <linux/memfd.h>
#include <linux/mman.h>
#else
// Hugepage flags are Linux-only; define stubs so non-Linux TUs compile.
// These constants are never used at runtime on non-Linux platforms.
#ifndef MAP_HUGETLB
#define MAP_HUGETLB 0
#endif
#ifndef MAP_HUGE_2MB
#define MAP_HUGE_2MB 0
#endif
#ifndef MAP_HUGE_1GB
#define MAP_HUGE_1GB 0
#endif
#ifndef MFD_HUGETLB
#define MFD_HUGETLB 0
#endif
#ifndef MFD_HUGE_2MB
#define MFD_HUGE_2MB 0
#endif
#ifndef MFD_HUGE_1GB
#define MFD_HUGE_1GB 0
#endif
#endif  // __linux__

#include <Slab.h>
#include <glog/logging.h>

#include "common/byte_size.h"

namespace mooncake {

constexpr size_t SZ_2MB = 2 * 1024 * 1024;
constexpr size_t SZ_512MB = 512 * 1024 * 1024;

// 512MiB hugepages on arm64 kernels with 64K base pages may not be
// defined by older glibc/kernel headers.
#ifndef MAP_HUGE_512MB
#define MAP_HUGE_512MB (29 << 26)  // MAP_HUGE_SHIFT = 26
#endif
#ifndef MFD_HUGE_512MB
#define MFD_HUGE_512MB (29 << 26)  // MFD_HUGE_SHIFT = 26
#endif
constexpr size_t SZ_1GB = 1024 * 1024 * 1024;
constexpr double BYTES_PER_GIB = static_cast<double>(SZ_1GB);

void* allocate_buffer_allocator_memory(
    size_t total_size, const std::string& protocol = "",
    size_t alignment = facebook::cachelib::Slab::kSize,
    bool use_spdk_dma = false);

void free_memory(const std::string& protocol, void* ptr,
                 bool use_spdk_dma = false);

inline size_t align_up(size_t size, size_t alignment) {
    if (alignment == 0) {
        return size;
    }
    return ((size + alignment - 1) / alignment) * alignment;
}

[[nodiscard]] inline size_t get_hugepage_size_from_env(
    unsigned int* out_flags = nullptr, bool use_memfd = false) {
    if (std::getenv("MC_STORE_USE_HUGEPAGE") == nullptr) {
        return 0;
    }

    size_t size = SZ_2MB;
    if (const char* size_env = std::getenv("MC_STORE_HUGEPAGE_SIZE")) {
        const size_t parsed_size = string_to_byte_size(size_env);
        if (parsed_size == SZ_2MB || parsed_size == SZ_512MB ||
            parsed_size == SZ_1GB) {
            size = parsed_size;
        } else {
            LOG(WARNING) << "Invalid MC_STORE_HUGEPAGE_SIZE='" << size_env
                         << "'. Supported: 2MB, 512MB, 1GB. Fallback to 2MB.";
        }
    }

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
#endif  // __linux__
    return size;
}

void populate_hugetlb_mapping(void* ptr, size_t total_size);

void populate_hugetlb_numa_mapping(void* ptr, size_t total_size,
                                   const std::vector<int>& numa_nodes);

void* allocate_buffer_mmap_memory(size_t total_size, size_t alignment);

void* allocate_buffer_mmap_memory(size_t total_size, size_t alignment,
                                  bool defer_hugetlb_population);

[[nodiscard]] bool is_mmap_arena_allocation(const void* ptr);

void free_buffer_mmap_memory(void* ptr, size_t total_size);

void* allocate_buffer_numa_segments(size_t total_size,
                                    const std::vector<int>& numa_nodes,
                                    size_t page_size = 0);

}  // namespace mooncake
