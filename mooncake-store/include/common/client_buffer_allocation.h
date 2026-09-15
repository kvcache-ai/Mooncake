#pragma once

#include <cstddef>
#include <linux/memfd.h>
#include <linux/mman.h>
#include <string>
#include <vector>

#include <Slab.h>

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

[[nodiscard]] size_t get_hugepage_size_from_env(
    unsigned int* out_flags = nullptr, bool use_memfd = false);

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
