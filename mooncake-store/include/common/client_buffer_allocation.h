#pragma once

#include <cstddef>
#include <cstdlib>
#include <linux/memfd.h>
#include <linux/mman.h>
#include <string>
#include <vector>

#include <Slab.h>
#include <glog/logging.h>

#include "common/byte_size.h"

namespace mooncake {

constexpr size_t SZ_2MB = 2 * 1024 * 1024;
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
        if (parsed_size == SZ_2MB || parsed_size == SZ_1GB) {
            size = parsed_size;
        } else {
            LOG(WARNING) << "Invalid MC_STORE_HUGEPAGE_SIZE='" << size_env
                         << "'. Supported: 2MB, 1GB. Fallback to 2MB.";
        }
    }

    if (out_flags == nullptr) {
        return size;
    }
    if (use_memfd) {
        *out_flags |= MFD_HUGETLB;
        *out_flags |= size == SZ_2MB ? MFD_HUGE_2MB : MFD_HUGE_1GB;
    } else {
        *out_flags |= MAP_HUGETLB;
        *out_flags |= size == SZ_2MB ? MAP_HUGE_2MB : MAP_HUGE_1GB;
    }
    LOG(INFO) << "Using hugepage size: " << (size == SZ_2MB ? "2MB" : "1GB");
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
