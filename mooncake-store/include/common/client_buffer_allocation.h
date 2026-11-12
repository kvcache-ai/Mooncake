#pragma once

#include <cstddef>
#include <string>
#include <linux/memfd.h>
#include <linux/mman.h>

#include <Slab.h>

namespace mooncake {

constexpr size_t SZ_2MB = 2 * 1024 * 1024;
constexpr size_t SZ_1GB = 1024 * 1024 * 1024;

/**
 * @brief Allocates memory for the `BufferAllocator` class.
 * @param total_size The total size of the memory to allocate.
 * @return A pointer to the allocated memory.
 */
void* allocate_buffer_allocator_memory(
    size_t total_size, const std::string& protocol = "",
    size_t alignment = facebook::cachelib::Slab::kSize);

void free_memory(const std::string& protocol, void* ptr);

inline size_t align_up(size_t size, size_t alignment) {
    if (alignment == 0) {
        return size;
    }
    return ((size + alignment - 1) / alignment) * alignment;
}

/**
 * @brief Get hugepage size from env and optionally set the corresponding memfd
 * flags.
 * * @param out_flags Optional pointer to an int. If provided,
 * MAP_HUGETLB and MAP_HUGE_2MB/1GB will be OR-ed into it.
 * @return size_t Hugepage size in bytes, or 0 if disabled.
 */
[[nodiscard]] size_t get_hugepage_size_from_env(
    unsigned int* out_flags = nullptr, bool use_memfd = false);

// Hugepage-backed allocation helpers (MAP_HUGETLB + MADV_HUGEPAGE)
void* allocate_buffer_mmap_memory(size_t total_size, size_t alignment);
void free_buffer_mmap_memory(void* ptr, size_t total_size);

}  // namespace mooncake
