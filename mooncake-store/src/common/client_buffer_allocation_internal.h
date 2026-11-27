#pragma once

#include <cstddef>
#include <string>

namespace mooncake {
class DmaBufferAllocator;

namespace detail {
void free_memory(const std::string &protocol, void *ptr,
                 DmaBufferAllocator *dma_allocator, size_t total_size,
                 size_t min_discard_size);
}  // namespace detail
}  // namespace mooncake
