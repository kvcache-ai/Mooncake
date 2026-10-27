#include "memory_alloc.h"

#include "nof/nof_runtime.h"

namespace {
std::shared_ptr<mooncake::DmaBufferAllocator> DefaultDmaAllocator() {
    // CreateDefaultDmaAllocator(): build only the DMA allocator on demand,
    // without constructing an initiator this path would never use; returns
    // nullptr in non-USE_NOF builds, preserving the historical behavior.
    static std::shared_ptr<mooncake::DmaBufferAllocator> allocator =
        mooncake::CreateDefaultDmaAllocator();
    return allocator;
}
}  // namespace

void *hugepage_memory_alloc(size_t size) {
    auto allocator = DefaultDmaAllocator();
    if (!allocator) {
        return nullptr;
    }
    return allocator->Alloc(size, 0x1000, -1);
}

void hugepage_memory_free(void *ptr) {
    auto allocator = DefaultDmaAllocator();
    if (allocator) {
        allocator->Free(ptr);
    }
}
