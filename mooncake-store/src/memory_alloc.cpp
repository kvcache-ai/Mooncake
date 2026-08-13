#include "memory_alloc.h"

#include "nof/nof_runtime.h"

namespace {
std::shared_ptr<mooncake::DmaBufferAllocator> DefaultDmaAllocator() {
    // CreateDefaultDmaAllocator():按需只构造 DMA allocator,不为这里
    // 白造一个用不到的 initiator(评审 #10);非 USE_NOF 返回 nullptr,
    // 保持历史行为。
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
