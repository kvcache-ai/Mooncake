#pragma once

#include <cstddef>
#include <cstdlib>

namespace mooncake {

// SPDK serves Mooncake as two things: an NVMe-oF initiator and a hugepage
// DMA allocator. The call sites never overlap, so the roles get separate
// interfaces. An allocator object remembers who it is — the alloc/free
// mirror contract (former C5) and the static-destruction-order hazard
// (former C7) disappear by construction: users hold a shared_ptr and the
// allocator outlives them.
class DmaBufferAllocator {
   public:
    virtual ~DmaBufferAllocator() = default;

    // nullptr on failure. align semantics match the backing allocator
    // (spdk_zmalloc / aligned_alloc respectively).
    virtual void* Alloc(size_t size, size_t align, int socket_id = -1) = 0;

    // Must accept nullptr (no-op).
    virtual void Free(void* ptr) = 0;
};

class SystemDmaAllocator : public DmaBufferAllocator {
   public:
    void* Alloc(size_t size, size_t align, int /*socket_id*/) override {
        return aligned_alloc(align, size);
    }

    void Free(void* ptr) override { free(ptr); }
};

}  // namespace mooncake
