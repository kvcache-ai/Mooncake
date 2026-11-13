#pragma once

#include <atomic>
#include <memory>

#include "allocator.h"

namespace mooncake {

class AllocatorManager;
class ScopedNoFSegmentAccess;
class ScopedSegmentAccess;

// Immutable binding of one BufferAllocatorBase and one SegmentLifetime
// generation, paired with an atomic drain/invalidation gate. Allocator or
// session replacements publish a new SegmentAllocatorRegistration under
// segment_mutex_ instead of mutating an existing instance in place.
class SegmentAllocatorRegistration {
   public:
    // Whether this segment currently accepts new allocations: its drain
    // flag and the liveness of the incarnation it belongs to.
    [[nodiscard]] bool IsAllocatable() const noexcept;
    [[nodiscard]] std::unique_ptr<AllocatedBuffer> Allocate(size_t size) const;
    [[nodiscard]] const std::shared_ptr<BufferAllocatorBase>& GetAllocator()
        const noexcept {
        return allocator_;
    }

   private:
    SegmentAllocatorRegistration(std::shared_ptr<BufferAllocatorBase> allocator,
                                 SegmentLifetime lifetime,
                                 bool allocatable = true);

    [[nodiscard]] const ClientSessionSharedPtr& GetClientSession()
        const noexcept;
    [[nodiscard]] std::shared_ptr<SegmentAllocatorRegistration> WithAllocator(
        std::shared_ptr<BufferAllocatorBase> replacement) const;
    [[nodiscard]] std::shared_ptr<SegmentAllocatorRegistration>
    ReplaceClientSession(ClientSessionSharedPtr session,
                         std::shared_ptr<BufferAllocatorBase> allocator,
                         bool allocatable);
    void BindBuffer(AllocatedBuffer& buffer) const;
    void SetAllocatable(bool allocatable) noexcept;
    void Invalidate() noexcept;

    const std::shared_ptr<BufferAllocatorBase> allocator_;
    // A registration property, not part of the incarnation state that buffers
    // carry: draining toggles this flag without invalidating existing buffers.
    std::atomic<bool> allocatable_{true};
    SegmentLifetime lifetime_;

    friend class AllocatorManager;
    friend class ScopedNoFSegmentAccess;
    friend class ScopedSegmentAccess;
};

}  // namespace mooncake
