#pragma once

#include <atomic>
#include <memory>

#include "allocator.h"
#include "common/atomic_shared_ptr.h"

namespace mooncake {

class ScopedNoFSegmentAccess;
class ScopedSegmentAccess;
class SegmentAllocatorRegistration {
   public:
    // Whether this segment currently accepts new allocations: its drain
    // flag and the liveness of the incarnation it belongs to.
    [[nodiscard]] bool IsAllocatable() const;
    [[nodiscard]] std::unique_ptr<AllocatedBuffer> Allocate(size_t size) const;
    [[nodiscard]] std::shared_ptr<BufferAllocatorBase> GetAllocator() const;

   private:
    SegmentAllocatorRegistration(std::shared_ptr<BufferAllocatorBase> allocator,
                                 ClientSessionSharedPtr owner_session);

    [[nodiscard]] SegmentLifetime SnapshotLifetime() const;
    void BindAllocator(std::shared_ptr<BufferAllocatorBase> replacement);
    void BindClientSession(ClientSessionSharedPtr record);
    void BindBuffer(AllocatedBuffer& buffer) const;
    [[nodiscard]] bool OwnsBuffer(const AllocatedBuffer& buffer) const;
    void SetAllocatable(bool allocatable);
    void Invalidate();
    AtomicSharedPtr<BufferAllocatorBase> allocator_;
    // A registration property, not part of the incarnation state that buffers
    // carry: it survives a session rebind and no buffer ever reads it.
    std::atomic<bool> allocatable_{true};
    // Only registrations publish replacement generations. Buffers retain a
    // plain shared_ptr snapshot and never load this atomic on read paths.
    AtomicSharedPtr<SegmentLifetime::State> lifetime_;
    friend class AllocatorManager;
    friend class ScopedNoFSegmentAccess;
    friend class ScopedSegmentAccess;
};

}  // namespace mooncake
