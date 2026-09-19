#pragma once

#include <memory>

#include "allocator.h"

namespace mooncake {

class ScopedNoFSegmentAccess;
class ScopedSegmentAccess;
class SegmentSerializer;
template <typename T>
class Serializer;
class SegmentAllocatorRegistration {
   public:
    [[nodiscard]] bool IsServing() const;
    [[nodiscard]] std::unique_ptr<AllocatedBuffer> Allocate(size_t size) const;
    [[nodiscard]] std::shared_ptr<BufferAllocatorBase> GetAllocator() const;

   private:
    SegmentAllocatorRegistration(std::shared_ptr<BufferAllocatorBase> allocator,
                                 ClientSessionSharedPtr owner_session);

    void BindAllocator(std::shared_ptr<BufferAllocatorBase> replacement);
    void BindClientSession(ClientSessionSharedPtr record);
    void BindBuffer(AllocatedBuffer& buffer) const;
    [[nodiscard]] bool OwnsBuffer(const AllocatedBuffer& buffer) const;
    void SetAllocatable(bool allocatable);
    void Invalidate();
    std::shared_ptr<BufferAllocatorBase> allocator_;
    SegmentLifetime lifetime_;
    friend class AllocatorManager;
    friend class ScopedNoFSegmentAccess;
    friend class ScopedSegmentAccess;
    friend class SegmentSerializer;
    friend class Serializer<AllocatedBuffer>;
};

}  // namespace mooncake
