#pragma once

#include <memory>
#include <string>

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

    // Host id of the segment this registration belongs to, set once at mount
    // (empty when unknown). Allocation reads it to keep an object's replicas on
    // different hosts.
    [[nodiscard]] const std::string& Host() const { return host_; }

   private:
    SegmentAllocatorRegistration(
        std::shared_ptr<BufferAllocatorBase> allocator, std::string host,
        std::shared_ptr<ClientLivenessRecord> client_liveness);

    void BindAllocator(std::shared_ptr<BufferAllocatorBase> replacement);
    void BindClientLiveness(std::shared_ptr<ClientLivenessRecord> record);
    void BindBuffer(AllocatedBuffer& buffer) const;
    [[nodiscard]] bool OwnsBuffer(const AllocatedBuffer& buffer) const;
    void SetAllocatable(bool allocatable);
    void Invalidate();
    std::shared_ptr<BufferAllocatorBase> allocator_;
    std::string host_;
    SegmentLifetime allocation_lifetime_;
    SegmentLifetime buffer_lifetime_;
    std::shared_ptr<ClientLivenessRecord> client_liveness_;
    friend class AllocatorManager;
    friend class ScopedNoFSegmentAccess;
    friend class ScopedSegmentAccess;
    friend class SegmentSerializer;
    friend class Serializer<AllocatedBuffer>;
};

}  // namespace mooncake
