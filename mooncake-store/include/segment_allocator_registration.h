#pragma once

#include <atomic>
#include <cstddef>
#include <memory>
#include <mutex>
#include <utility>

#include "allocator.h"

namespace mooncake {

class ScopedNoFSegmentAccess;
class ScopedSegmentAccess;
class SegmentSerializer;
template <typename T>
class Serializer;
class SegmentAllocatorRegistration {
   public:
    ~SegmentAllocatorRegistration();
    // Each registration owns one serving-count contribution.
    SegmentAllocatorRegistration(const SegmentAllocatorRegistration&) = delete;
    SegmentAllocatorRegistration& operator=(
        const SegmentAllocatorRegistration&) = delete;
    [[nodiscard]] bool IsServing() const;
    [[nodiscard]] std::unique_ptr<AllocatedBuffer> Allocate(size_t size) const;
    [[nodiscard]] std::shared_ptr<BufferAllocatorBase> GetAllocator() const;

   private:
    SegmentAllocatorRegistration(
        std::shared_ptr<BufferAllocatorBase> allocator,
        std::shared_ptr<ClientLivenessRecord> client_liveness);

    void BindAllocator(std::shared_ptr<BufferAllocatorBase> replacement);
    void BindClientLiveness(std::shared_ptr<ClientLivenessRecord> record);
    void BindBuffer(AllocatedBuffer& buffer) const;
    [[nodiscard]] bool OwnsBuffer(const AllocatedBuffer& buffer) const;
    void SetAllocatable(bool allocatable);
    void Invalidate();
    void TrackServingName(std::shared_ptr<ServingNameCounter> counter);
    void UntrackServingName();
    void AddServingCount();
    void RemoveServingCount();
    std::shared_ptr<BufferAllocatorBase> allocator_;
    SegmentLifetime allocation_lifetime_;
    SegmentLifetime buffer_lifetime_;
    std::shared_ptr<ClientLivenessRecord> client_liveness_;
    // Updated under the owning SegmentManager's segment_mutex_. Liveness
    // records hold only the counter, never this registration or manager.
    std::shared_ptr<ServingNameCounter> serving_name_counter_;
    friend class AllocatorManager;
    friend class ScopedNoFSegmentAccess;
    friend class ScopedSegmentAccess;
    friend class SegmentSerializer;
    friend class Serializer<AllocatedBuffer>;
};

}  // namespace mooncake
