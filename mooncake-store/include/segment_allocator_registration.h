#pragma once

#include <atomic>
#include <cstddef>
#include <memory>
#include <mutex>
#include <utility>

#include "allocator.h"

namespace mooncake {

// One counter per distinct segment name. Multiple registrations of the same
// name contribute only once to the live AllocatorManager's aggregate.
class ServingNameCounter {
   public:
    explicit ServingNameCounter(std::shared_ptr<std::atomic<size_t>> total)
        : total_(std::move(total)) {}
    void Update(bool serving);

   private:
    std::mutex mutex_;
    size_t serving_registrations_ = 0;
    std::shared_ptr<std::atomic<size_t>> total_;
};

class ScopedNoFSegmentAccess;
class ScopedSegmentAccess;
class SegmentSerializer;
template <typename T>
class Serializer;
class SegmentAllocatorRegistration {
   public:
    ~SegmentAllocatorRegistration();
    // The registration address identifies its liveness subscription.
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
    void SubscribeServingCount();
    void UnsubscribeServingCount();
    std::shared_ptr<BufferAllocatorBase> allocator_;
    SegmentLifetime allocation_lifetime_;
    SegmentLifetime buffer_lifetime_;
    std::shared_ptr<ClientLivenessRecord> client_liveness_;
    // Updated under the owning SegmentManager's segment_mutex_. Client state
    // callbacks capture only the counter, never this registration or manager.
    std::shared_ptr<ServingNameCounter> serving_name_counter_;
    friend class AllocatorManager;
    friend class ScopedNoFSegmentAccess;
    friend class ScopedSegmentAccess;
    friend class SegmentSerializer;
    friend class Serializer<AllocatedBuffer>;
};

}  // namespace mooncake
