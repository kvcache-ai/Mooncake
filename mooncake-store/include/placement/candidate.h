#pragma once

#include <cstddef>
#include <memory>
#include <utility>

#include "allocator.h"

namespace mooncake {

enum class AllocationCandidateKind {
    NATIVE = 0,
    CXL,
};

inline constexpr size_t kAllocationCandidateKindCount =
    static_cast<size_t>(AllocationCandidateKind::CXL) + 1;

// A stable allocation endpoint published to PlacementIndex. RegionResource
// owns the candidate and must outlive every placement reference to it.
class AllocationCandidate {
   public:
    virtual ~AllocationCandidate() = default;

    virtual std::unique_ptr<AllocatedBuffer> Allocate(size_t size) const = 0;
    virtual AllocationCandidateKind Kind() const noexcept = 0;

    // Liveness and buffer identity belong to a mounted region, not its
    // allocator (CXL regions can share one allocator).
    bool IsServing() const {
        const auto record = std::atomic_load_explicit(
            &client_liveness_, std::memory_order_acquire);
        return allocation_lifetime_.isAvailable() &&
               (!record || record->IsServing());
    }

    void BindClientLiveness(std::shared_ptr<ClientLivenessRecord> record) {
        std::atomic_store_explicit(&client_liveness_, std::move(record),
                                   std::memory_order_release);
    }
    void BindBuffer(AllocatedBuffer& buffer) const {
        buffer.bindSegmentLifetime(buffer_lifetime_);
        buffer.bindClientLiveness(std::atomic_load_explicit(
            &client_liveness_, std::memory_order_acquire));
    }
    bool OwnsBuffer(const AllocatedBuffer& buffer) const {
        return buffer.segment_lifetime_ == buffer_lifetime_;
    }
    void SetAvailability(bool allocatable, bool readable) {
        allocation_lifetime_.setAvailable(allocatable);
        buffer_lifetime_.setAvailable(readable);
    }
    // Allocator replacement keeps the region's identity so recovered buffers
    // can be rebound after client liveness records are reconstructed.
    void InheritBinding(const AllocationCandidate& previous) {
        allocation_lifetime_ = previous.allocation_lifetime_;
        buffer_lifetime_ = previous.buffer_lifetime_;
        BindClientLiveness(std::atomic_load_explicit(
            &previous.client_liveness_, std::memory_order_acquire));
    }

    size_t Capacity() const { return allocator_->capacity(); }
    size_t Used() const { return allocator_->size(); }

    const std::shared_ptr<BufferAllocatorBase>& allocator_handle()
        const noexcept {
        return allocator_;
    }

   protected:
    explicit AllocationCandidate(std::shared_ptr<BufferAllocatorBase> allocator)
        : allocator_(std::move(allocator)) {}

    BufferAllocatorBase& allocator() const noexcept { return *allocator_; }

    std::unique_ptr<AllocatedBuffer> AllocateRegistered(size_t size) const {
        if (!IsServing()) return nullptr;
        const auto record = std::atomic_load_explicit(
            &client_liveness_, std::memory_order_acquire);
        auto buffer = allocator().allocate(size);
        if (!buffer) return nullptr;
        buffer->bindSegmentLifetime(buffer_lifetime_);
        buffer->bindClientLiveness(record);
        if (!IsServing() ||
            record != std::atomic_load_explicit(&client_liveness_,
                                                std::memory_order_acquire)) {
            return nullptr;
        }
        return buffer;
    }

   private:
    std::shared_ptr<BufferAllocatorBase> allocator_;
    SegmentLifetime allocation_lifetime_;
    SegmentLifetime buffer_lifetime_;
    std::shared_ptr<ClientLivenessRecord> client_liveness_;
};

}  // namespace mooncake
