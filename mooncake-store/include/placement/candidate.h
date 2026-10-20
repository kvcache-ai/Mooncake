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

    // Availability and ownership belong to the mounted region itself, so
    // placement and the buffers it hands out consult the same state.
    bool IsServing() const { return lifetime_.CanAllocate(); }

    void BindClientLiveness(std::shared_ptr<ClientLivenessRecord> owner) {
        lifetime_.BindClientLiveness(std::move(owner));
    }

    void SetStatus(SegmentStatus status) { lifetime_.SetStatus(status); }

    void Invalidate() { lifetime_.Invalidate(); }

    void BindBuffer(AllocatedBuffer& buffer) const {
        buffer.bindSegmentLifetime(lifetime_);
    }

    bool OwnsBuffer(const AllocatedBuffer& buffer) const {
        return buffer.isBoundTo(lifetime_);
    }

    // Replacing the allocator of an existing mount preserves the region
    // identity, so buffers bound before the replacement follow the new one.
    void InheritLifetime(const AllocationCandidate& previous) {
        lifetime_ = previous.lifetime_;
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
        return AllocateBoundTo(allocator(), lifetime_, size);
    }

   private:
    std::shared_ptr<BufferAllocatorBase> allocator_;
    SegmentLifetime lifetime_;
};

}  // namespace mooncake
