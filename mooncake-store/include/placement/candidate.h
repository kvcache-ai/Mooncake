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
    virtual ~AllocationCandidate() { lifetime_.Invalidate(); }

    std::unique_ptr<AllocatedBuffer> Allocate(size_t size) const {
        if (!IsServing()) {
            return nullptr;
        }
        auto buffer = DoAllocate(size);
        if (!buffer) {
            return nullptr;
        }
        // Re-check after allocating so a concurrent transition to SUSPECTED or
        // OFFLINE does not publish a buffer from a non-serving session.
        if (!IsServing()) {
            return nullptr;
        }
        buffer->bindSegmentLifetime(lifetime_);
        return buffer;
    }

    virtual AllocationCandidateKind Kind() const noexcept = 0;

    [[nodiscard]] bool IsServing() const noexcept {
        return lifetime_.isServing();
    }

    // Mutators are non-const so read-only PlacementIndex views cannot invoke
    // them; callers must hold the Pool write lock.
    void InvalidateLifetime() noexcept { lifetime_.Invalidate(); }

    void ResetLifetime(ClientSessionSharedPtr owner_session) {
        lifetime_.Invalidate();
        lifetime_ = SegmentLifetime(std::move(owner_session));
    }

    size_t Capacity() const { return allocator_->capacity(); }
    size_t Used() const { return allocator_->size(); }

    const std::shared_ptr<BufferAllocatorBase>& allocator_handle()
        const noexcept {
        return allocator_;
    }

   protected:
    explicit AllocationCandidate(std::shared_ptr<BufferAllocatorBase> allocator,
                                 ClientSessionSharedPtr owner_session = nullptr)
        : allocator_(std::move(allocator)),
          lifetime_(std::move(owner_session)) {}

    virtual std::unique_ptr<AllocatedBuffer> DoAllocate(size_t size) const = 0;

    void BindBuffer(AllocatedBuffer& buffer) const {
        buffer.bindSegmentLifetime(lifetime_);
    }

    BufferAllocatorBase& allocator() const noexcept { return *allocator_; }

   private:
    std::shared_ptr<BufferAllocatorBase> allocator_;
    SegmentLifetime lifetime_;
};

}  // namespace mooncake
