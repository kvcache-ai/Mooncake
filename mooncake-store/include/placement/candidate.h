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

   private:
    std::shared_ptr<BufferAllocatorBase> allocator_;
};

}  // namespace mooncake
