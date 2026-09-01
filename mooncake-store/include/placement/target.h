#pragma once

#include <cstddef>
#include <memory>
#include <utility>

#include "allocator.h"

namespace mooncake {

enum class PlacementTargetKind {
    NATIVE = 0,
    CXL,
};

inline constexpr size_t kPlacementTargetKindCount =
    static_cast<size_t>(PlacementTargetKind::CXL) + 1;

// A stable allocation endpoint published to PlacementIndex. RegionResource
// owns the target and must outlive every placement reference to it.
class PlacementTarget {
   public:
    virtual ~PlacementTarget() = default;

    virtual std::unique_ptr<AllocatedBuffer> Allocate(size_t size) const = 0;
    virtual PlacementTargetKind Kind() const noexcept = 0;

    size_t Capacity() const { return allocator_->capacity(); }
    size_t Used() const { return allocator_->size(); }

   protected:
    explicit PlacementTarget(std::shared_ptr<BufferAllocatorBase> allocator)
        : allocator_(std::move(allocator)) {}

    BufferAllocatorBase& allocator() const noexcept { return *allocator_; }

   private:
    std::shared_ptr<BufferAllocatorBase> allocator_;
};

}  // namespace mooncake
