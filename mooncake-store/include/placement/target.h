#pragma once

#include <cstddef>
#include <memory>
#include <string>
#include <utility>

#include "allocator.h"

namespace mooncake {

enum class AllocationTargetKind {
    NATIVE = 0,
    CXL,
};

// A stable, non-owning allocation endpoint published to PlacementIndex.
// The owning RegionResource must outlive every placement reference to it.
class AllocationTarget final {
   public:
    AllocationTarget(BufferAllocatorBase* allocator, AllocationTargetKind kind,
                     std::string cxl_binding_name = {})
        : allocator_(allocator),
          kind_(kind),
          cxl_binding_name_(std::move(cxl_binding_name)) {}

    std::unique_ptr<AllocatedBuffer> Allocate(size_t size) const {
        auto buffer = allocator_->allocate(size);
        if (buffer && kind_ == AllocationTargetKind::CXL) [[unlikely]] {
            buffer->change_to_cxl(cxl_binding_name_);
        }
        return buffer;
    }

    size_t Capacity() const { return allocator_->capacity(); }
    size_t Used() const { return allocator_->size(); }
    AllocationTargetKind kind() const noexcept { return kind_; }

   private:
    BufferAllocatorBase* allocator_;
    AllocationTargetKind kind_;
    std::string cxl_binding_name_;
};

}  // namespace mooncake
