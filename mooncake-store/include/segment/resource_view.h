#pragma once

#include <memory>
#include <optional>
#include <string_view>

#include "allocator.h"
#include "segment/region.h"

namespace mooncake {

class ScopedSegmentPoolReadAccess;
class SegmentPool;

// Borrowed resource view. Keep the originating Pool read access alive while
// using it.
class RegionResourceReadView final {
   public:
    ErrorCode GetSegment(const std::shared_ptr<BufferAllocatorBase>& allocator,
                         Segment& segment) const;
    std::shared_ptr<BufferAllocatorBase> GetAllocator(
        const UUID& region_id) const;
    std::optional<BufferAllocatorType> GetMemoryAllocatorType() const;
    bool HasKind(RegionKind kind) const;
    ErrorCode QueryGroup(std::string_view name, size_t& used,
                         size_t& capacity) const;
    ErrorCode QueryRegion(const UUID& id, size_t& used, size_t& capacity) const;
    bool IsInactive(const std::shared_ptr<BufferAllocatorBase>& allocator,
                    std::string_view allocation_binding) const;

   private:
    explicit RegionResourceReadView(const SegmentPool* segment_pool)
        : segment_pool_(segment_pool) {}

    const SegmentPool* segment_pool_;

    friend class ScopedSegmentPoolReadAccess;
};

}  // namespace mooncake
