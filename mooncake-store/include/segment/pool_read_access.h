#pragma once

#include <shared_mutex>

#include "placement/index.h"
#include "segment/catalog.h"
#include "segment/resource_view.h"

namespace mooncake {

class SegmentPool;

class ScopedSegmentPoolReadAccess final {
   public:
    ScopedSegmentPoolReadAccess(ScopedSegmentPoolReadAccess&&) noexcept =
        default;
    ScopedSegmentPoolReadAccess& operator=(
        ScopedSegmentPoolReadAccess&&) noexcept = default;
    ScopedSegmentPoolReadAccess(const ScopedSegmentPoolReadAccess&) = delete;
    ScopedSegmentPoolReadAccess& operator=(const ScopedSegmentPoolReadAccess&) =
        delete;

    const RegionCatalog& Catalog() const;
    RegionResourceReadView Resources() const;
    PlacementReadView Placement() const;

   private:
    explicit ScopedSegmentPoolReadAccess(const SegmentPool* segment_pool);

    const SegmentPool* segment_pool_;
    std::shared_lock<std::shared_mutex> lock_;

    friend class SegmentPool;
};

}  // namespace mooncake
