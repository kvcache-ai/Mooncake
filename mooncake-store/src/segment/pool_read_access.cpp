#include "segment/pool_read_access.h"

#include "segment/pool.h"

namespace mooncake {

ScopedSegmentPoolReadAccess::ScopedSegmentPoolReadAccess(
    const SegmentPool* segment_pool)
    : segment_pool_(segment_pool), lock_(segment_pool->pool_mutex_) {}

const RegionCatalog& ScopedSegmentPoolReadAccess::Catalog() const {
    return segment_pool_->catalog_;
}

RegionResourceReadView ScopedSegmentPoolReadAccess::Resources() const {
    return RegionResourceReadView(segment_pool_);
}

PlacementReadView ScopedSegmentPoolReadAccess::Placement() const {
    return segment_pool_->placement_index_.GetView();
}

}  // namespace mooncake
