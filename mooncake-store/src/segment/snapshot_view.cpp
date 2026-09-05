#include "segment/snapshot_view.h"

#include "segment/pool.h"

namespace mooncake {

const RegionCatalog& SegmentPoolSnapshotView::Catalog() const {
    return segment_pool_->catalog_;
}

RegionResourceReadView SegmentPoolSnapshotView::Resources() const {
    return RegionResourceReadView(segment_pool_);
}

PlacementReadView SegmentPoolSnapshotView::Placement() const {
    return segment_pool_->placement_index_.GetView();
}

}  // namespace mooncake
