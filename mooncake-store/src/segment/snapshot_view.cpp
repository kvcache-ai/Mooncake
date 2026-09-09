#include "segment/snapshot_view.h"

#include "segment/pool.h"

namespace mooncake {

const RegionCatalog& SegmentPoolSnapshotView::Catalog() const {
    return segment_pool_->catalog_;
}

std::shared_ptr<BufferAllocatorBase> SegmentPoolSnapshotView::GetAllocator(
    const UUID& region_id) const {
    const auto* mounted = Catalog().Find(region_id);
    const auto* resource =
        mounted ? segment_pool_->GetResource(*mounted) : nullptr;
    return resource ? resource->allocator() : nullptr;
}

std::optional<BufferAllocatorType>
SegmentPoolSnapshotView::GetMemoryAllocatorType() const {
    const auto* driver = segment_pool_->GetDriver(RegionKind::HOST_MEMORY);
    return driver ? driver->allocator_type() : std::nullopt;
}

bool SegmentPoolSnapshotView::HasKind(RegionKind kind) const {
    for (const auto& mounted : Catalog().Regions()) {
        if (mounted.kind == kind) {
            return true;
        }
    }
    return false;
}

const PlacementIndex& SegmentPoolSnapshotView::Placement() const {
    return segment_pool_->placement_index_;
}

}  // namespace mooncake
