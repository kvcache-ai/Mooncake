#include "segment/pool_read_access.h"

namespace mooncake {

const RegionDriver* SegmentPool::ReadAccess::GetDriver(RegionKind kind) const {
    auto driver = drivers_.find(kind);
    return driver == drivers_.end() ? nullptr : driver->second.get();
}

const RegionResource* SegmentPool::ReadAccess::GetResource(
    const MountedRegion& mounted) const {
    const auto* driver = GetDriver(mounted.kind);
    return driver ? driver->GetResource(mounted.segment.id) : nullptr;
}

std::shared_ptr<BufferAllocatorBase> SegmentPool::ReadAccess::GetAllocator(
    const UUID& region_id) const {
    auto mounted = catalog_.Find(region_id);
    if (mounted == nullptr) {
        return nullptr;
    }
    const auto* resource = GetResource(*mounted);
    return resource ? resource->allocator() : nullptr;
}

ErrorCode SegmentPool::ReadAccess::QueryAllocationCandidates(
    std::string_view name, AllocationCandidateKind kind, size_t& used,
    size_t& capacity) const {
    auto* entry = placement_.Find(name, kind);
    if (!entry) {
        return ErrorCode::SEGMENT_NOT_FOUND;
    }
    used = 0;
    capacity = 0;
    for (const auto* candidate : entry->candidates) {
        used += candidate->Used();
        capacity += candidate->Capacity();
    }
    return capacity == 0 ? ErrorCode::SEGMENT_NOT_FOUND : ErrorCode::OK;
}

bool SegmentPool::ReadAccess::IsInactive(
    const std::shared_ptr<BufferAllocatorBase>& allocator,
    std::string_view allocation_binding) const {
    if (!allocator) {
        return false;
    }
    for (const auto& mounted : catalog_.Regions()) {
        const auto* resource = GetResource(mounted);
        if (!resource || resource->allocator() != allocator ||
            (mounted.kind == RegionKind::CXL &&
             mounted.segment.name != allocation_binding)) {
            continue;
        }

        // Draining and graceful states stop new placement while existing
        // replicas remain readable. Only final/immediate unmount is stale.
        if (mounted.status != SegmentStatus::UNMOUNTING) {
            return false;
        }
    }
    return true;
}

SegmentPool::ReadAccess::ReadAccess(AccessKey, const SegmentPool& segment_pool)
    : lock_(segment_pool.pool_mutex_),
      catalog_(segment_pool.catalog_),
      drivers_(segment_pool.region_drivers_),
      placement_(segment_pool.placement_index_) {}

const RegionCatalog& SegmentPool::ReadAccess::Catalog() const {
    return catalog_;
}

const PlacementIndex& SegmentPool::ReadAccess::Placement() const {
    return placement_;
}

}  // namespace mooncake
