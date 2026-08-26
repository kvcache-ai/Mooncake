#include "segment/pool.h"

#include "segment/pool_write_access.h"
#include "segment/pool_read_access.h"

#include "master_metric_manager.h"

#include <utility>

namespace mooncake {

SegmentPool::~SegmentPool() { ReleaseCapacityMetrics(); }

ScopedSegmentPoolWriteAccess SegmentPool::AcquireWriteAccess() {
    return ScopedSegmentPoolWriteAccess(this);
}

ScopedSegmentPoolReadAccess SegmentPool::AcquireReadAccess() const {
    return ScopedSegmentPoolReadAccess(this);
}

ScopedPlacementReadAccess SegmentPool::AcquirePlacementAccess() const {
    return ScopedPlacementReadAccess(
        placement_index_, catalog_.regions_by_host_,
        catalog_.owner_client_by_group_name_, pool_mutex_);
}

RegionDriver* SegmentPool::GetDriver(RegionKind kind) {
    return const_cast<RegionDriver*>(std::as_const(*this).GetDriver(kind));
}

const RegionDriver* SegmentPool::GetDriver(RegionKind kind) const {
    auto it = region_drivers_.find(kind);
    return it == region_drivers_.end() ? nullptr : it->second.get();
}

RegionResource* SegmentPool::GetResource(const MountedRegion& mounted) {
    return const_cast<RegionResource*>(
        std::as_const(*this).GetResource(mounted));
}

const RegionResource* SegmentPool::GetResource(
    const MountedRegion& mounted) const {
    auto* driver = GetDriver(mounted.kind);
    return driver ? driver->GetResource(mounted.segment.id) : nullptr;
}

void SegmentPool::ReleaseCapacityMetrics() {
    std::unordered_set<std::string> segment_names;
    for (const auto& id : catalog_.capacity_accounted_region_ids_) {
        auto mounted = catalog_.mounted_regions_.find(id);
        if (mounted != catalog_.mounted_regions_.end()) {
            MasterMetricManager::instance().dec_total_mem_capacity(
                mounted->second.segment.name, mounted->second.segment.size);
            segment_names.insert(mounted->second.segment.name);
        }
    }
    for (const auto& name : segment_names) {
        MasterMetricManager::instance().remove_segment_metrics(name);
    }
    catalog_.capacity_accounted_region_ids_.clear();
}

}  // namespace mooncake
