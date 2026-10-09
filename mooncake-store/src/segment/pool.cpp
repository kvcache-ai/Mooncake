#include "segment/pool.h"

#include "segment/pool_write_access.h"
#include "segment/pool_read_access.h"

#include "master_metric_manager.h"

#include <utility>
#include <unordered_set>

namespace mooncake {

SegmentPool::~SegmentPool() { ReleaseCapacityMetrics(); }

SegmentPool::WriteAccess SegmentPool::AcquireWriteAccess() {
    return WriteAccess(AccessKey{}, *this);
}

SegmentPool::ReadAccess SegmentPool::AcquireReadAccess() const {
    return ReadAccess(AccessKey{}, *this);
}

ScopedPlacementReadAccess SegmentPool::AcquirePlacementAccess() const {
    return ScopedPlacementReadAccess(placement_index_, catalog_, pool_mutex_);
}

tl::expected<Replica, ErrorCode> SegmentPool::AllocateInSegment(
    std::string_view segment_name, AllocationCandidateKind kind, size_t size,
    ReplicaType replica_type) const {
    auto access = AcquirePlacementAccess();
    return ReplicaAllocator(PreferredOnlyPlacementPolicy(kind))
        .AllocateFrom(access, size, segment_name, replica_type);
}

StorageUsageSnapshot SegmentPool::GetMemoryUsageSnapshot() const {
    std::shared_lock lock(pool_mutex_);
    StorageUsageSnapshot snapshot;
    std::unordered_set<const BufferAllocatorBase*> counted;
    for (const auto& mounted : catalog_.Regions()) {
        const auto* resource = GetResource(mounted);
        if (!resource || !resource->allocator() ||
            !counted.insert(resource->allocator().get()).second) {
            continue;
        }
        const size_t used = resource->allocator()->size();
        const size_t capacity = resource->allocator()->capacity();
        snapshot.used_bytes += used;
        snapshot.capacity_bytes += capacity;
        auto& region =
            snapshot.segments[resource->allocator()->getSegmentName()];
        region.used_bytes += used;
        region.capacity_bytes += capacity;
    }
    return snapshot;
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
    for (const auto& id : capacity_accounted_region_ids_) {
        auto mounted = catalog_.Find(id);
        if (mounted != nullptr) {
            MasterMetricManager::instance().dec_total_mem_capacity(
                mounted->segment.name, mounted->segment.size);
            segment_names.insert(mounted->segment.name);
        }
    }
    for (const auto& name : segment_names) {
        MasterMetricManager::instance().remove_segment_metrics(name);
    }
    capacity_accounted_region_ids_.clear();
}

}  // namespace mooncake
