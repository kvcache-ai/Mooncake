#include "segment/pool.h"
#include "pool_operations_internal.h"

#include "segment/pool_write_access.h"
#include "segment/pool_read_access.h"

#include "master_metric_manager.h"

#include <glog/logging.h>

#include <utility>
#include <unordered_set>

namespace mooncake {

SegmentPool::SegmentPool(RegionDriverRegistry region_drivers,
                         PlacementPolicyType policy,
                         const LocalSsdManager* local_ssd)
    : allocation_(policy, ReplicaPlacement::Backend::Memory, local_ssd),
      region_drivers_(std::move(region_drivers)) {
    if (allocation_.UsesHostAffinity()) {
        LOG(INFO) << "Local-first allocation strategy enabled";
    }
    for (const auto& [kind, driver] : region_drivers_) {
        if (auto allocator = driver->GetSharedAllocator()) {
            allocator->AttachUsageTracker(usage_tracker_);
        }
    }
}

tl::expected<std::vector<Replica>, ErrorCode> SegmentPool::AllocateReplicas(
    const ReplicaAllocationRequest& request,
    AllocationDiagnostics* diagnostics) const {
    auto access = AcquirePlacementAccess();
    auto resolved = request;
    if (request.replicas.count != 1 ||
        (!allocation_.UsesHostAffinity() &&
         !request.host_affinity.prefer_alloc_in_same_node)) {
        resolved.host_affinity = {};
    }
    return allocation_.Allocate(access, resolved, diagnostics);
}

tl::expected<Replica, ErrorCode> SegmentPool::AllocateInSegment(
    std::string_view name, size_t size) const {
    auto access = AcquirePlacementAccess();
    return allocation_.AllocateFrom(access, name, size);
}

bool SegmentPool::SupportsAllocatorSnapshots() const {
    const auto* driver = GetDriver(RegionKind::HOST_MEMORY);
    return driver && driver->allocator_type() == BufferAllocatorType::OFFSET;
}

bool SegmentPool::IsAllocationSizeSupported(size_t size) const {
    const auto* driver = GetDriver(RegionKind::HOST_MEMORY);
    return !driver ||
           driver->allocator_type() != BufferAllocatorType::CACHELIB ||
           size <= kMaxSliceSize;
}

SegmentPool::~SegmentPool() {
    ClearRecovery();
    for (const auto& mounted : catalog_.Regions()) {
        if (auto* resource = GetResource(mounted)) {
            resource->candidate->SetAvailability(false, false);
        }
    }
    ReleaseCapacityMetrics();
}

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
    const auto add_allocator = [&](const auto& allocator) {
        if (!allocator || !counted.insert(allocator.get()).second) {
            return;
        }
        const size_t used = allocator->size();
        const size_t capacity = allocator->capacity();
        snapshot.used_bytes += used;
        snapshot.capacity_bytes += capacity;
        auto& region = snapshot.segments[allocator->getSegmentName()];
        region.used_bytes += used;
        region.capacity_bytes += capacity;
    };
    for (const auto& [kind, driver] : region_drivers_) {
        add_allocator(driver->GetSharedAllocator());
    }
    for (const auto& mounted : catalog_.Regions()) {
        if (const auto* resource = GetResource(mounted)) {
            add_allocator(resource->allocator());
        }
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
