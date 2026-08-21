#include "segment/pool.h"

#include "segment/pool_write_access.h"
#include "segment/pool_read_access.h"

#include <algorithm>
#include <limits>
#include <numeric>

#include "master_config.h"
#include "master_metric_manager.h"
#include "segment/region_initial_state.h"

namespace mooncake {
namespace {

RegionResourceSpec MakeResourceSpec(const Segment& segment) {
    return RegionResourceSpec{segment.id, segment.name, segment.base,
                              segment.size, segment.te_endpoint};
}

}  // namespace

SegmentPool::SegmentPool(const MasterServiceConfig& config)
    : SegmentPool(RegionDriverConfig{config.memory_allocator, config.enable_cxl,
                                     config.cxl_path, config.cxl_size}) {}

SegmentPool::SegmentPool(const RegionDriverConfig& config)
    : SegmentPool(CreateRegionDrivers(config)) {}

ScopedSegmentPoolWriteAccess SegmentPool::AcquireWriteAccess() {
    return ScopedSegmentPoolWriteAccess(this, pool_mutex_);
}

ScopedSegmentPoolReadAccess SegmentPool::AcquireReadAccess() const {
    return ScopedSegmentPoolReadAccess(this);
}

RegionDriver* SegmentPool::GetDriver(RegionKind kind) {
    auto it = region_drivers_.find(kind);
    return it == region_drivers_.end() ? nullptr : it->second.get();
}

const RegionDriver* SegmentPool::GetDriver(RegionKind kind) const {
    auto it = region_drivers_.find(kind);
    return it == region_drivers_.end() ? nullptr : it->second.get();
}

RegionResource* SegmentPool::GetResource(const MountedRegion& mounted) {
    auto* driver = GetDriver(mounted.kind);
    return driver ? driver->GetResource(mounted.segment.id) : nullptr;
}

const RegionResource* SegmentPool::GetResource(
    const MountedRegion& mounted) const {
    auto* driver = GetDriver(mounted.kind);
    return driver ? driver->GetResource(mounted.segment.id) : nullptr;
}

tl::expected<std::vector<Replica>, ErrorCode> SegmentPool::Allocate(
    PlacementPolicyType policy_type, const ReplicaPlacementRequest& request,
    std::optional<LocalSSDMetricsView> local_ssd_metrics,
    PlacementDiagnostics* diagnostics) {
    ScopedPlacementReadAccess placement(placement_index_, regions_by_host_,
                                        owner_client_by_group_name_,
                                        pool_mutex_);
    if (diagnostics) {
        diagnostics->has_sufficient_active_group_count =
            placement.GetView().size() >= request.replica_count;
    }

    std::span<PlacementGroup* const> host_ordered_groups;
    if (!request.writer_host_id.empty()) {
        thread_local std::vector<PlacementGroup*> host_ordered_scratch;
        placement.GetHostOrderedGroups(
            request.writer_host_id, request.object_key, host_ordered_scratch);
        host_ordered_groups = host_ordered_scratch;
    }

    const ReplicaAllocationRequest resolved{
        .size = request.size,
        .replica_count = request.replica_count,
        .preferred_group = request.preferred_group,
        .preferred_groups = request.preferred_groups,
        .resolved_preferred_groups = host_ordered_groups,
        .excluded_groups = request.excluded_groups,
        .replica_type = request.replica_type,
    };
    return replica_allocator_.Allocate(placement, policy_type, resolved,
                                       local_ssd_metrics);
}

tl::expected<Replica, ErrorCode> SegmentPool::AllocateFrom(
    size_t size, std::string_view group_name, ReplicaType replica_type) {
    ScopedPlacementReadAccess placement(placement_index_, regions_by_host_,
                                        owner_client_by_group_name_,
                                        pool_mutex_);
    return replica_allocator_.AllocateFrom(placement, size, group_name,
                                           replica_type);
}

std::optional<UUID> SegmentPool::GetOwnerClientId(
    std::string_view group_name) const {
    std::shared_lock lock(pool_mutex_);
    auto owner = owner_client_by_group_name_.find(group_name);
    return owner == owner_client_by_group_name_.end()
               ? std::nullopt
               : std::optional<UUID>(owner->second);
}

tl::expected<RegionInitialState, ErrorCode>
SegmentPool::BuildRegionInitialState(
    const Segment& segment,
    std::span<const AllocatedBuffer::Descriptor> descriptors) const {
    return mooncake::BuildRegionInitialState(MakeResourceSpec(segment),
                                             descriptors);
}

void SegmentPool::ReleaseCapacityMetrics() {
    std::unordered_set<std::string> segment_names;
    for (const auto& id : capacity_accounted_regions_) {
        auto mounted = mounted_regions_.find(id);
        if (mounted != mounted_regions_.end()) {
            MasterMetricManager::instance().dec_total_mem_capacity(
                mounted->second.segment.name, mounted->second.segment.size);
            segment_names.insert(mounted->second.segment.name);
        }
    }
    for (const auto& name : segment_names) {
        MasterMetricManager::instance().remove_segment_metrics(name);
    }
    capacity_accounted_regions_.clear();
}

bool SegmentPool::HasSegmentByEndpoint(const std::string& endpoint) const {
    std::shared_lock lock(pool_mutex_);
    return std::any_of(mounted_regions_.begin(), mounted_regions_.end(),
                       [&](const auto& entry) {
                           return entry.second.segment.te_endpoint == endpoint;
                       });
}

bool SegmentPool::IsResourceInactive(
    const std::shared_ptr<BufferAllocatorBase>& allocator) const {
    if (!allocator) {
        return false;
    }
    std::shared_lock lock(pool_mutex_);
    for (const auto& [_, mounted] : mounted_regions_) {
        const auto* resource = GetResource(mounted);
        if (resource && resource->allocator == allocator) {
            return !resource->active || mounted.status != SegmentStatus::OK;
        }
    }
    return false;
}

bool SegmentPool::GetSegmentBasicInfo(const UUID& segment_id,
                                      std::string& segment_name,
                                      std::string& te_endpoint) const {
    std::shared_lock lock(pool_mutex_);
    auto mounted = mounted_regions_.find(segment_id);
    if (mounted == mounted_regions_.end()) {
        return false;
    }
    segment_name = mounted->second.segment.name;
    te_endpoint = mounted->second.segment.te_endpoint;
    return true;
}

}  // namespace mooncake
