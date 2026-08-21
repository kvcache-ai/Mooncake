#pragma once

#include <boost/functional/hash.hpp>
#include <optional>
#include <shared_mutex>
#include <span>
#include <string>
#include <string_view>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "placement/index.h"
#include "placement/replica_allocator.h"
#include "segment/region_driver.h"
#include "segment/pool_types.h"

namespace mooncake {

class MasterServiceConfig;
class ScopedSegmentPoolWriteAccess;
class ScopedSegmentPoolReadAccess;

class SegmentPool final {
   public:
    explicit SegmentPool(const MasterServiceConfig& config);
    explicit SegmentPool(const RegionDriverConfig& config);
    explicit SegmentPool(RegionDriverRegistry region_drivers)
        : region_drivers_(std::move(region_drivers)) {}

    void ReleaseCapacityMetrics();

    ScopedSegmentPoolWriteAccess AcquireWriteAccess();
    ScopedSegmentPoolReadAccess AcquireReadAccess() const;

    tl::expected<std::vector<Replica>, ErrorCode> Allocate(
        PlacementPolicyType policy_type, const ReplicaPlacementRequest& request,
        std::optional<LocalSSDMetricsView> local_ssd_metrics = std::nullopt,
        PlacementDiagnostics* diagnostics = nullptr);
    tl::expected<Replica, ErrorCode> AllocateFrom(
        size_t size, std::string_view group_name,
        ReplicaType replica_type = ReplicaType::MEMORY);
    std::optional<UUID> GetOwnerClientId(std::string_view group_name) const;
    tl::expected<RegionInitialState, ErrorCode> BuildRegionInitialState(
        const Segment& segment,
        std::span<const AllocatedBuffer::Descriptor> descriptors) const;

    bool HasSegmentByEndpoint(const std::string& endpoint) const;
    bool IsResourceInactive(
        const std::shared_ptr<BufferAllocatorBase>& allocator) const;
    bool GetSegmentBasicInfo(const UUID& segment_id, std::string& segment_name,
                             std::string& te_endpoint) const;

   private:
    RegionDriver* GetDriver(RegionKind kind);
    const RegionDriver* GetDriver(RegionKind kind) const;
    RegionResource* GetResource(const MountedRegion& mounted);
    const RegionResource* GetResource(const MountedRegion& mounted) const;

    mutable std::shared_mutex pool_mutex_;
    PlacementIndex placement_index_;
    ReplicaAllocator replica_allocator_;
    RegionDriverRegistry region_drivers_;
    std::unordered_map<UUID, MountedRegion, boost::hash<UUID>> mounted_regions_;
    std::unordered_map<UUID, std::vector<UUID>, boost::hash<UUID>>
        region_ids_by_client_;
    OwnerClientByGroupName owner_client_by_group_name_;
    std::unordered_map<std::string, std::vector<UUID>, TransparentStringHash,
                       std::equal_to<>>
        region_ids_by_group_name_;
    HostRegionIndex regions_by_host_;
    std::unordered_set<UUID, boost::hash<UUID>> capacity_accounted_regions_;

    friend class ScopedSegmentPoolWriteAccess;
    friend class ScopedSegmentPoolReadAccess;
    friend class SegmentTest;
};

}  // namespace mooncake
