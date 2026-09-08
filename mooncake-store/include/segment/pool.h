#pragma once

#include <shared_mutex>
#include <unordered_set>
#include <utility>

#include "placement/index.h"
#include "placement/replica_allocator.h"
#include "segment/catalog.h"
#include "segment/region_driver.h"
#include "segment/usage.h"
#include "storage_usage.h"

namespace mooncake {

class SegmentPool final {
    struct AccessKey {
        explicit AccessKey() = default;
    };

   public:
    class WriteAccess;
    class ReadAccess;

    explicit SegmentPool(RegionDriverRegistry region_drivers)
        : region_drivers_(std::move(region_drivers)) {}
    ~SegmentPool();

    WriteAccess AcquireWriteAccess();
    ReadAccess AcquireReadAccess() const;

    // Holds the Pool read lock through candidate selection and allocation.
    template <ReplicaPlacementPolicy Policy = RandomPlacementPolicy>
    tl::expected<std::vector<Replica>, ErrorCode> AllocateReplicas(
        const ReplicaAllocationRequest& request, Policy policy = {},
        PlacementDiagnostics* diagnostics = nullptr) const {
        auto access = AcquirePlacementAccess();
        return ReplicaAllocator<Policy>(std::move(policy))
            .Allocate(access, request, diagnostics);
    }
    // Allocates one replica under the read lock, with no segment/kind fallback.
    tl::expected<Replica, ErrorCode> AllocateInSegment(
        std::string_view segment_name, AllocationCandidateKind kind,
        size_t size, ReplicaType replica_type = ReplicaType::MEMORY) const;

    [[nodiscard]] StorageUsageSnapshot GetMemoryUsageSnapshot() const;
    [[nodiscard]] StorageUsage GetMemoryUsage() const noexcept {
        return usage_tracker_->GetUsage();
    }

   private:
    ScopedPlacementReadAccess AcquirePlacementAccess() const;
    void ReleaseCapacityMetrics();
    RegionDriver* GetDriver(RegionKind kind);
    const RegionDriver* GetDriver(RegionKind kind) const;
    RegionResource* GetResource(const MountedRegion& mounted);
    const RegionResource* GetResource(const MountedRegion& mounted) const;

    mutable std::shared_mutex pool_mutex_;
    PlacementIndex placement_index_;
    RegionDriverRegistry region_drivers_;
    RegionCatalog catalog_;
    std::unordered_set<UUID, boost::hash<UUID>> capacity_accounted_region_ids_;
    std::shared_ptr<StorageUsageTracker> usage_tracker_ =
        std::make_shared<StorageUsageTracker>();
};

}  // namespace mooncake
