#pragma once

#include <shared_mutex>
#include <unordered_set>
#include <utility>

#include "placement/index.h"
#include "placement/replica_allocator.h"
#include "placement/replica_placement.h"
#include "segment/catalog.h"
#include "segment/region_driver.h"
#include "segment/snapshot.h"
#include "segment/usage.h"
#include "segment/recovery.h"
#include "storage_usage.h"

namespace mooncake {

class SegmentPool final {
    struct AccessKey {
        explicit AccessKey() = default;
    };

   public:
    class WriteAccess;
    class ReadAccess;

    explicit SegmentPool(
        RegionDriverRegistry region_drivers,
        PlacementPolicyType policy = PlacementPolicyType::RANDOM,
        const LocalSsdManager* local_ssd = nullptr);

    tl::expected<std::vector<Replica>, ErrorCode> AllocateReplicas(
        const ReplicaAllocationRequest& request,
        PlacementDiagnostics* diagnostics = nullptr) const;
    tl::expected<Replica, ErrorCode> AllocateInSegment(std::string_view name,
                                                       size_t size) const;
    bool UsesHostAffinity() const { return allocation_.UsesHostAffinity(); }
    bool SupportsAllocatorSnapshots() const;
    bool IsAllocationSizeSupported(size_t size) const;
    ~SegmentPool();

    tl::expected<RemountRequest, ErrorCode> PlanRemount(
        std::span<const Segment> segments, const UUID& client_id) const;
    tl::expected<PreparedRemount, ErrorCode> PrepareRemount(
        RemountRequest request, std::shared_ptr<ClientLivenessRecord> liveness);
    std::unordered_set<std::string> InstallRecovery(
        std::unique_ptr<SegmentRecovery> recovery);
    bool RestoreBufferBindings(
        const std::unordered_map<UUID, std::shared_ptr<ClientLivenessRecord>,
                                 boost::hash<UUID>>& clients,
        std::span<AllocatedBuffer* const> buffers);

    WriteAccess AcquireWriteAccess();
    ReadAccess AcquireReadAccess() const;

    // Capture detached data without acquiring pool/allocator locks. Only call
    // in a forked snapshot child or with all relevant state externally
    // quiesced. After capture, the result is independent of this pool's
    // lifetime/mutations.
    tl::expected<SegmentPoolSnapshot, ErrorCode> CaptureSnapshot() const;

    // Consumes the snapshot, staging all resources before replacing the pool.
    // Preparation failure leaves published state unchanged. Temporary readers
    // may omit capacity accounting.
    tl::expected<void, ErrorCode> RestoreSnapshot(
        SegmentPoolSnapshot snapshot, bool account_capacity_metrics);

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
    void ClearRecovery();
    ErrorCode ValidateRemount(std::span<const Segment> segments,
                              const UUID& client_id) const;
    friend class PreparedRemount;
    std::unique_ptr<SegmentRecovery> recovery_;
    friend class Serializer<AllocatedBuffer>;

    RegionDriver* GetDriver(RegionKind kind);
    const RegionDriver* GetDriver(RegionKind kind) const;
    RegionResource* GetResource(const MountedRegion& mounted);
    const RegionResource* GetResource(const MountedRegion& mounted) const;

    struct UnmountState;
    std::unordered_map<UUID, std::unique_ptr<UnmountState>, boost::hash<UUID>>
        unmounts_;
    std::unordered_map<UUID, UUID, boost::hash<UUID>> unmount_by_region_;
    std::unordered_map<std::string, size_t> reserved_names_;

    ReplicaPlacement allocation_;
    mutable std::shared_mutex pool_mutex_;
    PlacementIndex placement_index_;
    RegionDriverRegistry region_drivers_;
    RegionCatalog catalog_;
    std::unordered_set<UUID, boost::hash<UUID>> capacity_accounted_region_ids_;
    std::shared_ptr<StorageUsageTracker> usage_tracker_ =
        std::make_shared<StorageUsageTracker>();
};

}  // namespace mooncake
