#pragma once

#include <shared_mutex>
#include <utility>

#include "placement/index.h"
#include "segment/catalog.h"
#include "segment/region_driver.h"
#include "segment/usage.h"
#include "storage_usage.h"

namespace mooncake {

class ScopedSegmentPoolWriteAccess;
class ScopedSegmentPoolReadAccess;
class SegmentPoolSnapshotView;

class SegmentPool final {
   public:
    explicit SegmentPool(RegionDriverRegistry region_drivers)
        : region_drivers_(std::move(region_drivers)) {}
    ~SegmentPool();

    ScopedSegmentPoolWriteAccess AcquireWriteAccess();
    ScopedSegmentPoolReadAccess AcquireReadAccess() const;
    ScopedPlacementReadAccess AcquirePlacementAccess() const;
    SegmentPoolSnapshotView GetSnapshotView() const noexcept;

    [[nodiscard]] StorageUsageSnapshot GetMemoryUsageSnapshot() const;
    [[nodiscard]] StorageUsage GetMemoryUsage() const noexcept {
        return usage_tracker_->GetUsage();
    }

   private:
    void ReleaseCapacityMetrics();
    RegionDriver* GetDriver(RegionKind kind);
    const RegionDriver* GetDriver(RegionKind kind) const;
    RegionResource* GetResource(const MountedRegion& mounted);
    const RegionResource* GetResource(const MountedRegion& mounted) const;

    mutable std::shared_mutex pool_mutex_;
    PlacementIndex placement_index_;
    RegionDriverRegistry region_drivers_;
    RegionCatalog catalog_;
    std::shared_ptr<StorageUsageTracker> usage_tracker_ =
        std::make_shared<StorageUsageTracker>();

    friend class ScopedSegmentPoolWriteAccess;
    friend class ScopedSegmentPoolReadAccess;
    friend class RegionResourceReadView;
    friend class SegmentPoolSnapshotView;
    friend class SegmentTest;
};

}  // namespace mooncake
