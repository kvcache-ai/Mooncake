#pragma once

#include <memory>
#include <utility>
#include <vector>

#include "segment/mounted_region.h"
#include "segment/region_driver.h"

namespace mooncake {

class ScopedSegmentPoolWriteAccess;

// A prepared mount that has not yet been published to the catalog or
// placement index. Destroying an uncommitted transaction releases the staged
// driver resource.
class RegionMountTxn final {
   public:
    RegionMountTxn(RegionMountTxn&&) noexcept = default;
    RegionMountTxn& operator=(RegionMountTxn&&) noexcept = default;
    RegionMountTxn(const RegionMountTxn&) = delete;
    RegionMountTxn& operator=(const RegionMountTxn&) = delete;

    const std::vector<std::unique_ptr<AllocatedBuffer>>& imported_buffers()
        const noexcept {
        return resource_.imported_buffers();
    }
    std::vector<std::unique_ptr<AllocatedBuffer>> TakeImportedBuffers() {
        return resource_.TakeImportedBuffers();
    }
    uint64_t imported_requested_bytes() const noexcept {
        return imported_requested_bytes_;
    }

   private:
    RegionMountTxn(MountedRegion mounted, bool existed,
                   bool account_capacity_metrics,
                   PreparedRegionResource resource,
                   uint64_t imported_requested_bytes = 0)
        : mounted_(std::move(mounted)),
          existed_(existed),
          account_capacity_metrics_(account_capacity_metrics),
          resource_(std::move(resource)),
          imported_requested_bytes_(imported_requested_bytes) {}

    MountedRegion mounted_;
    bool existed_;
    bool account_capacity_metrics_;
    PreparedRegionResource resource_;
    uint64_t imported_requested_bytes_;
    bool committed_{false};

    friend class ScopedSegmentPoolWriteAccess;
};

// Immediate unmount spans metadata cleanup while the Pool lock is released.
// It therefore requires an explicit commit or rollback instead of destructor
// rollback.
class RegionUnmountTxn final {
   public:
    RegionUnmountTxn(RegionUnmountTxn&&) noexcept = default;
    RegionUnmountTxn& operator=(RegionUnmountTxn&&) noexcept = default;
    RegionUnmountTxn(const RegionUnmountTxn&) = delete;
    RegionUnmountTxn& operator=(const RegionUnmountTxn&) = delete;

    const Segment& segment() const noexcept { return segment_; }

   private:
    RegionUnmountTxn(Segment segment, UUID client_id)
        : segment_(std::move(segment)), client_id_(client_id) {}

    Segment segment_;
    UUID client_id_;
    bool finished_{false};

    friend class ScopedSegmentPoolWriteAccess;
};

}  // namespace mooncake
