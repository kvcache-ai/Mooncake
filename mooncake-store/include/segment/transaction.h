#pragma once

#include <memory>
#include <utility>
#include <vector>

#include "segment/pool_write_access.h"
#include "segment/mounted_region.h"
#include "segment/region_driver.h"

namespace mooncake {

class PlacementIndex;

// A prepared mount that has not yet been published to the catalog or
// placement index. Destroying an uncommitted transaction releases the staged
// driver resource.
class SegmentPool::WriteAccess::RegionMountTxn final {
   public:
    RegionMountTxn(RegionMountTxn&&) noexcept = default;
    RegionMountTxn& operator=(RegionMountTxn&&) noexcept = default;
    RegionMountTxn(const RegionMountTxn&) = delete;
    RegionMountTxn& operator=(const RegionMountTxn&) = delete;

    // Commit with the write access that prepared this transaction, before
    // releasing its lock. A conflicting catalog/resource change is rejected
    // before the staged resource is published.
    [[nodiscard]] ErrorCode Commit(SegmentPool::WriteAccess& access) noexcept;

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

    RegionMountTxn(TransactionKey, MountedRegion mounted, bool existed,
                   bool account_capacity_metrics,
                   PreparedRegionResource resource,
                   uint64_t imported_requested_bytes = 0,
                   std::weak_ptr<BufferAllocatorBase> previous_allocator = {})
        : mounted_(std::move(mounted)),
          existed_(existed),
          account_capacity_metrics_(account_capacity_metrics),
          resource_(std::move(resource)),
          imported_requested_bytes_(imported_requested_bytes),
          previous_allocator_(std::move(previous_allocator)) {}

   private:
    MountedRegion mounted_;
    bool existed_;
    bool account_capacity_metrics_;
    PreparedRegionResource resource_;
    uint64_t imported_requested_bytes_;
    std::weak_ptr<BufferAllocatorBase> previous_allocator_;
    bool committed_{false};
};

// Immediate unmount spans metadata cleanup while the Pool lock is released.
// It therefore requires an explicit commit or rollback instead of destructor
// rollback.
class SegmentPool::WriteAccess::RegionUnmountTxn final {
   public:
    RegionUnmountTxn(RegionUnmountTxn&& other) noexcept;
    RegionUnmountTxn& operator=(RegionUnmountTxn&&) = delete;
    RegionUnmountTxn(const RegionUnmountTxn&) = delete;
    RegionUnmountTxn& operator=(const RegionUnmountTxn&) = delete;

    // Requires write access to the originating Pool; it may be reacquired
    // after metadata cleanup. Each call consumes the transaction, including
    // on failure; the moved-from transaction cannot be used again.
    ErrorCode Commit(SegmentPool::WriteAccess& access) &&;
    ErrorCode Rollback(SegmentPool::WriteAccess& access) &&;

    const Segment& segment() const noexcept { return segment_; }

    RegionUnmountTxn(TransactionKey, SegmentPool::WriteAccess& access,
                     const MountedRegion& mounted, RegionResource& resource);

   private:
    Segment segment_;
    UUID client_id_;
    SegmentStatus previous_status_;
    uint64_t generation_;
    bool finished_{false};
};

// Stops new placement while existing replicas remain readable. Destruction
// leaves the region draining; callers may prepare again to resume finalization.
class SegmentPool::WriteAccess::RegionGracefulUnmountTxn final {
   public:
    RegionGracefulUnmountTxn(RegionGracefulUnmountTxn&& other) noexcept;
    RegionGracefulUnmountTxn& operator=(RegionGracefulUnmountTxn&&) = delete;
    RegionGracefulUnmountTxn(const RegionGracefulUnmountTxn&) = delete;
    RegionGracefulUnmountTxn& operator=(const RegionGracefulUnmountTxn&) =
        delete;

    // Reacquire write access to the originating Pool after draining replicas.
    // Finalize consumes the transaction, including on failure.
    [[nodiscard]] ErrorCode Finalize(SegmentPool::WriteAccess& access) &&;

    RegionGracefulUnmountTxn(TransactionKey, SegmentPool::WriteAccess& access,
                             const MountedRegion& mounted,
                             RegionResource& resource);

   private:
    UUID segment_id_;
    UUID client_id_;
    uint64_t generation_;
    bool finished_{false};
};

}  // namespace mooncake
