#pragma once

#include <shared_mutex>

#include "segment/pool.h"
#include "segment/queries.h"
#include "placement/index.h"
#include "segment/catalog.h"
#include "segment/region_driver.h"

namespace mooncake {

class SegmentPool::ReadAccess final : public SegmentQueries {
   public:
    ReadAccess(ReadAccess&&) noexcept = default;
    ReadAccess& operator=(ReadAccess&&) noexcept = delete;
    ReadAccess(const ReadAccess&) = delete;
    ReadAccess& operator=(const ReadAccess&) = delete;

    tl::expected<BufferSnapshot, ErrorCode> CaptureBufferSnapshot(
        const AllocatedBuffer& buffer) const;
    tl::expected<std::unique_ptr<AllocatedBuffer>, ErrorCode> RestoreBuffer(
        const BufferSnapshot& snapshot) const;

    // Retained owner identity, not a snapshot of its changing liveness state.
    // Null means absent or awaiting explicit snapshot owner reconstruction.
    ClientSessionPtr FindClientSession(const UUID& region_id) const;

    StorageUsage GetResourceUsage(const UUID& region_id) const;
    ReadAccess(AccessKey, const SegmentPool& segment_pool);

   private:
    friend class SegmentPoolTestPeer;

    std::shared_ptr<BufferAllocatorBase> GetAllocator(
        const UUID& region_id) const;

    std::shared_lock<std::shared_mutex> lock_;
    const RegionCatalog& catalog_;
    const SegmentPool& pool_;
};

}  // namespace mooncake
