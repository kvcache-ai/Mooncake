#pragma once

#include <memory>
#include <shared_mutex>
#include <span>
#include <string_view>
#include <utility>
#include <vector>

#include <ylt/util/tl/expected.hpp>

#include "segment/catalog.h"
#include "segment/pool.h"

namespace mooncake {

class SegmentPool::WriteAccess final {
    struct TransactionKey {
        explicit TransactionKey() = default;
    };

   public:
    class RegionMountTxn;
    class RegionUnmountTxn;
    class RegionGracefulUnmountTxn;

    WriteAccess(AccessKey, SegmentPool& segment_pool);

    ErrorCode MountSegment(const Segment& segment,
                           ClientSessionSharedPtr owner_session);
    tl::expected<RegionMountTxn, ErrorCode> PrepareMount(
        const Segment& segment, ClientSessionSharedPtr owner_session);
    // Descriptors must use the canonical transport endpoint. The recovery
    // caller resolves segment-name aliases before entering the Pool.
    tl::expected<RegionMountTxn, ErrorCode> PrepareRestore(
        const Segment& segment, ClientSessionSharedPtr owner_session,
        std::span<const AllocatedBuffer::Descriptor> descriptors);
    tl::expected<RegionMountTxn, ErrorCode> PrepareAdopt(
        MountedRegion mounted, std::shared_ptr<BufferAllocatorBase> allocator,
        bool account_capacity_metrics);
    ErrorCode ValidateRemountSegment(const Segment& segment,
                                     const UUID& client_id) const;
    void BindClientSession(const ClientSessionSharedPtr& owner_session);
    void Clear() noexcept;

    tl::expected<RegionUnmountTxn, ErrorCode> PrepareUnmount(
        const UUID& segment_id, const UUID& client_id);
    tl::expected<RegionGracefulUnmountTxn, ErrorCode> PrepareGracefulUnmount(
        const UUID& segment_id, const UUID& client_id);

    const RegionCatalog& Catalog() const;
    ErrorCode SetSegmentStatusByName(std::string_view segment_name,
                                     SegmentStatus status);

   private:
    ErrorCode PublishMount(
        const MountedRegion& mounted, bool existed,
        bool account_capacity_metrics, PreparedRegionResource& prepared,
        const std::weak_ptr<BufferAllocatorBase>& previous_allocator) noexcept;
    ErrorCode RestoreUnmountedRegion(const UUID& segment_id,
                                     const UUID& client_id,
                                     SegmentStatus previous_status,
                                     uint64_t generation);
    tl::expected<RegionMountTxn, ErrorCode> PrepareWithLiveAllocations(
        const Segment& segment, ClientSessionSharedPtr owner_session,
        const std::vector<LiveAllocation>& live_allocations,
        uint64_t imported_requested_bytes);
    ErrorCode EraseUnmountedRegion(const UUID& segment_id,
                                   const UUID& client_id,
                                   SegmentStatus expected_status,
                                   uint64_t generation);

    ErrorCode TransitionRegion(const MountedRegion& mounted,
                               RegionResource& resource, SegmentStatus status);

    SegmentPool& segment_pool_;
    std::unique_lock<std::shared_mutex> lock_;
    RegionCatalog& catalog_;
};

using RegionMountTxn = SegmentPool::WriteAccess::RegionMountTxn;
using RegionUnmountTxn = SegmentPool::WriteAccess::RegionUnmountTxn;
using RegionGracefulUnmountTxn =
    SegmentPool::WriteAccess::RegionGracefulUnmountTxn;

}  // namespace mooncake

#include "segment/transaction.h"
