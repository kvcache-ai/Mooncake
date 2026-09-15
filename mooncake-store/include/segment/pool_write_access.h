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
#include "segment/queries.h"
#include "segment/operations.h"

namespace mooncake {

class SegmentPool::WriteAccess final : public SegmentQueries {
    struct TransactionKey {
        explicit TransactionKey() = default;
    };

   public:
    class RegionMountTxn;
    class RegionUnmountTxn;
    class RegionGracefulUnmountTxn;

    enum class AdoptMode { Insert, ReplacePool };

    WriteAccess(AccessKey, SegmentPool& segment_pool);

    ClientUnmountBatch BeginClientUnmount(const UUID& client_id);
    tl::expected<SegmentUnmountOperation, ErrorCode> BeginUnmount(
        const UUID& segment_id, const UUID& client_id);
    ErrorCode ReleaseUnmountedResources(const UUID& operation_id);
    ErrorCode AcknowledgeUnmount(const UUID& operation_id);
    ErrorCode StartGracefulUnmount(const UUID& segment_id,
                                   const UUID& client_id);
    ErrorCode StartDrain(std::span<const std::string> sources,
                         std::span<const std::string> targets);
    void FinishDrain(std::string_view name);
    void CancelDrain(std::span<const std::string> names);
    bool IsNameReserved(std::string_view name) const;

    ErrorCode MountSegment(const Segment& segment, const UUID& client_id,
                           std::shared_ptr<ClientLivenessRecord> liveness = {});
    tl::expected<RegionMountTxn, ErrorCode> PrepareMount(const Segment& segment,
                                                         const UUID& client_id);
    // Descriptors must use the canonical transport endpoint. The recovery
    // caller resolves segment-name aliases before entering the Pool.
    tl::expected<RegionMountTxn, ErrorCode> PrepareRestore(
        const Segment& segment, const UUID& client_id,
        std::span<const AllocatedBuffer::Descriptor> descriptors);
    // ReplacePool ignores conflicts with the old catalog. The caller must
    // validate the replacement and Clear() before committing transactions.
    tl::expected<RegionMountTxn, ErrorCode> PrepareAdopt(
        MountedRegion mounted, std::shared_ptr<BufferAllocatorBase> allocator,
        bool account_capacity_metrics, AdoptMode mode = AdoptMode::Insert);
    void Clear() noexcept;

    tl::expected<RegionUnmountTxn, ErrorCode> PrepareUnmount(
        const UUID& segment_id, const UUID& client_id);
    tl::expected<RegionGracefulUnmountTxn, ErrorCode> PrepareGracefulUnmount(
        const UUID& segment_id, const UUID& client_id);

    void BindClientLiveness(
        const UUID& client_id,
        const std::shared_ptr<ClientLivenessRecord>& client_liveness);
    bool BindBufferToSegment(const UUID& segment_id, AllocatedBuffer& buffer);
    bool RebindBufferToOwningSegment(AllocatedBuffer& buffer);

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
        const Segment& segment, const UUID& client_id,
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
