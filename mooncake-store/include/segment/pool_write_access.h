#pragma once

#include <memory>
#include <shared_mutex>
#include <span>
#include <string_view>
#include <utility>
#include <vector>

#include <ylt/util/tl/expected.hpp>

#include "segment/transaction.h"

namespace mooncake {

class SegmentPool;

class ScopedSegmentPoolWriteAccess final {
   public:
    ErrorCode MountSegment(const Segment& segment, const UUID& client_id);
    tl::expected<RegionMountTxn, ErrorCode> PrepareMount(const Segment& segment,
                                                         const UUID& client_id);
    tl::expected<RegionMountTxn, ErrorCode> PrepareRestore(
        const Segment& segment, const UUID& client_id,
        std::span<const AllocatedBuffer::Descriptor> descriptors);
    tl::expected<RegionMountTxn, ErrorCode> PrepareAdopt(
        MountedRegion mounted, std::shared_ptr<BufferAllocatorBase> allocator,
        bool account_capacity_metrics);
    void CommitMount(RegionMountTxn& transaction) noexcept;
    void Clear() noexcept;

    ErrorCode ValidateRemount(std::span<const Segment> segments,
                              const UUID& client_id) const;
    bool GetSegment(const UUID& segment_id, Segment& segment) const;
    tl::expected<RegionUnmountTxn, ErrorCode> PrepareUnmount(
        const UUID& segment_id, const UUID& client_id);
    ErrorCode RollbackUnmount(RegionUnmountTxn& transaction);
    ErrorCode CommitUnmount(RegionUnmountTxn& transaction);
    ErrorCode PrepareGracefulUnmountSegment(const UUID& segment_id,
                                            const UUID& client_id);
    ErrorCode FinalizeGracefulUnmount(const UUID& segment_id,
                                      const UUID& client_id);

    ErrorCode GetClientSegments(const UUID& client_id,
                                std::vector<Segment>& segments) const;
    bool ExistsSegmentName(std::string_view segment_name) const;
    bool IsSegmentAllocatable(std::string_view segment_name) const;
    ErrorCode GetSegmentStatusByName(std::string_view segment_name,
                                     SegmentStatus& status) const;
    ErrorCode SetSegmentStatusByName(std::string_view segment_name,
                                     SegmentStatus status);

   private:
    explicit ScopedSegmentPoolWriteAccess(SegmentPool* segment_pool);

    tl::expected<RegionMountTxn, ErrorCode> PrepareWithInitialState(
        const Segment& segment, const UUID& client_id,
        const RegionInitialState& initial_state,
        uint64_t imported_requested_bytes);
    ErrorCode EraseUnmountedRegion(const UUID& segment_id,
                                   const UUID& client_id,
                                   SegmentStatus expected_status);

    SegmentPool* segment_pool_;
    std::unique_lock<std::shared_mutex> lock_;

    friend class SegmentPool;
};

}  // namespace mooncake
