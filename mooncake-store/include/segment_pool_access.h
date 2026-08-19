#pragma once

#include <memory>
#include <shared_mutex>
#include <string>
#include <utility>
#include <vector>

#include <ylt/util/tl/expected.hpp>

#include "region_driver.h"
#include "segment_pool_types.h"

namespace mooncake {

class SegmentPool;

class PreparedMountedRegion final {
   public:
    PreparedMountedRegion(PreparedMountedRegion&&) noexcept = default;
    PreparedMountedRegion& operator=(PreparedMountedRegion&&) noexcept =
        default;
    PreparedMountedRegion(const PreparedMountedRegion&) = delete;
    PreparedMountedRegion& operator=(const PreparedMountedRegion&) = delete;

    const Segment& segment() const noexcept { return mounted_.segment; }
    bool existed() const noexcept { return existed_; }
    RegionResource* resource() const { return resource_.resource(); }
    const std::vector<std::unique_ptr<AllocatedBuffer>>& imported_buffers()
        const noexcept {
        return resource_.imported_buffers();
    }
    std::vector<std::unique_ptr<AllocatedBuffer>> TakeImportedBuffers() {
        return resource_.TakeImportedBuffers();
    }

   private:
    PreparedMountedRegion(MountedRegion mounted, bool existed,
                          bool account_capacity_metrics,
                          PreparedRegionResource resource)
        : mounted_(std::move(mounted)),
          existed_(existed),
          account_capacity_metrics_(account_capacity_metrics),
          resource_(std::move(resource)) {}

    MountedRegion mounted_;
    bool existed_;
    bool account_capacity_metrics_;
    PreparedRegionResource resource_;

    friend class ScopedSegmentPoolAccess;
};

class ScopedSegmentPoolAccess final {
   public:
    ScopedSegmentPoolAccess(SegmentPool* segment_pool, std::shared_mutex& mutex)
        : segment_pool_(segment_pool), lock_(mutex) {}

    ErrorCode MountSegment(const Segment& segment, const UUID& client_id);
    tl::expected<PreparedMountedRegion, ErrorCode> PrepareMount(
        const Segment& segment, const UUID& client_id,
        const RegionInitialState& initial_state = {});
    tl::expected<PreparedMountedRegion, ErrorCode> PrepareAdopt(
        MountedRegion mounted, std::shared_ptr<BufferAllocatorBase> allocator);
    void CommitMount(PreparedMountedRegion& prepared) noexcept;
    void Clear() noexcept;

    ErrorCode ValidateRemountSegment(const Segment& segment,
                                     const UUID& client_id) const;
    bool GetSegment(const UUID& segment_id, Segment& segment) const;
    ErrorCode PrepareUnmountSegment(const UUID& segment_id,
                                    size_t& metrics_dec_capacity);
    ErrorCode RollbackUnmountSegment(const UUID& segment_id);
    ErrorCode PrepareGracefulUnmountSegment(const UUID& segment_id);
    ErrorCode CommitUnmountSegment(const UUID& segment_id,
                                   const UUID& client_id,
                                   const size_t& metrics_dec_capacity);

    ErrorCode GetClientSegments(const UUID& client_id,
                                std::vector<Segment>& segments) const;
    ErrorCode GetAllSegments(std::vector<std::string>& all_segments);
    ErrorCode GetAllSegments(
        std::vector<std::pair<Segment, UUID>>& all_segments);
    ErrorCode GetAllSegmentNames(std::vector<std::string>& all_segment_names);
    ErrorCode QuerySegments(const std::string& segment, size_t& used,
                            size_t& capacity);
    ErrorCode GetUnreadySegments(
        std::vector<std::pair<Segment, UUID>>& unready_segments) const;
    ErrorCode GetClientIdBySegmentName(const std::string& segment_name,
                                       UUID& client_id) const;
    bool ExistsSegmentName(const std::string& segment_name) const;
    bool IsSegmentAllocatable(const std::string& segment_name) const;
    ErrorCode GetSegmentStatusByName(const std::string& segment_name,
                                     SegmentStatus& status) const;
    ErrorCode GetSegmentStatusById(const UUID& segment_id,
                                   SegmentStatus& status) const;
    ErrorCode SetSegmentStatusByName(const std::string& segment_name,
                                     SegmentStatus status);

   private:
    SegmentPool* segment_pool_;
    std::unique_lock<std::shared_mutex> lock_;
};

}  // namespace mooncake
