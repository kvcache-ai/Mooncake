#pragma once

#include <memory>
#include <optional>
#include <shared_mutex>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "segment/region_driver.h"
#include "segment/pool_types.h"

namespace mooncake {

class SegmentPool;

struct RegionResourceView final {
    // Raw pointers remain valid while the owning SegmentPoolView holds the
    // pool's shared lock.
    const BufferAllocatorBase* allocator;
    const AllocationTarget* target;
    bool active;
};

class SegmentPoolView final {
   public:
    explicit SegmentPoolView(const SegmentPool* segment_pool);
    SegmentPoolView(SegmentPoolView&&) noexcept = default;
    SegmentPoolView& operator=(SegmentPoolView&&) noexcept = default;
    SegmentPoolView(const SegmentPoolView&) = delete;
    SegmentPoolView& operator=(const SegmentPoolView&) = delete;

    ErrorCode GetSegment(const std::shared_ptr<BufferAllocatorBase>& allocator,
                         Segment& segment) const;
    ErrorCode GetMountedRegion(const UUID& segment_id,
                               MountedRegion& mounted_region) const;
    std::optional<RegionResourceView> GetResourceView(
        const UUID& segment_id) const;
    std::shared_ptr<BufferAllocatorBase> GetAllocator(
        const UUID& segment_id) const;
    std::optional<BufferAllocatorType> GetMemoryAllocatorType() const;
    bool HasKind(RegionKind kind) const;
    ErrorCode GetClientSegments(const UUID& client_id,
                                std::vector<Segment>& segments) const;
    void GetMountedRegions(
        std::vector<std::pair<UUID, MountedRegion>>& regions) const;
    void GetActiveGroupNames(std::vector<std::string>& names) const;
    void GetAllSegmentNames(std::vector<std::string>& names) const;
    ErrorCode QuerySegments(std::string_view name, size_t& used,
                            size_t& capacity) const;
    void GetUnreadyRegions(
        std::vector<std::pair<UUID, MountedRegion>>& regions) const;
    ErrorCode GetClientIdBySegmentName(std::string_view name,
                                       UUID& client_id) const;
    bool ExistsSegmentName(std::string_view name) const;
    bool IsSegmentAllocatable(std::string_view name) const;
    ErrorCode GetSegmentStatusByName(std::string_view name,
                                     SegmentStatus& status) const;
    ErrorCode GetSegmentStatusById(const UUID& id, SegmentStatus& status) const;
    void GetClientRegions(
        std::vector<std::pair<UUID, std::vector<UUID>>>& clients) const;

   private:
    const SegmentPool* segment_pool_;
    std::shared_lock<std::shared_mutex> lock_;
};

}  // namespace mooncake
