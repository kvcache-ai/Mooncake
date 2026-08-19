#pragma once

#include <memory>
#include <optional>
#include <shared_mutex>
#include <string>
#include <utility>
#include <vector>

#include "segment/region_driver.h"
#include "segment/pool_types.h"

namespace mooncake {

class SegmentPool;

struct RegionResourceView final {
    const RegionResourceSpec* spec;
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
    void GetMountedRegions(
        std::vector<std::pair<UUID, MountedRegion>>& regions) const;
    void GetActiveGroupNames(std::vector<std::string>& names) const;
    void GetClientRegions(
        std::vector<std::pair<UUID, std::vector<UUID>>>& clients) const;

   private:
    const SegmentPool* segment_pool_;
    std::shared_lock<std::shared_mutex> lock_;
};

}  // namespace mooncake
