#pragma once

#include <shared_mutex>

#include "segment/pool.h"
#include "placement/index.h"
#include "segment/catalog.h"
#include "segment/region_driver.h"

namespace mooncake {

class SegmentPool::ReadAccess final {
   public:
    ReadAccess(ReadAccess&&) noexcept = default;
    ReadAccess& operator=(ReadAccess&&) noexcept = delete;
    ReadAccess(const ReadAccess&) = delete;
    ReadAccess& operator=(const ReadAccess&) = delete;

    const RegionCatalog& Catalog() const;
    const PlacementIndex& Placement() const;

    std::shared_ptr<BufferAllocatorBase> GetAllocator(
        const UUID& region_id) const;
    // Usage of active placement candidates for this segment name and kind.
    ErrorCode QueryAllocationCandidates(std::string_view name,
                                        AllocationCandidateKind kind,
                                        size_t& used, size_t& capacity) const;
    bool IsInactive(const std::shared_ptr<BufferAllocatorBase>& allocator,
                    std::string_view allocation_binding) const;

    ReadAccess(AccessKey, const SegmentPool& segment_pool);

   private:
    const RegionDriver* GetDriver(RegionKind kind) const;
    const RegionResource* GetResource(const MountedRegion& mounted) const;

    std::shared_lock<std::shared_mutex> lock_;
    const RegionCatalog& catalog_;
    const RegionDriverRegistry& drivers_;
    const PlacementIndex& placement_;
};

}  // namespace mooncake
