#pragma once

#include <boost/functional/hash.hpp>
#include <memory>
#include <shared_mutex>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <vector>

#include "placement/index.h"
#include "rpc_types.h"
#include "segment/status.h"

namespace mooncake {

namespace test {
class MasterServiceTenantQuotaTest;
}

struct MountedNoFSegment {
    NoFSegment segment;
    UUID client_id;
    SegmentStatus status;
    std::shared_ptr<BufferAllocatorBase> allocator;
    std::unique_ptr<AllocationTarget> target;
};

struct MountedNoFSegmentSnapshot {
    UUID client_id;
    NoFSegment segment;
    SegmentStatus status;
};

class NoFSegmentManager;

class ScopedNoFSegmentWriteAccess final {
   public:
    ErrorCode MountSegment(const NoFSegment& segment, const UUID& client_id);
    ErrorCode ReMountSegment(const std::vector<NoFSegment>& segments,
                             const UUID& client_id);
    ErrorCode PrepareUnmountSegment(const UUID& segment_id,
                                    const UUID& client_id);
    ErrorCode CommitUnmountSegment(const UUID& segment_id,
                                   const UUID& client_id);

   private:
    explicit ScopedNoFSegmentWriteAccess(NoFSegmentManager* manager);

    NoFSegmentManager* nof_segment_manager_;
    std::unique_lock<std::shared_mutex> lock_;

    friend class NoFSegmentManager;
};

class NoFSegmentManager final {
   public:
    explicit NoFSegmentManager(
        BufferAllocatorType memory_allocator = BufferAllocatorType::CACHELIB)
        : memory_allocator_(memory_allocator) {}

    ScopedNoFSegmentWriteAccess AcquireWriteAccess();
    ScopedPlacementReadAccess AcquirePlacementAccess() const;
    void GetMountedSegmentsSnapshot(
        std::vector<MountedNoFSegmentSnapshot>& segments) const;
    tl::expected<std::vector<NoFSegmentOwnerInfo>, ErrorCode> GetSegmentsByName(
        std::string_view segment_name) const;

   private:
    mutable std::shared_mutex manager_mutex_;
    const BufferAllocatorType memory_allocator_;
    PlacementIndex placement_index_;
    std::unordered_map<UUID, MountedNoFSegment, boost::hash<UUID>>
        mounted_segments_;

    friend class ScopedNoFSegmentWriteAccess;
    friend class SegmentTest;
    friend class test::MasterServiceTenantQuotaTest;
};

}  // namespace mooncake
