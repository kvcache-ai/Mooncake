#pragma once

#include <boost/functional/hash.hpp>
#include <memory>
#include <ostream>
#include <shared_mutex>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "placement/replica_allocator.h"
#include "rpc_types.h"
#include "segment/pool_types.h"

namespace mooncake {

namespace test {
class MasterServiceTenantQuotaTest;
}

struct MountedNoFSegment {
    NoFSegment segment;
    UUID client_id;
    SegmentStatus status;
    std::shared_ptr<BufferAllocatorBase> buf_allocator;
    std::unique_ptr<AllocationTarget> allocation_target;
};

struct MountedNoFSegmentSnapshot {
    UUID segment_id;
    UUID client_id;
    NoFSegment segment;
    SegmentStatus status;
};

inline std::ostream& operator<<(
    std::ostream& os, const MountedNoFSegmentSnapshot& snapshot) noexcept {
    os << "{segment_id=" << snapshot.segment_id
       << ", client_id=" << snapshot.client_id
       << ", segment.id=" << snapshot.segment.id
       << ", segment.name=" << snapshot.segment.name
       << ", segment.base=" << snapshot.segment.base
       << ", segment.size=" << snapshot.segment.size
       << ", segment.te_endpoint=" << snapshot.segment.te_endpoint
       << ", status=" << snapshot.status << "}";
    return os;
}

class NoFSegmentManager;

class ScopedNoFSegmentWriteAccess final {
   public:
    ScopedNoFSegmentWriteAccess(NoFSegmentManager* manager,
                                std::shared_mutex& mutex)
        : nof_segment_manager_(manager), lock_(mutex) {}

    ErrorCode MountSegment(const NoFSegment& segment, const UUID& client_id);
    ErrorCode ReMountSegment(const std::vector<NoFSegment>& segments,
                             const UUID& client_id);
    ErrorCode PrepareUnmountSegment(const UUID& segment_id,
                                    size_t& metrics_dec_capacity);
    ErrorCode CommitUnmountSegment(const UUID& segment_id,
                                   const UUID& client_id,
                                   const size_t& metrics_dec_capacity);
    ErrorCode GetClientSegments(const UUID& client_id,
                                std::vector<NoFSegment>& segments) const;
    ErrorCode GetMountedSegments(
        std::vector<MountedNoFSegmentSnapshot>& segments) const;
    ErrorCode GetAllSegments(std::vector<std::string>& all_segments);
    ErrorCode QuerySegments(const std::string& segment, size_t& used,
                            size_t& capacity);

   private:
    NoFSegmentManager* nof_segment_manager_;
    std::unique_lock<std::shared_mutex> lock_;
};

class NoFSegmentManager final {
   public:
    explicit NoFSegmentManager(
        BufferAllocatorType memory_allocator = BufferAllocatorType::CACHELIB)
        : memory_allocator_(memory_allocator) {}

    ScopedNoFSegmentWriteAccess AcquireWriteAccess() {
        return ScopedNoFSegmentWriteAccess(this, manager_mutex_);
    }
    tl::expected<std::vector<Replica>, ErrorCode> Allocate(
        PlacementPolicyType policy_type,
        const ReplicaPlacementRequest& request);
    void GetMountedSegmentsSnapshot(
        std::vector<MountedNoFSegmentSnapshot>& segments) const;
    tl::expected<std::vector<NoFSegmentOwnerInfo>, ErrorCode> GetSegmentsByName(
        const std::string& segment_name) const {
        std::shared_lock lock(manager_mutex_);
        std::vector<NoFSegmentOwnerInfo> result;
        for (const auto& [segment_id, mounted] : mounted_segments_) {
            if (mounted.segment.name == segment_name) {
                result.emplace_back(segment_id, mounted.client_id);
            }
        }
        if (result.empty()) {
            return tl::make_unexpected(ErrorCode::SEGMENT_NOT_FOUND);
        }
        return result;
    }

   private:
    mutable std::shared_mutex manager_mutex_;
    const BufferAllocatorType memory_allocator_;
    PlacementIndex placement_index_;
    ReplicaAllocator replica_allocator_;
    std::unordered_map<UUID, MountedNoFSegment, boost::hash<UUID>>
        mounted_segments_;
    std::unordered_map<UUID, std::vector<UUID>, boost::hash<UUID>>
        segment_ids_by_client_;
    OwnerClientByGroupName owner_client_by_group_name_;

    friend class ScopedNoFSegmentWriteAccess;
    friend class SegmentTest;
    friend class test::MasterServiceTenantQuotaTest;
};

}  // namespace mooncake
