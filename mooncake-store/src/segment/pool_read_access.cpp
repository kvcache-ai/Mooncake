#include "segment/pool_read_access.h"

namespace mooncake {

ClientSessionPtr SegmentPool::ReadAccess::FindClientSession(
    const UUID& region_id) const {
    const auto* mounted = catalog_.Find(region_id);
    const auto* resource = mounted ? pool_.GetResource(*mounted) : nullptr;
    return resource ? resource->candidate->client_session() : nullptr;
}

StorageUsage SegmentPool::ReadAccess::GetResourceUsage(
    const UUID& region_id) const {
    const auto allocator = GetAllocator(region_id);
    return allocator ? StorageUsage{allocator->size(), allocator->capacity()}
                     : StorageUsage{};
}

std::shared_ptr<BufferAllocatorBase> SegmentPool::ReadAccess::GetAllocator(
    const UUID& region_id) const {
    auto mounted = catalog_.Find(region_id);
    if (mounted == nullptr) {
        return nullptr;
    }
    const auto* resource = pool_.GetResource(*mounted);
    return resource ? resource->allocator() : nullptr;
}

SegmentPool::ReadAccess::ReadAccess(AccessKey, const SegmentPool& segment_pool)
    : SegmentQueries(segment_pool.catalog_, segment_pool.placement_index_,
                     segment_pool.allocation_.Kind()),
      lock_(segment_pool.pool_mutex_),
      catalog_(segment_pool.catalog_),
      pool_(segment_pool) {}

}  // namespace mooncake
