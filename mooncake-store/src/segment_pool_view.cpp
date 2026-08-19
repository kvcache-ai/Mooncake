#include "segment_pool_view.h"

#include <algorithm>

#include "segment_pool.h"

namespace mooncake {

SegmentPoolView::SegmentPoolView(const SegmentPool* segment_pool)
    : segment_pool_(segment_pool), lock_(segment_pool->pool_mutex_) {}

ErrorCode SegmentPoolView::GetSegment(
    const std::shared_ptr<BufferAllocatorBase>& allocator,
    Segment& segment) const {
    if (!allocator) {
        return ErrorCode::INVALID_PARAMS;
    }
    for (const auto& [_, mounted] : segment_pool_->mounted_regions_) {
        const auto* resource = segment_pool_->GetResource(mounted);
        if (resource && resource->allocator == allocator) {
            segment = mounted.segment;
            return ErrorCode::OK;
        }
    }
    return ErrorCode::SEGMENT_NOT_FOUND;
}

ErrorCode SegmentPoolView::GetMountedRegion(
    const UUID& segment_id, MountedRegion& mounted_region) const {
    auto mounted = segment_pool_->mounted_regions_.find(segment_id);
    if (mounted == segment_pool_->mounted_regions_.end()) {
        return ErrorCode::SEGMENT_NOT_FOUND;
    }
    mounted_region = mounted->second;
    return ErrorCode::OK;
}

std::optional<RegionResourceView> SegmentPoolView::GetResourceView(
    const UUID& segment_id) const {
    auto mounted = segment_pool_->mounted_regions_.find(segment_id);
    if (mounted == segment_pool_->mounted_regions_.end()) {
        return std::nullopt;
    }
    const auto* resource = segment_pool_->GetResource(mounted->second);
    if (!resource) {
        return std::nullopt;
    }
    return RegionResourceView{&resource->spec, resource->allocator.get(),
                              &resource->target, resource->active};
}

std::shared_ptr<BufferAllocatorBase> SegmentPoolView::GetAllocator(
    const UUID& segment_id) const {
    auto mounted = segment_pool_->mounted_regions_.find(segment_id);
    if (mounted == segment_pool_->mounted_regions_.end()) {
        return nullptr;
    }
    const auto* resource = segment_pool_->GetResource(mounted->second);
    return resource ? resource->allocator : nullptr;
}

std::optional<BufferAllocatorType> SegmentPoolView::GetMemoryAllocatorType()
    const {
    const auto* driver = segment_pool_->GetDriver(RegionKind::HOST_MEMORY);
    return driver ? driver->allocator_type() : std::nullopt;
}

bool SegmentPoolView::HasKind(RegionKind kind) const {
    return std::any_of(
        segment_pool_->mounted_regions_.begin(),
        segment_pool_->mounted_regions_.end(),
        [kind](const auto& entry) { return entry.second.kind == kind; });
}

void SegmentPoolView::GetMountedRegions(
    std::vector<std::pair<UUID, MountedRegion>>& regions) const {
    regions.clear();
    regions.reserve(segment_pool_->mounted_regions_.size());
    for (const auto& entry : segment_pool_->mounted_regions_) {
        regions.push_back(entry);
    }
}

void SegmentPoolView::GetActiveGroupNames(
    std::vector<std::string>& names) const {
    names.clear();
    const auto groups =
        segment_pool_->placement_index_.GetView().active_groups();
    names.reserve(groups.size());
    for (const auto* group : groups) {
        names.push_back(group->name);
    }
}

void SegmentPoolView::GetClientRegions(
    std::vector<std::pair<UUID, std::vector<UUID>>>& clients) const {
    clients.clear();
    clients.reserve(segment_pool_->client_segments_.size());
    for (const auto& entry : segment_pool_->client_segments_) {
        clients.push_back(entry);
    }
}

}  // namespace mooncake
