#include "segment/resource_view.h"

#include <algorithm>

#include "segment/pool.h"

namespace mooncake {

ErrorCode RegionResourceReadView::GetSegment(
    const std::shared_ptr<BufferAllocatorBase>& allocator,
    Segment& segment) const {
    if (!allocator) {
        return ErrorCode::INVALID_PARAMS;
    }
    for (const auto& [_, mounted] : segment_pool_->catalog_.mounted_regions_) {
        const auto* resource = segment_pool_->GetResource(mounted);
        if (resource && resource->allocator == allocator) {
            segment = mounted.segment;
            return ErrorCode::OK;
        }
    }
    return ErrorCode::SEGMENT_NOT_FOUND;
}

std::shared_ptr<BufferAllocatorBase> RegionResourceReadView::GetAllocator(
    const UUID& region_id) const {
    auto mounted = segment_pool_->catalog_.mounted_regions_.find(region_id);
    if (mounted == segment_pool_->catalog_.mounted_regions_.end()) {
        return nullptr;
    }
    const auto* resource = segment_pool_->GetResource(mounted->second);
    return resource ? resource->allocator : nullptr;
}

std::optional<BufferAllocatorType>
RegionResourceReadView::GetMemoryAllocatorType() const {
    const auto* driver = segment_pool_->GetDriver(RegionKind::HOST_MEMORY);
    return driver ? driver->allocator_type() : std::nullopt;
}

bool RegionResourceReadView::HasKind(RegionKind kind) const {
    return std::any_of(
        segment_pool_->catalog_.mounted_regions_.begin(),
        segment_pool_->catalog_.mounted_regions_.end(),
        [kind](const auto& entry) { return entry.second.kind == kind; });
}

ErrorCode RegionResourceReadView::QueryGroup(std::string_view name,
                                             size_t& used,
                                             size_t& capacity) const {
    auto* group = segment_pool_->placement_index_.GetView().Find(name);
    if (!group) {
        return ErrorCode::SEGMENT_NOT_FOUND;
    }
    used = 0;
    capacity = 0;
    for (const auto* target : group->targets) {
        used += target->Used();
        capacity += target->Capacity();
    }
    return capacity == 0 ? ErrorCode::SEGMENT_NOT_FOUND : ErrorCode::OK;
}

ErrorCode RegionResourceReadView::QueryRegion(const UUID& id, size_t& used,
                                              size_t& capacity) const {
    const auto mounted = segment_pool_->catalog_.mounted_regions_.find(id);
    if (mounted == segment_pool_->catalog_.mounted_regions_.end()) {
        return ErrorCode::SEGMENT_NOT_FOUND;
    }
    const auto* resource = segment_pool_->GetResource(mounted->second);
    if (!resource) {
        return ErrorCode::INTERNAL_ERROR;
    }
    used = resource->target.Used();
    capacity = resource->target.Capacity();
    return ErrorCode::OK;
}

bool RegionResourceReadView::IsInactive(
    const std::shared_ptr<BufferAllocatorBase>& allocator,
    std::string_view allocation_binding) const {
    if (!allocator) {
        return false;
    }
    bool matched_unmounting = false;
    for (const auto& [_, mounted] : segment_pool_->catalog_.mounted_regions_) {
        const auto* resource = segment_pool_->GetResource(mounted);
        if (!resource || resource->allocator != allocator ||
            (mounted.kind == RegionKind::CXL &&
             mounted.segment.name != allocation_binding)) {
            continue;
        }

        // Draining and graceful states stop new placement while existing
        // replicas remain readable. Only final/immediate unmount is stale.
        if (mounted.status != SegmentStatus::UNMOUNTING) {
            return false;
        }
        matched_unmounting = true;
    }
    return matched_unmounting;
}

}  // namespace mooncake
