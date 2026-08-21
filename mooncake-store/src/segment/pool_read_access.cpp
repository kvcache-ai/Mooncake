#include "segment/pool_read_access.h"

#include <algorithm>

#include "segment/pool.h"

namespace mooncake {

ScopedSegmentPoolReadAccess::ScopedSegmentPoolReadAccess(
    const SegmentPool* segment_pool)
    : segment_pool_(segment_pool), lock_(segment_pool->pool_mutex_) {}

ErrorCode ScopedSegmentPoolReadAccess::GetSegment(
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

ErrorCode ScopedSegmentPoolReadAccess::GetMountedRegion(
    const UUID& segment_id, MountedRegion& mounted_region) const {
    auto mounted = segment_pool_->mounted_regions_.find(segment_id);
    if (mounted == segment_pool_->mounted_regions_.end()) {
        return ErrorCode::SEGMENT_NOT_FOUND;
    }
    mounted_region = mounted->second;
    return ErrorCode::OK;
}

std::optional<RegionResourceView> ScopedSegmentPoolReadAccess::GetResourceView(
    const UUID& segment_id) const {
    auto mounted = segment_pool_->mounted_regions_.find(segment_id);
    if (mounted == segment_pool_->mounted_regions_.end()) {
        return std::nullopt;
    }
    const auto* resource = segment_pool_->GetResource(mounted->second);
    if (!resource) {
        return std::nullopt;
    }
    return RegionResourceView{resource->allocator.get(), &resource->target,
                              resource->active};
}

std::shared_ptr<BufferAllocatorBase> ScopedSegmentPoolReadAccess::GetAllocator(
    const UUID& segment_id) const {
    auto mounted = segment_pool_->mounted_regions_.find(segment_id);
    if (mounted == segment_pool_->mounted_regions_.end()) {
        return nullptr;
    }
    const auto* resource = segment_pool_->GetResource(mounted->second);
    return resource ? resource->allocator : nullptr;
}

std::optional<BufferAllocatorType>
ScopedSegmentPoolReadAccess::GetMemoryAllocatorType() const {
    const auto* driver = segment_pool_->GetDriver(RegionKind::HOST_MEMORY);
    return driver ? driver->allocator_type() : std::nullopt;
}

bool ScopedSegmentPoolReadAccess::HasKind(RegionKind kind) const {
    return std::any_of(
        segment_pool_->mounted_regions_.begin(),
        segment_pool_->mounted_regions_.end(),
        [kind](const auto& entry) { return entry.second.kind == kind; });
}

ErrorCode ScopedSegmentPoolReadAccess::GetClientSegments(
    const UUID& client_id, std::vector<Segment>& segments) const {
    auto client = segment_pool_->region_ids_by_client_.find(client_id);
    if (client == segment_pool_->region_ids_by_client_.end()) {
        return ErrorCode::SEGMENT_NOT_FOUND;
    }
    segments.clear();
    for (const auto& id : client->second) {
        auto mounted = segment_pool_->mounted_regions_.find(id);
        if (mounted != segment_pool_->mounted_regions_.end()) {
            segments.push_back(mounted->second.segment);
        }
    }
    return ErrorCode::OK;
}

void ScopedSegmentPoolReadAccess::GetMountedRegions(
    std::vector<std::pair<UUID, MountedRegion>>& regions) const {
    regions.clear();
    regions.reserve(segment_pool_->mounted_regions_.size());
    for (const auto& entry : segment_pool_->mounted_regions_) {
        regions.push_back(entry);
    }
}

void ScopedSegmentPoolReadAccess::GetActiveGroupNames(
    std::vector<std::string>& names) const {
    names.clear();
    const auto groups =
        segment_pool_->placement_index_.GetView().active_groups();
    names.reserve(groups.size());
    for (const auto* group : groups) {
        names.push_back(group->name);
    }
}

void ScopedSegmentPoolReadAccess::GetAllSegmentNames(
    std::vector<std::string>& names) const {
    names.clear();
    names.reserve(segment_pool_->region_ids_by_group_name_.size());
    for (const auto& [name, _] : segment_pool_->region_ids_by_group_name_) {
        names.push_back(name);
    }
}

ErrorCode ScopedSegmentPoolReadAccess::QuerySegments(std::string_view name,
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

void ScopedSegmentPoolReadAccess::GetUnreadyRegions(
    std::vector<std::pair<UUID, MountedRegion>>& regions) const {
    regions.clear();
    for (const auto& [id, mounted] : segment_pool_->mounted_regions_) {
        if (mounted.status != SegmentStatus::OK) {
            regions.emplace_back(id, mounted);
        }
    }
}

ErrorCode ScopedSegmentPoolReadAccess::GetClientIdBySegmentName(
    std::string_view name, UUID& client_id) const {
    auto owner = segment_pool_->owner_client_by_group_name_.find(name);
    if (owner == segment_pool_->owner_client_by_group_name_.end()) {
        return ErrorCode::SEGMENT_NOT_FOUND;
    }
    client_id = owner->second;
    return ErrorCode::OK;
}

bool ScopedSegmentPoolReadAccess::ExistsSegmentName(
    std::string_view name) const {
    return segment_pool_->region_ids_by_group_name_.contains(name);
}

bool ScopedSegmentPoolReadAccess::IsSegmentAllocatable(
    std::string_view name) const {
    return segment_pool_->placement_index_.GetView().Find(name) != nullptr;
}

ErrorCode ScopedSegmentPoolReadAccess::GetSegmentStatusByName(
    std::string_view name, SegmentStatus& status) const {
    auto group = segment_pool_->region_ids_by_group_name_.find(name);
    if (group == segment_pool_->region_ids_by_group_name_.end() ||
        group->second.empty()) {
        return ErrorCode::SEGMENT_NOT_FOUND;
    }

    status = SegmentStatus::UNDEFINED;
    for (const auto& id : group->second) {
        auto mounted = segment_pool_->mounted_regions_.find(id);
        if (mounted == segment_pool_->mounted_regions_.end()) {
            return ErrorCode::INTERNAL_ERROR;
        }
        if (SegmentStatusAvailabilityRank(mounted->second.status) <
            SegmentStatusAvailabilityRank(status)) {
            status = mounted->second.status;
        }
    }
    return ErrorCode::OK;
}

ErrorCode ScopedSegmentPoolReadAccess::GetSegmentStatusById(
    const UUID& id, SegmentStatus& status) const {
    auto mounted = segment_pool_->mounted_regions_.find(id);
    if (mounted == segment_pool_->mounted_regions_.end()) {
        return ErrorCode::SEGMENT_NOT_FOUND;
    }
    status = mounted->second.status;
    return ErrorCode::OK;
}

void ScopedSegmentPoolReadAccess::GetClientRegions(
    std::vector<std::pair<UUID, std::vector<UUID>>>& clients) const {
    clients.clear();
    clients.reserve(segment_pool_->region_ids_by_client_.size());
    for (const auto& entry : segment_pool_->region_ids_by_client_) {
        clients.push_back(entry);
    }
}

}  // namespace mooncake
