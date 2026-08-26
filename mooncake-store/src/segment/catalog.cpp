#include "segment/catalog.h"

#include <algorithm>

namespace mooncake {

ErrorCode RegionCatalog::GetMountedRegion(const UUID& region_id,
                                          MountedRegion& mounted_region) const {
    auto mounted = mounted_regions_.find(region_id);
    if (mounted == mounted_regions_.end()) {
        return ErrorCode::SEGMENT_NOT_FOUND;
    }
    mounted_region = mounted->second;
    return ErrorCode::OK;
}

ErrorCode RegionCatalog::GetClientSegments(
    const UUID& client_id, std::vector<Segment>& segments) const {
    auto client = region_ids_by_client_.find(client_id);
    if (client == region_ids_by_client_.end()) {
        return ErrorCode::SEGMENT_NOT_FOUND;
    }
    segments.clear();
    for (const auto& id : client->second) {
        auto mounted = mounted_regions_.find(id);
        if (mounted != mounted_regions_.end()) {
            segments.push_back(mounted->second.segment);
        }
    }
    return ErrorCode::OK;
}

void RegionCatalog::GetMountedRegions(
    std::vector<std::pair<UUID, MountedRegion>>& regions) const {
    regions.clear();
    regions.reserve(mounted_regions_.size());
    for (const auto& entry : mounted_regions_) {
        regions.push_back(entry);
    }
}

void RegionCatalog::GetAllGroupNames(std::vector<std::string>& names) const {
    names.clear();
    names.reserve(region_ids_by_group_name_.size());
    for (const auto& [name, _] : region_ids_by_group_name_) {
        names.push_back(name);
    }
}

void RegionCatalog::GetUnreadyRegions(
    std::vector<std::pair<UUID, MountedRegion>>& regions) const {
    regions.clear();
    for (const auto& [id, mounted] : mounted_regions_) {
        if (mounted.status != SegmentStatus::OK) {
            regions.emplace_back(id, mounted);
        }
    }
}

std::optional<UUID> RegionCatalog::FindOwnerClientId(
    std::string_view name) const {
    auto owner = owner_client_by_group_name_.find(name);
    return owner == owner_client_by_group_name_.end()
               ? std::nullopt
               : std::optional<UUID>(owner->second);
}

bool RegionCatalog::HasRegionByEndpoint(std::string_view endpoint) const {
    return std::any_of(mounted_regions_.begin(), mounted_regions_.end(),
                       [endpoint](const auto& entry) {
                           return entry.second.segment.te_endpoint == endpoint;
                       });
}

bool RegionCatalog::ContainsGroup(std::string_view name) const {
    return region_ids_by_group_name_.contains(name);
}

ErrorCode RegionCatalog::GetGroupStatus(std::string_view name,
                                        SegmentStatus& status) const {
    auto group = region_ids_by_group_name_.find(name);
    if (group == region_ids_by_group_name_.end() || group->second.empty()) {
        return ErrorCode::SEGMENT_NOT_FOUND;
    }

    status = SegmentStatus::UNDEFINED;
    for (const auto& id : group->second) {
        auto mounted = mounted_regions_.find(id);
        if (mounted == mounted_regions_.end()) {
            return ErrorCode::INTERNAL_ERROR;
        }
        if (SegmentStatusAvailabilityRank(mounted->second.status) <
            SegmentStatusAvailabilityRank(status)) {
            status = mounted->second.status;
        }
    }
    return ErrorCode::OK;
}

ErrorCode RegionCatalog::GetRegionStatus(const UUID& id,
                                         SegmentStatus& status) const {
    auto mounted = mounted_regions_.find(id);
    if (mounted == mounted_regions_.end()) {
        return ErrorCode::SEGMENT_NOT_FOUND;
    }
    status = mounted->second.status;
    return ErrorCode::OK;
}

void RegionCatalog::GetClientRegions(
    std::vector<std::pair<UUID, std::vector<UUID>>>& clients) const {
    clients.clear();
    clients.reserve(region_ids_by_client_.size());
    for (const auto& entry : region_ids_by_client_) {
        clients.push_back(entry);
    }
}

}  // namespace mooncake
