#pragma once

#include <boost/functional/hash.hpp>
#include <optional>
#include <string>
#include <string_view>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "placement/index.h"
#include "segment/mounted_region.h"
#include "utils/transparent_string_hash.h"

namespace mooncake {

class RegionResourceReadView;
class ScopedSegmentPoolWriteAccess;
class SegmentPool;

using MountedRegionById =
    std::unordered_map<UUID, MountedRegion, boost::hash<UUID>>;
using RegionIdsByClient =
    std::unordered_map<UUID, std::vector<UUID>, boost::hash<UUID>>;
using RegionIdsByGroupName =
    std::unordered_map<std::string, std::vector<UUID>, TransparentStringHash,
                       std::equal_to<>>;
using CapacityAccountedRegionIds = std::unordered_set<UUID, boost::hash<UUID>>;

class RegionCatalog final {
   public:
    ErrorCode GetMountedRegion(const UUID& region_id,
                               MountedRegion& mounted_region) const;
    ErrorCode GetClientSegments(const UUID& client_id,
                                std::vector<Segment>& segments) const;
    void GetMountedRegions(
        std::vector<std::pair<UUID, MountedRegion>>& regions) const;
    void GetAllGroupNames(std::vector<std::string>& names) const;
    void GetUnreadyRegions(
        std::vector<std::pair<UUID, MountedRegion>>& regions) const;
    std::optional<UUID> FindOwnerClientId(std::string_view name) const;
    bool HasRegionByEndpoint(std::string_view endpoint) const;
    bool ContainsGroup(std::string_view name) const;
    ErrorCode GetGroupStatus(std::string_view name,
                             SegmentStatus& status) const;
    ErrorCode GetRegionStatus(const UUID& id, SegmentStatus& status) const;
    void GetClientRegions(
        std::vector<std::pair<UUID, std::vector<UUID>>>& clients) const;

   private:
    MountedRegionById mounted_regions_;
    RegionIdsByClient region_ids_by_client_;
    OwnerClientByGroupName owner_client_by_group_name_;
    RegionIdsByGroupName region_ids_by_group_name_;
    HostRegionIndex regions_by_host_;
    CapacityAccountedRegionIds capacity_accounted_region_ids_;

    friend class SegmentPool;
    friend class ScopedSegmentPoolWriteAccess;
    friend class RegionResourceReadView;
    friend class SegmentTest;
};

}  // namespace mooncake
