#pragma once

#include <array>
#include <cstddef>
#include <map>
#include <memory>
#include <optional>
#include <set>
#include <shared_mutex>
#include <span>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

#include "placement/target.h"
#include "types.h"
#include "utils/transparent_string_hash.h"

namespace mooncake {

// A placement group contains allocation-equivalent targets. The index keys
// groups by both logical name and target kind, so native and CXL targets may
// share a name without being mixed during allocation.
struct PlacementGroup final {
    PlacementGroup(std::string_view group_name, PlacementTargetKind group_kind)
        : name(group_name), kind(group_kind) {}

    bool AddTarget(const PlacementTarget& target);
    bool RemoveTarget(const PlacementTarget& target);
    bool ReplaceTarget(const PlacementTarget& expected,
                       const PlacementTarget& replacement);

    std::string name;
    const PlacementTargetKind kind;
    std::vector<const PlacementTarget*> targets;
};

class PlacementIndex final {
   public:
    PlacementIndex() = default;
    PlacementIndex(PlacementIndex&&) = default;
    PlacementIndex& operator=(PlacementIndex&&) = default;
    PlacementIndex(const PlacementIndex&) = delete;
    PlacementIndex& operator=(const PlacementIndex&) = delete;

    bool AddTarget(std::string_view name, const PlacementTarget* target);
    bool RemoveTarget(std::string_view name, const PlacementTarget* target);
    bool ReplaceTarget(std::string_view name, const PlacementTarget* expected,
                       const PlacementTarget* replacement);
    void Clear();

    const PlacementGroup* Find(std::string_view name,
                               PlacementTargetKind kind) const;
    bool Contains(std::string_view name, PlacementTargetKind kind) const {
        return Find(name, kind) != nullptr;
    }
    void GetActiveGroupNames(PlacementTargetKind kind,
                             std::vector<std::string>& names) const;
    std::span<const PlacementGroup* const> active_groups(
        PlacementTargetKind kind) const {
        return active_groups_by_kind_[KindIndex(kind)];
    }
    size_t size(PlacementTargetKind kind) const noexcept {
        return active_groups_by_kind_[KindIndex(kind)].size();
    }
    bool empty(PlacementTargetKind kind) const noexcept {
        return active_groups_by_kind_[KindIndex(kind)].empty();
    }

   private:
    static constexpr size_t KindIndex(PlacementTargetKind kind) noexcept {
        return static_cast<size_t>(kind);
    }

    using GroupMap =
        std::unordered_map<std::string, std::unique_ptr<PlacementGroup>,
                           TransparentStringHash, std::equal_to<>>;
    std::array<GroupMap, kPlacementTargetKindCount> groups_by_kind_;
    std::array<std::vector<const PlacementGroup*>, kPlacementTargetKindCount>
        active_groups_by_kind_;
};

using HostRegionIndex =
    std::map<std::string, std::map<std::string, std::set<UUID>>, std::less<>>;
using OwnerClientByGroupName =
    std::unordered_map<std::string, UUID, TransparentStringHash,
                       std::equal_to<>>;

class ScopedPlacementReadAccess final {
   public:
    ScopedPlacementReadAccess(const PlacementIndex& placement,
                              std::shared_mutex& mutex)
        : placement_(placement), lock_(mutex) {}

    ScopedPlacementReadAccess(
        const PlacementIndex& placement, const HostRegionIndex& regions_by_host,
        const OwnerClientByGroupName& owner_client_by_group_name,
        std::shared_mutex& mutex)
        : placement_(placement),
          regions_by_host_(&regions_by_host),
          owner_client_by_group_name_(&owner_client_by_group_name),
          lock_(mutex) {}

    const PlacementIndex& GetView() const { return placement_; }

    void GetHostOrderedGroups(std::string_view writer_host_id,
                              std::string_view key,
                              PlacementTargetKind target_kind,
                              std::vector<const PlacementGroup*>& output) const;
    std::optional<UUID> GetOwnerClientId(std::string_view group_name) const;

   private:
    const PlacementIndex& placement_;
    const HostRegionIndex* regions_by_host_{nullptr};
    const OwnerClientByGroupName* owner_client_by_group_name_{nullptr};
    std::shared_lock<std::shared_mutex> lock_;
};

}  // namespace mooncake
