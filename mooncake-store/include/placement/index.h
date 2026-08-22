#pragma once

#include <cstddef>
#include <functional>
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

namespace mooncake {

struct PlacementGroup final {
    std::string name;
    std::vector<AllocationTarget*> targets;
};

struct TransparentStringHash {
    using is_transparent = void;

    size_t operator()(std::string_view value) const noexcept {
        return std::hash<std::string_view>{}(value);
    }
    size_t operator()(const std::string& value) const noexcept {
        return (*this)(std::string_view(value));
    }
    size_t operator()(const char* value) const noexcept {
        return (*this)(std::string_view(value));
    }
};

class PlacementReadView;

class PlacementIndex final {
   public:
    PlacementIndex() = default;
    PlacementIndex(PlacementIndex&&) = default;
    PlacementIndex& operator=(PlacementIndex&&) = default;
    PlacementIndex(const PlacementIndex&) = delete;
    PlacementIndex& operator=(const PlacementIndex&) = delete;

    bool AddTarget(std::string_view name, AllocationTarget* target);
    bool RemoveTarget(std::string_view name, AllocationTarget* target);
    bool ReplaceTarget(std::string_view name, AllocationTarget* expected,
                       AllocationTarget* replacement);
    bool Contains(std::string_view name, const AllocationTarget* target) const;
    void Clear();

    PlacementReadView GetView() const;

   private:
    std::unordered_map<std::string, PlacementGroup*, TransparentStringHash,
                       std::equal_to<>>
        by_name_;
    std::vector<PlacementGroup*> active_groups_;
    std::vector<std::unique_ptr<PlacementGroup>> owned_groups_;

    friend class PlacementReadView;
};

class PlacementReadView final {
   public:
    explicit PlacementReadView(const PlacementIndex* index) : index_(index) {}

    PlacementGroup* Find(std::string_view name) const;
    std::span<PlacementGroup* const> active_groups() const {
        return index_->active_groups_;
    }
    size_t size() const noexcept { return index_->active_groups_.size(); }
    bool empty() const noexcept { return index_->active_groups_.empty(); }

   private:
    const PlacementIndex* index_;
};

using HostRegionIndex =
    std::map<std::string, std::map<std::string, std::set<UUID>>, std::less<>>;
using ClientByRegionName =
    std::unordered_map<std::string, UUID, TransparentStringHash,
                       std::equal_to<>>;

class ScopedPlacementAccess final {
   public:
    ScopedPlacementAccess(const PlacementIndex& placement,
                          std::shared_mutex& mutex)
        : placement_(placement), lock_(mutex) {}

    ScopedPlacementAccess(const PlacementIndex& placement,
                          const HostRegionIndex& regions_by_host,
                          const ClientByRegionName& client_by_name,
                          std::shared_mutex& mutex)
        : placement_(placement),
          regions_by_host_(&regions_by_host),
          client_by_name_(&client_by_name),
          lock_(mutex) {}

    PlacementReadView view() const { return placement_.GetView(); }

    void GetHostOrderedGroups(std::string_view writer_host_id,
                              std::string_view key,
                              std::vector<PlacementGroup*>& output) const;
    std::optional<UUID> GetOwnerClientId(std::string_view group_name) const;

   private:
    const PlacementIndex& placement_;
    const HostRegionIndex* regions_by_host_{nullptr};
    const ClientByRegionName* client_by_name_{nullptr};
    std::shared_lock<std::shared_mutex> lock_;
};

}  // namespace mooncake
