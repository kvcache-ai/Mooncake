#include "placement/index.h"

#include <algorithm>
#include <iterator>

namespace mooncake {
namespace {

bool Contains(const std::vector<PlacementGroup*>& groups,
              const PlacementGroup* group) {
    return std::find(groups.begin(), groups.end(), group) != groups.end();
}

void AppendUnique(std::vector<PlacementGroup*>& groups, PlacementGroup* group) {
    if (group && !Contains(groups, group)) {
        groups.push_back(group);
    }
}

}  // namespace

bool PlacementIndex::AddTarget(std::string_view name,
                               AllocationTarget* target) {
    if (name.empty() || !target) {
        return false;
    }
    auto it = by_name_.find(name);
    PlacementGroup* group = nullptr;
    if (it == by_name_.end()) {
        auto owned = std::make_unique<PlacementGroup>();
        owned->name = name;
        group = owned.get();
        owned_groups_.push_back(std::move(owned));
        by_name_.emplace(group->name, group);
        active_groups_.push_back(group);
    } else {
        group = it->second;
    }
    if (std::find(group->targets.begin(), group->targets.end(), target) !=
        group->targets.end()) {
        return false;
    }
    group->targets.push_back(target);
    return true;
}

bool PlacementIndex::RemoveTarget(std::string_view name,
                                  AllocationTarget* target) {
    auto it = by_name_.find(name);
    if (it == by_name_.end()) {
        return false;
    }
    PlacementGroup* group = it->second;
    auto target_it =
        std::find(group->targets.begin(), group->targets.end(), target);
    if (target_it == group->targets.end()) {
        return false;
    }
    *target_it = group->targets.back();
    group->targets.pop_back();
    if (!group->targets.empty()) {
        return true;
    }

    auto active_it =
        std::find(active_groups_.begin(), active_groups_.end(), group);
    if (active_it != active_groups_.end()) {
        *active_it = active_groups_.back();
        active_groups_.pop_back();
    }
    by_name_.erase(it);
    auto owned_it =
        std::find_if(owned_groups_.begin(), owned_groups_.end(),
                     [&](const auto& owned) { return owned.get() == group; });
    if (owned_it != owned_groups_.end()) {
        *owned_it = std::move(owned_groups_.back());
        owned_groups_.pop_back();
    }
    return true;
}

bool PlacementIndex::ReplaceTarget(std::string_view name,
                                   AllocationTarget* expected,
                                   AllocationTarget* replacement) {
    if (!replacement) {
        return false;
    }
    auto it = by_name_.find(name);
    if (it == by_name_.end()) {
        return false;
    }
    auto target_it = std::find(it->second->targets.begin(),
                               it->second->targets.end(), expected);
    if (target_it == it->second->targets.end()) {
        return false;
    }
    *target_it = replacement;
    return true;
}

void PlacementIndex::Clear() {
    by_name_.clear();
    active_groups_.clear();
    owned_groups_.clear();
}

PlacementReadView PlacementIndex::GetView() const {
    return PlacementReadView(*this);
}

PlacementGroup* PlacementReadView::Find(std::string_view name) const {
    auto it = index_.by_name_.find(name);
    return it == index_.by_name_.end() ? nullptr : it->second;
}

void PlacementReadView::GetActiveGroupNames(
    std::vector<std::string>& names) const {
    names.clear();
    names.reserve(index_.active_groups_.size());
    for (const auto* group : index_.active_groups_) {
        names.push_back(group->name);
    }
}

void ScopedPlacementReadAccess::GetHostOrderedGroups(
    std::string_view writer_host_id, std::string_view key,
    std::vector<PlacementGroup*>& output) const {
    output.clear();
    if (!regions_by_host_ || writer_host_id.empty() ||
        regions_by_host_->empty()) {
        return;
    }

    auto host_it = regions_by_host_->find(writer_host_id);
    if (host_it == regions_by_host_->end()) {
        host_it = regions_by_host_->lower_bound(writer_host_id);
        if (host_it == regions_by_host_->end()) {
            host_it = regions_by_host_->begin();
        }
    }

    auto placement = GetView();
    for (size_t host_index = 0; host_index < regions_by_host_->size();
         ++host_index) {
        const auto& groups = host_it->second;
        if (!groups.empty()) {
            const size_t start =
                std::hash<std::string_view>{}(key) % groups.size();
            auto group_it = groups.begin();
            std::advance(group_it, start);
            for (size_t i = 0; i < groups.size(); ++i) {
                if (!group_it->second.empty()) {
                    AppendUnique(output, placement.Find(group_it->first));
                }
                ++group_it;
                if (group_it == groups.end()) {
                    group_it = groups.begin();
                }
            }
        }
        ++host_it;
        if (host_it == regions_by_host_->end()) {
            host_it = regions_by_host_->begin();
        }
    }
}

std::optional<UUID> ScopedPlacementReadAccess::GetOwnerClientId(
    std::string_view group_name) const {
    if (!owner_client_by_group_name_) {
        return std::nullopt;
    }
    auto it = owner_client_by_group_name_->find(group_name);
    return it == owner_client_by_group_name_->end()
               ? std::nullopt
               : std::optional<UUID>(it->second);
}

}  // namespace mooncake
