#include "placement/index.h"

#include <algorithm>
#include <functional>
#include <iterator>

namespace mooncake {

bool PlacementGroup::AddTarget(const PlacementTarget& target) {
    if (std::find(targets.begin(), targets.end(), &target) != targets.end()) {
        return false;
    }
    targets.push_back(&target);
    return true;
}

bool PlacementGroup::RemoveTarget(const PlacementTarget& target) {
    auto it = std::find(targets.begin(), targets.end(), &target);
    if (it == targets.end()) {
        return false;
    }
    *it = targets.back();
    targets.pop_back();
    return true;
}

bool PlacementGroup::ReplaceTarget(const PlacementTarget& expected,
                                   const PlacementTarget& replacement) {
    auto it = std::find(targets.begin(), targets.end(), &expected);
    if (it == targets.end()) {
        return false;
    }
    *it = &replacement;
    return true;
}

bool PlacementIndex::AddTarget(std::string_view name,
                               const PlacementTarget* target) {
    if (name.empty() || !target) {
        return false;
    }
    auto it = groups_.find(name);
    if (it != groups_.end()) {
        return it->second->AddTarget(*target);
    }

    auto group = std::make_unique<PlacementGroup>(name, *target);
    const auto* group_ptr = group.get();
    groups_.emplace(group->name, std::move(group));
    active_groups_.push_back(group_ptr);
    return true;
}

bool PlacementIndex::RemoveTarget(std::string_view name,
                                  const PlacementTarget* target) {
    if (!target) {
        return false;
    }
    auto it = groups_.find(name);
    if (it == groups_.end() || !it->second->RemoveTarget(*target)) {
        return false;
    }
    if (!it->second->targets.empty()) {
        return true;
    }

    const auto* group = it->second.get();
    auto active_it =
        std::find(active_groups_.begin(), active_groups_.end(), group);
    if (active_it != active_groups_.end()) {
        *active_it = active_groups_.back();
        active_groups_.pop_back();
    }
    groups_.erase(it);
    return true;
}

bool PlacementIndex::ReplaceTarget(std::string_view name,
                                   const PlacementTarget* expected,
                                   const PlacementTarget* replacement) {
    if (!expected || !replacement) {
        return false;
    }
    auto it = groups_.find(name);
    return it != groups_.end() &&
           it->second->ReplaceTarget(*expected, *replacement);
}

void PlacementIndex::Clear() {
    groups_.clear();
    active_groups_.clear();
}

const PlacementGroup* PlacementIndex::Find(std::string_view name) const {
    auto it = groups_.find(name);
    return it == groups_.end() ? nullptr : it->second.get();
}

void PlacementIndex::GetActiveGroupNames(
    std::vector<std::string>& names) const {
    names.clear();
    names.reserve(active_groups_.size());
    for (const auto* group : active_groups_) {
        names.push_back(group->name);
    }
}

void ScopedPlacementReadAccess::GetHostOrderedGroups(
    std::string_view writer_host_id, std::string_view key,
    std::vector<const PlacementGroup*>& output) const {
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
                    const auto* group = placement_.Find(group_it->first);
                    if (group && std::find(output.begin(), output.end(),
                                           group) == output.end()) {
                        output.push_back(group);
                    }
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
