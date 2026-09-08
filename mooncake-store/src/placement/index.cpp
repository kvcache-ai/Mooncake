#include "placement/index.h"

#include <algorithm>
#include <functional>
#include <iterator>

namespace mooncake {

bool PlacementGroup::AddTarget(const PlacementTarget& target) {
    if (target.Kind() != kind ||
        std::find(targets.begin(), targets.end(), &target) != targets.end()) {
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
    if (expected.Kind() != kind || replacement.Kind() != kind) {
        return false;
    }
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
    const size_t kind_index = KindIndex(target->Kind());
    if (kind_index >= kPlacementTargetKindCount) {
        return false;
    }
    auto& groups = groups_by_kind_[kind_index];
    auto it = groups.find(name);
    if (it != groups.end()) {
        return it->second->AddTarget(*target);
    }

    auto group = std::make_unique<PlacementGroup>(name, target->Kind());
    if (!group->AddTarget(*target)) {
        return false;
    }
    const auto* group_ptr = group.get();
    groups.emplace(group->name, std::move(group));
    active_groups_by_kind_[kind_index].push_back(group_ptr);
    return true;
}

bool PlacementIndex::RemoveTarget(std::string_view name,
                                  const PlacementTarget* target) {
    if (!target) {
        return false;
    }
    const size_t kind_index = KindIndex(target->Kind());
    if (kind_index >= kPlacementTargetKindCount) {
        return false;
    }
    auto& groups = groups_by_kind_[kind_index];
    auto it = groups.find(name);
    if (it == groups.end() || !it->second->RemoveTarget(*target)) {
        return false;
    }
    if (!it->second->targets.empty()) {
        return true;
    }

    const auto* group = it->second.get();
    auto& active_groups = active_groups_by_kind_[kind_index];
    auto active_it =
        std::find(active_groups.begin(), active_groups.end(), group);
    if (active_it != active_groups.end()) {
        *active_it = active_groups.back();
        active_groups.pop_back();
    }
    groups.erase(it);
    return true;
}

bool PlacementIndex::ReplaceTarget(std::string_view name,
                                   const PlacementTarget* expected,
                                   const PlacementTarget* replacement) {
    if (!expected || !replacement) {
        return false;
    }
    if (expected->Kind() != replacement->Kind()) {
        if (!AddTarget(name, replacement)) {
            return false;
        }
        if (RemoveTarget(name, expected)) {
            return true;
        }
        (void)RemoveTarget(name, replacement);
        return false;
    }

    const size_t kind_index = KindIndex(expected->Kind());
    if (kind_index >= kPlacementTargetKindCount) {
        return false;
    }
    auto& groups = groups_by_kind_[kind_index];
    auto it = groups.find(name);
    return it != groups.end() &&
           it->second->ReplaceTarget(*expected, *replacement);
}

void PlacementIndex::Clear() {
    for (auto& groups : groups_by_kind_) {
        groups.clear();
    }
    for (auto& active_groups : active_groups_by_kind_) {
        active_groups.clear();
    }
}

const PlacementGroup* PlacementIndex::Find(std::string_view name,
                                           PlacementTargetKind kind) const {
    const size_t kind_index = KindIndex(kind);
    if (kind_index >= kPlacementTargetKindCount) {
        return nullptr;
    }
    const auto& groups = groups_by_kind_[kind_index];
    auto it = groups.find(name);
    return it == groups.end() ? nullptr : it->second.get();
}

void PlacementIndex::GetActiveGroupNames(
    PlacementTargetKind kind, std::vector<std::string>& names) const {
    names.clear();
    const size_t kind_index = KindIndex(kind);
    if (kind_index >= kPlacementTargetKindCount) {
        return;
    }
    const auto& active_groups = active_groups_by_kind_[kind_index];
    names.reserve(active_groups.size());
    for (const auto* group : active_groups) {
        names.push_back(group->name);
    }
}

void ScopedPlacementReadAccess::GetHostOrderedGroups(
    std::string_view writer_host_id, std::string_view key,
    PlacementTargetKind target_kind,
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
                    const auto* group =
                        placement_.Find(group_it->first, target_kind);
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
