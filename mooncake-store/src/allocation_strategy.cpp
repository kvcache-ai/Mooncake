#include "allocation_strategy.h"

#include <algorithm>
#include <iterator>
#include <limits>

#include "local_ssd/manager.h"
#include "random.h"

namespace mooncake {
namespace {

constexpr size_t kMaxRetryLimit = 100;
constexpr size_t kCandidateMultiplier = 6;

size_t RandomTargetIndex(size_t upper_bound) noexcept {
    return randomIndex(upper_bound);
}

struct Candidate {
    PlacementGroup* group;
    double score;
};

struct PlacementScratch {
    std::vector<PlacementGroup*> preferred;
    std::vector<PlacementGroup*> excluded;
    std::vector<PlacementGroup*> used;
    std::vector<Candidate> candidates;

    void Clear() {
        preferred.clear();
        excluded.clear();
        used.clear();
        candidates.clear();
    }
};

PlacementScratch& GetPlacementScratch() {
    thread_local PlacementScratch scratch;
    scratch.Clear();
    return scratch;
}

bool Contains(const std::vector<PlacementGroup*>& groups,
              const PlacementGroup* group) {
    return std::find(groups.begin(), groups.end(), group) != groups.end();
}

void AppendUnique(std::vector<PlacementGroup*>& groups, PlacementGroup* group) {
    if (group && !Contains(groups, group)) {
        groups.push_back(group);
    }
}

template <bool CxlOnly>
std::unique_ptr<AllocatedBuffer> TryTarget(AllocationTarget* target,
                                           size_t size) {
    if constexpr (CxlOnly) {
        if (target->kind() != AllocationTargetKind::CXL) {
            return nullptr;
        }
        return target->Allocate(size);
    }
    return target->Allocate(size);
}

template <bool CxlOnly = false>
[[gnu::always_inline]] inline std::unique_ptr<AllocatedBuffer>
AllocateFromGroup(PlacementGroup* group, size_t size) {
    if (!group || group->targets.empty()) {
        return nullptr;
    }
    if (group->targets.size() == 1) {
        auto* target = group->targets.front();
        return TryTarget<CxlOnly>(target, size);
    }

    size_t index = RandomTargetIndex(group->targets.size());
    for (size_t i = 0; i < group->targets.size(); ++i) {
        auto* target = group->targets[index];
        if (auto buffer = TryTarget<CxlOnly>(target, size)) [[likely]] {
            return buffer;
        }
        if (++index == group->targets.size()) {
            index = 0;
        }
    }
    return nullptr;
}

double GetFreeRatio(const PlacementGroup& group) {
    uint64_t total_capacity = 0;
    uint64_t total_free = 0;
    for (const auto* target : group.targets) {
        const uint64_t capacity = target->Capacity();
        const uint64_t used = target->Used();
        total_capacity += capacity;
        total_free += capacity - std::min(capacity, used);
    }
    return total_capacity == 0 ? 0.0
                               : static_cast<double>(total_free) /
                                     static_cast<double>(total_capacity);
}

struct RandomPolicy {
    static constexpr bool kRanked = false;
    static constexpr bool kCxl = false;
};

struct FreeRatioFirstPolicy {
    static constexpr bool kRanked = true;
    static constexpr bool kCxl = false;

    double Score(PlacementGroup* group) const { return GetFreeRatio(*group); }
};

struct SsdFreeRatioFirstPolicy {
    static constexpr bool kRanked = true;
    static constexpr bool kCxl = false;

    const ScopedPlacementAccess& placement;
    const std::optional<LocalSSDMetricsView>& metrics;

    double Score(PlacementGroup* group) const {
        if (!metrics) {
            return 1.0;
        }
        auto owner = placement.GetOwnerClientId(group->name);
        if (!owner) {
            return 1.0;
        }
        return metrics->GetFreeRatio(*owner).value_or(1.0);
    }
};

struct CxlPolicy {
    static constexpr bool kRanked = false;
    static constexpr bool kCxl = true;
};

void ResolveRequest(const PlacementReadView& view,
                    const ReplicaAllocationRequest& request,
                    PlacementScratch& scratch) {
    for (const auto& excluded : request.excluded_groups) {
        AppendUnique(scratch.excluded, view.Find(excluded));
    }

    if (!request.preferred_group.empty()) {
        AppendUnique(scratch.preferred, view.Find(request.preferred_group));
    } else {
        for (const auto& preferred : request.preferred_groups) {
            AppendUnique(scratch.preferred, view.Find(preferred));
        }
    }
    for (auto* preferred : request.resolved_preferred_groups) {
        AppendUnique(scratch.preferred, preferred);
    }
}

template <typename Policy>
tl::expected<std::vector<Replica>, ErrorCode> AllocateWithPolicy(
    const PlacementReadView& view, const ReplicaAllocationRequest& request,
    const Policy& policy) {
    if (request.size == 0 || request.replica_count == 0) {
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    if (view.empty()) {
        return tl::make_unexpected(ErrorCode::NO_AVAILABLE_HANDLE);
    }

    auto active_groups = view.active_groups();
    if constexpr (!Policy::kCxl) {
        if (active_groups.size() == 1 && request.preferred_group.empty() &&
            request.preferred_groups.empty() &&
            request.resolved_preferred_groups.empty() &&
            request.excluded_groups.empty()) {
            auto buffer =
                AllocateFromGroup(active_groups.front(), request.size);
            if (!buffer) {
                return tl::make_unexpected(ErrorCode::NO_AVAILABLE_HANDLE);
            }
            std::vector<Replica> replicas;
            replicas.reserve(1);
            replicas.emplace_back(std::move(buffer), ReplicaStatus::PROCESSING,
                                  request.replica_type);
            return replicas;
        }
    }

    auto& scratch = GetPlacementScratch();
    ResolveRequest(view, request, scratch);

    if constexpr (Policy::kCxl) {
        const bool has_preference = !request.preferred_group.empty() ||
                                    !request.preferred_groups.empty() ||
                                    !request.resolved_preferred_groups.empty();
        if (!has_preference) {
            return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
        }
        if (scratch.preferred.empty()) {
            return tl::make_unexpected(ErrorCode::NO_AVAILABLE_HANDLE);
        }
        PlacementGroup* group = scratch.preferred.front();
        if (Contains(scratch.excluded, group)) {
            return tl::make_unexpected(ErrorCode::NO_AVAILABLE_HANDLE);
        }
        auto buffer = AllocateFromGroup<true>(group, request.size);
        if (!buffer) {
            return tl::make_unexpected(ErrorCode::NO_AVAILABLE_HANDLE);
        }
        std::vector<Replica> replicas;
        replicas.reserve(1);
        replicas.emplace_back(std::move(buffer), ReplicaStatus::PROCESSING,
                              request.replica_type);
        return replicas;
    }

    std::vector<Replica> replicas;
    replicas.reserve(request.replica_count);

    if (active_groups.size() == 1) {
        PlacementGroup* group = active_groups.front();
        if (Contains(scratch.excluded, group)) {
            return tl::make_unexpected(ErrorCode::NO_AVAILABLE_HANDLE);
        }
        auto buffer = AllocateFromGroup(group, request.size);
        if (!buffer) {
            return tl::make_unexpected(ErrorCode::NO_AVAILABLE_HANDLE);
        }
        replicas.emplace_back(std::move(buffer), ReplicaStatus::PROCESSING,
                              request.replica_type);
        return replicas;
    }

    for (auto* group : scratch.preferred) {
        if (Contains(scratch.excluded, group) ||
            Contains(scratch.used, group)) {
            continue;
        }
        auto buffer = AllocateFromGroup(group, request.size);
        if (!buffer) {
            continue;
        }
        replicas.emplace_back(std::move(buffer), ReplicaStatus::PROCESSING,
                              request.replica_type);
        scratch.used.push_back(group);
        if (replicas.size() == request.replica_count) {
            return replicas;
        }
    }

    if constexpr (Policy::kRanked) {
        const size_t remaining = request.replica_count - replicas.size();
        const size_t sample_count =
            std::min(active_groups.size(), kCandidateMultiplier * remaining);
        const size_t start = randomIndex(active_groups.size());
        scratch.candidates.reserve(
            std::max(scratch.candidates.capacity(), sample_count));
        for (size_t i = 0; i < sample_count; ++i) {
            auto* group = active_groups[(start + i) % active_groups.size()];
            if (Contains(scratch.excluded, group) ||
                Contains(scratch.used, group)) {
                continue;
            }
            scratch.candidates.push_back({group, policy.Score(group)});
        }
        std::sort(scratch.candidates.begin(), scratch.candidates.end(),
                  [](const Candidate& lhs, const Candidate& rhs) {
                      return lhs.score > rhs.score;
                  });
        for (const auto& candidate : scratch.candidates) {
            if (replicas.size() == request.replica_count) {
                return replicas;
            }
            auto buffer = AllocateFromGroup(candidate.group, request.size);
            if (!buffer) {
                continue;
            }
            replicas.emplace_back(std::move(buffer), ReplicaStatus::PROCESSING,
                                  request.replica_type);
            scratch.used.push_back(candidate.group);
        }
    }

    size_t index = randomIndex(active_groups.size());
    const size_t max_retry = std::min(kMaxRetryLimit, active_groups.size());
    for (size_t attempt = 0;
         attempt < max_retry && replicas.size() < request.replica_count;
         ++attempt, ++index) {
        auto* group = active_groups[index % active_groups.size()];
        if (Contains(scratch.excluded, group) ||
            Contains(scratch.used, group)) {
            continue;
        }
        auto buffer = AllocateFromGroup(group, request.size);
        if (!buffer) {
            continue;
        }
        replicas.emplace_back(std::move(buffer), ReplicaStatus::PROCESSING,
                              request.replica_type);
        scratch.used.push_back(group);
    }

    if (replicas.empty()) {
        return tl::make_unexpected(ErrorCode::NO_AVAILABLE_HANDLE);
    }
    return replicas;
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

bool PlacementIndex::Contains(std::string_view name,
                              const AllocationTarget* target) const {
    auto it = by_name_.find(name);
    return it != by_name_.end() &&
           std::find(it->second->targets.begin(), it->second->targets.end(),
                     target) != it->second->targets.end();
}

void PlacementIndex::Clear() {
    by_name_.clear();
    active_groups_.clear();
    owned_groups_.clear();
}

PlacementReadView PlacementIndex::GetView() const {
    return PlacementReadView(this);
}

PlacementGroup* PlacementReadView::Find(std::string_view name) const {
    auto it = index_->by_name_.find(name);
    return it == index_->by_name_.end() ? nullptr : it->second;
}

void ScopedPlacementAccess::GetHostOrderedGroups(
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

    auto placement = view();
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

std::optional<UUID> ScopedPlacementAccess::GetOwnerClientId(
    std::string_view group_name) const {
    if (!client_by_name_) {
        return std::nullopt;
    }
    auto it = client_by_name_->find(group_name);
    return it == client_by_name_->end() ? std::nullopt
                                        : std::optional<UUID>(it->second);
}

std::optional<double> LocalSSDMetricsView::GetFreeRatio(
    const UUID& client_id) const {
    auto usage = local_ssd_->GetUsage(client_id);
    if (!usage || usage->total_capacity_bytes <= 0) {
        return std::nullopt;
    }
    const int64_t used =
        std::clamp<int64_t>(usage->used_bytes, 0, usage->total_capacity_bytes);
    return static_cast<double>(usage->total_capacity_bytes - used) /
           static_cast<double>(usage->total_capacity_bytes);
}

tl::expected<std::vector<Replica>, ErrorCode> ReplicaAllocator::Allocate(
    ScopedPlacementAccess& placement, PlacementPolicyType policy_type,
    const ReplicaAllocationRequest& request,
    std::optional<LocalSSDMetricsView> local_ssd_metrics) const {
    auto view = placement.view();

    switch (policy_type) {
        case PlacementPolicyType::RANDOM:
        case PlacementPolicyType::LOCAL_FIRST:
            return AllocateWithPolicy(view, request, RandomPolicy{});
        case PlacementPolicyType::FREE_RATIO_FIRST:
            return AllocateWithPolicy(view, request, FreeRatioFirstPolicy{});
        case PlacementPolicyType::SSD_FREE_RATIO_FIRST:
            return AllocateWithPolicy(
                view, request,
                SsdFreeRatioFirstPolicy{placement, local_ssd_metrics});
        case PlacementPolicyType::CXL:
            return AllocateWithPolicy(view, request, CxlPolicy{});
    }
    return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
}

tl::expected<Replica, ErrorCode> ReplicaAllocator::AllocateFrom(
    ScopedPlacementAccess& placement, size_t size, std::string_view group_name,
    ReplicaType replica_type) const {
    if (size == 0) {
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    PlacementGroup* group = placement.view().Find(group_name);
    if (!group) {
        return tl::make_unexpected(ErrorCode::SEGMENT_NOT_FOUND);
    }
    auto buffer = AllocateFromGroup(group, size);
    if (!buffer) {
        return tl::make_unexpected(ErrorCode::NO_AVAILABLE_HANDLE);
    }
    return Replica(std::move(buffer), ReplicaStatus::PROCESSING, replica_type);
}

PlacementPolicyType EffectiveNoFPlacementPolicy(
    PlacementPolicyType memory_policy) {
    return memory_policy == PlacementPolicyType::SSD_FREE_RATIO_FIRST
               ? PlacementPolicyType::RANDOM
               : memory_policy;
}

}  // namespace mooncake
