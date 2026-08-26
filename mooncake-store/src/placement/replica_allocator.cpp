#include "placement/replica_allocator.h"

#include <algorithm>

#include "local_ssd/manager.h"
#include "placement/index.h"
#include "random.h"

namespace mooncake {
namespace {

constexpr size_t kMaxRetryLimit = 100;
constexpr size_t kCandidateMultiplier = 6;

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

struct ResolvedReplicaAllocationRequest final {
    size_t size;
    size_t replica_count;
    std::string_view preferred_group;
    std::span<const std::string> preferred_groups;
    std::span<PlacementGroup* const> resolved_preferred_groups;
    std::span<const std::string> excluded_groups;
    ReplicaType replica_type;
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

    size_t index = randomIndex(group->targets.size());
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

    const ScopedPlacementReadAccess& placement;
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
                    const ResolvedReplicaAllocationRequest& request,
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
    const PlacementReadView& view,
    const ResolvedReplicaAllocationRequest& request, const Policy& policy) {
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
    ScopedPlacementReadAccess& placement,
    const ReplicaAllocationRequest& request,
    PlacementDiagnostics* diagnostics) const {
    if (diagnostics) {
        diagnostics->has_sufficient_active_group_count =
            placement.GetView().size() >= request.replica_count;
    }

    std::span<PlacementGroup* const> host_ordered_groups;
    if (!request.writer_host_id.empty()) {
        thread_local std::vector<PlacementGroup*> host_ordered_scratch;
        placement.GetHostOrderedGroups(
            request.writer_host_id, request.object_key, host_ordered_scratch);
        host_ordered_groups = host_ordered_scratch;
    }

    const ResolvedReplicaAllocationRequest resolved{
        .size = request.size,
        .replica_count = request.replica_count,
        .preferred_group = request.preferred_group,
        .preferred_groups = request.preferred_groups,
        .resolved_preferred_groups = host_ordered_groups,
        .excluded_groups = request.excluded_groups,
        .replica_type = request.replica_type,
    };
    auto view = placement.GetView();

    switch (policy_type_) {
        case PlacementPolicyType::RANDOM:
        case PlacementPolicyType::LOCAL_FIRST:
            return AllocateWithPolicy(view, resolved, RandomPolicy{});
        case PlacementPolicyType::FREE_RATIO_FIRST:
            return AllocateWithPolicy(view, resolved, FreeRatioFirstPolicy{});
        case PlacementPolicyType::SSD_FREE_RATIO_FIRST:
            return AllocateWithPolicy(
                view, resolved,
                SsdFreeRatioFirstPolicy{placement, local_ssd_metrics_});
        case PlacementPolicyType::CXL:
            return AllocateWithPolicy(view, resolved, CxlPolicy{});
    }
    return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
}

tl::expected<Replica, ErrorCode> ReplicaAllocator::AllocateFrom(
    ScopedPlacementReadAccess& placement, size_t size,
    std::string_view group_name, ReplicaType replica_type) const {
    if (size == 0) {
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    PlacementGroup* group = placement.GetView().Find(group_name);
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
