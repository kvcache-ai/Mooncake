#include "placement/replica_allocator.h"

#include <algorithm>

#include "local_ssd/manager.h"
#include "placement/index.h"
#include "random.h"

namespace mooncake {
namespace {

constexpr size_t kMaxRetryLimit = 100;
constexpr size_t kCandidateMultiplier = 6;

struct Candidate final {
    const PlacementGroup* group;
    double score;
};

class GroupList final {
   public:
    void Clear() { groups_.clear(); }

    void Add(const PlacementGroup* group) {
        if (group && !Contains(group)) {
            groups_.push_back(group);
        }
    }

    bool Contains(const PlacementGroup* group) const {
        return std::find(groups_.begin(), groups_.end(), group) !=
               groups_.end();
    }

    bool empty() const noexcept { return groups_.empty(); }
    const PlacementGroup* front() const { return groups_.front(); }
    auto begin() const noexcept { return groups_.begin(); }
    auto end() const noexcept { return groups_.end(); }

   private:
    std::vector<const PlacementGroup*> groups_;
};

struct PlacementScratch final {
    GroupList preferred;
    GroupList excluded;
    GroupList used;
    std::vector<Candidate> candidates;

    void Clear() {
        preferred.Clear();
        excluded.Clear();
        used.Clear();
        candidates.clear();
    }
};

PlacementScratch& GetPlacementScratch() {
    thread_local PlacementScratch scratch;
    scratch.Clear();
    return scratch;
}

template <typename TargetPredicate>
std::unique_ptr<AllocatedBuffer> TryAllocateFromGroupIf(
    const PlacementGroup* group, size_t size,
    const TargetPredicate& accepts_target) {
    if (!group || group->targets.empty()) {
        return nullptr;
    }

    auto try_target = [size, &accepts_target](const PlacementTarget* target) {
        if (!accepts_target(*target)) {
            return std::unique_ptr<AllocatedBuffer>{};
        }
        return target->Allocate(size);
    };
    if (group->targets.size() == 1) {
        return try_target(group->targets.front());
    }

    size_t index = randomIndex(group->targets.size());
    for (size_t i = 0; i < group->targets.size(); ++i) {
        if (auto buffer = try_target(group->targets[index])) [[likely]] {
            return buffer;
        }
        if (++index == group->targets.size()) {
            index = 0;
        }
    }
    return nullptr;
}

std::unique_ptr<AllocatedBuffer> TryAllocateFromGroup(
    const PlacementGroup* group, size_t size) {
    return TryAllocateFromGroupIf(group, size,
                                  [](const PlacementTarget&) { return true; });
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

struct RandomRanker final {
    static constexpr bool kRanked = false;
};

struct FreeRatioRanker final {
    static constexpr bool kRanked = true;

    double Score(const PlacementGroup* group) const {
        return GetFreeRatio(*group);
    }
};

struct SsdFreeRatioRanker final {
    static constexpr bool kRanked = true;

    const ScopedPlacementReadAccess& placement;
    const LocalSSDMetricsView& metrics;

    double Score(const PlacementGroup* group) const {
        auto owner = placement.GetOwnerClientId(group->name);
        if (!owner) {
            return 1.0;
        }
        return metrics.GetFreeRatio(*owner).value_or(1.0);
    }
};

bool HasExplicitPreference(const PlacementConstraints& constraints) {
    return !constraints.preferred_group.empty() ||
           !constraints.preferred_groups.empty();
}

void ResolveGroups(const PlacementIndex& index,
                   const PlacementConstraints& constraints,
                   std::span<const PlacementGroup* const> affinity_groups,
                   PlacementScratch& scratch) {
    for (const auto& excluded : constraints.excluded_groups) {
        scratch.excluded.Add(index.Find(excluded));
    }

    if (!constraints.preferred_group.empty()) {
        scratch.preferred.Add(index.Find(constraints.preferred_group));
    } else {
        for (const auto& preferred : constraints.preferred_groups) {
            scratch.preferred.Add(index.Find(preferred));
        }
    }
    for (const auto* group : affinity_groups) {
        scratch.preferred.Add(group);
    }
}

tl::expected<std::vector<Replica>, ErrorCode> AllocatePreferredOnly(
    const PlacementIndex& index, const ReplicaAllocationRequest& request,
    PlacementTargetKind required_kind, PlacementScratch& scratch) {
    const auto& replicas = request.replicas;
    if (replicas.size == 0 || replicas.count == 0) {
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    if (!HasExplicitPreference(request.placement)) {
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    if (index.empty()) {
        return tl::make_unexpected(ErrorCode::NO_AVAILABLE_HANDLE);
    }
    if (scratch.preferred.empty()) {
        return tl::make_unexpected(ErrorCode::NO_AVAILABLE_HANDLE);
    }

    const auto* group = scratch.preferred.front();
    if (scratch.excluded.Contains(group)) {
        return tl::make_unexpected(ErrorCode::NO_AVAILABLE_HANDLE);
    }
    auto buffer = TryAllocateFromGroupIf(
        group, replicas.size, [required_kind](const PlacementTarget& target) {
            return target.Kind() == required_kind;
        });
    if (!buffer) {
        return tl::make_unexpected(ErrorCode::NO_AVAILABLE_HANDLE);
    }

    std::vector<Replica> result;
    result.reserve(1);
    result.emplace_back(std::move(buffer), ReplicaStatus::PROCESSING,
                        replicas.type);
    return result;
}

bool TryAddReplica(const PlacementGroup* group,
                   const ReplicaRequirements& requirements,
                   PlacementScratch& scratch, std::vector<Replica>& result) {
    auto buffer = TryAllocateFromGroup(group, requirements.size);
    if (!buffer) {
        return false;
    }
    result.emplace_back(std::move(buffer), ReplicaStatus::PROCESSING,
                        requirements.type);
    scratch.used.Add(group);
    return true;
}

void AllocatePreferredGroups(const ReplicaRequirements& requirements,
                             PlacementScratch& scratch,
                             std::vector<Replica>& result) {
    for (const auto* group : scratch.preferred) {
        if (scratch.excluded.Contains(group) || scratch.used.Contains(group)) {
            continue;
        }
        if (TryAddReplica(group, requirements, scratch, result) &&
            result.size() == requirements.count) {
            return;
        }
    }
}

template <typename Ranker>
void AllocateRankedGroups(std::span<const PlacementGroup* const> active_groups,
                          const ReplicaRequirements& requirements,
                          const Ranker& ranker, PlacementScratch& scratch,
                          std::vector<Replica>& result) {
    const size_t remaining = requirements.count - result.size();
    const size_t sample_count =
        std::min(active_groups.size(), kCandidateMultiplier * remaining);
    const size_t start = randomIndex(active_groups.size());
    scratch.candidates.reserve(sample_count);
    for (size_t i = 0; i < sample_count; ++i) {
        const auto* group = active_groups[(start + i) % active_groups.size()];
        if (scratch.excluded.Contains(group) || scratch.used.Contains(group)) {
            continue;
        }
        scratch.candidates.push_back({group, ranker.Score(group)});
    }
    std::sort(scratch.candidates.begin(), scratch.candidates.end(),
              [](const Candidate& lhs, const Candidate& rhs) {
                  return lhs.score > rhs.score;
              });
    for (const auto& candidate : scratch.candidates) {
        if (TryAddReplica(candidate.group, requirements, scratch, result) &&
            result.size() == requirements.count) {
            return;
        }
    }
}

void AllocateFallbackGroups(
    std::span<const PlacementGroup* const> active_groups,
    const ReplicaRequirements& requirements, PlacementScratch& scratch,
    std::vector<Replica>& result) {
    size_t index_offset = randomIndex(active_groups.size());
    const size_t max_retry = std::min(kMaxRetryLimit, active_groups.size());
    for (size_t attempt = 0;
         attempt < max_retry && result.size() < requirements.count;
         ++attempt, ++index_offset) {
        const auto* group = active_groups[index_offset % active_groups.size()];
        if (scratch.excluded.Contains(group) || scratch.used.Contains(group)) {
            continue;
        }
        TryAddReplica(group, requirements, scratch, result);
    }
}

template <typename Ranker>
tl::expected<std::vector<Replica>, ErrorCode> AllocateWithRanker(
    const PlacementIndex& index, const ReplicaAllocationRequest& request,
    PlacementScratch& scratch, [[maybe_unused]] const Ranker& ranker) {
    const auto& requirements = request.replicas;
    if (requirements.size == 0 || requirements.count == 0) {
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    if (index.empty()) {
        return tl::make_unexpected(ErrorCode::NO_AVAILABLE_HANDLE);
    }

    const auto active_groups = index.active_groups();
    std::vector<Replica> result;
    result.reserve(std::min(requirements.count, active_groups.size()));

    if (active_groups.size() == 1) {
        const auto* group = active_groups.front();
        if (scratch.excluded.Contains(group) ||
            !TryAddReplica(group, requirements, scratch, result)) {
            return tl::make_unexpected(ErrorCode::NO_AVAILABLE_HANDLE);
        }
        return result;
    }

    AllocatePreferredGroups(requirements, scratch, result);
    if (result.size() == requirements.count) {
        return result;
    }

    if constexpr (Ranker::kRanked) {
        AllocateRankedGroups(active_groups, requirements, ranker, scratch,
                             result);
        if (result.size() == requirements.count) {
            return result;
        }
    }

    AllocateFallbackGroups(active_groups, requirements, scratch, result);
    if (result.empty()) {
        return tl::make_unexpected(ErrorCode::NO_AVAILABLE_HANDLE);
    }
    return result;
}

void UpdateDiagnostics(const PlacementIndex& index,
                       const ReplicaAllocationRequest& request,
                       PlacementDiagnostics* diagnostics) {
    if (diagnostics) {
        diagnostics->has_sufficient_active_group_count =
            index.size() >= request.replicas.count;
    }
}

template <typename Ranker>
tl::expected<std::vector<Replica>, ErrorCode> AllocateUsingRanker(
    ScopedPlacementReadAccess& placement,
    const ReplicaAllocationRequest& request, PlacementDiagnostics* diagnostics,
    const Ranker& ranker, bool use_host_affinity) {
    const auto& index = placement.GetView();
    UpdateDiagnostics(index, request, diagnostics);

    std::span<const PlacementGroup* const> affinity_groups;
    if (use_host_affinity && !request.host_affinity.writer_host_id.empty()) {
        thread_local std::vector<const PlacementGroup*> affinity_scratch;
        placement.GetHostOrderedGroups(request.host_affinity.writer_host_id,
                                       request.host_affinity.object_key,
                                       affinity_scratch);
        affinity_groups = affinity_scratch;
    }

    auto& scratch = GetPlacementScratch();
    ResolveGroups(index, request.placement, affinity_groups, scratch);
    return AllocateWithRanker(index, request, scratch, ranker);
}

tl::expected<std::vector<Replica>, ErrorCode> AllocateUsingPreferences(
    ScopedPlacementReadAccess& placement,
    const ReplicaAllocationRequest& request, PlacementDiagnostics* diagnostics,
    PlacementTargetKind required_kind) {
    const auto& index = placement.GetView();
    UpdateDiagnostics(index, request, diagnostics);

    auto& scratch = GetPlacementScratch();
    ResolveGroups(index, request.placement, {}, scratch);
    return AllocatePreferredOnly(index, request, required_kind, scratch);
}

tl::expected<Replica, ErrorCode> AllocateFromNamedGroup(
    ScopedPlacementReadAccess& placement, size_t size,
    std::string_view group_name, ReplicaType replica_type) {
    if (size == 0) {
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    const auto* group = placement.GetView().Find(group_name);
    if (!group) {
        return tl::make_unexpected(ErrorCode::SEGMENT_NOT_FOUND);
    }
    auto buffer = TryAllocateFromGroup(group, size);
    if (!buffer) {
        return tl::make_unexpected(ErrorCode::NO_AVAILABLE_HANDLE);
    }
    return Replica(std::move(buffer), ReplicaStatus::PROCESSING, replica_type);
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

template <ReplicaPlacementPolicy Policy>
tl::expected<std::vector<Replica>, ErrorCode>
ReplicaAllocator<Policy>::Allocate(ScopedPlacementReadAccess& placement,
                                   const ReplicaAllocationRequest& request,
                                   PlacementDiagnostics* diagnostics) const {
    if constexpr (std::same_as<Policy, PreferredOnlyPlacementPolicy>) {
        return AllocateUsingPreferences(placement, request, diagnostics,
                                        policy_.required_kind);
    } else if constexpr (std::same_as<Policy, FreeRatioFirstPlacementPolicy>) {
        return AllocateUsingRanker(placement, request, diagnostics,
                                   FreeRatioRanker{}, false);
    } else if constexpr (std::same_as<Policy,
                                      SsdFreeRatioFirstPlacementPolicy>) {
        return AllocateUsingRanker(
            placement, request, diagnostics,
            SsdFreeRatioRanker{placement, policy_.metrics}, false);
    } else {
        constexpr bool kUseHostAffinity =
            std::same_as<Policy, LocalFirstPlacementPolicy>;
        return AllocateUsingRanker(placement, request, diagnostics,
                                   RandomRanker{}, kUseHostAffinity);
    }
}

template <ReplicaPlacementPolicy Policy>
tl::expected<Replica, ErrorCode> ReplicaAllocator<Policy>::AllocateFrom(
    ScopedPlacementReadAccess& placement, size_t size,
    std::string_view group_name, ReplicaType replica_type) const {
    return AllocateFromNamedGroup(placement, size, group_name, replica_type);
}

template class ReplicaAllocator<RandomPlacementPolicy>;
template class ReplicaAllocator<FreeRatioFirstPlacementPolicy>;
template class ReplicaAllocator<SsdFreeRatioFirstPlacementPolicy>;
template class ReplicaAllocator<LocalFirstPlacementPolicy>;
template class ReplicaAllocator<PreferredOnlyPlacementPolicy>;

}  // namespace mooncake
