#include "placement/replica_allocator.h"

#include <algorithm>
#include <unordered_set>

#include "local_ssd/manager.h"
#include "placement/index.h"
#include "random.h"

namespace mooncake {
namespace {

constexpr size_t kMaxRetryLimit = 100;
constexpr size_t kCandidateMultiplier = 6;

struct ScoredPlacementEntry final {
    const PlacementIndex::Entry* entry;
    double score;
};

class EntryList final {
   public:
    void Clear() {
        entries_.clear();
        members_.clear();
    }

    void Add(const PlacementIndex::Entry* entry) {
        if (entry && members_.insert(entry).second) {
            entries_.push_back(entry);
        }
    }

    bool Contains(const PlacementIndex::Entry* entry) const {
        return members_.contains(entry);
    }

    bool empty() const noexcept { return entries_.empty(); }
    const PlacementIndex::Entry* front() const { return entries_.front(); }
    auto begin() const noexcept { return entries_.begin(); }
    auto end() const noexcept { return entries_.end(); }

   private:
    // Preserve preference order; the set is only for membership queries.
    std::vector<const PlacementIndex::Entry*> entries_;
    std::unordered_set<const PlacementIndex::Entry*> members_;
};

struct PlacementScratch final {
    std::vector<const PlacementIndex::Entry*> serving_entries;
    EntryList preferred;
    std::unordered_set<const PlacementIndex::Entry*> excluded;
    std::vector<const PlacementIndex::Entry*> used;
    std::vector<ScoredPlacementEntry> candidates;

    bool HasUsed(const PlacementIndex::Entry* entry) const {
        return std::find(used.begin(), used.end(), entry) != used.end();
    }

    void Clear() {
        serving_entries.clear();
        preferred.Clear();
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

std::unique_ptr<AllocatedBuffer> TryAllocateFromEntry(
    const PlacementIndex::Entry* entry, size_t size) {
    if (!entry || entry->candidates.empty()) {
        return nullptr;
    }
    if (entry->candidates.size() == 1) {
        const auto* candidate = *entry->candidates.begin();
        return candidate->IsServing() ? candidate->Allocate(size) : nullptr;
    }

    // Candidates remain indexed while their client is suspected. Sample only
    // serving candidates so retained regions do not bias the starting point.
    thread_local std::vector<const AllocationCandidate*> serving_candidates;
    serving_candidates.clear();
    serving_candidates.reserve(entry->candidates.size());
    for (const auto* candidate : entry->candidates) {
        if (candidate->IsServing()) {
            serving_candidates.push_back(candidate);
        }
    }
    if (serving_candidates.empty()) {
        return nullptr;
    }

    size_t index = randomIndex(serving_candidates.size());
    for (size_t i = 0; i < serving_candidates.size(); ++i) {
        if (auto buffer = serving_candidates[index]->Allocate(size))
            [[likely]] {
            return buffer;
        }
        if (++index == serving_candidates.size()) {
            index = 0;
        }
    }
    return nullptr;
}

double GetFreeRatio(const PlacementIndex::Entry& entry) {
    uint64_t total_capacity = 0;
    uint64_t total_free = 0;
    for (const auto* target : entry.candidates) {
        if (!target->IsServing()) continue;
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

    double Score(const PlacementIndex::Entry* entry) const {
        return GetFreeRatio(*entry);
    }
};

struct SsdFreeRatioRanker final {
    static constexpr bool kRanked = true;

    const ScopedPlacementReadAccess& placement;
    const LocalSSDMetricsView& metrics;

    double Score(const PlacementIndex::Entry* entry) const {
        auto owner = placement.GetOwnerClientId(entry->segment_name);
        if (!owner) {
            return 1.0;
        }
        return metrics.GetFreeRatio(*owner).value_or(1.0);
    }
};

bool HasExplicitPreference(const PlacementConstraints& constraints) {
    return !constraints.preferred_segment_name.empty() ||
           !constraints.preferred_segment_names.empty();
}

void ResolveEntries(const PlacementIndex& index,
                    const PlacementConstraints& constraints,
                    AllocationCandidateKind target_kind,
                    PlacementScratch& scratch) {
    // Client liveness can change without reindexing. Use one serving snapshot
    // for both bounded sampling and replica-count diagnostics.
    const auto active_entries = index.active_entries(target_kind);
    scratch.serving_entries.reserve(active_entries.size());
    for (const auto* entry : active_entries) {
        if (std::any_of(
                entry->candidates.begin(), entry->candidates.end(),
                [](const auto* candidate) { return candidate->IsServing(); })) {
            scratch.serving_entries.push_back(entry);
        }
    }

    for (const auto& excluded : constraints.excluded_segment_names) {
        if (const auto* entry = index.Find(excluded, target_kind)) {
            scratch.excluded.insert(entry);
        }
    }

    if (!constraints.preferred_segment_name.empty()) {
        scratch.preferred.Add(
            index.Find(constraints.preferred_segment_name, target_kind));
    } else {
        for (const auto& preferred : constraints.preferred_segment_names) {
            scratch.preferred.Add(index.Find(preferred, target_kind));
        }
    }
}

tl::expected<std::vector<Replica>, ErrorCode> AllocatePreferredOnly(
    const ReplicaAllocationRequest& request, PlacementScratch& scratch) {
    const auto& replicas = request.replicas;
    if (replicas.size == 0 || replicas.count == 0) {
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    if (!HasExplicitPreference(request.placement)) {
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    if (scratch.serving_entries.empty()) {
        return tl::make_unexpected(ErrorCode::NO_AVAILABLE_HANDLE);
    }
    if (scratch.preferred.empty()) {
        return tl::make_unexpected(ErrorCode::NO_AVAILABLE_HANDLE);
    }

    const auto* entry = scratch.preferred.front();
    if (scratch.excluded.contains(entry)) {
        return tl::make_unexpected(ErrorCode::NO_AVAILABLE_HANDLE);
    }
    auto buffer = TryAllocateFromEntry(entry, replicas.size);
    if (!buffer) {
        return tl::make_unexpected(ErrorCode::NO_AVAILABLE_HANDLE);
    }

    std::vector<Replica> result;
    result.reserve(1);
    result.emplace_back(std::move(buffer), ReplicaStatus::PROCESSING,
                        replicas.type);
    return result;
}

bool TryAddReplica(const PlacementIndex::Entry* entry,
                   const ReplicaRequirements& requirements,
                   PlacementScratch& scratch, std::vector<Replica>& result) {
    auto buffer = TryAllocateFromEntry(entry, requirements.size);
    if (!buffer) {
        return false;
    }
    result.emplace_back(std::move(buffer), ReplicaStatus::PROCESSING,
                        requirements.type);
    scratch.used.push_back(entry);
    return true;
}

void AllocatePreferredEntries(const ReplicaRequirements& requirements,
                              PlacementScratch& scratch,
                              std::vector<Replica>& result) {
    for (const auto* entry : scratch.preferred) {
        if (scratch.excluded.contains(entry) || scratch.HasUsed(entry)) {
            continue;
        }
        if (TryAddReplica(entry, requirements, scratch, result) &&
            result.size() == requirements.count) {
            return;
        }
    }
}

template <typename Ranker>
void AllocateRankedEntries(
    std::span<const PlacementIndex::Entry* const> active_entries,
    const ReplicaRequirements& requirements, const Ranker& ranker,
    PlacementScratch& scratch, std::vector<Replica>& result) {
    const size_t remaining = requirements.count - result.size();
    const size_t sample_count =
        std::min(active_entries.size(), kCandidateMultiplier * remaining);
    const size_t start = randomIndex(active_entries.size());
    scratch.candidates.reserve(sample_count);
    for (size_t i = 0; i < sample_count; ++i) {
        const auto* entry = active_entries[(start + i) % active_entries.size()];
        if (scratch.excluded.contains(entry) || scratch.HasUsed(entry)) {
            continue;
        }
        scratch.candidates.push_back({entry, ranker.Score(entry)});
    }
    std::sort(
        scratch.candidates.begin(), scratch.candidates.end(),
        [](const ScoredPlacementEntry& lhs, const ScoredPlacementEntry& rhs) {
            return lhs.score > rhs.score;
        });
    for (const auto& candidate : scratch.candidates) {
        if (TryAddReplica(candidate.entry, requirements, scratch, result) &&
            result.size() == requirements.count) {
            return;
        }
    }
}

void AllocateFallbackEntries(
    std::span<const PlacementIndex::Entry* const> active_entries,
    const ReplicaRequirements& requirements, PlacementScratch& scratch,
    std::vector<Replica>& result) {
    size_t index_offset = randomIndex(active_entries.size());
    const size_t max_retry = std::min(kMaxRetryLimit, active_entries.size());
    for (size_t attempt = 0;
         attempt < max_retry && result.size() < requirements.count;
         ++attempt, ++index_offset) {
        const auto* entry =
            active_entries[index_offset % active_entries.size()];
        if (scratch.excluded.contains(entry) || scratch.HasUsed(entry)) {
            continue;
        }
        TryAddReplica(entry, requirements, scratch, result);
    }
}

template <typename Ranker>
tl::expected<std::vector<Replica>, ErrorCode> AllocateWithRanker(
    ScopedPlacementReadAccess& placement,
    const ReplicaAllocationRequest& request,
    AllocationCandidateKind target_kind, PlacementScratch& scratch,
    [[maybe_unused]] const Ranker& ranker, bool use_host_affinity) {
    const auto& index = placement.GetView();
    const auto& requirements = request.replicas;
    if (requirements.size == 0 || requirements.count == 0) {
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    const std::span<const PlacementIndex::Entry* const> active_entries =
        scratch.serving_entries;
    if (active_entries.empty()) {
        return tl::make_unexpected(ErrorCode::NO_AVAILABLE_HANDLE);
    }

    std::vector<Replica> result;
    result.reserve(std::min(requirements.count, active_entries.size()));

    if (active_entries.size() == 1) {
        const auto* entry = active_entries.front();
        if (scratch.excluded.contains(entry) ||
            !TryAddReplica(entry, requirements, scratch, result)) {
            return tl::make_unexpected(ErrorCode::NO_AVAILABLE_HANDLE);
        }
        return result;
    }

    AllocatePreferredEntries(requirements, scratch, result);
    if (result.size() == requirements.count) {
        return result;
    }

    if (use_host_affinity && !request.host_affinity.writer_host_id.empty()) {
        auto visit = [&](std::string_view name) {
            const auto* entry = index.Find(name, target_kind);
            if (!entry || scratch.preferred.Contains(entry) ||
                scratch.excluded.contains(entry) || scratch.HasUsed(entry)) {
                return false;
            }
            return TryAddReplica(entry, requirements, scratch, result) &&
                   result.size() == requirements.count;
        };
        index.VisitHostOrderedSegmentNames(request.host_affinity.writer_host_id,
                                           request.host_affinity.object_key,
                                           std::ref(visit));
        if (result.size() == requirements.count) return result;
    }

    if constexpr (Ranker::kRanked) {
        AllocateRankedEntries(active_entries, requirements, ranker, scratch,
                              result);
        if (result.size() == requirements.count) {
            return result;
        }
    }

    AllocateFallbackEntries(active_entries, requirements, scratch, result);
    if (result.empty()) {
        return tl::make_unexpected(ErrorCode::NO_AVAILABLE_HANDLE);
    }
    return result;
}

void UpdateDiagnostics(const ReplicaAllocationRequest& request,
                       const PlacementScratch& scratch,
                       PlacementDiagnostics* diagnostics) {
    if (diagnostics) {
        // Use the same serving-entry count as sampling. Retained non-serving
        // entries cannot satisfy strict replica count, even after eviction.
        diagnostics->has_sufficient_active_entry_count =
            scratch.serving_entries.size() >= request.replicas.count;
    }
}

template <typename Ranker>
tl::expected<std::vector<Replica>, ErrorCode> AllocateUsingRanker(
    ScopedPlacementReadAccess& placement,
    const ReplicaAllocationRequest& request, PlacementDiagnostics* diagnostics,
    const Ranker& ranker, bool use_host_affinity) {
    constexpr auto kTargetKind = AllocationCandidateKind::NATIVE;
    const auto& index = placement.GetView();
    auto& scratch = GetPlacementScratch();
    ResolveEntries(index, request.placement, kTargetKind, scratch);
    UpdateDiagnostics(request, scratch, diagnostics);
    return AllocateWithRanker(placement, request, kTargetKind, scratch, ranker,
                              use_host_affinity);
}

tl::expected<std::vector<Replica>, ErrorCode> AllocateUsingPreferences(
    ScopedPlacementReadAccess& placement,
    const ReplicaAllocationRequest& request, PlacementDiagnostics* diagnostics,
    AllocationCandidateKind required_kind) {
    const auto& index = placement.GetView();
    auto& scratch = GetPlacementScratch();
    ResolveEntries(index, request.placement, required_kind, scratch);
    UpdateDiagnostics(request, scratch, diagnostics);
    return AllocatePreferredOnly(request, scratch);
}

tl::expected<Replica, ErrorCode> AllocateFromNamedEntry(
    ScopedPlacementReadAccess& placement, size_t size,
    std::string_view segment_name, ReplicaType replica_type,
    AllocationCandidateKind kind) {
    if (size == 0 || segment_name.empty()) {
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    const auto* entry = placement.GetView().Find(segment_name, kind);
    if (!entry) {
        return tl::make_unexpected(ErrorCode::SEGMENT_NOT_FOUND);
    }
    auto buffer = TryAllocateFromEntry(entry, size);
    if (!buffer) {
        return tl::make_unexpected(ErrorCode::NO_AVAILABLE_HANDLE);
    }
    return Replica(std::move(buffer), ReplicaStatus::PROCESSING, replica_type);
}

}  // namespace

std::optional<double> LocalSSDMetricsView::GetFreeRatio(
    const UUID& client_id) const {
    auto usage = local_ssd_.GetUsage(client_id);
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
    std::string_view segment_name, ReplicaType replica_type) const {
    constexpr bool preferred_only =
        std::same_as<Policy, PreferredOnlyPlacementPolicy>;
    if constexpr (preferred_only) {
        return AllocateFromNamedEntry(placement, size, segment_name,
                                      replica_type, policy_.required_kind);
    } else {
        return AllocateFromNamedEntry(placement, size, segment_name,
                                      replica_type,
                                      AllocationCandidateKind::NATIVE);
    }
}

template class ReplicaAllocator<RandomPlacementPolicy>;
template class ReplicaAllocator<FreeRatioFirstPlacementPolicy>;
template class ReplicaAllocator<SsdFreeRatioFirstPlacementPolicy>;
template class ReplicaAllocator<LocalFirstPlacementPolicy>;
template class ReplicaAllocator<PreferredOnlyPlacementPolicy>;

}  // namespace mooncake
