// Copyright 2026 Mooncake Authors

#include "replica_selector.h"

#include <array>
#include <cmath>
#include <limits>
#include <utility>

#include "replica_selection.h"

namespace mooncake {
namespace {

constexpr size_t kInlineCandidateCapacity = 16;

ReplicaRef MakeRef(const Replica::Descriptor& replica, size_t master_order) {
    return {master_order, replica.id};
}

std::optional<ReplicaRef> FindRef(std::span<const Replica::Descriptor> replicas,
                                  const Replica::Descriptor* replica) {
    if (!replica) return std::nullopt;
    // Equality is defined for unrelated pointers; relational comparison and
    // subtraction are not. Scan the small master-ordered list so the defensive
    // foreign-pointer path remains well-defined too.
    for (size_t index = 0; index < replicas.size(); ++index) {
        if (replica == std::addressof(replicas[index])) {
            return MakeRef(replicas[index], index);
        }
    }
    return std::nullopt;
}

std::optional<ReplicaRef> SelectStructural(
    std::span<const Replica::Descriptor> replicas,
    const std::unordered_set<std::string>& local_endpoints,
    std::span<const ReplicaRef> hard_vetoes = {}) {
    const auto is_hard_vetoed = [hard_vetoes](size_t master_order) {
        for (const auto& veto : hard_vetoes) {
            if (veto.master_order == master_order) return true;
        }
        return false;
    };
    std::optional<ReplicaRef> first_memory;
    std::optional<ReplicaRef> first_nof;
    for (size_t i = 0; i < replicas.size(); ++i) {
        const auto& replica = replicas[i];
        if (replica.status != ReplicaStatus::COMPLETE) continue;
        if (replica.is_memory_replica()) {
            const auto& endpoint = replica.get_memory_descriptor()
                                       .buffer_descriptor.transport_endpoint_;
            if (local_endpoints.count(endpoint)) return MakeRef(replica, i);
            if (is_hard_vetoed(i)) continue;
            if (!first_memory) first_memory = MakeRef(replica, i);
        } else if (replica.is_nof_replica()) {
            const auto& endpoint = replica.get_nof_descriptor()
                                       .buffer_descriptor.transport_endpoint_;
            if (local_endpoints.count(endpoint)) return MakeRef(replica, i);
            if (!first_nof) first_nof = MakeRef(replica, i);
        }
    }
    if (first_memory) return first_memory;
    if (first_nof) return first_nof;

    std::optional<ReplicaRef> best;
    for (size_t i = 0; i < replicas.size(); ++i) {
        const auto& replica = replicas[i];
        if (replica.status != ReplicaStatus::COMPLETE) continue;
        if (replica.is_local_disk_replica()) {
            best = MakeRef(replica, i);
        } else if (replica.is_disk_replica() && !best) {
            best = MakeRef(replica, i);
        }
    }
    return best;
}

bool IsLocalBuffered(const Replica::Descriptor& replica,
                     const std::unordered_set<std::string>& local_endpoints) {
    if (replica.is_memory_replica()) {
        return local_endpoints.count(
                   replica.get_memory_descriptor()
                       .buffer_descriptor.transport_endpoint_) != 0;
    }
    if (replica.is_nof_replica()) {
        return local_endpoints.count(
                   replica.get_nof_descriptor()
                       .buffer_descriptor.transport_endpoint_) != 0;
    }
    return false;
}

bool ComputeValidTotal(const ReplicaScoreBreakdown& breakdown, double& total) {
    if (!std::isfinite(breakdown.topology_cost) ||
        !std::isfinite(breakdown.load_cost) ||
        !std::isfinite(breakdown.health_cost) ||
        breakdown.topology_cost < 0.0 || breakdown.load_cost < 0.0 ||
        breakdown.health_cost < 0.0) {
        return false;
    }
    total =
        breakdown.topology_cost + breakdown.load_cost + breakdown.health_cost;
    return std::isfinite(total);
}

ReplicaSelectionOutcome OutcomeForUnavailable(
    ReplicaScoreUnavailableReason reason) {
    switch (reason) {
        case ReplicaScoreUnavailableReason::STALE:
            return ReplicaSelectionOutcome::FALLBACK_SIGNAL_STALE;
        case ReplicaScoreUnavailableReason::MISSING_SIGNAL:
            return ReplicaSelectionOutcome::FALLBACK_MISSING_SIGNAL;
        case ReplicaScoreUnavailableReason::SIGNAL_SOURCE_UNAVAILABLE:
            return ReplicaSelectionOutcome::FALLBACK_SIGNAL_SOURCE_UNAVAILABLE;
    }
    return ReplicaSelectionOutcome::FALLBACK_INVALID_RESULT;
}

int UnavailablePriority(ReplicaScoreUnavailableReason reason) noexcept {
    switch (reason) {
        case ReplicaScoreUnavailableReason::MISSING_SIGNAL:
            return 0;
        case ReplicaScoreUnavailableReason::STALE:
            return 1;
        case ReplicaScoreUnavailableReason::SIGNAL_SOURCE_UNAVAILABLE:
            return 2;
    }
    // An unknown enum value is a provider contract violation and must dominate
    // valid soft-unavailable reasons so the batch reports INVALID_RESULT.
    return 3;
}

void MergeUnavailable(std::optional<ReplicaScoreUnavailableReason>& aggregate,
                      ReplicaScoreUnavailableReason reason) noexcept {
    if (!aggregate ||
        UnavailablePriority(reason) > UnavailablePriority(*aggregate)) {
        aggregate = reason;
    }
}

}  // namespace

const Replica::Descriptor* ResolveReplica(
    std::span<const Replica::Descriptor> replicas,
    const std::optional<ReplicaRef>& reference) noexcept {
    if (!reference || reference->master_order >= replicas.size()) {
        return nullptr;
    }
    const auto& replica = replicas[reference->master_order];
    return replica.id == reference->replica_id ? &replica : nullptr;
}

ReplicaSelector::ReplicaSelector(
    ReplicaSelectionMode mode,
    std::shared_ptr<const ReplicaScoreProvider> provider,
    ReplicaSelectionDiagnostics diagnostics)
    : mode_(mode), provider_(std::move(provider)), diagnostics_(diagnostics) {}

ReplicaSelectionDecision ReplicaSelector::Select(
    const std::vector<Replica::Descriptor>& replicas,
    const std::unordered_set<std::string>& local_endpoints,
    const ReplicaSelectionContext& context) const {
    ReplicaSelectionDecision decision;
    decision.mode = mode_;
    const bool collect_trace =
        diagnostics_ == ReplicaSelectionDiagnostics::ENABLED;

    if (mode_ == ReplicaSelectionMode::LEGACY) {
        // LEGACY intentionally preserves the process-global V1 policy. No V2
        // provider or separate structural diagnostic scan runs in this mode.
        decision.selected =
            FindRef(replicas, SelectBestReplica(replicas, local_endpoints));
        decision.recommendation = decision.selected;
        return decision;
    }

    // SHADOW must be a strict online no-op, including when the existing V1
    // scorer is enabled. Its V2 recommendation remains based on the
    // instance-scoped provider and scorer-free structural fallback.
    decision.structural_baseline = SelectStructural(replicas, local_endpoints);
    if (mode_ == ReplicaSelectionMode::SHADOW) {
        const auto* baseline =
            ResolveReplica(replicas, decision.structural_baseline);
        decision.selected =
            FindRef(replicas, detail::ApplyRemoteReplicaScoring(
                                  replicas, local_endpoints, baseline));
    } else {
        decision.selected = decision.structural_baseline;
    }

    if (decision.structural_baseline) {
        const auto& baseline =
            replicas[decision.structural_baseline->master_order];
        if (IsLocalBuffered(baseline, local_endpoints)) {
            decision.outcome = ReplicaSelectionOutcome::LOCAL_FAST_PATH;
            decision.recommendation = decision.structural_baseline;
            if (collect_trace) {
                decision.trace.skipped.push_back(
                    {*decision.structural_baseline, ReplicaSkipReason::LOCAL});
            }
            return decision;
        }
    }

    std::array<ReplicaScoreCandidateView, kInlineCandidateCapacity>
        inline_candidates;
    std::vector<ReplicaScoreCandidateView> overflow_candidates;
    size_t candidate_count = 0;
    for (size_t i = 0; i < replicas.size(); ++i) {
        const auto& replica = replicas[i];
        if (replica.status != ReplicaStatus::COMPLETE) {
            if (collect_trace) {
                decision.trace.skipped.push_back(
                    {MakeRef(replica, i), ReplicaSkipReason::INCOMPLETE});
            }
            continue;
        }
        if (!replica.is_memory_replica()) {
            if (collect_trace) {
                decision.trace.skipped.push_back(
                    {MakeRef(replica, i), ReplicaSkipReason::NON_MEMORY});
            }
            continue;
        }
        const auto& buffer = replica.get_memory_descriptor().buffer_descriptor;
        ReplicaScoreCandidateView candidate{
            MakeRef(replica, i), buffer.transport_endpoint_, buffer.protocol_};
        if (candidate_count < kInlineCandidateCapacity) {
            inline_candidates[candidate_count] = candidate;
        } else {
            if (overflow_candidates.empty()) {
                overflow_candidates.reserve(replicas.size());
                overflow_candidates.insert(overflow_candidates.end(),
                                           inline_candidates.begin(),
                                           inline_candidates.end());
            }
            overflow_candidates.push_back(candidate);
        }
        ++candidate_count;
    }

    if (candidate_count == 0) {
        decision.outcome = ReplicaSelectionOutcome::NO_REMOTE_MEMORY;
        decision.recommendation = decision.structural_baseline;
        return decision;
    }

    const std::span<const ReplicaScoreCandidateView> candidates =
        overflow_candidates.empty()
            ? std::span<const ReplicaScoreCandidateView>(
                  inline_candidates.data(), candidate_count)
            : std::span<const ReplicaScoreCandidateView>(overflow_candidates);

    if (!provider_) {
        decision.outcome = ReplicaSelectionOutcome::FALLBACK_NO_PROVIDER;
        decision.recommendation = decision.structural_baseline;
        return decision;
    }

    std::array<ReplicaScoreVerdict, kInlineCandidateCapacity> inline_verdicts;
    std::vector<ReplicaScoreVerdict> overflow_verdicts;
    std::span<ReplicaScoreVerdict> verdicts;
    if (candidate_count <= kInlineCandidateCapacity) {
        verdicts = {inline_verdicts.data(), candidate_count};
    } else {
        overflow_verdicts.resize(candidate_count);
        verdicts = overflow_verdicts;
    }

    try {
        provider_->ScoreAll(context, candidates, verdicts);
    } catch (...) {
        decision.outcome = ReplicaSelectionOutcome::FALLBACK_PROVIDER_EXCEPTION;
        decision.recommendation = decision.structural_baseline;
        return decision;
    }

    // Validate the complete batch before publishing scores or vetoes. A
    // provider exception or unset element therefore cannot leak partial state.
    for (const auto& verdict : verdicts) {
        if (std::holds_alternative<std::monostate>(verdict)) {
            decision.outcome = ReplicaSelectionOutcome::FALLBACK_INVALID_RESULT;
            decision.recommendation = decision.structural_baseline;
            return decision;
        }
    }

    std::array<ReplicaRef, kInlineCandidateCapacity> inline_hard_vetoes;
    std::vector<ReplicaRef> overflow_hard_vetoes;
    size_t hard_veto_count = 0;
    const auto add_hard_veto = [&](ReplicaRef reference) {
        if (hard_veto_count < kInlineCandidateCapacity) {
            inline_hard_vetoes[hard_veto_count] = reference;
        } else {
            if (overflow_hard_vetoes.empty()) {
                overflow_hard_vetoes.reserve(candidate_count);
                overflow_hard_vetoes.insert(overflow_hard_vetoes.end(),
                                            inline_hard_vetoes.begin(),
                                            inline_hard_vetoes.end());
            }
            overflow_hard_vetoes.push_back(reference);
        }
        ++hard_veto_count;
    };
    std::optional<ReplicaScoreUnavailableReason> unavailable;
    std::optional<ReplicaRef> best;
    double best_total = std::numeric_limits<double>::max();
    bool invalid_score = false;

    for (size_t i = 0; i < verdicts.size(); ++i) {
        const auto& candidate = candidates[i];
        const auto& verdict = verdicts[i];
        if (const auto* score = std::get_if<ReplicaEligibleScore>(&verdict)) {
            double total = 0.0;
            if (!ComputeValidTotal(score->breakdown, total)) {
                invalid_score = true;
                continue;
            }
            if (!best || total < best_total) {
                best = candidate.replica;
                best_total = total;
            }
        } else if (const auto* veto = std::get_if<ReplicaHardVeto>(&verdict)) {
            add_hard_veto(candidate.replica);
            if (collect_trace) {
                decision.trace.hard_vetoes.push_back(
                    {candidate.replica, veto->reason});
            }
        } else if (const auto* missing =
                       std::get_if<ReplicaScoreUnavailable>(&verdict)) {
            MergeUnavailable(unavailable, missing->reason);
            if (collect_trace) {
                decision.trace.unavailable_scores.push_back(
                    {candidate.replica, missing->reason});
            }
        }
    }

    if (invalid_score || unavailable) {
        // Numeric scores are never applied partially. Hard vetoes from this
        // complete atomic batch remain authoritative.
        decision.trace.candidate_scores.clear();
        const std::span<const ReplicaRef> hard_vetoes =
            overflow_hard_vetoes.empty()
                ? std::span<const ReplicaRef>(inline_hard_vetoes.data(),
                                              hard_veto_count)
                : std::span<const ReplicaRef>(overflow_hard_vetoes);
        decision.recommendation =
            SelectStructural(replicas, local_endpoints, hard_vetoes);
        if (invalid_score) {
            decision.outcome = ReplicaSelectionOutcome::FALLBACK_INVALID_SCORE;
        } else {
            decision.outcome = OutcomeForUnavailable(*unavailable);
        }
        if (mode_ == ReplicaSelectionMode::ACTIVE) {
            decision.selected = decision.recommendation;
        }
        return decision;
    }

    if (collect_trace) {
        for (size_t i = 0; i < verdicts.size(); ++i) {
            const auto* score = std::get_if<ReplicaEligibleScore>(&verdicts[i]);
            if (!score) continue;
            double total = 0.0;
            const bool valid = ComputeValidTotal(score->breakdown, total);
            if (valid) {
                decision.trace.candidate_scores.push_back(
                    {candidates[i].replica, score->breakdown, total});
            }
        }
    }

    if (best) {
        decision.outcome = ReplicaSelectionOutcome::SCORED_REMOTE_MEMORY;
        decision.recommendation = best;
    } else {
        const std::span<const ReplicaRef> hard_vetoes =
            overflow_hard_vetoes.empty()
                ? std::span<const ReplicaRef>(inline_hard_vetoes.data(),
                                              hard_veto_count)
                : std::span<const ReplicaRef>(overflow_hard_vetoes);
        decision.recommendation =
            SelectStructural(replicas, local_endpoints, hard_vetoes);
        decision.outcome =
            decision.recommendation
                ? ReplicaSelectionOutcome::SCORED_STORAGE_FALLBACK
                : ReplicaSelectionOutcome::NO_SAFE_REPLICA;
    }
    if (mode_ == ReplicaSelectionMode::ACTIVE) {
        decision.selected = decision.recommendation;
    }
    return decision;
}

}  // namespace mooncake
