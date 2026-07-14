// Copyright 2026 Mooncake Authors

#include "cached_replica_score_provider.h"

#include <algorithm>
#include <cmath>
#include <functional>
#include <limits>
#include <stdexcept>
#include <utility>

namespace mooncake {
namespace {

constexpr double kMaximumRawCost = 1.0;

class SteadyReplicaSignalClock final : public ReplicaSignalClock {
   public:
    [[nodiscard]] TimePoint Now() const noexcept override {
        return std::chrono::steady_clock::now();
    }
};

size_t HashCombine(size_t seed, std::string_view value) noexcept {
    constexpr size_t kHashMix = sizeof(size_t) == sizeof(uint64_t)
                                    ? 0x9e3779b97f4a7c15ULL
                                    : 0x9e3779b9UL;
    const size_t hash = std::hash<std::string_view>{}(value);
    return seed ^ (hash + kHashMix + (seed << 6) + (seed >> 2));
}

size_t HashEndpointProtocol(ReplicaEndpointProtocolView key) noexcept {
    size_t seed = 0;
    seed = HashCombine(seed, key.transport_endpoint);
    return HashCombine(seed, key.protocol);
}

size_t HashTopologyRoute(ReplicaTopologyRouteView key) noexcept {
    size_t seed = 0;
    seed = HashCombine(seed, key.requester_endpoint);
    seed = HashCombine(seed, key.destination_location);
    seed = HashCombine(seed, key.candidate_endpoint);
    return HashCombine(seed, key.protocol);
}

enum class SnapshotValidationResult : uint8_t {
    VALID = 0,
    INVALID,
    LIMIT_EXCEEDED,
};

bool ValidCost(double cost) noexcept {
    return std::isfinite(cost) && cost >= 0.0 && cost <= kMaximumRawCost;
}

bool ValidHealthState(ReplicaHealthState state) noexcept {
    switch (state) {
        case ReplicaHealthState::HEALTHY:
        case ReplicaHealthState::DEGRADED:
        case ReplicaHealthState::UNHEALTHY:
        case ReplicaHealthState::CIRCUIT_OPEN:
            return true;
    }
    return false;
}

bool ValidPolicy(const ReplicaSignalSourcePolicy& policy) noexcept {
    if (!std::isfinite(policy.weight) || policy.weight < 0.0) return false;
    if (!policy.enabled) {
        return !policy.required && policy.weight == 0.0;
    }
    return policy.ttl > std::chrono::nanoseconds::zero();
}

bool ValidEndpointProtocolKey(const ReplicaEndpointProtocolKey& key,
                              size_t max_string_bytes) noexcept {
    return !key.transport_endpoint.empty() &&
           key.transport_endpoint.size() <= max_string_bytes &&
           key.protocol.size() <= max_string_bytes;
}

bool ValidTopologyRouteKey(const ReplicaTopologyRouteKey& key,
                           size_t max_string_bytes) noexcept {
    return !key.candidate_endpoint.empty() &&
           key.requester_endpoint.size() <= max_string_bytes &&
           key.destination_location.size() <= max_string_bytes &&
           key.candidate_endpoint.size() <= max_string_bytes &&
           key.protocol.size() <= max_string_bytes;
}

bool ExceedsTotalEntryLimit(const ReplicaSignalSnapshot& snapshot,
                            size_t maximum) noexcept {
    if (snapshot.topology.size() > maximum) return true;
    size_t remaining = maximum - snapshot.topology.size();
    if (snapshot.load.size() > remaining) return true;
    remaining -= snapshot.load.size();
    return snapshot.health.size() > remaining;
}

bool ValidLimits(const ReplicaSignalLimits& limits) noexcept {
    return limits.max_topology_entries > 0 && limits.max_load_entries > 0 &&
           limits.max_health_entries > 0 && limits.max_total_entries > 0 &&
           limits.max_string_bytes > 0 && limits.max_candidates_per_batch > 0;
}

ReplicaSignalProviderConfig ValidateAndNormalizeConfig(
    ReplicaSignalProviderConfig config) {
    if (!ValidPolicy(config.topology_source) ||
        !ValidPolicy(config.load_source) ||
        !ValidPolicy(config.health_source) ||
        !ValidCost(config.degraded_health_cost_floor) ||
        !ValidLimits(config.limits)) {
        throw std::invalid_argument("invalid replica signal provider config");
    }
    double weight_sum = 0.0;
    for (const auto* policy : {&config.topology_source, &config.load_source,
                               &config.health_source}) {
        if (policy->enabled) weight_sum += policy->weight;
    }
    if (!std::isfinite(weight_sum) || weight_sum <= 0.0) {
        throw std::invalid_argument(
            "enabled replica signal weights must have a finite positive sum");
    }
    for (auto* policy : {&config.topology_source, &config.load_source,
                         &config.health_source}) {
        if (policy->enabled) policy->weight /= weight_sum;
    }
    return config;
}

bool ValidObservation(const ReplicaSignalSourcePolicy& policy,
                      const ReplicaSignalSourceObservation& observation,
                      bool entries_empty,
                      ReplicaSignalClock::TimePoint publish_time) noexcept {
    if (!policy.enabled) return !observation.ready && entries_empty;
    if (!observation.ready) return entries_empty;
    return observation.observed_at <= publish_time;
}

SnapshotValidationResult ValidateSnapshot(
    const ReplicaSignalSnapshot& snapshot,
    const ReplicaSignalProviderConfig& config,
    ReplicaSignalClock::TimePoint publish_time) {
    if (snapshot.generation == 0 ||
        !ValidObservation(config.topology_source, snapshot.topology_source,
                          snapshot.topology.empty(), publish_time) ||
        !ValidObservation(config.load_source, snapshot.load_source,
                          snapshot.load.empty(), publish_time) ||
        !ValidObservation(config.health_source, snapshot.health_source,
                          snapshot.health.empty(), publish_time)) {
        return SnapshotValidationResult::INVALID;
    }
    const auto& limits = config.limits;
    if (snapshot.topology.size() > limits.max_topology_entries ||
        snapshot.load.size() > limits.max_load_entries ||
        snapshot.health.size() > limits.max_health_entries ||
        ExceedsTotalEntryLimit(snapshot, limits.max_total_entries)) {
        return SnapshotValidationResult::LIMIT_EXCEEDED;
    }

    if (config.topology_source.enabled && snapshot.topology_source.ready) {
        for (const auto& [key, cost] : snapshot.topology) {
            if (!ValidTopologyRouteKey(key, limits.max_string_bytes) ||
                !ValidCost(cost)) {
                return SnapshotValidationResult::INVALID;
            }
        }
    }
    if (config.load_source.enabled && snapshot.load_source.ready) {
        for (const auto& [key, signal] : snapshot.load) {
            if (!ValidEndpointProtocolKey(key, limits.max_string_bytes) ||
                signal.saturation_bytes == 0 ||
                !ValidCost(signal.utilization)) {
                return SnapshotValidationResult::INVALID;
            }
        }
    }
    if (config.health_source.enabled && snapshot.health_source.ready) {
        for (const auto& [key, signal] : snapshot.health) {
            if (!ValidEndpointProtocolKey(key, limits.max_string_bytes) ||
                !ValidHealthState(signal.state) || !ValidCost(signal.cost)) {
                return SnapshotValidationResult::INVALID;
            }
        }
    }
    return SnapshotValidationResult::VALID;
}

enum class SourceAvailability : uint8_t {
    DISABLED = 0,
    AVAILABLE,
    STALE,
    SOURCE_UNAVAILABLE,
};

SourceAvailability GetAvailability(
    const ReplicaSignalSourcePolicy& policy,
    const ReplicaSignalSourceObservation& observation,
    ReplicaSignalClock::TimePoint now) noexcept {
    if (!policy.enabled) return SourceAvailability::DISABLED;
    // A clock rollback crosses the validated snapshot's time horizon. Treat it
    // as source failure rather than accepting a future observation as fresh.
    if (!observation.ready || now < observation.observed_at) {
        return SourceAvailability::SOURCE_UNAVAILABLE;
    }
    if (now - observation.observed_at > policy.ttl) {
        return SourceAvailability::STALE;
    }
    return SourceAvailability::AVAILABLE;
}

enum class BatchIssue : uint8_t {
    NONE = 0,
    MISSING,
    STALE,
    SOURCE_UNAVAILABLE,
};

void MergeIssue(BatchIssue& aggregate, BatchIssue issue) noexcept {
    if (static_cast<uint8_t>(issue) > static_cast<uint8_t>(aggregate)) {
        aggregate = issue;
    }
}

ReplicaScoreUnavailableReason ToUnavailableReason(BatchIssue issue) {
    switch (issue) {
        case BatchIssue::MISSING:
            return ReplicaScoreUnavailableReason::MISSING_SIGNAL;
        case BatchIssue::STALE:
            return ReplicaScoreUnavailableReason::STALE;
        case BatchIssue::SOURCE_UNAVAILABLE:
            return ReplicaScoreUnavailableReason::SIGNAL_SOURCE_UNAVAILABLE;
        case BatchIssue::NONE:
            break;
    }
    throw std::logic_error("cannot map an empty replica signal issue");
}

BatchIssue SourceIssue(const ReplicaSignalSourcePolicy& policy,
                       SourceAvailability availability) noexcept {
    if (!policy.enabled || !policy.required) return BatchIssue::NONE;
    if (availability == SourceAvailability::SOURCE_UNAVAILABLE) {
        return BatchIssue::SOURCE_UNAVAILABLE;
    }
    if (availability == SourceAvailability::STALE) return BatchIssue::STALE;
    return BatchIssue::NONE;
}

bool SourceCanSupplyCost(SourceAvailability availability) noexcept {
    return availability == SourceAvailability::AVAILABLE;
}

ReplicaEndpointProtocolView EndpointProtocol(
    const ReplicaScoreCandidateView& candidate) noexcept {
    return {candidate.transport_endpoint, candidate.protocol};
}

ReplicaTopologyRouteView TopologyRoute(
    const ReplicaSelectionContext& context,
    const ReplicaScoreCandidateView& candidate) noexcept {
    return {context.requester_endpoint, context.destination_location,
            candidate.transport_endpoint, candidate.protocol};
}

double LoadCost(const ReplicaLoadSignal& signal,
                uint64_t requested_bytes) noexcept {
    const long double queued = static_cast<long double>(signal.queued_bytes);
    const long double requested = static_cast<long double>(requested_bytes);
    const long double saturation =
        static_cast<long double>(signal.saturation_bytes);
    const double byte_cost = static_cast<double>(std::min<long double>(
        kMaximumRawCost, (queued + requested) / saturation));
    return std::max(signal.utilization, byte_cost);
}

double HealthCost(const ReplicaHealthSignal& signal,
                  double degraded_floor) noexcept {
    if (signal.state == ReplicaHealthState::DEGRADED) {
        return std::max(signal.cost, degraded_floor);
    }
    return signal.cost;
}

ReplicaHardVetoReason ToHardVeto(ReplicaHealthState state) {
    if (state == ReplicaHealthState::UNHEALTHY) {
        return ReplicaHardVetoReason::UNHEALTHY;
    }
    if (state == ReplicaHealthState::CIRCUIT_OPEN) {
        return ReplicaHardVetoReason::CIRCUIT_OPEN;
    }
    throw std::logic_error("healthy replica cannot be converted to a veto");
}

bool IsHardVeto(ReplicaHealthState state) noexcept {
    return state == ReplicaHealthState::UNHEALTHY ||
           state == ReplicaHealthState::CIRCUIT_OPEN;
}

}  // namespace

size_t ReplicaEndpointProtocolKeyHash::operator()(
    const ReplicaEndpointProtocolKey& key) const noexcept {
    return HashEndpointProtocol({key.transport_endpoint, key.protocol});
}

size_t ReplicaEndpointProtocolKeyHash::operator()(
    ReplicaEndpointProtocolView key) const noexcept {
    return HashEndpointProtocol(key);
}

bool ReplicaEndpointProtocolKeyEqual::operator()(
    const ReplicaEndpointProtocolKey& lhs,
    const ReplicaEndpointProtocolKey& rhs) const noexcept {
    return lhs == rhs;
}

bool ReplicaEndpointProtocolKeyEqual::operator()(
    const ReplicaEndpointProtocolKey& lhs,
    ReplicaEndpointProtocolView rhs) const noexcept {
    return lhs.transport_endpoint == rhs.transport_endpoint &&
           lhs.protocol == rhs.protocol;
}

bool ReplicaEndpointProtocolKeyEqual::operator()(
    ReplicaEndpointProtocolView lhs,
    const ReplicaEndpointProtocolKey& rhs) const noexcept {
    return operator()(rhs, lhs);
}

size_t ReplicaTopologyRouteKeyHash::operator()(
    const ReplicaTopologyRouteKey& key) const noexcept {
    return HashTopologyRoute({key.requester_endpoint, key.destination_location,
                              key.candidate_endpoint, key.protocol});
}

size_t ReplicaTopologyRouteKeyHash::operator()(
    ReplicaTopologyRouteView key) const noexcept {
    return HashTopologyRoute(key);
}

bool ReplicaTopologyRouteKeyEqual::operator()(
    const ReplicaTopologyRouteKey& lhs,
    const ReplicaTopologyRouteKey& rhs) const noexcept {
    return lhs == rhs;
}

bool ReplicaTopologyRouteKeyEqual::operator()(
    const ReplicaTopologyRouteKey& lhs,
    ReplicaTopologyRouteView rhs) const noexcept {
    return lhs.requester_endpoint == rhs.requester_endpoint &&
           lhs.destination_location == rhs.destination_location &&
           lhs.candidate_endpoint == rhs.candidate_endpoint &&
           lhs.protocol == rhs.protocol;
}

bool ReplicaTopologyRouteKeyEqual::operator()(
    ReplicaTopologyRouteView lhs,
    const ReplicaTopologyRouteKey& rhs) const noexcept {
    return operator()(rhs, lhs);
}

CachedReplicaScoreProvider::CachedReplicaScoreProvider(
    ReplicaSignalProviderConfig config,
    std::shared_ptr<const ReplicaSignalClock> clock)
    : clock_(clock ? std::move(clock)
                   : std::make_shared<SteadyReplicaSignalClock>()),
      config_(ValidateAndNormalizeConfig(std::move(config))),
      snapshot_(nullptr) {}

ReplicaSignalPublishStatus CachedReplicaScoreProvider::PublishSnapshot(
    ReplicaSignalSnapshot snapshot) {
    // Snapshot timestamps and every entry are checked against one publish-time
    // observation before any generation CAS can expose the update.
    const auto publish_time = clock_->Now();
    const auto validation = ValidateSnapshot(snapshot, config_, publish_time);
    if (validation == SnapshotValidationResult::INVALID) {
        return ReplicaSignalPublishStatus::INVALID_SNAPSHOT;
    }
    if (validation == SnapshotValidationResult::LIMIT_EXCEEDED) {
        return ReplicaSignalPublishStatus::LIMIT_EXCEEDED;
    }

    std::shared_ptr<const ReplicaSignalSnapshot> update =
        std::make_shared<ReplicaSignalSnapshot>(std::move(snapshot));
    auto current = snapshot_.load(std::memory_order_acquire);
    while (true) {
        if (current && update->generation <= current->generation) {
            return ReplicaSignalPublishStatus::GENERATION_NOT_INCREASING;
        }
        if (snapshot_.compare_exchange_weak(current, update,
                                            std::memory_order_release,
                                            std::memory_order_acquire)) {
            return ReplicaSignalPublishStatus::PUBLISHED;
        }
    }
}

uint64_t CachedReplicaScoreProvider::CurrentGeneration() const noexcept {
    const auto snapshot = snapshot_.load(std::memory_order_acquire);
    return snapshot ? snapshot->generation : 0;
}

void CachedReplicaScoreProvider::ScoreAll(
    const ReplicaSelectionContext& context,
    std::span<const ReplicaScoreCandidateView> candidates,
    std::span<ReplicaScoreVerdict> verdicts) const {
    if (candidates.size() != verdicts.size()) {
        throw std::invalid_argument(
            "candidate and verdict batch sizes must match");
    }
    if (candidates.size() > config_.limits.max_candidates_per_batch) {
        throw std::length_error("replica score candidate limit exceeded");
    }

    // Exactly one acquire load and one clock read define the complete batch.
    const auto snapshot = snapshot_.load(std::memory_order_acquire);
    const auto now = clock_->Now();
    if (!snapshot) {
        for (auto& verdict : verdicts) {
            verdict = ReplicaScoreUnavailable{
                ReplicaScoreUnavailableReason::SIGNAL_SOURCE_UNAVAILABLE};
        }
        return;
    }

    const auto topology_availability = GetAvailability(
        config_.topology_source, snapshot->topology_source, now);
    const auto load_availability =
        GetAvailability(config_.load_source, snapshot->load_source, now);
    const auto health_availability =
        GetAvailability(config_.health_source, snapshot->health_source, now);

    BatchIssue issue = BatchIssue::NONE;
    MergeIssue(issue,
               SourceIssue(config_.topology_source, topology_availability));
    MergeIssue(issue, SourceIssue(config_.load_source, load_availability));
    MergeIssue(issue, SourceIssue(config_.health_source, health_availability));

    const bool topology_available = SourceCanSupplyCost(topology_availability);
    const bool load_available = SourceCanSupplyCost(load_availability);
    const bool health_available = SourceCanSupplyCost(health_availability);

    for (size_t i = 0; i < candidates.size(); ++i) {
        const auto& candidate = candidates[i];
        const auto endpoint = EndpointProtocol(candidate);

        const ReplicaHealthSignal* health_signal = nullptr;
        if (config_.health_source.enabled && health_available) {
            const auto health = snapshot->health.find(endpoint);
            if (health != snapshot->health.end())
                health_signal = &health->second;
        }

        const bool hard_veto =
            health_signal && IsHardVeto(health_signal->state);

        if (issue != BatchIssue::NONE) {
            // A fresh health veto remains authoritative even when another
            // required source makes the rest of the batch unavailable.
            verdicts[i] =
                hard_veto
                    ? ReplicaScoreVerdict(
                          ReplicaHardVeto{ToHardVeto(health_signal->state)})
                    : ReplicaScoreVerdict(
                          ReplicaScoreUnavailable{ToUnavailableReason(issue)});
            continue;
        }

        ReplicaScoreBreakdown breakdown;
        if (config_.topology_source.enabled) {
            double cost = kMaximumRawCost;
            if (topology_available) {
                const auto topology =
                    snapshot->topology.find(TopologyRoute(context, candidate));
                if (topology != snapshot->topology.end()) {
                    cost = topology->second;
                } else if (config_.topology_source.required) {
                    MergeIssue(issue, BatchIssue::MISSING);
                }
            }
            breakdown.topology_cost = config_.topology_source.weight * cost;
        }
        if (config_.load_source.enabled) {
            double cost = kMaximumRawCost;
            if (load_available) {
                const auto load = snapshot->load.find(endpoint);
                if (load != snapshot->load.end()) {
                    cost = LoadCost(load->second, context.requested_bytes);
                } else if (config_.load_source.required) {
                    MergeIssue(issue, BatchIssue::MISSING);
                }
            }
            breakdown.load_cost = config_.load_source.weight * cost;
        }
        if (config_.health_source.enabled) {
            double cost = kMaximumRawCost;
            if (health_available) {
                if (health_signal) {
                    cost = HealthCost(*health_signal,
                                      config_.degraded_health_cost_floor);
                } else if (config_.health_source.required) {
                    MergeIssue(issue, BatchIssue::MISSING);
                }
            }
            breakdown.health_cost = config_.health_source.weight * cost;
        }
        verdicts[i] =
            hard_veto ? ReplicaScoreVerdict(
                            ReplicaHardVeto{ToHardVeto(health_signal->state)})
                      : ReplicaScoreVerdict(ReplicaEligibleScore{breakdown});
    }

    // A required entry missing anywhere invalidates the complete batch. Keep
    // fresh health vetoes, but replace every provisional score so callers
    // cannot mix scored and structural-fallback decisions.
    if (issue != BatchIssue::NONE) {
        for (auto& verdict : verdicts) {
            if (!std::holds_alternative<ReplicaHardVeto>(verdict)) {
                verdict = ReplicaScoreUnavailable{ToUnavailableReason(issue)};
            }
        }
    }
}

}  // namespace mooncake
