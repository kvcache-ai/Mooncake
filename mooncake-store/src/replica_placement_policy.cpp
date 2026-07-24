// Copyright 2026 Mooncake Authors

#include "replica_placement_policy.h"

#include <limits>
#include <stdexcept>
#include <utility>

namespace mooncake {
namespace {

size_t TemperatureIndex(ReplicaTemperature temperature) {
    return static_cast<size_t>(temperature);
}

uint32_t SaturatingAdd(uint32_t lhs, uint32_t rhs) {
    if (rhs > std::numeric_limits<uint32_t>::max() - lhs) {
        return std::numeric_limits<uint32_t>::max();
    }
    return lhs + rhs;
}

}  // namespace

ReplicaPlacementPolicy::ReplicaPlacementPolicy(
    ReplicaPlacementPolicyConfig config)
    : config_(std::move(config)) {
    if (config_.min_complete_replicas == 0 ||
        config_.max_total_replicas < config_.min_complete_replicas) {
        throw std::invalid_argument("invalid replica placement safety bounds");
    }
    for (const auto& temperature_targets : config_.targets) {
        uint64_t total = 0;
        for (const auto& target : temperature_targets) {
            if (target.required && target.desired == 0) {
                throw std::invalid_argument(
                    "required replica placement tier has zero target");
            }
            total += target.desired;
        }
        if (total < config_.min_complete_replicas ||
            total > config_.max_total_replicas) {
            throw std::invalid_argument(
                "replica placement target violates total replica bounds");
        }
    }
}

ReplicaPlacementPlan ReplicaPlacementPolicy::Plan(
    ReplicaTemperature temperature,
    std::span<const ReplicaTierObservation, kReplicaPlacementTierCount>
        observations) const {
    const size_t temperature_index = TemperatureIndex(temperature);
    if (temperature_index >= kReplicaTemperatureCount) {
        throw std::invalid_argument("invalid replica temperature");
    }

    ReplicaPlacementPlan plan;
    uint64_t projected_complete = 0;
    for (size_t i = 0; i < kReplicaPlacementTierCount; ++i) {
        const auto tier = static_cast<ReplicaPlacementTier>(i);
        const auto& observation = observations[i];
        const auto& target = config_.targets[temperature_index][i];
        auto& adjustment = plan.adjustments[i];
        adjustment.tier = tier;

        const uint32_t present =
            SaturatingAdd(observation.complete, observation.pending);
        const bool allocation_known_available =
            observation.allocation_state == ReplicaTierSignalState::AVAILABLE;
        const bool health_known_available =
            observation.health_state == ReplicaTierSignalState::AVAILABLE;
        const bool health_known_unavailable =
            observation.health_state == ReplicaTierSignalState::UNAVAILABLE;
        const bool can_add =
            allocation_known_available && health_known_available;
        adjustment.effective_target = target.desired;
        if (target.desired > present && !can_add) {
            adjustment.effective_target = present;
        }
        if (target.required &&
            plan.degraded_reason_count < plan.degraded_reasons.size()) {
            if (health_known_unavailable) {
                plan.degraded_reasons[plan.degraded_reason_count++] =
                    ReplicaPlacementDegradedReason::REQUIRED_TIER_UNHEALTHY;
            } else if (observation.health_state ==
                       ReplicaTierSignalState::UNKNOWN) {
                plan.degraded_reasons[plan.degraded_reason_count++] =
                    ReplicaPlacementDegradedReason::
                        REQUIRED_TIER_SIGNAL_UNKNOWN;
            } else if (target.desired > present &&
                       observation.allocation_state ==
                           ReplicaTierSignalState::UNAVAILABLE) {
                plan.degraded_reasons[plan.degraded_reason_count++] =
                    ReplicaPlacementDegradedReason::REQUIRED_TIER_UNAVAILABLE;
            } else if (target.desired > present &&
                       observation.allocation_state ==
                           ReplicaTierSignalState::UNKNOWN) {
                plan.degraded_reasons[plan.degraded_reason_count++] =
                    ReplicaPlacementDegradedReason::
                        REQUIRED_TIER_SIGNAL_UNKNOWN;
            }
        }

        if (adjustment.effective_target > present) {
            adjustment.add = adjustment.effective_target - present;
        } else if (adjustment.effective_target < observation.complete &&
                   observation.pending == 0 && health_known_available) {
            adjustment.remove =
                observation.complete - adjustment.effective_target;
        }
        // The safety floor is explicitly about replicas that are COMPLETE at
        // planning time. Pending replicas and add intents suppress duplicate
        // additions, but they cannot justify deleting the last readable copy:
        // either may still fail before becoming COMPLETE.
        if (health_known_available) {
            projected_complete += observation.complete - adjustment.remove;
        }
    }

    // Never emit destructive intent when the available/addable result cannot
    // preserve the configured minimum. Existing replicas are safer than a
    // policy target under partial tier failure.
    if (projected_complete < config_.min_complete_replicas) {
        for (auto& adjustment : plan.adjustments) {
            adjustment.remove = 0;
        }
        if (plan.degraded_reason_count < plan.degraded_reasons.size()) {
            plan.degraded_reasons[plan.degraded_reason_count++] =
                ReplicaPlacementDegradedReason::MIN_COMPLETE_UNSATISFIED;
        }
    }
    return plan;
}

}  // namespace mooncake
