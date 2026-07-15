// Copyright 2026 Mooncake Authors

#include "replica_placement_shadow.h"

#include <limits>
#include <stdexcept>
#include <utility>

namespace mooncake {
namespace {

class SteadyReplicaPlacementClock final : public ReplicaPlacementSignalClock {
   public:
    [[nodiscard]] TimePoint Now() const noexcept override {
        return std::chrono::steady_clock::now();
    }
};

bool IsValidSignalState(ReplicaTierSignalState state) noexcept {
    return state == ReplicaTierSignalState::UNKNOWN ||
           state == ReplicaTierSignalState::AVAILABLE ||
           state == ReplicaTierSignalState::UNAVAILABLE;
}

uint32_t SaturatingIncrement(uint32_t value) noexcept {
    return value == std::numeric_limits<uint32_t>::max() ? value : value + 1;
}

size_t TierIndex(const Replica::Descriptor& replica) noexcept {
    if (replica.is_memory_replica()) {
        return static_cast<size_t>(ReplicaPlacementTier::MEMORY);
    }
    if (replica.is_local_disk_replica()) {
        return static_cast<size_t>(ReplicaPlacementTier::LOCAL_DISK);
    }
    if (replica.is_nof_replica()) {
        return static_cast<size_t>(ReplicaPlacementTier::NOF_SSD);
    }
    return static_cast<size_t>(ReplicaPlacementTier::REMOTE_STORE);
}

size_t ObservationIndex(ReplicaTemperature temperature,
                        ReplicaPlacementShadowSignalStatus status) noexcept {
    return static_cast<size_t>(temperature) *
               kReplicaPlacementShadowSignalStatusCount +
           static_cast<size_t>(status);
}

size_t IntentIndex(ReplicaTemperature temperature, size_t tier_index) noexcept {
    return static_cast<size_t>(temperature) * kReplicaPlacementTierCount +
           tier_index;
}

}  // namespace

ReplicaPlacementInventory BuildReplicaPlacementInventory(
    std::span<const Replica::Descriptor> replicas) noexcept {
    ReplicaPlacementInventory inventory{};
    for (const auto& replica : replicas) {
        const size_t tier = TierIndex(replica);
        if (replica.status == ReplicaStatus::COMPLETE) {
            inventory[tier].complete =
                SaturatingIncrement(inventory[tier].complete);
        } else if (replica.status == ReplicaStatus::INITIALIZED ||
                   replica.status == ReplicaStatus::PROCESSING) {
            inventory[tier].pending =
                SaturatingIncrement(inventory[tier].pending);
        }
    }
    return inventory;
}

ReplicaPlacementShadowEvaluator::ReplicaPlacementShadowEvaluator(
    ReplicaPlacementShadowConfig config,
    std::shared_ptr<const ReplicaPlacementSignalClock> clock)
    : config_(std::move(config)),
      policy_(config_.policy),
      sketch_(config_.sketch_width, config_.sketch_depth),
      clock_(clock ? std::move(clock)
                   : std::make_shared<SteadyReplicaPlacementClock>()),
      snapshot_(nullptr) {
    if (config_.warm_threshold == 0 ||
        config_.hot_threshold <= config_.warm_threshold ||
        config_.signal_ttl <= std::chrono::nanoseconds::zero() ||
        config_.sketch_width == 0 || config_.sketch_depth == 0) {
        throw std::invalid_argument("invalid replica placement shadow config");
    }
}

ReplicaPlacementSignalPublishStatus
ReplicaPlacementShadowEvaluator::PublishSignalSnapshot(
    ReplicaPlacementSignalSnapshot snapshot) {
    if (snapshot.generation == 0 || snapshot.observed_at > clock_->Now()) {
        return ReplicaPlacementSignalPublishStatus::INVALID_SNAPSHOT;
    }
    for (const auto& tier : snapshot.tiers) {
        if (!IsValidSignalState(tier.allocation_state) ||
            !IsValidSignalState(tier.health_state)) {
            return ReplicaPlacementSignalPublishStatus::INVALID_SNAPSHOT;
        }
        if (!snapshot.ready &&
            (tier.allocation_state != ReplicaTierSignalState::UNKNOWN ||
             tier.health_state != ReplicaTierSignalState::UNKNOWN)) {
            return ReplicaPlacementSignalPublishStatus::INVALID_SNAPSHOT;
        }
    }

    std::lock_guard lock(snapshot_mutex_);
    const auto current = snapshot_;
    if (current && snapshot.generation <= current->generation) {
        return ReplicaPlacementSignalPublishStatus::GENERATION_NOT_INCREASING;
    }
    snapshot_ = std::make_shared<const ReplicaPlacementSignalSnapshot>(
        std::move(snapshot));
    return ReplicaPlacementSignalPublishStatus::PUBLISHED;
}

uint64_t ReplicaPlacementShadowEvaluator::CurrentGeneration() const noexcept {
    std::lock_guard lock(snapshot_mutex_);
    const auto snapshot = snapshot_;
    return snapshot ? snapshot->generation : 0;
}

ReplicaPlacementShadowResult ReplicaPlacementShadowEvaluator::Observe(
    const std::string& tenant_scoped_key,
    std::span<const Replica::Descriptor> replicas) {
    ReplicaPlacementShadowResult result;
    result.estimated_frequency = sketch_.increment(tenant_scoped_key);
    result.temperature = result.estimated_frequency >= config_.hot_threshold
                             ? ReplicaTemperature::HOT
                         : result.estimated_frequency >= config_.warm_threshold
                             ? ReplicaTemperature::WARM
                             : ReplicaTemperature::COLD;
    result.inventory = BuildReplicaPlacementInventory(replicas);

    std::array<ReplicaTierObservation, kReplicaPlacementTierCount>
        observations{};
    for (size_t i = 0; i < kReplicaPlacementTierCount; ++i) {
        observations[i].complete = result.inventory[i].complete;
        observations[i].pending = result.inventory[i].pending;
    }

    std::shared_ptr<const ReplicaPlacementSignalSnapshot> snapshot;
    {
        std::lock_guard lock(snapshot_mutex_);
        snapshot = snapshot_;
    }
    if (!snapshot) {
        result.signal_status =
            ReplicaPlacementShadowSignalStatus::MISSING_SNAPSHOT;
    } else if (!snapshot->ready) {
        result.signal_status =
            ReplicaPlacementShadowSignalStatus::SOURCE_UNAVAILABLE;
    } else if (clock_->Now() - snapshot->observed_at > config_.signal_ttl) {
        result.signal_status =
            ReplicaPlacementShadowSignalStatus::STALE_SNAPSHOT;
    } else {
        result.signal_status = ReplicaPlacementShadowSignalStatus::READY;
        for (size_t i = 0; i < kReplicaPlacementTierCount; ++i) {
            observations[i].allocation_state =
                snapshot->tiers[i].allocation_state;
            observations[i].health_state = snapshot->tiers[i].health_state;
        }
    }

    result.plan = policy_.Plan(result.temperature, observations);
    observations_[ObservationIndex(result.temperature, result.signal_status)]
        .fetch_add(1, std::memory_order_relaxed);
    for (size_t i = 0; i < kReplicaPlacementTierCount; ++i) {
        const size_t index = IntentIndex(result.temperature, i);
        add_intents_[index].fetch_add(result.plan.adjustments[i].add,
                                      std::memory_order_relaxed);
        remove_intents_[index].fetch_add(result.plan.adjustments[i].remove,
                                         std::memory_order_relaxed);
    }
    for (size_t i = 0; i < result.plan.degraded_reason_count; ++i) {
        const size_t reason =
            static_cast<size_t>(result.plan.degraded_reasons[i]);
        if (reason < degraded_.size()) {
            degraded_[reason].fetch_add(1, std::memory_order_relaxed);
        }
    }
    return result;
}

ReplicaPlacementShadowCountersSnapshot
ReplicaPlacementShadowEvaluator::Counters() const noexcept {
    ReplicaPlacementShadowCountersSnapshot result;
    for (size_t i = 0; i < observations_.size(); ++i) {
        result.observations[i] =
            observations_[i].load(std::memory_order_relaxed);
    }
    for (size_t i = 0; i < add_intents_.size(); ++i) {
        result.add_intents[i] = add_intents_[i].load(std::memory_order_relaxed);
        result.remove_intents[i] =
            remove_intents_[i].load(std::memory_order_relaxed);
    }
    for (size_t i = 0; i < degraded_.size(); ++i) {
        result.degraded[i] = degraded_[i].load(std::memory_order_relaxed);
    }
    return result;
}

}  // namespace mooncake
