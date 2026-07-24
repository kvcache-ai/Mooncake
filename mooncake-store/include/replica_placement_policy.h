// Copyright 2026 Mooncake Authors

#pragma once

#include <array>
#include <cstddef>
#include <cstdint>
#include <span>

namespace mooncake {

enum class ReplicaPlacementTier : uint8_t {
    MEMORY = 0,
    LOCAL_DISK,
    NOF_SSD,
    REMOTE_STORE,
    COUNT,
};

enum class ReplicaTemperature : uint8_t {
    COLD = 0,
    WARM,
    HOT,
    COUNT,
};

inline constexpr size_t kReplicaPlacementTierCount =
    static_cast<size_t>(ReplicaPlacementTier::COUNT);
inline constexpr size_t kReplicaTemperatureCount =
    static_cast<size_t>(ReplicaTemperature::COUNT);

struct ReplicaTierTarget {
    uint32_t desired{0};
    bool required{false};
};

struct ReplicaPlacementPolicyConfig {
    std::array<std::array<ReplicaTierTarget, kReplicaPlacementTierCount>,
               kReplicaTemperatureCount>
        targets{};
    uint32_t min_complete_replicas{1};
    uint32_t max_total_replicas{16};
};

enum class ReplicaTierSignalState : uint8_t {
    UNKNOWN = 0,
    AVAILABLE,
    UNAVAILABLE,
};

struct ReplicaTierObservation {
    uint32_t complete{0};
    uint32_t pending{0};
    ReplicaTierSignalState allocation_state{ReplicaTierSignalState::UNKNOWN};
    ReplicaTierSignalState health_state{ReplicaTierSignalState::UNKNOWN};
};

enum class ReplicaPlacementDegradedReason : uint8_t {
    REQUIRED_TIER_UNAVAILABLE = 0,
    REQUIRED_TIER_UNHEALTHY,
    REQUIRED_TIER_SIGNAL_UNKNOWN,
    MIN_COMPLETE_UNSATISFIED,
};

struct ReplicaTierAdjustment {
    ReplicaPlacementTier tier{ReplicaPlacementTier::MEMORY};
    uint32_t add{0};
    uint32_t remove{0};
    uint32_t effective_target{0};
};

struct ReplicaPlacementPlan {
    std::array<ReplicaTierAdjustment, kReplicaPlacementTierCount> adjustments{};
    std::array<ReplicaPlacementDegradedReason, kReplicaPlacementTierCount + 1>
        degraded_reasons{};
    size_t degraded_reason_count{0};

    [[nodiscard]] bool degraded() const noexcept {
        return degraded_reason_count != 0;
    }
};

// Pure, immutable policy. It produces intent only; callers remain responsible
// for admission, source/target selection, task submission, and transactional
// metadata changes.
class ReplicaPlacementPolicy final {
   public:
    explicit ReplicaPlacementPolicy(ReplicaPlacementPolicyConfig config);

    [[nodiscard]] ReplicaPlacementPlan Plan(
        ReplicaTemperature temperature,
        std::span<const ReplicaTierObservation, kReplicaPlacementTierCount>
            observations) const;

   private:
    ReplicaPlacementPolicyConfig config_;
};

}  // namespace mooncake
