// Copyright 2026 Mooncake Authors

#pragma once

#include <array>
#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>
#include <span>
#include <string>

#include "count_min_sketch.h"
#include "replica.h"
#include "replica_placement_policy.h"

namespace mooncake {

class ReplicaPlacementSignalClock {
   public:
    using TimePoint = std::chrono::steady_clock::time_point;

    virtual ~ReplicaPlacementSignalClock() = default;
    [[nodiscard]] virtual TimePoint Now() const noexcept = 0;
};

struct ReplicaTierCounts {
    uint32_t complete{0};
    uint32_t pending{0};
};

using ReplicaPlacementInventory =
    std::array<ReplicaTierCounts, kReplicaPlacementTierCount>;

[[nodiscard]] ReplicaPlacementInventory BuildReplicaPlacementInventory(
    std::span<const Replica::Descriptor> replicas) noexcept;

struct ReplicaPlacementTierSignals {
    ReplicaTierSignalState allocation_state{ReplicaTierSignalState::UNKNOWN};
    ReplicaTierSignalState health_state{ReplicaTierSignalState::UNKNOWN};
};

struct ReplicaPlacementSignalSnapshot {
    uint64_t generation{0};
    bool ready{false};
    ReplicaPlacementSignalClock::TimePoint observed_at{};
    std::array<ReplicaPlacementTierSignals, kReplicaPlacementTierCount> tiers{};
};

enum class ReplicaPlacementSignalPublishStatus : uint8_t {
    PUBLISHED = 0,
    INVALID_SNAPSHOT,
    GENERATION_NOT_INCREASING,
    NOT_ENABLED,
};

enum class ReplicaPlacementShadowSignalStatus : uint8_t {
    READY = 0,
    MISSING_SNAPSHOT,
    SOURCE_UNAVAILABLE,
    STALE_SNAPSHOT,
    COUNT,
};

inline constexpr size_t kReplicaPlacementShadowSignalStatusCount =
    static_cast<size_t>(ReplicaPlacementShadowSignalStatus::COUNT);
inline constexpr size_t kReplicaPlacementDegradedReasonCount =
    static_cast<size_t>(
        ReplicaPlacementDegradedReason::MIN_COMPLETE_UNSATISFIED) +
    1;

struct ReplicaPlacementShadowConfig {
    ReplicaPlacementPolicyConfig policy;
    uint8_t warm_threshold{2};
    uint8_t hot_threshold{8};
    std::chrono::nanoseconds signal_ttl{std::chrono::seconds(30)};
    size_t sketch_width{4096};
    size_t sketch_depth{4};
};

struct ReplicaPlacementShadowResult {
    uint8_t estimated_frequency{0};
    ReplicaTemperature temperature{ReplicaTemperature::COLD};
    ReplicaPlacementShadowSignalStatus signal_status{
        ReplicaPlacementShadowSignalStatus::MISSING_SNAPSHOT};
    ReplicaPlacementInventory inventory{};
    ReplicaPlacementPlan plan;
};

struct ReplicaPlacementShadowCountersSnapshot {
    std::array<uint64_t, kReplicaTemperatureCount *
                             kReplicaPlacementShadowSignalStatusCount>
        observations{};
    std::array<uint64_t, kReplicaTemperatureCount * kReplicaPlacementTierCount>
        add_intents{};
    std::array<uint64_t, kReplicaTemperatureCount * kReplicaPlacementTierCount>
        remove_intents{};
    std::array<uint64_t, kReplicaPlacementDegradedReasonCount> degraded{};
};

// Opt-in SHADOW evaluator. It never submits Copy/Move tasks and never mutates
// replica metadata. Keys are used only by CountMinSketch and never retained or
// exposed as metric labels.
class ReplicaPlacementShadowEvaluator final {
   public:
    explicit ReplicaPlacementShadowEvaluator(
        ReplicaPlacementShadowConfig config,
        std::shared_ptr<const ReplicaPlacementSignalClock> clock = nullptr);

    [[nodiscard]] ReplicaPlacementSignalPublishStatus PublishSignalSnapshot(
        ReplicaPlacementSignalSnapshot snapshot);

    [[nodiscard]] uint64_t CurrentGeneration() const noexcept;

    [[nodiscard]] ReplicaPlacementShadowResult Observe(
        const std::string& tenant_scoped_key,
        std::span<const Replica::Descriptor> replicas);

    [[nodiscard]] ReplicaPlacementShadowCountersSnapshot Counters()
        const noexcept;

   private:
    ReplicaPlacementShadowConfig config_;
    ReplicaPlacementPolicy policy_;
    CountMinSketch sketch_;
    std::shared_ptr<const ReplicaPlacementSignalClock> clock_;
    std::atomic<std::shared_ptr<const ReplicaPlacementSignalSnapshot>>
        snapshot_;
    mutable std::mutex publish_mutex_;
    std::array<std::atomic<uint64_t>,
               kReplicaTemperatureCount *
                   kReplicaPlacementShadowSignalStatusCount>
        observations_{};
    std::array<std::atomic<uint64_t>,
               kReplicaTemperatureCount * kReplicaPlacementTierCount>
        add_intents_{};
    std::array<std::atomic<uint64_t>,
               kReplicaTemperatureCount * kReplicaPlacementTierCount>
        remove_intents_{};
    std::array<std::atomic<uint64_t>, kReplicaPlacementDegradedReasonCount>
        degraded_{};
};

}  // namespace mooncake
