// Copyright 2026 Mooncake Authors

#include "replica_placement_policy.h"

#include <array>
#include <atomic>
#include <stdexcept>
#include <thread>
#include <vector>

#include <gtest/gtest.h>

namespace mooncake {
namespace {

size_t Index(ReplicaPlacementTier tier) { return static_cast<size_t>(tier); }

ReplicaTierObservation Available(uint32_t complete = 0, uint32_t pending = 0) {
    return {.complete = complete,
            .pending = pending,
            .allocation_state = ReplicaTierSignalState::AVAILABLE,
            .health_state = ReplicaTierSignalState::AVAILABLE};
}

ReplicaPlacementPolicyConfig TestConfig() {
    ReplicaPlacementPolicyConfig config;
    config.targets[static_cast<size_t>(ReplicaTemperature::COLD)] = {
        ReplicaTierTarget{0, false}, ReplicaTierTarget{1, true},
        ReplicaTierTarget{0, false}, ReplicaTierTarget{1, true}};
    config.targets[static_cast<size_t>(ReplicaTemperature::WARM)] = {
        ReplicaTierTarget{1, true}, ReplicaTierTarget{1, false},
        ReplicaTierTarget{1, false}, ReplicaTierTarget{1, true}};
    config.targets[static_cast<size_t>(ReplicaTemperature::HOT)] = {
        ReplicaTierTarget{2, true}, ReplicaTierTarget{1, false},
        ReplicaTierTarget{1, false}, ReplicaTierTarget{1, true}};
    config.min_complete_replicas = 1;
    config.max_total_replicas = 8;
    return config;
}

TEST(ReplicaPlacementPolicyTest, ProducesDeterministicHotTierDiff) {
    ReplicaPlacementPolicy policy(TestConfig());
    std::array<ReplicaTierObservation, kReplicaPlacementTierCount> observed{{
        Available(1),
        Available(2),
        Available(),
        Available(1),
    }};

    const auto plan = policy.Plan(ReplicaTemperature::HOT, observed);

    EXPECT_FALSE(plan.degraded());
    EXPECT_EQ(plan.adjustments[Index(ReplicaPlacementTier::MEMORY)].add, 1);
    EXPECT_EQ(plan.adjustments[Index(ReplicaPlacementTier::LOCAL_DISK)].remove,
              1);
    EXPECT_EQ(plan.adjustments[Index(ReplicaPlacementTier::NOF_SSD)].add, 1);
    EXPECT_EQ(plan.adjustments[Index(ReplicaPlacementTier::REMOTE_STORE)].add,
              0);
}

TEST(ReplicaPlacementPolicyTest, PendingReplicaCountsTowardTarget) {
    ReplicaPlacementPolicy policy(TestConfig());
    std::array<ReplicaTierObservation, kReplicaPlacementTierCount> observed{{
        Available(1, 1),
        Available(1),
        Available(0, 1),
        Available(1),
    }};

    const auto plan = policy.Plan(ReplicaTemperature::HOT, observed);

    for (const auto& adjustment : plan.adjustments) {
        EXPECT_EQ(adjustment.add, 0);
        EXPECT_EQ(adjustment.remove, 0);
    }
}

TEST(ReplicaPlacementPolicyTest, DefersRemovalWhileTierHasPendingReplica) {
    ReplicaPlacementPolicy policy(TestConfig());
    std::array<ReplicaTierObservation, kReplicaPlacementTierCount> observed{{
        Available(3, 1),
        Available(1),
        Available(1),
        Available(1),
    }};

    const auto plan = policy.Plan(ReplicaTemperature::HOT, observed);

    EXPECT_EQ(plan.adjustments[Index(ReplicaPlacementTier::MEMORY)].add, 0);
    EXPECT_EQ(plan.adjustments[Index(ReplicaPlacementTier::MEMORY)].remove, 0);
}

TEST(ReplicaPlacementPolicyTest, DoesNotRemoveFromUnhealthyTier) {
    ReplicaPlacementPolicy policy(TestConfig());
    std::array<ReplicaTierObservation, kReplicaPlacementTierCount> observed{{
        {.complete = 3,
         .allocation_state = ReplicaTierSignalState::AVAILABLE,
         .health_state = ReplicaTierSignalState::UNAVAILABLE},
        Available(1),
        Available(1),
        Available(1),
    }};

    const auto plan = policy.Plan(ReplicaTemperature::HOT, observed);

    EXPECT_TRUE(plan.degraded());
    EXPECT_EQ(plan.adjustments[Index(ReplicaPlacementTier::MEMORY)].remove, 0);
}

TEST(ReplicaPlacementPolicyTest, RequiredUnavailableTierDegradesWithoutAdd) {
    ReplicaPlacementPolicy policy(TestConfig());
    std::array<ReplicaTierObservation, kReplicaPlacementTierCount> observed{{
        {.allocation_state = ReplicaTierSignalState::UNAVAILABLE,
         .health_state = ReplicaTierSignalState::AVAILABLE},
        Available(1),
        Available(1),
        Available(1),
    }};

    const auto plan = policy.Plan(ReplicaTemperature::HOT, observed);

    ASSERT_TRUE(plan.degraded());
    ASSERT_EQ(plan.degraded_reason_count, 1);
    EXPECT_EQ(plan.degraded_reasons[0],
              ReplicaPlacementDegradedReason::REQUIRED_TIER_UNAVAILABLE);
    EXPECT_EQ(plan.adjustments[Index(ReplicaPlacementTier::MEMORY)].add, 0);
}

TEST(ReplicaPlacementPolicyTest, RequiredUnhealthyTierHasDistinctReason) {
    ReplicaPlacementPolicy policy(TestConfig());
    std::array<ReplicaTierObservation, kReplicaPlacementTierCount> observed{{
        {.allocation_state = ReplicaTierSignalState::AVAILABLE,
         .health_state = ReplicaTierSignalState::UNAVAILABLE},
        Available(1),
        Available(1),
        Available(1),
    }};

    const auto plan = policy.Plan(ReplicaTemperature::HOT, observed);

    ASSERT_EQ(plan.degraded_reason_count, 1);
    EXPECT_EQ(plan.degraded_reasons[0],
              ReplicaPlacementDegradedReason::REQUIRED_TIER_UNHEALTHY);
}

TEST(ReplicaPlacementPolicyTest, InsufficientTargetRejectsConfiguration) {
    auto config = TestConfig();
    config.targets[static_cast<size_t>(ReplicaTemperature::COLD)] = {};
    EXPECT_THROW(ReplicaPlacementPolicy(std::move(config)),
                 std::invalid_argument);
}

TEST(ReplicaPlacementPolicyTest, RequiredUnknownSignalIsNotFabricated) {
    ReplicaPlacementPolicy policy(TestConfig());
    std::array<ReplicaTierObservation, kReplicaPlacementTierCount> observed{{
        {},
        Available(1),
        Available(1),
        Available(1),
    }};

    const auto plan = policy.Plan(ReplicaTemperature::HOT, observed);

    ASSERT_EQ(plan.degraded_reason_count, 1);
    EXPECT_EQ(plan.degraded_reasons[0],
              ReplicaPlacementDegradedReason::REQUIRED_TIER_SIGNAL_UNKNOWN);
    EXPECT_EQ(plan.adjustments[Index(ReplicaPlacementTier::MEMORY)].add, 0);
    EXPECT_EQ(plan.adjustments[Index(ReplicaPlacementTier::MEMORY)].remove, 0);
}

TEST(ReplicaPlacementPolicyTest, ExcessiveTargetRejectsConfiguration) {
    auto config = TestConfig();
    config
        .targets[static_cast<size_t>(ReplicaTemperature::HOT)]
                [Index(ReplicaPlacementTier::MEMORY)]
        .desired = 9;
    EXPECT_THROW(ReplicaPlacementPolicy(std::move(config)),
                 std::invalid_argument);
}

TEST(ReplicaPlacementPolicyTest, RequiredZeroTargetRejectsConfiguration) {
    auto config = TestConfig();
    config
        .targets[static_cast<size_t>(ReplicaTemperature::HOT)]
                [Index(ReplicaPlacementTier::NOF_SSD)]
        .required = true;
    config
        .targets[static_cast<size_t>(ReplicaTemperature::HOT)]
                [Index(ReplicaPlacementTier::NOF_SSD)]
        .desired = 0;
    EXPECT_THROW(ReplicaPlacementPolicy(std::move(config)),
                 std::invalid_argument);
}

TEST(ReplicaPlacementPolicyTest, InvalidTemperatureIsRejected) {
    ReplicaPlacementPolicy policy(TestConfig());
    std::array<ReplicaTierObservation, kReplicaPlacementTierCount> observed{};
    EXPECT_THROW(
        (void)policy.Plan(static_cast<ReplicaTemperature>(255), observed),
        std::invalid_argument);
}

TEST(ReplicaPlacementPolicyTest,
     DoesNotRemoveLastReplicaWhenTargetUnavailable) {
    auto config = TestConfig();
    config.min_complete_replicas = 2;
    ReplicaPlacementPolicy policy(std::move(config));
    std::array<ReplicaTierObservation, kReplicaPlacementTierCount> observed{{
        {.complete = 1,
         .allocation_state = ReplicaTierSignalState::UNAVAILABLE,
         .health_state = ReplicaTierSignalState::AVAILABLE},
        Available(1),
        {.allocation_state = ReplicaTierSignalState::UNAVAILABLE,
         .health_state = ReplicaTierSignalState::AVAILABLE},
        {.allocation_state = ReplicaTierSignalState::UNAVAILABLE,
         .health_state = ReplicaTierSignalState::AVAILABLE},
    }};

    const auto plan = policy.Plan(ReplicaTemperature::COLD, observed);

    EXPECT_TRUE(plan.degraded());
    EXPECT_EQ(plan.adjustments[Index(ReplicaPlacementTier::MEMORY)].remove, 0);
}

TEST(ReplicaPlacementPolicyTest,
     PendingReplicaCannotSatisfyCompletedReplicaSafetyFloor) {
    ReplicaPlacementPolicy policy(TestConfig());
    std::array<ReplicaTierObservation, kReplicaPlacementTierCount> observed{{
        Available(1),
        Available(0, 1),
        Available(),
        Available(),
    }};

    const auto plan = policy.Plan(ReplicaTemperature::COLD, observed);

    EXPECT_TRUE(plan.degraded());
    EXPECT_EQ(plan.adjustments[Index(ReplicaPlacementTier::MEMORY)].remove, 0);
    ASSERT_GT(plan.degraded_reason_count, 0);
    EXPECT_EQ(plan.degraded_reasons[plan.degraded_reason_count - 1],
              ReplicaPlacementDegradedReason::MIN_COMPLETE_UNSATISFIED);
}

TEST(ReplicaPlacementPolicyTest, ConcurrentPlanningIsDeterministic) {
    const ReplicaPlacementPolicy policy(TestConfig());
    const std::array<ReplicaTierObservation, kReplicaPlacementTierCount>
        observed{{
            Available(1),
            Available(2),
            Available(),
            Available(1),
        }};
    const auto expected = policy.Plan(ReplicaTemperature::HOT, observed);
    std::atomic<uint64_t> mismatches{0};
    constexpr size_t kThreadCount = 32;
    constexpr size_t kIterationsPerThread = 10'000;
    std::vector<std::thread> threads;
    threads.reserve(kThreadCount);

    for (size_t thread = 0; thread < kThreadCount; ++thread) {
        threads.emplace_back([&]() {
            for (size_t iteration = 0; iteration < kIterationsPerThread;
                 ++iteration) {
                const auto actual =
                    policy.Plan(ReplicaTemperature::HOT, observed);
                if (actual.degraded_reason_count !=
                    expected.degraded_reason_count) {
                    mismatches.fetch_add(1, std::memory_order_relaxed);
                    continue;
                }
                for (size_t i = 0; i < kReplicaPlacementTierCount; ++i) {
                    const auto& lhs = actual.adjustments[i];
                    const auto& rhs = expected.adjustments[i];
                    if (lhs.tier != rhs.tier || lhs.add != rhs.add ||
                        lhs.remove != rhs.remove ||
                        lhs.effective_target != rhs.effective_target) {
                        mismatches.fetch_add(1, std::memory_order_relaxed);
                        break;
                    }
                }
            }
        });
    }
    for (auto& thread : threads) {
        thread.join();
    }

    EXPECT_EQ(mismatches.load(std::memory_order_relaxed), 0);
}

}  // namespace
}  // namespace mooncake
