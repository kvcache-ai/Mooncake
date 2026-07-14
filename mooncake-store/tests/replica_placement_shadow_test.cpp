// Copyright 2026 Mooncake Authors

#include "replica_placement_shadow.h"

#include <atomic>
#include <chrono>
#include <memory>
#include <span>
#include <stdexcept>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include <gtest/gtest.h>

namespace mooncake {
namespace {

using namespace std::chrono_literals;

class FakeClock final : public ReplicaPlacementSignalClock {
   public:
    [[nodiscard]] TimePoint Now() const noexcept override {
        return TimePoint(std::chrono::nanoseconds(now_ns_.load()));
    }

    void Set(std::chrono::nanoseconds now) {
        now_ns_.store(now.count(), std::memory_order_relaxed);
    }

   private:
    std::atomic<int64_t> now_ns_{0};
};

size_t Tier(ReplicaPlacementTier tier) { return static_cast<size_t>(tier); }

size_t Observation(ReplicaTemperature temperature,
                   ReplicaPlacementShadowSignalStatus status) {
    return static_cast<size_t>(temperature) *
               kReplicaPlacementShadowSignalStatusCount +
           static_cast<size_t>(status);
}

size_t Intent(ReplicaTemperature temperature, ReplicaPlacementTier tier) {
    return static_cast<size_t>(temperature) * kReplicaPlacementTierCount +
           Tier(tier);
}

ReplicaPlacementPolicyConfig PolicyConfig() {
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

ReplicaPlacementShadowConfig ShadowConfig() {
    ReplicaPlacementShadowConfig config;
    config.policy = PolicyConfig();
    config.warm_threshold = 2;
    config.hot_threshold = 3;
    config.signal_ttl = 10s;
    config.sketch_width = 1024;
    config.sketch_depth = 4;
    return config;
}

Replica::Descriptor Memory(ReplicaID id, ReplicaStatus status) {
    Replica::Descriptor descriptor;
    descriptor.id = id;
    descriptor.descriptor_variant = MemoryDescriptor{};
    descriptor.status = status;
    return descriptor;
}

Replica::Descriptor LocalDisk(ReplicaID id, ReplicaStatus status) {
    Replica::Descriptor descriptor;
    descriptor.id = id;
    descriptor.descriptor_variant = LocalDiskDescriptor{};
    descriptor.status = status;
    return descriptor;
}

Replica::Descriptor NoF(ReplicaID id, ReplicaStatus status) {
    Replica::Descriptor descriptor;
    descriptor.id = id;
    descriptor.descriptor_variant = NoFDescriptor{};
    descriptor.status = status;
    return descriptor;
}

Replica::Descriptor RemoteStore(ReplicaID id, ReplicaStatus status) {
    Replica::Descriptor descriptor;
    descriptor.id = id;
    descriptor.descriptor_variant = DiskDescriptor{};
    descriptor.status = status;
    return descriptor;
}

ReplicaPlacementSignalSnapshot AvailableSnapshot(
    uint64_t generation, ReplicaPlacementSignalClock::TimePoint observed_at) {
    ReplicaPlacementSignalSnapshot snapshot;
    snapshot.generation = generation;
    snapshot.ready = true;
    snapshot.observed_at = observed_at;
    for (auto& tier : snapshot.tiers) {
        tier.allocation_state = ReplicaTierSignalState::AVAILABLE;
        tier.health_state = ReplicaTierSignalState::AVAILABLE;
    }
    return snapshot;
}

TEST(ReplicaPlacementShadowTest,
     BuildsFourTierInventoryAndIgnoresTerminalNoise) {
    const std::vector<Replica::Descriptor> replicas = {
        Memory(1, ReplicaStatus::COMPLETE),
        Memory(2, ReplicaStatus::PROCESSING),
        LocalDisk(3, ReplicaStatus::COMPLETE),
        NoF(4, ReplicaStatus::INITIALIZED),
        RemoteStore(5, ReplicaStatus::COMPLETE),
        RemoteStore(6, ReplicaStatus::FAILED),
        LocalDisk(7, ReplicaStatus::REMOVED),
    };

    const auto inventory = BuildReplicaPlacementInventory(replicas);

    EXPECT_EQ(inventory[Tier(ReplicaPlacementTier::MEMORY)].complete, 1);
    EXPECT_EQ(inventory[Tier(ReplicaPlacementTier::MEMORY)].pending, 1);
    EXPECT_EQ(inventory[Tier(ReplicaPlacementTier::LOCAL_DISK)].complete, 1);
    EXPECT_EQ(inventory[Tier(ReplicaPlacementTier::NOF_SSD)].pending, 1);
    EXPECT_EQ(inventory[Tier(ReplicaPlacementTier::REMOTE_STORE)].complete, 1);
}

TEST(ReplicaPlacementShadowTest, RejectsInvalidShadowConfiguration) {
    auto clock = std::make_shared<FakeClock>();
    auto config = ShadowConfig();
    config.hot_threshold = config.warm_threshold;
    EXPECT_THROW(ReplicaPlacementShadowEvaluator(std::move(config), clock),
                 std::invalid_argument);
}

TEST(ReplicaPlacementShadowTest, MissingSnapshotNeverFabricatesSignals) {
    auto clock = std::make_shared<FakeClock>();
    ReplicaPlacementShadowEvaluator evaluator(ShadowConfig(), clock);
    const std::vector<Replica::Descriptor> replicas = {
        LocalDisk(1, ReplicaStatus::COMPLETE),
        RemoteStore(2, ReplicaStatus::COMPLETE),
    };

    const auto result = evaluator.Observe("tenant/key", replicas);

    EXPECT_EQ(result.temperature, ReplicaTemperature::COLD);
    EXPECT_EQ(result.signal_status,
              ReplicaPlacementShadowSignalStatus::MISSING_SNAPSHOT);
    EXPECT_TRUE(result.plan.degraded());
    EXPECT_EQ(result.plan.adjustments[Tier(ReplicaPlacementTier::MEMORY)].add,
              0);
    const auto counters = evaluator.Counters();
    EXPECT_EQ(counters.observations[Observation(
                  ReplicaTemperature::COLD,
                  ReplicaPlacementShadowSignalStatus::MISSING_SNAPSHOT)],
              1);
}

TEST(ReplicaPlacementShadowTest, ValidatesUnavailableAndGenerationSnapshots) {
    auto clock = std::make_shared<FakeClock>();
    clock->Set(10s);
    ReplicaPlacementShadowEvaluator evaluator(ShadowConfig(), clock);

    ReplicaPlacementSignalSnapshot unavailable;
    unavailable.generation = 1;
    unavailable.observed_at = clock->Now();
    EXPECT_EQ(evaluator.PublishSignalSnapshot(unavailable),
              ReplicaPlacementSignalPublishStatus::PUBLISHED);
    EXPECT_EQ(evaluator.CurrentGeneration(), 1);

    auto invalid = unavailable;
    invalid.generation = 2;
    invalid.tiers[0].health_state = ReplicaTierSignalState::AVAILABLE;
    EXPECT_EQ(evaluator.PublishSignalSnapshot(invalid),
              ReplicaPlacementSignalPublishStatus::INVALID_SNAPSHOT);
    EXPECT_EQ(evaluator.CurrentGeneration(), 1);

    unavailable.generation = 1;
    EXPECT_EQ(evaluator.PublishSignalSnapshot(unavailable),
              ReplicaPlacementSignalPublishStatus::GENERATION_NOT_INCREASING);
}

TEST(ReplicaPlacementShadowTest, FreshSignalsDriveColdWarmHotPlansAndCounters) {
    auto clock = std::make_shared<FakeClock>();
    clock->Set(10s);
    ReplicaPlacementShadowEvaluator evaluator(ShadowConfig(), clock);
    ASSERT_EQ(
        evaluator.PublishSignalSnapshot(AvailableSnapshot(1, clock->Now())),
        ReplicaPlacementSignalPublishStatus::PUBLISHED);
    const std::vector<Replica::Descriptor> replicas = {
        LocalDisk(1, ReplicaStatus::COMPLETE),
        RemoteStore(2, ReplicaStatus::COMPLETE),
    };

    const auto cold = evaluator.Observe("tenant/hot-key", replicas);
    const auto warm = evaluator.Observe("tenant/hot-key", replicas);
    const auto hot = evaluator.Observe("tenant/hot-key", replicas);

    EXPECT_EQ(cold.temperature, ReplicaTemperature::COLD);
    EXPECT_EQ(warm.temperature, ReplicaTemperature::WARM);
    EXPECT_EQ(hot.temperature, ReplicaTemperature::HOT);
    EXPECT_EQ(warm.plan.adjustments[Tier(ReplicaPlacementTier::MEMORY)].add, 1);
    EXPECT_EQ(hot.plan.adjustments[Tier(ReplicaPlacementTier::MEMORY)].add, 2);
    EXPECT_EQ(hot.plan.adjustments[Tier(ReplicaPlacementTier::NOF_SSD)].add, 1);

    const auto counters = evaluator.Counters();
    EXPECT_EQ(counters.add_intents[Intent(ReplicaTemperature::WARM,
                                          ReplicaPlacementTier::MEMORY)],
              1);
    EXPECT_EQ(counters.add_intents[Intent(ReplicaTemperature::HOT,
                                          ReplicaPlacementTier::MEMORY)],
              2);
}

TEST(ReplicaPlacementShadowTest, StaleAndUnavailableSnapshotsStayNonActuating) {
    auto clock = std::make_shared<FakeClock>();
    clock->Set(10s);
    ReplicaPlacementShadowEvaluator evaluator(ShadowConfig(), clock);
    ASSERT_EQ(
        evaluator.PublishSignalSnapshot(AvailableSnapshot(1, clock->Now())),
        ReplicaPlacementSignalPublishStatus::PUBLISHED);
    clock->Set(21s);
    const auto stale = evaluator.Observe(
        "tenant/key-a", std::span<const Replica::Descriptor>{});
    EXPECT_EQ(stale.signal_status,
              ReplicaPlacementShadowSignalStatus::STALE_SNAPSHOT);
    for (const auto& adjustment : stale.plan.adjustments) {
        EXPECT_EQ(adjustment.add, 0);
        EXPECT_EQ(adjustment.remove, 0);
    }

    ReplicaPlacementSignalSnapshot unavailable;
    unavailable.generation = 2;
    unavailable.observed_at = clock->Now();
    ASSERT_EQ(evaluator.PublishSignalSnapshot(unavailable),
              ReplicaPlacementSignalPublishStatus::PUBLISHED);
    const auto down = evaluator.Observe("tenant/key-b",
                                        std::span<const Replica::Descriptor>{});
    EXPECT_EQ(down.signal_status,
              ReplicaPlacementShadowSignalStatus::SOURCE_UNAVAILABLE);
    for (const auto& adjustment : down.plan.adjustments) {
        EXPECT_EQ(adjustment.add, 0);
        EXPECT_EQ(adjustment.remove, 0);
    }
}

TEST(ReplicaPlacementShadowTest, ConcurrentObserveAndPublishKeepsExactCount) {
    auto clock = std::make_shared<FakeClock>();
    clock->Set(10s);
    ReplicaPlacementShadowEvaluator evaluator(ShadowConfig(), clock);
    ASSERT_EQ(
        evaluator.PublishSignalSnapshot(AvailableSnapshot(1, clock->Now())),
        ReplicaPlacementSignalPublishStatus::PUBLISHED);
    const std::vector<Replica::Descriptor> replicas = {
        Memory(1, ReplicaStatus::COMPLETE),
        LocalDisk(2, ReplicaStatus::COMPLETE),
        RemoteStore(3, ReplicaStatus::COMPLETE),
    };
    constexpr size_t kThreadCount = 8;
    constexpr size_t kIterations = 2'000;
    std::atomic<uint64_t> publish_failures{0};
    std::vector<std::thread> threads;
    threads.reserve(kThreadCount + 1);

    for (size_t thread = 0; thread < kThreadCount; ++thread) {
        threads.emplace_back([&, thread]() {
            const std::string key = "tenant/key-" + std::to_string(thread);
            for (size_t i = 0; i < kIterations; ++i) {
                (void)evaluator.Observe(key, replicas);
            }
        });
    }
    threads.emplace_back([&]() {
        for (uint64_t generation = 2; generation <= 101; ++generation) {
            if (evaluator.PublishSignalSnapshot(
                    AvailableSnapshot(generation, clock->Now())) !=
                ReplicaPlacementSignalPublishStatus::PUBLISHED) {
                publish_failures.fetch_add(1, std::memory_order_relaxed);
            }
        }
    });
    for (auto& thread : threads) {
        thread.join();
    }

    const auto counters = evaluator.Counters();
    uint64_t total = 0;
    for (uint64_t count : counters.observations) {
        total += count;
    }
    EXPECT_EQ(total, kThreadCount * kIterations);
    EXPECT_EQ(publish_failures.load(std::memory_order_relaxed), 0);
    EXPECT_EQ(evaluator.CurrentGeneration(), 101);
}

}  // namespace
}  // namespace mooncake
