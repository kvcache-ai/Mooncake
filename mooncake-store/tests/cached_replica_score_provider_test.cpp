// Copyright 2026 Mooncake Authors

#include "cached_replica_score_provider.h"

#include <gtest/gtest.h>

#include <array>
#include <atomic>
#include <chrono>
#include <cmath>
#include <cstdint>
#include <limits>
#include <memory>
#include <span>
#include <stdexcept>
#include <string>
#include <thread>
#include <utility>
#include <vector>

namespace mooncake {
namespace {

using TimePoint = ReplicaSignalClock::TimePoint;

class FakeReplicaSignalClock final : public ReplicaSignalClock {
   public:
    explicit FakeReplicaSignalClock(TimePoint now) { Set(now); }

    [[nodiscard]] TimePoint Now() const noexcept override {
        calls.fetch_add(1, std::memory_order_relaxed);
        return TimePoint(
            std::chrono::nanoseconds(ticks_.load(std::memory_order_relaxed)));
    }

    void Set(TimePoint now) noexcept {
        ticks_.store(std::chrono::duration_cast<std::chrono::nanoseconds>(
                         now.time_since_epoch())
                         .count(),
                     std::memory_order_relaxed);
    }

    mutable std::atomic<uint64_t> calls{0};

   private:
    std::atomic<int64_t> ticks_{0};
};

TimePoint At(std::chrono::milliseconds since_epoch) {
    return TimePoint(since_epoch);
}

ReplicaSignalSourcePolicy Policy(
    bool enabled, bool required, double weight,
    std::chrono::nanoseconds ttl = std::chrono::seconds(1)) {
    ReplicaSignalSourcePolicy policy;
    policy.enabled = enabled;
    policy.required = required;
    policy.ttl = ttl;
    policy.weight = weight;
    return policy;
}

ReplicaSignalSourceObservation Observation(bool ready, TimePoint observed_at) {
    return {ready, observed_at};
}

ReplicaSignalProviderConfig TopologyConfig(bool required = true) {
    ReplicaSignalProviderConfig config;
    config.topology_source = Policy(true, required, 1.0);
    return config;
}

ReplicaSignalProviderConfig LoadConfig(bool required = true) {
    ReplicaSignalProviderConfig config;
    config.load_source = Policy(true, required, 1.0);
    return config;
}

ReplicaSignalProviderConfig HealthConfig(bool required = true,
                                         double degraded_floor = 0.5) {
    ReplicaSignalProviderConfig config;
    config.health_source = Policy(true, required, 1.0);
    config.degraded_health_cost_floor = degraded_floor;
    return config;
}

ReplicaTopologyRouteKey Route(std::string requester, std::string destination,
                              std::string candidate, std::string protocol) {
    return {std::move(requester), std::move(destination), std::move(candidate),
            std::move(protocol)};
}

ReplicaEndpointProtocolKey Endpoint(std::string endpoint,
                                    std::string protocol) {
    return {std::move(endpoint), std::move(protocol)};
}

ReplicaScoreCandidateView Candidate(size_t order, ReplicaID id,
                                    std::string_view endpoint,
                                    std::string_view protocol = "rdma") {
    return {{order, id}, endpoint, protocol};
}

ReplicaSignalSnapshot TopologySnapshot(uint64_t generation, TimePoint now,
                                       double first_cost = 0.2,
                                       double second_cost = 0.8) {
    ReplicaSignalSnapshot snapshot;
    snapshot.generation = generation;
    snapshot.topology_source = Observation(true, now);
    snapshot.topology.emplace(Route("requester", "rack-a", "first", "rdma"),
                              first_cost);
    snapshot.topology.emplace(Route("requester", "rack-a", "second", "rdma"),
                              second_cost);
    return snapshot;
}

ReplicaSignalSnapshot LoadSnapshot(uint64_t generation, TimePoint now,
                                   uint64_t queued = 25,
                                   uint64_t saturation = 100) {
    ReplicaSignalSnapshot snapshot;
    snapshot.generation = generation;
    snapshot.load_source = Observation(true, now);
    snapshot.load.emplace(Endpoint("first", "rdma"),
                          ReplicaLoadSignal{queued, saturation, 0.0});
    return snapshot;
}

ReplicaSelectionContext Context(uint64_t requested_bytes = 0) {
    ReplicaSelectionContext context;
    context.requester_endpoint = "requester";
    context.destination_location = "rack-a";
    context.requested_bytes = requested_bytes;
    return context;
}

const ReplicaEligibleScore* Eligible(const ReplicaScoreVerdict& verdict) {
    return std::get_if<ReplicaEligibleScore>(&verdict);
}

const ReplicaScoreUnavailable* Unavailable(const ReplicaScoreVerdict& verdict) {
    return std::get_if<ReplicaScoreUnavailable>(&verdict);
}

TEST(CachedReplicaScoreProviderTest,
     EmptyProviderReturnsOneConsistentSourceUnavailableBatch) {
    auto clock = std::make_shared<FakeReplicaSignalClock>(
        At(std::chrono::milliseconds(1)));
    CachedReplicaScoreProvider provider(TopologyConfig(), clock);
    const std::array candidates = {Candidate(0, 1, "first"),
                                   Candidate(1, 2, "second")};
    std::array<ReplicaScoreVerdict, 2> verdicts;

    provider.ScoreAll(Context(), candidates, verdicts);

    for (const auto& verdict : verdicts) {
        ASSERT_NE(Unavailable(verdict), nullptr);
        EXPECT_EQ(Unavailable(verdict)->reason,
                  ReplicaScoreUnavailableReason::SIGNAL_SOURCE_UNAVAILABLE);
    }
    EXPECT_EQ(clock->calls.load(std::memory_order_relaxed), 1u);
    EXPECT_EQ(provider.CurrentGeneration(), 0u);
}

TEST(CachedReplicaScoreProviderTest,
     PublishOwnsDataAndNormalizesAllEnabledWeights) {
    const auto now = At(std::chrono::milliseconds(100));
    auto clock = std::make_shared<FakeReplicaSignalClock>(now);
    ReplicaSignalProviderConfig config;
    config.topology_source = Policy(true, true, 2.0);
    config.load_source = Policy(true, true, 3.0);
    config.health_source = Policy(true, true, 5.0);
    CachedReplicaScoreProvider provider(config, clock);
    ReplicaSignalSnapshot snapshot;
    snapshot.generation = 1;
    snapshot.topology_source = Observation(true, now);
    snapshot.load_source = Observation(true, now);
    snapshot.health_source = Observation(true, now);
    snapshot.topology.emplace(Route("requester", "rack-a", "first", "rdma"),
                              0.5);
    snapshot.load.emplace(Endpoint("first", "rdma"),
                          ReplicaLoadSignal{25, 100, 0.0});
    snapshot.health.emplace(
        Endpoint("first", "rdma"),
        ReplicaHealthSignal{ReplicaHealthState::HEALTHY, 0.1});
    ASSERT_EQ(provider.PublishSnapshot(snapshot),
              ReplicaSignalPublishStatus::PUBLISHED);

    snapshot.topology.begin()->second = 1.0;
    snapshot.load.begin()->second.queued_bytes = 100;
    snapshot.health.begin()->second.cost = 1.0;
    const std::array candidates = {Candidate(0, 1, "first")};
    std::array<ReplicaScoreVerdict, 1> verdicts;
    provider.ScoreAll(Context(), candidates, verdicts);

    ASSERT_NE(Eligible(verdicts[0]), nullptr);
    EXPECT_DOUBLE_EQ(Eligible(verdicts[0])->breakdown.topology_cost, 0.1);
    EXPECT_DOUBLE_EQ(Eligible(verdicts[0])->breakdown.load_cost, 0.075);
    EXPECT_DOUBLE_EQ(Eligible(verdicts[0])->breakdown.health_cost, 0.05);
}

TEST(CachedReplicaScoreProviderTest,
     TopologyRouteUsesCompleteRequesterDestinationEndpointProtocolKey) {
    const auto now = At(std::chrono::milliseconds(100));
    auto clock = std::make_shared<FakeReplicaSignalClock>(now);
    CachedReplicaScoreProvider provider(TopologyConfig(), clock);
    ASSERT_EQ(provider.PublishSnapshot(TopologySnapshot(1, now)),
              ReplicaSignalPublishStatus::PUBLISHED);
    const std::array good = {Candidate(0, 1, "first", "rdma")};
    const std::array wrong_protocol = {Candidate(0, 1, "first", "tcp")};
    std::array<ReplicaScoreVerdict, 1> verdicts;

    provider.ScoreAll(Context(), good, verdicts);
    ASSERT_NE(Eligible(verdicts[0]), nullptr);
    auto wrong_destination = Context();
    wrong_destination.destination_location = "rack-b";
    provider.ScoreAll(wrong_destination, good, verdicts);
    ASSERT_NE(Unavailable(verdicts[0]), nullptr);
    EXPECT_EQ(Unavailable(verdicts[0])->reason,
              ReplicaScoreUnavailableReason::MISSING_SIGNAL);
    provider.ScoreAll(Context(), wrong_protocol, verdicts);
    ASSERT_NE(Unavailable(verdicts[0]), nullptr);
    EXPECT_EQ(Unavailable(verdicts[0])->reason,
              ReplicaScoreUnavailableReason::MISSING_SIGNAL);
}

TEST(CachedReplicaScoreProviderTest,
     EndpointProtocolKeySeparatesLoadSignalsAtOneEndpoint) {
    const auto now = At(std::chrono::milliseconds(100));
    auto clock = std::make_shared<FakeReplicaSignalClock>(now);
    CachedReplicaScoreProvider provider(LoadConfig(), clock);
    ReplicaSignalSnapshot snapshot;
    snapshot.generation = 1;
    snapshot.load_source = Observation(true, now);
    snapshot.load.emplace(Endpoint("same", "rdma"),
                          ReplicaLoadSignal{0, 100, 0.0});
    snapshot.load.emplace(Endpoint("same", "tcp"),
                          ReplicaLoadSignal{100, 100, 0.0});
    ASSERT_EQ(provider.PublishSnapshot(std::move(snapshot)),
              ReplicaSignalPublishStatus::PUBLISHED);
    const std::array candidates = {Candidate(0, 1, "same", "rdma"),
                                   Candidate(1, 2, "same", "tcp")};
    std::array<ReplicaScoreVerdict, 2> verdicts;

    provider.ScoreAll(Context(), candidates, verdicts);

    ASSERT_NE(Eligible(verdicts[0]), nullptr);
    ASSERT_NE(Eligible(verdicts[1]), nullptr);
    EXPECT_DOUBLE_EQ(Eligible(verdicts[0])->breakdown.load_cost, 0.0);
    EXPECT_DOUBLE_EQ(Eligible(verdicts[1])->breakdown.load_cost, 1.0);
}

TEST(CachedReplicaScoreProviderTest,
     RequiredBatchIssuePriorityIsSourceDownThenStaleThenMissing) {
    const auto now = At(std::chrono::milliseconds(1000));
    auto clock = std::make_shared<FakeReplicaSignalClock>(now);
    ReplicaSignalProviderConfig config;
    config.topology_source = Policy(true, true, 1.0);
    config.load_source =
        Policy(true, true, 1.0, std::chrono::milliseconds(100));
    config.health_source = Policy(true, true, 1.0);
    CachedReplicaScoreProvider provider(config, clock);
    ReplicaSignalSnapshot snapshot;
    snapshot.generation = 1;
    snapshot.topology_source = Observation(true, now);
    snapshot.load_source =
        Observation(true, At(std::chrono::milliseconds(800)));
    snapshot.health_source = Observation(false, now);
    snapshot.load.emplace(Endpoint("first", "rdma"),
                          ReplicaLoadSignal{0, 100, 0.0});
    ASSERT_EQ(provider.PublishSnapshot(std::move(snapshot)),
              ReplicaSignalPublishStatus::PUBLISHED);
    const std::array candidates = {Candidate(0, 1, "first")};
    std::array<ReplicaScoreVerdict, 1> verdicts;

    provider.ScoreAll(Context(), candidates, verdicts);

    ASSERT_NE(Unavailable(verdicts[0]), nullptr);
    EXPECT_EQ(Unavailable(verdicts[0])->reason,
              ReplicaScoreUnavailableReason::SIGNAL_SOURCE_UNAVAILABLE);

    ReplicaSignalProviderConfig stale_config;
    stale_config.topology_source = Policy(true, true, 1.0);
    stale_config.load_source =
        Policy(true, true, 1.0, std::chrono::milliseconds(100));
    CachedReplicaScoreProvider stale_provider(stale_config, clock);
    auto stale_over_missing = TopologySnapshot(1, now);
    stale_over_missing.topology.clear();
    stale_over_missing.load_source =
        Observation(true, At(std::chrono::milliseconds(800)));
    stale_over_missing.load.emplace(Endpoint("first", "rdma"),
                                    ReplicaLoadSignal{0, 100, 0.0});
    ASSERT_EQ(stale_provider.PublishSnapshot(std::move(stale_over_missing)),
              ReplicaSignalPublishStatus::PUBLISHED);
    stale_provider.ScoreAll(Context(), candidates, verdicts);
    ASSERT_NE(Unavailable(verdicts[0]), nullptr);
    EXPECT_EQ(Unavailable(verdicts[0])->reason,
              ReplicaScoreUnavailableReason::STALE);
}

TEST(CachedReplicaScoreProviderTest,
     FreshHealthVetoSurvivesAnotherRequiredSourceFailure) {
    const auto now = At(std::chrono::milliseconds(100));
    auto clock = std::make_shared<FakeReplicaSignalClock>(now);
    ReplicaSignalProviderConfig config;
    config.topology_source = Policy(true, true, 1.0);
    config.health_source = Policy(true, false, 0.0);
    CachedReplicaScoreProvider provider(config, clock);
    ReplicaSignalSnapshot snapshot;
    snapshot.generation = 1;
    snapshot.topology_source = Observation(false, now);
    snapshot.health_source = Observation(true, now);
    snapshot.health.emplace(
        Endpoint("first", "rdma"),
        ReplicaHealthSignal{ReplicaHealthState::UNHEALTHY, 0.0});
    snapshot.health.emplace(
        Endpoint("second", "rdma"),
        ReplicaHealthSignal{ReplicaHealthState::HEALTHY, 0.0});
    ASSERT_EQ(provider.PublishSnapshot(std::move(snapshot)),
              ReplicaSignalPublishStatus::PUBLISHED);
    const std::array candidates = {Candidate(0, 1, "first"),
                                   Candidate(1, 2, "second")};
    std::array<ReplicaScoreVerdict, 2> verdicts;

    provider.ScoreAll(Context(), candidates, verdicts);

    const auto* veto = std::get_if<ReplicaHardVeto>(&verdicts[0]);
    ASSERT_NE(veto, nullptr);
    EXPECT_EQ(veto->reason, ReplicaHardVetoReason::UNHEALTHY);
    ASSERT_NE(Unavailable(verdicts[1]), nullptr);
    EXPECT_EQ(Unavailable(verdicts[1])->reason,
              ReplicaScoreUnavailableReason::SIGNAL_SOURCE_UNAVAILABLE);
}

TEST(CachedReplicaScoreProviderTest,
     LaterMissingRequiredEntryInvalidatesEarlierScoreButPreservesVeto) {
    const auto now = At(std::chrono::milliseconds(100));
    auto clock = std::make_shared<FakeReplicaSignalClock>(now);
    ReplicaSignalProviderConfig config;
    config.topology_source = Policy(true, true, 1.0);
    config.health_source = Policy(true, false, 1.0);
    CachedReplicaScoreProvider provider(config, clock);
    ReplicaSignalSnapshot snapshot;
    snapshot.generation = 1;
    snapshot.topology_source = Observation(true, now);
    snapshot.health_source = Observation(true, now);
    snapshot.topology.emplace(Route("requester", "rack-a", "first", "rdma"),
                              0.2);
    snapshot.health.emplace(
        Endpoint("first", "rdma"),
        ReplicaHealthSignal{ReplicaHealthState::HEALTHY, 0.0});
    snapshot.health.emplace(
        Endpoint("second", "rdma"),
        ReplicaHealthSignal{ReplicaHealthState::UNHEALTHY, 0.0});
    ASSERT_EQ(provider.PublishSnapshot(std::move(snapshot)),
              ReplicaSignalPublishStatus::PUBLISHED);
    const std::array candidates = {Candidate(0, 1, "first"),
                                   Candidate(1, 2, "second")};
    std::array<ReplicaScoreVerdict, 2> verdicts;

    provider.ScoreAll(Context(), candidates, verdicts);

    ASSERT_NE(Unavailable(verdicts[0]), nullptr);
    EXPECT_EQ(Unavailable(verdicts[0])->reason,
              ReplicaScoreUnavailableReason::MISSING_SIGNAL);
    const auto* veto = std::get_if<ReplicaHardVeto>(&verdicts[1]);
    ASSERT_NE(veto, nullptr);
    EXPECT_EQ(veto->reason, ReplicaHardVetoReason::UNHEALTHY);
}

TEST(CachedReplicaScoreProviderTest, StaleHealthNeverCreatesAHardVeto) {
    const auto now = At(std::chrono::milliseconds(1000));
    auto clock = std::make_shared<FakeReplicaSignalClock>(now);
    auto required_config = HealthConfig(true);
    required_config.health_source.ttl = std::chrono::milliseconds(100);
    CachedReplicaScoreProvider provider(required_config, clock);
    ReplicaSignalSnapshot required;
    required.generation = 1;
    required.health_source =
        Observation(true, At(std::chrono::milliseconds(800)));
    required.health.emplace(
        Endpoint("first", "rdma"),
        ReplicaHealthSignal{ReplicaHealthState::CIRCUIT_OPEN, 0.0});
    ASSERT_EQ(provider.PublishSnapshot(std::move(required)),
              ReplicaSignalPublishStatus::PUBLISHED);
    const std::array candidates = {Candidate(0, 1, "first")};
    std::array<ReplicaScoreVerdict, 1> verdicts;

    provider.ScoreAll(Context(), candidates, verdicts);
    ASSERT_NE(Unavailable(verdicts[0]), nullptr);
    EXPECT_EQ(Unavailable(verdicts[0])->reason,
              ReplicaScoreUnavailableReason::STALE);

    auto optional_config = HealthConfig(false);
    optional_config.health_source.ttl = std::chrono::milliseconds(100);
    CachedReplicaScoreProvider optional_provider(optional_config, clock);
    ReplicaSignalSnapshot optional;
    optional.generation = 1;
    optional.health_source =
        Observation(true, At(std::chrono::milliseconds(800)));
    optional.health.emplace(
        Endpoint("first", "rdma"),
        ReplicaHealthSignal{ReplicaHealthState::CIRCUIT_OPEN, 0.0});
    ASSERT_EQ(optional_provider.PublishSnapshot(std::move(optional)),
              ReplicaSignalPublishStatus::PUBLISHED);
    optional_provider.ScoreAll(Context(), candidates, verdicts);
    ASSERT_NE(Eligible(verdicts[0]), nullptr);
    EXPECT_DOUBLE_EQ(Eligible(verdicts[0])->breakdown.health_cost, 1.0);
}

TEST(CachedReplicaScoreProviderTest,
     OptionalUnavailableSourcesUseMaximumNormalizedContribution) {
    const auto now = At(std::chrono::milliseconds(100));
    auto clock = std::make_shared<FakeReplicaSignalClock>(now);
    ReplicaSignalProviderConfig config;
    config.topology_source = Policy(true, false, 2.0);
    config.load_source = Policy(true, false, 3.0);
    CachedReplicaScoreProvider provider(config, clock);
    ReplicaSignalSnapshot source_down;
    source_down.generation = 1;
    source_down.topology_source = Observation(false, now);
    source_down.load_source = Observation(true, now);
    source_down.load.emplace(Endpoint("first", "rdma"),
                             ReplicaLoadSignal{50, 100, 0.0});
    ASSERT_EQ(provider.PublishSnapshot(std::move(source_down)),
              ReplicaSignalPublishStatus::PUBLISHED);
    const std::array candidates = {Candidate(0, 1, "first")};
    std::array<ReplicaScoreVerdict, 1> verdicts;

    provider.ScoreAll(Context(), candidates, verdicts);
    ASSERT_NE(Eligible(verdicts[0]), nullptr);
    EXPECT_DOUBLE_EQ(Eligible(verdicts[0])->breakdown.topology_cost, 0.4);
    EXPECT_DOUBLE_EQ(Eligible(verdicts[0])->breakdown.load_cost, 0.3);

    ReplicaSignalSnapshot missing;
    missing.generation = 2;
    missing.topology_source = Observation(true, now);
    missing.load_source = Observation(true, now);
    missing.load.emplace(Endpoint("first", "rdma"),
                         ReplicaLoadSignal{0, 100, 0.0});
    ASSERT_EQ(provider.PublishSnapshot(std::move(missing)),
              ReplicaSignalPublishStatus::PUBLISHED);
    provider.ScoreAll(Context(), candidates, verdicts);
    ASSERT_NE(Eligible(verdicts[0]), nullptr);
    EXPECT_DOUBLE_EQ(Eligible(verdicts[0])->breakdown.topology_cost, 0.4);
    EXPECT_DOUBLE_EQ(Eligible(verdicts[0])->breakdown.load_cost, 0.0);
}

TEST(CachedReplicaScoreProviderTest,
     DisabledSourcesAreIgnoredAndMustHaveZeroWeight) {
    const auto now = At(std::chrono::milliseconds(100));
    auto clock = std::make_shared<FakeReplicaSignalClock>(now);
    CachedReplicaScoreProvider provider(LoadConfig(), clock);
    auto snapshot = LoadSnapshot(1, now, 0, 100);
    ASSERT_EQ(provider.PublishSnapshot(std::move(snapshot)),
              ReplicaSignalPublishStatus::PUBLISHED);
    const std::array candidates = {Candidate(0, 1, "first")};
    std::array<ReplicaScoreVerdict, 1> verdicts;
    provider.ScoreAll(Context(), candidates, verdicts);
    ASSERT_NE(Eligible(verdicts[0]), nullptr);
    EXPECT_DOUBLE_EQ(Eligible(verdicts[0])->breakdown.topology_cost, 0.0);

    auto disabled_entries = LoadSnapshot(2, now);
    disabled_entries.topology.emplace(Route("", "", "", ""), 0.0);
    EXPECT_EQ(provider.PublishSnapshot(std::move(disabled_entries)),
              ReplicaSignalPublishStatus::INVALID_SNAPSHOT);
    auto down_entries = TopologySnapshot(2, now);
    down_entries.topology_source = Observation(false, now);
    EXPECT_EQ(provider.PublishSnapshot(std::move(down_entries)),
              ReplicaSignalPublishStatus::INVALID_SNAPSHOT);
    CachedReplicaScoreProvider topology_provider(TopologyConfig(), clock);
    auto enabled_but_down = TopologySnapshot(1, now);
    enabled_but_down.topology_source = Observation(false, now);
    EXPECT_EQ(topology_provider.PublishSnapshot(std::move(enabled_but_down)),
              ReplicaSignalPublishStatus::INVALID_SNAPSHOT);
    EXPECT_EQ(provider.CurrentGeneration(), 1u);

    auto nonzero_weight = LoadConfig();
    nonzero_weight.topology_source = Policy(false, false, 0.1);
    EXPECT_THROW(CachedReplicaScoreProvider(nonzero_weight, clock),
                 std::invalid_argument);
    auto disabled_required = LoadConfig();
    disabled_required.topology_source = Policy(false, true, 0.0);
    EXPECT_THROW(CachedReplicaScoreProvider(disabled_required, clock),
                 std::invalid_argument);
}

TEST(CachedReplicaScoreProviderTest,
     InvalidCostsAndEntriesNeverReplaceTheLastSnapshot) {
    const auto now = At(std::chrono::milliseconds(100));
    auto clock = std::make_shared<FakeReplicaSignalClock>(now);
    CachedReplicaScoreProvider load_provider(LoadConfig(), clock);
    ASSERT_EQ(load_provider.PublishSnapshot(LoadSnapshot(10, now, 25, 100)),
              ReplicaSignalPublishStatus::PUBLISHED);
    auto zero_saturation = LoadSnapshot(11, now);
    zero_saturation.load.begin()->second.saturation_bytes = 0;
    EXPECT_EQ(load_provider.PublishSnapshot(std::move(zero_saturation)),
              ReplicaSignalPublishStatus::INVALID_SNAPSHOT);
    auto invalid_utilization = LoadSnapshot(11, now);
    invalid_utilization.load.begin()->second.utilization =
        std::numeric_limits<double>::quiet_NaN();
    EXPECT_EQ(load_provider.PublishSnapshot(std::move(invalid_utilization)),
              ReplicaSignalPublishStatus::INVALID_SNAPSHOT);
    EXPECT_EQ(load_provider.CurrentGeneration(), 10u);
    const std::array candidates = {Candidate(0, 1, "first")};
    std::array<ReplicaScoreVerdict, 1> verdicts;
    load_provider.ScoreAll(Context(), candidates, verdicts);
    ASSERT_NE(Eligible(verdicts[0]), nullptr);
    EXPECT_DOUBLE_EQ(Eligible(verdicts[0])->breakdown.load_cost, 0.25);

    CachedReplicaScoreProvider topology_provider(TopologyConfig(), clock);
    ASSERT_EQ(topology_provider.PublishSnapshot(TopologySnapshot(10, now)),
              ReplicaSignalPublishStatus::PUBLISHED);
    for (const double invalid_cost :
         {-0.1, 1.1, std::numeric_limits<double>::infinity(),
          std::numeric_limits<double>::quiet_NaN()}) {
        auto invalid = TopologySnapshot(11, now);
        invalid.topology.begin()->second = invalid_cost;
        EXPECT_EQ(topology_provider.PublishSnapshot(std::move(invalid)),
                  ReplicaSignalPublishStatus::INVALID_SNAPSHOT);
    }
    EXPECT_EQ(topology_provider.CurrentGeneration(), 10u);

    CachedReplicaScoreProvider health_provider(HealthConfig(), clock);
    ReplicaSignalSnapshot healthy;
    healthy.generation = 10;
    healthy.health_source = Observation(true, now);
    healthy.health.emplace(
        Endpoint("first", "rdma"),
        ReplicaHealthSignal{ReplicaHealthState::HEALTHY, 0.0});
    ASSERT_EQ(health_provider.PublishSnapshot(std::move(healthy)),
              ReplicaSignalPublishStatus::PUBLISHED);
    ReplicaSignalSnapshot invalid_health;
    invalid_health.generation = 11;
    invalid_health.health_source = Observation(true, now);
    invalid_health.health.emplace(
        Endpoint("first", "rdma"),
        ReplicaHealthSignal{static_cast<ReplicaHealthState>(255), 0.0});
    EXPECT_EQ(health_provider.PublishSnapshot(std::move(invalid_health)),
              ReplicaSignalPublishStatus::INVALID_SNAPSHOT);
    EXPECT_EQ(health_provider.CurrentGeneration(), 10u);
}

TEST(CachedReplicaScoreProviderTest,
     InvalidProviderPoliciesAreRejectedBeforeUse) {
    auto clock = std::make_shared<FakeReplicaSignalClock>(
        At(std::chrono::milliseconds(1)));
    for (const double invalid_weight :
         {-1.0, 0.0, std::numeric_limits<double>::infinity(),
          std::numeric_limits<double>::quiet_NaN()}) {
        auto config = TopologyConfig();
        config.topology_source.weight = invalid_weight;
        EXPECT_THROW(CachedReplicaScoreProvider(config, clock),
                     std::invalid_argument);
    }
    auto zero_ttl = TopologyConfig();
    zero_ttl.topology_source.ttl = std::chrono::nanoseconds::zero();
    EXPECT_THROW(CachedReplicaScoreProvider(zero_ttl, clock),
                 std::invalid_argument);
    auto invalid_floor = TopologyConfig();
    invalid_floor.degraded_health_cost_floor = 1.1;
    EXPECT_THROW(CachedReplicaScoreProvider(invalid_floor, clock),
                 std::invalid_argument);
}

TEST(CachedReplicaScoreProviderTest,
     GenerationMustStrictlyIncreaseAndInvalidDominatesGenerationCheck) {
    const auto now = At(std::chrono::milliseconds(100));
    auto clock = std::make_shared<FakeReplicaSignalClock>(now);
    CachedReplicaScoreProvider provider(LoadConfig(), clock);
    ASSERT_EQ(provider.PublishSnapshot(LoadSnapshot(5, now)),
              ReplicaSignalPublishStatus::PUBLISHED);
    EXPECT_EQ(provider.PublishSnapshot(LoadSnapshot(5, now)),
              ReplicaSignalPublishStatus::GENERATION_NOT_INCREASING);
    EXPECT_EQ(provider.PublishSnapshot(LoadSnapshot(4, now)),
              ReplicaSignalPublishStatus::GENERATION_NOT_INCREASING);

    auto invalid_old = LoadSnapshot(4, now);
    invalid_old.load.begin()->second.saturation_bytes = 0;
    EXPECT_EQ(provider.PublishSnapshot(std::move(invalid_old)),
              ReplicaSignalPublishStatus::INVALID_SNAPSHOT);
    auto generation_zero = LoadSnapshot(0, now);
    EXPECT_EQ(provider.PublishSnapshot(std::move(generation_zero)),
              ReplicaSignalPublishStatus::INVALID_SNAPSHOT);
    EXPECT_EQ(provider.PublishSnapshot(LoadSnapshot(9, now)),
              ReplicaSignalPublishStatus::PUBLISHED);
    EXPECT_EQ(provider.CurrentGeneration(), 9u);
}

TEST(CachedReplicaScoreProviderTest,
     SnapshotEntryStringAndCandidateLimitsAreEnforced) {
    const auto now = At(std::chrono::milliseconds(100));
    auto clock = std::make_shared<FakeReplicaSignalClock>(now);
    ReplicaSignalProviderConfig config;
    config.topology_source = Policy(true, false, 1.0);
    config.load_source = Policy(true, false, 1.0);
    config.health_source = Policy(true, false, 1.0);
    config.limits.max_topology_entries = 1;
    config.limits.max_load_entries = 1;
    config.limits.max_health_entries = 1;
    config.limits.max_total_entries = 2;
    config.limits.max_string_bytes = 4;
    config.limits.max_candidates_per_batch = 2;
    CachedReplicaScoreProvider provider(config, clock);

    auto too_many = TopologySnapshot(1, now);
    EXPECT_EQ(provider.PublishSnapshot(std::move(too_many)),
              ReplicaSignalPublishStatus::LIMIT_EXCEEDED);
    auto long_string = LoadSnapshot(1, now);
    long_string.load.clear();
    long_string.load.emplace(Endpoint("12345", "r"),
                             ReplicaLoadSignal{0, 100, 0.0});
    EXPECT_EQ(provider.PublishSnapshot(std::move(long_string)),
              ReplicaSignalPublishStatus::INVALID_SNAPSHOT);

    ReplicaSignalSnapshot total;
    total.generation = 1;
    total.topology_source = Observation(true, now);
    total.load_source = Observation(true, now);
    total.health_source = Observation(true, now);
    total.topology.emplace(Route("r", "d", "a", "p"), 0.0);
    total.load.emplace(Endpoint("a", "p"), ReplicaLoadSignal{0, 1, 0.0});
    total.health.emplace(Endpoint("a", "p"),
                         ReplicaHealthSignal{ReplicaHealthState::HEALTHY, 0.0});
    EXPECT_EQ(provider.PublishSnapshot(std::move(total)),
              ReplicaSignalPublishStatus::LIMIT_EXCEEDED);

    const std::array candidates = {Candidate(0, 1, "a"), Candidate(1, 2, "b"),
                                   Candidate(2, 3, "c")};
    std::array<ReplicaScoreVerdict, 3> verdicts;
    EXPECT_THROW(provider.ScoreAll(Context(), candidates, verdicts),
                 std::length_error);
    EXPECT_THROW(
        provider.ScoreAll(Context(), candidates,
                          std::span<ReplicaScoreVerdict>(verdicts).first(2)),
        std::invalid_argument);
}

TEST(CachedReplicaScoreProviderTest,
     LoadCostIsMonotonicInRequestedBytesAndSaturatesAtOne) {
    const auto now = At(std::chrono::milliseconds(100));
    auto clock = std::make_shared<FakeReplicaSignalClock>(now);
    CachedReplicaScoreProvider provider(LoadConfig(), clock);
    ASSERT_EQ(provider.PublishSnapshot(LoadSnapshot(1, now, 25, 100)),
              ReplicaSignalPublishStatus::PUBLISHED);
    const std::array candidates = {Candidate(0, 1, "first")};
    std::array<ReplicaScoreVerdict, 1> verdicts;
    std::array<double, 5> costs;
    const std::array<uint64_t, 5> sizes = {
        0, 25, 50, 100, std::numeric_limits<uint64_t>::max()};

    for (size_t i = 0; i < sizes.size(); ++i) {
        provider.ScoreAll(Context(sizes[i]), candidates, verdicts);
        ASSERT_NE(Eligible(verdicts[0]), nullptr);
        costs[i] = Eligible(verdicts[0])->breakdown.load_cost;
    }

    EXPECT_DOUBLE_EQ(costs[0], 0.25);
    EXPECT_DOUBLE_EQ(costs[1], 0.5);
    EXPECT_DOUBLE_EQ(costs[2], 0.75);
    EXPECT_DOUBLE_EQ(costs[3], 1.0);
    EXPECT_DOUBLE_EQ(costs[4], 1.0);
    EXPECT_TRUE(std::is_sorted(costs.begin(), costs.end()));
}

TEST(CachedReplicaScoreProviderTest,
     LoadUtilizationIsAValidatedMonotonicCostFloor) {
    const auto now = At(std::chrono::milliseconds(100));
    auto clock = std::make_shared<FakeReplicaSignalClock>(now);
    CachedReplicaScoreProvider provider(LoadConfig(), clock);
    auto snapshot = LoadSnapshot(1, now, 25, 100);
    snapshot.load.begin()->second.utilization = 0.6;
    ASSERT_EQ(provider.PublishSnapshot(std::move(snapshot)),
              ReplicaSignalPublishStatus::PUBLISHED);
    const std::array candidates = {Candidate(0, 1, "first")};
    std::array<ReplicaScoreVerdict, 1> verdicts;

    provider.ScoreAll(Context(0), candidates, verdicts);
    ASSERT_NE(Eligible(verdicts[0]), nullptr);
    EXPECT_DOUBLE_EQ(Eligible(verdicts[0])->breakdown.load_cost, 0.6);
    provider.ScoreAll(Context(50), candidates, verdicts);
    ASSERT_NE(Eligible(verdicts[0]), nullptr);
    EXPECT_DOUBLE_EQ(Eligible(verdicts[0])->breakdown.load_cost, 0.75);
}

TEST(CachedReplicaScoreProviderTest,
     DegradedHealthUsesImmutableConfiguredFloor) {
    const auto now = At(std::chrono::milliseconds(100));
    auto clock = std::make_shared<FakeReplicaSignalClock>(now);
    CachedReplicaScoreProvider provider(HealthConfig(true, 0.6), clock);
    ReplicaSignalSnapshot snapshot;
    snapshot.generation = 1;
    snapshot.health_source = Observation(true, now);
    snapshot.health.emplace(
        Endpoint("first", "rdma"),
        ReplicaHealthSignal{ReplicaHealthState::DEGRADED, 0.2});
    ASSERT_EQ(provider.PublishSnapshot(std::move(snapshot)),
              ReplicaSignalPublishStatus::PUBLISHED);
    const std::array candidates = {Candidate(0, 1, "first")};
    std::array<ReplicaScoreVerdict, 1> verdicts;

    provider.ScoreAll(Context(), candidates, verdicts);

    ASSERT_NE(Eligible(verdicts[0]), nullptr);
    EXPECT_DOUBLE_EQ(Eligible(verdicts[0])->breakdown.health_cost, 0.6);
}

TEST(CachedReplicaScoreProviderTest,
     EmptyProtocolIsAValidDistinctEndpointProtocolKey) {
    const auto now = At(std::chrono::milliseconds(100));
    auto clock = std::make_shared<FakeReplicaSignalClock>(now);
    CachedReplicaScoreProvider provider(LoadConfig(), clock);
    ReplicaSignalSnapshot snapshot;
    snapshot.generation = 1;
    snapshot.load_source = Observation(true, now);
    snapshot.load.emplace(Endpoint("same", ""), ReplicaLoadSignal{0, 100, 0.0});
    snapshot.load.emplace(Endpoint("same", "rdma"),
                          ReplicaLoadSignal{100, 100, 0.0});
    ASSERT_EQ(provider.PublishSnapshot(std::move(snapshot)),
              ReplicaSignalPublishStatus::PUBLISHED);
    const std::array candidates = {Candidate(0, 1, "same", ""),
                                   Candidate(1, 2, "same", "rdma")};
    std::array<ReplicaScoreVerdict, 2> verdicts;

    provider.ScoreAll(Context(), candidates, verdicts);

    ASSERT_NE(Eligible(verdicts[0]), nullptr);
    ASSERT_NE(Eligible(verdicts[1]), nullptr);
    EXPECT_DOUBLE_EQ(Eligible(verdicts[0])->breakdown.load_cost, 0.0);
    EXPECT_DOUBLE_EQ(Eligible(verdicts[1])->breakdown.load_cost, 1.0);
}

TEST(CachedReplicaScoreProviderTest,
     FuturePublishIsRejectedAndClockRollbackIsSourceUnavailable) {
    const auto now = At(std::chrono::milliseconds(100));
    auto clock = std::make_shared<FakeReplicaSignalClock>(now);
    CachedReplicaScoreProvider provider(LoadConfig(), clock);
    ASSERT_EQ(provider.PublishSnapshot(LoadSnapshot(1, now, 25, 100)),
              ReplicaSignalPublishStatus::PUBLISHED);
    auto future = LoadSnapshot(2, At(std::chrono::milliseconds(101)), 100, 100);

    EXPECT_EQ(provider.PublishSnapshot(std::move(future)),
              ReplicaSignalPublishStatus::INVALID_SNAPSHOT);
    EXPECT_EQ(provider.CurrentGeneration(), 1u);

    clock->Set(At(std::chrono::milliseconds(99)));
    const std::array candidates = {Candidate(0, 1, "first")};
    std::array<ReplicaScoreVerdict, 1> verdicts;
    provider.ScoreAll(Context(), candidates, verdicts);
    ASSERT_NE(Unavailable(verdicts[0]), nullptr);
    EXPECT_EQ(Unavailable(verdicts[0])->reason,
              ReplicaScoreUnavailableReason::SIGNAL_SOURCE_UNAVAILABLE);
    EXPECT_EQ(provider.CurrentGeneration(), 1u);
}

TEST(CachedReplicaScoreProviderTest,
     ClockIsReadOncePerBatchAndTtlBoundaryIsFresh) {
    const auto observed = At(std::chrono::milliseconds(100));
    auto clock = std::make_shared<FakeReplicaSignalClock>(
        At(std::chrono::milliseconds(200)));
    auto config = TopologyConfig();
    config.topology_source.ttl = std::chrono::milliseconds(100);
    CachedReplicaScoreProvider provider(config, clock);
    auto snapshot = TopologySnapshot(1, observed);
    ASSERT_EQ(provider.PublishSnapshot(std::move(snapshot)),
              ReplicaSignalPublishStatus::PUBLISHED);
    const std::array candidates = {Candidate(0, 1, "first"),
                                   Candidate(1, 2, "second")};
    std::array<ReplicaScoreVerdict, 2> verdicts;

    provider.ScoreAll(Context(), candidates, verdicts);
    EXPECT_NE(Eligible(verdicts[0]), nullptr);
    EXPECT_NE(Eligible(verdicts[1]), nullptr);
    // PublishSnapshot also captures one clock value.
    EXPECT_EQ(clock->calls.load(std::memory_order_relaxed), 2u);

    clock->Set(At(std::chrono::milliseconds(201)));
    provider.ScoreAll(Context(), candidates, verdicts);
    for (const auto& verdict : verdicts) {
        ASSERT_NE(Unavailable(verdict), nullptr);
        EXPECT_EQ(Unavailable(verdict)->reason,
                  ReplicaScoreUnavailableReason::STALE);
    }
    EXPECT_EQ(clock->calls.load(std::memory_order_relaxed), 3u);
}

TEST(CachedReplicaScoreProviderTest,
     ConcurrentPublishAndReadNeverMixGenerationsOrLoseOwnership) {
    constexpr uint64_t kLastGeneration = 2000;
    const auto now = At(std::chrono::milliseconds(100));
    auto clock = std::make_shared<FakeReplicaSignalClock>(now);
    CachedReplicaScoreProvider provider(TopologyConfig(), clock);
    ASSERT_EQ(provider.PublishSnapshot(TopologySnapshot(1, now, 0.8, 0.2)),
              ReplicaSignalPublishStatus::PUBLISHED);
    const std::array candidates = {Candidate(0, 1, "first"),
                                   Candidate(1, 2, "second")};
    const auto context = Context();
    std::atomic<bool> start{false};
    std::atomic<bool> publishers_done{false};
    std::atomic<uint64_t> unexpected_publish_results{0};
    std::atomic<uint64_t> mixed_batches{0};

    const auto publish = [&](uint64_t first_generation) {
        while (!start.load(std::memory_order_acquire)) {
            std::this_thread::yield();
        }
        for (uint64_t generation = first_generation;
             generation <= kLastGeneration; generation += 2) {
            const bool even = generation % 2 == 0;
            const auto status = provider.PublishSnapshot(TopologySnapshot(
                generation, now, even ? 0.2 : 0.8, even ? 0.8 : 0.2));
            if (status != ReplicaSignalPublishStatus::PUBLISHED &&
                status !=
                    ReplicaSignalPublishStatus::GENERATION_NOT_INCREASING) {
                unexpected_publish_results.fetch_add(1,
                                                     std::memory_order_relaxed);
            }
        }
    };
    std::thread even_publisher(publish, 2);
    std::thread odd_publisher(publish, 3);
    std::vector<std::thread> readers;
    for (size_t thread = 0; thread < 4; ++thread) {
        readers.emplace_back([&] {
            while (!start.load(std::memory_order_acquire)) {
                std::this_thread::yield();
            }
            size_t iterations = 0;
            do {
                std::array<ReplicaScoreVerdict, 2> verdicts;
                provider.ScoreAll(context, candidates, verdicts);
                const auto* first = Eligible(verdicts[0]);
                const auto* second = Eligible(verdicts[1]);
                const bool even_pattern =
                    first && second && first->breakdown.topology_cost == 0.2 &&
                    second->breakdown.topology_cost == 0.8;
                const bool odd_pattern =
                    first && second && first->breakdown.topology_cost == 0.8 &&
                    second->breakdown.topology_cost == 0.2;
                if (!even_pattern && !odd_pattern) {
                    mixed_batches.fetch_add(1, std::memory_order_relaxed);
                }
                ++iterations;
            } while (!publishers_done.load(std::memory_order_acquire) ||
                     iterations < 5000);
        });
    }

    start.store(true, std::memory_order_release);
    even_publisher.join();
    odd_publisher.join();
    publishers_done.store(true, std::memory_order_release);
    for (auto& reader : readers) reader.join();

    EXPECT_EQ(unexpected_publish_results.load(std::memory_order_relaxed), 0u);
    EXPECT_EQ(mixed_batches.load(std::memory_order_relaxed), 0u);
    EXPECT_EQ(provider.CurrentGeneration(), kLastGeneration);
}

}  // namespace
}  // namespace mooncake
