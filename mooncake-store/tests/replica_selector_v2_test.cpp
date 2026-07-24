// Copyright 2026 Mooncake Authors

#include "replica_selector.h"

#include <gtest/gtest.h>

#include <algorithm>
#include <atomic>
#include <cmath>
#include <cstdlib>
#include <functional>
#include <limits>
#include <memory>
#include <span>
#include <stdexcept>
#include <string>
#include <thread>
#include <unordered_set>
#include <utility>
#include <variant>
#include <vector>

#include "replica_selection.h"

namespace mooncake {
namespace {

static_assert(std::variant_size_v<ReplicaScoreVerdict> == 4);

Replica::Descriptor MakeMemory(ReplicaID id, const std::string& endpoint,
                               ReplicaStatus status = ReplicaStatus::COMPLETE,
                               const std::string& protocol = "rdma") {
    Replica::Descriptor descriptor;
    descriptor.id = id;
    MemoryDescriptor memory;
    memory.buffer_descriptor.size_ = 1024;
    memory.buffer_descriptor.buffer_address_ = 0x1000;
    memory.buffer_descriptor.protocol_ = protocol;
    memory.buffer_descriptor.transport_endpoint_ = endpoint;
    descriptor.descriptor_variant = std::move(memory);
    descriptor.status = status;
    return descriptor;
}

Replica::Descriptor MakeNoF(ReplicaID id, const std::string& endpoint) {
    Replica::Descriptor descriptor;
    descriptor.id = id;
    NoFDescriptor nof;
    nof.buffer_descriptor.size_ = 1024;
    nof.buffer_descriptor.buffer_address_ = 0x2000;
    nof.buffer_descriptor.protocol_ = "nvmeof";
    nof.buffer_descriptor.transport_endpoint_ = endpoint;
    descriptor.descriptor_variant = std::move(nof);
    descriptor.status = ReplicaStatus::COMPLETE;
    return descriptor;
}

Replica::Descriptor MakeDisk(ReplicaID id) {
    Replica::Descriptor descriptor;
    descriptor.id = id;
    descriptor.descriptor_variant = DiskDescriptor{"/tmp/object", 1024};
    descriptor.status = ReplicaStatus::COMPLETE;
    return descriptor;
}

Replica::Descriptor MakeLocalDisk(ReplicaID id) {
    Replica::Descriptor descriptor;
    descriptor.id = id;
    LocalDiskDescriptor local_disk;
    local_disk.object_size = 1024;
    local_disk.transport_endpoint = "client";
    descriptor.descriptor_variant = std::move(local_disk);
    descriptor.status = ReplicaStatus::COMPLETE;
    return descriptor;
}

ReplicaScoreVerdict Score(double topology, double load, double health) {
    return ReplicaEligibleScore{{topology, load, health}};
}

ReplicaScoreVerdict Veto(ReplicaHardVetoReason reason) {
    return ReplicaHardVeto{reason};
}

ReplicaScoreVerdict Unavailable(ReplicaScoreUnavailableReason reason) {
    return ReplicaScoreUnavailable{reason};
}

class FunctionBatchProvider final : public ReplicaScoreProvider {
   public:
    using Function =
        std::function<void(const ReplicaSelectionContext&,
                           std::span<const ReplicaScoreCandidateView>,
                           std::span<ReplicaScoreVerdict>)>;

    explicit FunctionBatchProvider(Function function)
        : function_(std::move(function)) {}

    void ScoreAll(const ReplicaSelectionContext& context,
                  std::span<const ReplicaScoreCandidateView> candidates,
                  std::span<ReplicaScoreVerdict> verdicts) const override {
        calls.fetch_add(1, std::memory_order_relaxed);
        function_(context, candidates, verdicts);
    }

    mutable std::atomic<uint64_t> calls{0};

   private:
    Function function_;
};

class AtomicPreferProvider final : public ReplicaScoreProvider {
   public:
    explicit AtomicPreferProvider(ReplicaID preferred_id)
        : preferred_id_(preferred_id) {}

    void ScoreAll(const ReplicaSelectionContext&,
                  std::span<const ReplicaScoreCandidateView> candidates,
                  std::span<ReplicaScoreVerdict> verdicts) const override {
        calls.fetch_add(1, std::memory_order_relaxed);
        for (size_t i = 0; i < candidates.size(); ++i) {
            verdicts[i] = candidates[i].replica.replica_id == preferred_id_
                              ? Score(0.0, 0.0, 0.0)
                              : Score(1.0, 1.0, 1.0);
        }
    }

    mutable std::atomic<uint64_t> calls{0};

   private:
    ReplicaID preferred_id_;
};

const Replica::Descriptor* Selected(
    const ReplicaSelectionDecision& decision,
    const std::vector<Replica::Descriptor>& replicas) {
    return ResolveReplica(replicas, decision.selected);
}

const Replica::Descriptor* Recommended(
    const ReplicaSelectionDecision& decision,
    const std::vector<Replica::Descriptor>& replicas) {
    return ResolveReplica(replicas, decision.recommendation);
}

void FillAll(std::span<ReplicaScoreVerdict> verdicts,
             const ReplicaScoreVerdict& verdict) {
    std::fill(verdicts.begin(), verdicts.end(), verdict);
}

class ReplicaSelectorV2Test : public ::testing::Test {
   protected:
    void TearDown() override { SetRemoteReplicaScorer(nullptr); }
};

TEST_F(ReplicaSelectorV2Test, DefaultLegacyUsesV1AndNeverCallsV2) {
    std::atomic<uint64_t> v1_calls{0};
    SetRemoteReplicaScorer([&](const Replica::Descriptor& replica) {
        v1_calls.fetch_add(1, std::memory_order_relaxed);
        return replica.id == 2 ? 0.0 : 10.0;
    });
    auto provider = std::make_shared<FunctionBatchProvider>(
        [](const ReplicaSelectionContext&,
           std::span<const ReplicaScoreCandidateView>,
           std::span<ReplicaScoreVerdict>) {
            ADD_FAILURE() << "V2 must not run in LEGACY mode";
        });
    ReplicaSelector selector(ReplicaSelectionMode::LEGACY, provider);
    std::vector<Replica::Descriptor> replicas = {MakeMemory(1, "first"),
                                                 MakeMemory(2, "second")};

    const auto decision = selector.Select(replicas, {});

    ASSERT_NE(Selected(decision, replicas), nullptr);
    EXPECT_EQ(Selected(decision, replicas)->id, 2u);
    EXPECT_FALSE(decision.structural_baseline);
    EXPECT_EQ(decision.outcome, ReplicaSelectionOutcome::LEGACY_MODE);
    EXPECT_EQ(v1_calls.load(std::memory_order_relaxed), 2u);
    EXPECT_EQ(provider->calls.load(std::memory_order_relaxed), 0u);
}

TEST_F(ReplicaSelectorV2Test, ActiveIgnoresThrowingGlobalV1Scorer) {
    std::atomic<uint64_t> v1_calls{0};
    SetRemoteReplicaScorer([&](const Replica::Descriptor&) -> double {
        v1_calls.fetch_add(1, std::memory_order_relaxed);
        throw std::runtime_error("V1 must be isolated");
    });
    auto provider = std::make_shared<AtomicPreferProvider>(2);
    ReplicaSelector selector(ReplicaSelectionMode::ACTIVE, provider);
    std::vector<Replica::Descriptor> replicas = {MakeMemory(1, "first"),
                                                 MakeMemory(2, "second")};

    ReplicaSelectionDecision decision;
    EXPECT_NO_THROW(decision = selector.Select(replicas, {}));

    ASSERT_NE(Selected(decision, replicas), nullptr);
    EXPECT_EQ(Selected(decision, replicas)->id, 2u);
    EXPECT_EQ(v1_calls.load(std::memory_order_relaxed), 0u);
    EXPECT_EQ(provider->calls.load(std::memory_order_relaxed), 1u);
}

TEST_F(ReplicaSelectorV2Test,
       ShadowPreservesV1SelectionAndKeepsRecommendationInstanceScoped) {
    std::atomic<uint64_t> v1_calls{0};
    SetRemoteReplicaScorer([&](const Replica::Descriptor& replica) {
        v1_calls.fetch_add(1, std::memory_order_relaxed);
        return replica.id == 2 ? 0.0 : 10.0;
    });
    auto provider = std::make_shared<AtomicPreferProvider>(3);
    ReplicaSelector selector(ReplicaSelectionMode::SHADOW, provider);
    std::vector<Replica::Descriptor> replicas = {MakeMemory(1, "first"),
                                                 MakeMemory(2, "second"),
                                                 MakeMemory(3, "third")};

    const auto decision = selector.Select(replicas, {});

    ASSERT_NE(Selected(decision, replicas), nullptr);
    ASSERT_NE(Recommended(decision, replicas), nullptr);
    ASSERT_TRUE(decision.structural_baseline);
    EXPECT_EQ(Selected(decision, replicas)->id, 2u);
    EXPECT_EQ(Recommended(decision, replicas)->id, 3u);
    EXPECT_EQ(decision.structural_baseline->replica_id, 1u);
    EXPECT_EQ(v1_calls.load(std::memory_order_relaxed), 3u);
    EXPECT_EQ(provider->calls.load(std::memory_order_relaxed), 1u);
}

TEST_F(ReplicaSelectorV2Test,
       ShadowSelectedMatchesLegacyAcrossReplicaFallbackKinds) {
    SetRemoteReplicaScorer([](const Replica::Descriptor& replica) {
        return replica.id == 2 ? 0.0 : 10.0;
    });
    const auto provider = std::make_shared<AtomicPreferProvider>(3);
    const ReplicaSelector legacy(ReplicaSelectionMode::LEGACY, provider);
    const ReplicaSelector shadow(ReplicaSelectionMode::SHADOW, provider);

    struct Scenario {
        std::vector<Replica::Descriptor> replicas;
        std::unordered_set<std::string> local_endpoints;
    };
    std::vector<Scenario> scenarios;
    scenarios.push_back({{MakeMemory(1, "first"), MakeMemory(2, "second"),
                          MakeMemory(3, "third")},
                         {}});
    scenarios.push_back(
        {{MakeMemory(1, "local"), MakeMemory(2, "remote")}, {"local"}});
    scenarios.push_back(
        {{MakeNoF(4, "nof"), MakeLocalDisk(5), MakeDisk(6)}, {}});
    scenarios.push_back(
        {{MakeMemory(7, "incomplete", ReplicaStatus::PROCESSING)}, {}});

    for (const auto& scenario : scenarios) {
        const auto legacy_decision =
            legacy.Select(scenario.replicas, scenario.local_endpoints);
        const auto shadow_decision =
            shadow.Select(scenario.replicas, scenario.local_endpoints);
        EXPECT_EQ(shadow_decision.selected, legacy_decision.selected);
    }
}

TEST_F(ReplicaSelectorV2Test, ShadowLocalFastPathsSkipV1AndV2Scorers) {
    std::atomic<uint64_t> v1_calls{0};
    SetRemoteReplicaScorer([&](const Replica::Descriptor&) {
        v1_calls.fetch_add(1, std::memory_order_relaxed);
        return 0.0;
    });
    auto provider = std::make_shared<FunctionBatchProvider>(
        [](const ReplicaSelectionContext&,
           std::span<const ReplicaScoreCandidateView>,
           std::span<ReplicaScoreVerdict>) {
            ADD_FAILURE() << "V2 must not run for a local fast path";
        });
    const ReplicaSelector legacy(ReplicaSelectionMode::LEGACY, provider);
    const ReplicaSelector shadow(ReplicaSelectionMode::SHADOW, provider);
    const std::vector<std::vector<Replica::Descriptor>> scenarios = {
        {MakeMemory(1, "remote"), MakeMemory(2, "local")},
        {MakeMemory(3, "remote"), MakeNoF(4, "local")}};

    for (const auto& replicas : scenarios) {
        const auto legacy_decision = legacy.Select(replicas, {"local"});
        const auto shadow_decision = shadow.Select(replicas, {"local"});

        EXPECT_EQ(shadow_decision.selected, legacy_decision.selected);
        EXPECT_EQ(shadow_decision.recommendation,
                  shadow_decision.structural_baseline);
        EXPECT_EQ(shadow_decision.outcome,
                  ReplicaSelectionOutcome::LOCAL_FAST_PATH);
    }
    EXPECT_EQ(v1_calls.load(std::memory_order_relaxed), 0u);
    EXPECT_EQ(provider->calls.load(std::memory_order_relaxed), 0u);
}

TEST_F(ReplicaSelectorV2Test, PureStructuralBaselineIgnoresV1) {
    std::atomic<uint64_t> calls{0};
    SetRemoteReplicaScorer([&](const Replica::Descriptor& replica) {
        calls.fetch_add(1, std::memory_order_relaxed);
        return replica.id == 2 ? 0.0 : 10.0;
    });
    std::vector<Replica::Descriptor> replicas = {
        MakeMemory(1, "first", ReplicaStatus::COMPLETE, "tcp"),
        MakeMemory(2, "second", ReplicaStatus::COMPLETE, "rdma")};

    const auto* structural =
        SelectBestReplicaWithoutRemoteScoring(replicas, {});

    ASSERT_NE(structural, nullptr);
    EXPECT_EQ(structural->id, 1u);
    EXPECT_EQ(calls.load(std::memory_order_relaxed), 0u);
    EXPECT_EQ(SelectBestReplica(replicas, {})->id, 2u);
    EXPECT_EQ(calls.load(std::memory_order_relaxed), 2u);
}

TEST_F(ReplicaSelectorV2Test, LocalMemoryAndNoFSkipProvider) {
    auto provider = std::make_shared<FunctionBatchProvider>(
        [](const ReplicaSelectionContext&,
           std::span<const ReplicaScoreCandidateView>,
           std::span<ReplicaScoreVerdict>) {
            ADD_FAILURE() << "local fast path must not score";
        });
    ReplicaSelector selector(ReplicaSelectionMode::ACTIVE, provider);

    std::vector<Replica::Descriptor> memory = {MakeMemory(1, "remote"),
                                               MakeMemory(2, "local")};
    std::vector<Replica::Descriptor> nof = {MakeNoF(3, "local"),
                                            MakeMemory(4, "remote")};

    const auto memory_decision = selector.Select(memory, {"local"});
    const auto nof_decision = selector.Select(nof, {"local"});

    EXPECT_EQ(Selected(memory_decision, memory)->id, 2u);
    EXPECT_EQ(Selected(nof_decision, nof)->id, 3u);
    EXPECT_EQ(memory_decision.outcome,
              ReplicaSelectionOutcome::LOCAL_FAST_PATH);
    EXPECT_EQ(nof_decision.outcome, ReplicaSelectionOutcome::LOCAL_FAST_PATH);
    EXPECT_EQ(provider->calls.load(std::memory_order_relaxed), 0u);
}

TEST_F(ReplicaSelectorV2Test, NoRemoteMemorySkipsProvider) {
    auto provider = std::make_shared<FunctionBatchProvider>(
        [](const ReplicaSelectionContext&,
           std::span<const ReplicaScoreCandidateView>,
           std::span<ReplicaScoreVerdict>) { ADD_FAILURE(); });
    ReplicaSelector selector(ReplicaSelectionMode::ACTIVE, provider);
    std::vector<Replica::Descriptor> replicas = {MakeDisk(1), MakeLocalDisk(2)};

    const auto decision = selector.Select(replicas, {});

    EXPECT_EQ(Selected(decision, replicas)->id, 2u);
    EXPECT_EQ(decision.outcome, ReplicaSelectionOutcome::NO_REMOTE_MEMORY);
    EXPECT_EQ(provider->calls.load(std::memory_order_relaxed), 0u);
}

TEST_F(ReplicaSelectorV2Test, BatchSeesContextAndCandidatesOnceInMasterOrder) {
    auto provider = std::make_shared<FunctionBatchProvider>(
        [](const ReplicaSelectionContext& context,
           std::span<const ReplicaScoreCandidateView> candidates,
           std::span<ReplicaScoreVerdict> verdicts) {
            EXPECT_EQ(context.tenant_id, "tenant-a");
            EXPECT_EQ(context.key, "model/layer/7");
            EXPECT_EQ(context.requester_endpoint, "decode-0");
            EXPECT_EQ(context.destination_location, "rack-b/gpu-3");
            EXPECT_EQ(context.requested_bytes, 4096u);
            EXPECT_EQ(context.request_nonce, 99u);
            ASSERT_EQ(candidates.size(), 2u);
            EXPECT_EQ(candidates[0].replica, (ReplicaRef{0, 1}));
            EXPECT_EQ(candidates[1].replica, (ReplicaRef{2, 2}));
            EXPECT_EQ(candidates[0].transport_endpoint, "store-0");
            EXPECT_EQ(candidates[1].protocol, "tcp");
            verdicts[0] = Score(5.0, 2.0, 1.0);
            verdicts[1] = Score(0.5, 0.25, 0.25);
        });
    ReplicaSelector selector(ReplicaSelectionMode::ACTIVE, provider);
    std::vector<Replica::Descriptor> replicas = {
        MakeMemory(1, "store-0"),
        MakeMemory(9, "incomplete", ReplicaStatus::PROCESSING),
        MakeMemory(2, "store-1", ReplicaStatus::COMPLETE, "tcp")};
    ReplicaSelectionContext context{
        "tenant-a", "model/layer/7", "decode-0", "rack-b/gpu-3", 4096, 99};

    const auto decision = selector.Select(replicas, {}, context);

    EXPECT_EQ(Selected(decision, replicas)->id, 2u);
    EXPECT_EQ(decision.outcome, ReplicaSelectionOutcome::SCORED_REMOTE_MEMORY);
    EXPECT_EQ(provider->calls.load(std::memory_order_relaxed), 1u);
}

TEST_F(ReplicaSelectorV2Test, TieKeepsMasterOrder) {
    auto provider = std::make_shared<FunctionBatchProvider>(
        [](const ReplicaSelectionContext&,
           std::span<const ReplicaScoreCandidateView>,
           std::span<ReplicaScoreVerdict> verdicts) {
            FillAll(verdicts, Score(1.0, 2.0, 3.0));
        });
    ReplicaSelector selector(ReplicaSelectionMode::ACTIVE, provider);
    std::vector<Replica::Descriptor> replicas = {MakeMemory(1, "first"),
                                                 MakeMemory(2, "second")};

    const auto decision = selector.Select(replicas, {});

    EXPECT_EQ(Selected(decision, replicas)->id, 1u);
}

TEST_F(ReplicaSelectorV2Test, MissingProviderUsesStructuralFallback) {
    ReplicaSelector selector(ReplicaSelectionMode::ACTIVE);
    std::vector<Replica::Descriptor> replicas = {MakeMemory(1, "first"),
                                                 MakeMemory(2, "second")};

    const auto decision = selector.Select(replicas, {});

    EXPECT_EQ(Selected(decision, replicas)->id, 1u);
    EXPECT_EQ(decision.outcome, ReplicaSelectionOutcome::FALLBACK_NO_PROVIDER);
}

TEST_F(ReplicaSelectorV2Test, StaleIsSoftAndDoesNotTriggerStorageFallback) {
    auto provider = std::make_shared<FunctionBatchProvider>(
        [](const ReplicaSelectionContext&,
           std::span<const ReplicaScoreCandidateView>,
           std::span<ReplicaScoreVerdict> verdicts) {
            FillAll(verdicts,
                    Unavailable(ReplicaScoreUnavailableReason::STALE));
        });
    ReplicaSelector selector(ReplicaSelectionMode::ACTIVE, provider);
    std::vector<Replica::Descriptor> replicas = {
        MakeMemory(1, "first"), MakeMemory(2, "second"), MakeDisk(3)};

    const auto decision = selector.Select(replicas, {});

    EXPECT_EQ(Selected(decision, replicas)->id, 1u);
    EXPECT_EQ(decision.outcome, ReplicaSelectionOutcome::FALLBACK_SIGNAL_STALE);
}

TEST_F(ReplicaSelectorV2Test, SignalSourceUnavailableIsSoftAndTraced) {
    auto provider = std::make_shared<FunctionBatchProvider>(
        [](const ReplicaSelectionContext&,
           std::span<const ReplicaScoreCandidateView>,
           std::span<ReplicaScoreVerdict> verdicts) {
            FillAll(
                verdicts,
                Unavailable(
                    ReplicaScoreUnavailableReason::SIGNAL_SOURCE_UNAVAILABLE));
        });
    ReplicaSelector selector(ReplicaSelectionMode::ACTIVE, provider,
                             ReplicaSelectionDiagnostics::ENABLED);
    std::vector<Replica::Descriptor> replicas = {MakeMemory(1, "first"),
                                                 MakeMemory(2, "second")};

    const auto decision = selector.Select(replicas, {});

    EXPECT_EQ(Selected(decision, replicas)->id, 1u);
    EXPECT_EQ(decision.outcome,
              ReplicaSelectionOutcome::FALLBACK_SIGNAL_SOURCE_UNAVAILABLE);
    ASSERT_EQ(decision.trace.unavailable_scores.size(), 2u);
    EXPECT_EQ(decision.trace.unavailable_scores[0].reason,
              ReplicaScoreUnavailableReason::SIGNAL_SOURCE_UNAVAILABLE);
    EXPECT_TRUE(decision.trace.candidate_scores.empty());
}

TEST_F(ReplicaSelectorV2Test,
       MixedUnavailableOutcomeDoesNotDependOnMasterOrder) {
    auto provider = std::make_shared<FunctionBatchProvider>(
        [](const ReplicaSelectionContext&,
           std::span<const ReplicaScoreCandidateView> candidates,
           std::span<ReplicaScoreVerdict> verdicts) {
            for (size_t i = 0; i < candidates.size(); ++i) {
                switch (candidates[i].replica.replica_id) {
                    case 1:
                        verdicts[i] = Unavailable(
                            ReplicaScoreUnavailableReason::MISSING_SIGNAL);
                        break;
                    case 2:
                        verdicts[i] =
                            Unavailable(ReplicaScoreUnavailableReason::STALE);
                        break;
                    case 3:
                        verdicts[i] =
                            Unavailable(ReplicaScoreUnavailableReason::
                                            SIGNAL_SOURCE_UNAVAILABLE);
                        break;
                    default:
                        FAIL() << "unexpected replica id";
                }
            }
        });
    ReplicaSelector selector(ReplicaSelectionMode::ACTIVE, provider);
    std::vector<ReplicaID> order = {1, 2, 3};

    do {
        std::vector<Replica::Descriptor> replicas;
        for (const auto id : order) {
            replicas.push_back(MakeMemory(id, "store-" + std::to_string(id)));
        }

        const auto decision = selector.Select(replicas, {});

        EXPECT_EQ(decision.outcome,
                  ReplicaSelectionOutcome::FALLBACK_SIGNAL_SOURCE_UNAVAILABLE);
        ASSERT_NE(Selected(decision, replicas), nullptr);
        EXPECT_EQ(Selected(decision, replicas)->id, order.front());
    } while (std::next_permutation(order.begin(), order.end()));
}

TEST_F(ReplicaSelectorV2Test,
       InvalidUnavailableReasonDominatesRegardlessOfMasterOrder) {
    constexpr auto invalid_reason =
        static_cast<ReplicaScoreUnavailableReason>(255);
    auto provider = std::make_shared<FunctionBatchProvider>(
        [&](const ReplicaSelectionContext&,
            std::span<const ReplicaScoreCandidateView> candidates,
            std::span<ReplicaScoreVerdict> verdicts) {
            for (size_t i = 0; i < candidates.size(); ++i) {
                verdicts[i] =
                    Unavailable(candidates[i].replica.replica_id == 1
                                    ? invalid_reason
                                    : ReplicaScoreUnavailableReason::STALE);
            }
        });
    ReplicaSelector selector(ReplicaSelectionMode::ACTIVE, provider);
    std::vector<ReplicaID> order = {1, 2};

    do {
        std::vector<Replica::Descriptor> replicas;
        for (const auto id : order) {
            replicas.push_back(MakeMemory(id, "store-" + std::to_string(id)));
        }

        const auto decision = selector.Select(replicas, {});

        EXPECT_EQ(decision.outcome,
                  ReplicaSelectionOutcome::FALLBACK_INVALID_RESULT);
    } while (std::next_permutation(order.begin(), order.end()));
}

TEST_F(ReplicaSelectorV2Test,
       HardVetoThenStaleFallsBackWithoutSelectingVetoedReplica) {
    auto provider = std::make_shared<FunctionBatchProvider>(
        [](const ReplicaSelectionContext&,
           std::span<const ReplicaScoreCandidateView>,
           std::span<ReplicaScoreVerdict> verdicts) {
            verdicts[0] = Veto(ReplicaHardVetoReason::UNHEALTHY);
            verdicts[1] = Unavailable(ReplicaScoreUnavailableReason::STALE);
        });
    ReplicaSelector selector(ReplicaSelectionMode::ACTIVE, provider);
    std::vector<Replica::Descriptor> replicas = {
        MakeMemory(1, "first"), MakeMemory(2, "second"), MakeDisk(3)};

    const auto decision = selector.Select(replicas, {});

    EXPECT_EQ(Selected(decision, replicas)->id, 2u);
    EXPECT_EQ(decision.outcome, ReplicaSelectionOutcome::FALLBACK_SIGNAL_STALE);
}

TEST_F(ReplicaSelectorV2Test,
       HardVetoThenMissingSignalFallsBackWithoutSelectingVetoedReplica) {
    auto provider = std::make_shared<FunctionBatchProvider>(
        [](const ReplicaSelectionContext&,
           std::span<const ReplicaScoreCandidateView>,
           std::span<ReplicaScoreVerdict> verdicts) {
            verdicts[0] = Veto(ReplicaHardVetoReason::CIRCUIT_OPEN);
            verdicts[1] =
                Unavailable(ReplicaScoreUnavailableReason::MISSING_SIGNAL);
        });
    ReplicaSelector selector(ReplicaSelectionMode::ACTIVE, provider);
    std::vector<Replica::Descriptor> replicas = {MakeMemory(1, "first"),
                                                 MakeMemory(2, "second")};

    const auto decision = selector.Select(replicas, {});

    EXPECT_EQ(Selected(decision, replicas)->id, 2u);
    EXPECT_EQ(decision.outcome,
              ReplicaSelectionOutcome::FALLBACK_MISSING_SIGNAL);
}

TEST_F(ReplicaSelectorV2Test, HardVetoThenEligibleSelectsEligibleReplica) {
    auto provider = std::make_shared<FunctionBatchProvider>(
        [](const ReplicaSelectionContext&,
           std::span<const ReplicaScoreCandidateView>,
           std::span<ReplicaScoreVerdict> verdicts) {
            verdicts[0] = Veto(ReplicaHardVetoReason::UNHEALTHY);
            verdicts[1] = Score(1.0, 1.0, 1.0);
        });
    ReplicaSelector selector(ReplicaSelectionMode::ACTIVE, provider);
    std::vector<Replica::Descriptor> replicas = {MakeMemory(1, "first"),
                                                 MakeMemory(2, "second")};

    const auto decision = selector.Select(replicas, {});

    EXPECT_EQ(Selected(decision, replicas)->id, 2u);
    EXPECT_EQ(decision.outcome, ReplicaSelectionOutcome::SCORED_REMOTE_MEMORY);
}

TEST_F(ReplicaSelectorV2Test, AllHardVetoUsesNoFStorageFallback) {
    auto provider = std::make_shared<FunctionBatchProvider>(
        [](const ReplicaSelectionContext&,
           std::span<const ReplicaScoreCandidateView>,
           std::span<ReplicaScoreVerdict> verdicts) {
            FillAll(verdicts, Veto(ReplicaHardVetoReason::CIRCUIT_OPEN));
        });
    ReplicaSelector selector(ReplicaSelectionMode::ACTIVE, provider);
    std::vector<Replica::Descriptor> replicas = {
        MakeMemory(1, "first"), MakeMemory(2, "second"),
        MakeNoF(3, "remote-nof"), MakeLocalDisk(4), MakeDisk(5)};

    const auto decision = selector.Select(replicas, {});

    EXPECT_EQ(Selected(decision, replicas)->id, 3u);
    EXPECT_EQ(decision.outcome,
              ReplicaSelectionOutcome::SCORED_STORAGE_FALLBACK);
}

TEST_F(ReplicaSelectorV2Test, AllHardVetoPrefersLocalDiskOverDisk) {
    auto provider = std::make_shared<FunctionBatchProvider>(
        [](const ReplicaSelectionContext&,
           std::span<const ReplicaScoreCandidateView>,
           std::span<ReplicaScoreVerdict> verdicts) {
            FillAll(verdicts, Veto(ReplicaHardVetoReason::UNHEALTHY));
        });
    ReplicaSelector selector(ReplicaSelectionMode::ACTIVE, provider);
    std::vector<Replica::Descriptor> replicas = {MakeMemory(1, "remote"),
                                                 MakeDisk(2), MakeLocalDisk(3)};

    const auto decision = selector.Select(replicas, {});

    EXPECT_EQ(Selected(decision, replicas)->id, 3u);
}

TEST_F(ReplicaSelectorV2Test, SeventeenHardVetoesUseOverflowFallback) {
    auto provider = std::make_shared<FunctionBatchProvider>(
        [](const ReplicaSelectionContext&,
           std::span<const ReplicaScoreCandidateView>,
           std::span<ReplicaScoreVerdict> verdicts) {
            FillAll(verdicts, Veto(ReplicaHardVetoReason::UNHEALTHY));
        });
    ReplicaSelector selector(ReplicaSelectionMode::ACTIVE, provider);
    std::vector<Replica::Descriptor> replicas;
    for (ReplicaID id = 1; id <= 17; ++id) {
        replicas.push_back(MakeMemory(id, "store-" + std::to_string(id)));
    }
    replicas.push_back(MakeDisk(18));

    const auto decision = selector.Select(replicas, {});

    EXPECT_EQ(Selected(decision, replicas)->id, 18u);
    EXPECT_EQ(decision.outcome,
              ReplicaSelectionOutcome::SCORED_STORAGE_FALLBACK);
}

TEST_F(ReplicaSelectorV2Test, AllHardVetoWithoutStorageFailsClosedInActive) {
    auto provider = std::make_shared<FunctionBatchProvider>(
        [](const ReplicaSelectionContext&,
           std::span<const ReplicaScoreCandidateView>,
           std::span<ReplicaScoreVerdict> verdicts) {
            FillAll(verdicts, Veto(ReplicaHardVetoReason::UNHEALTHY));
        });
    ReplicaSelector selector(ReplicaSelectionMode::ACTIVE, provider);
    std::vector<Replica::Descriptor> replicas = {MakeMemory(1, "first"),
                                                 MakeMemory(2, "second")};

    const auto decision = selector.Select(replicas, {});

    EXPECT_EQ(Selected(decision, replicas), nullptr);
    EXPECT_EQ(Recommended(decision, replicas), nullptr);
    EXPECT_EQ(decision.outcome, ReplicaSelectionOutcome::NO_SAFE_REPLICA);
}

TEST_F(ReplicaSelectorV2Test, ShadowKeepsBaselineWhenRecommendationIsEmpty) {
    auto provider = std::make_shared<FunctionBatchProvider>(
        [](const ReplicaSelectionContext&,
           std::span<const ReplicaScoreCandidateView>,
           std::span<ReplicaScoreVerdict> verdicts) {
            FillAll(verdicts, Veto(ReplicaHardVetoReason::UNHEALTHY));
        });
    ReplicaSelector selector(ReplicaSelectionMode::SHADOW, provider);
    std::vector<Replica::Descriptor> replicas = {MakeMemory(1, "first"),
                                                 MakeMemory(2, "second")};

    const auto decision = selector.Select(replicas, {});

    EXPECT_EQ(Selected(decision, replicas)->id, 1u);
    EXPECT_EQ(Recommended(decision, replicas), nullptr);
    EXPECT_EQ(decision.outcome, ReplicaSelectionOutcome::NO_SAFE_REPLICA);
}

TEST_F(ReplicaSelectorV2Test, ProviderExceptionPublishesNoPartialBatch) {
    auto provider = std::make_shared<FunctionBatchProvider>(
        [](const ReplicaSelectionContext&,
           std::span<const ReplicaScoreCandidateView>,
           std::span<ReplicaScoreVerdict> verdicts) {
            verdicts[0] = Veto(ReplicaHardVetoReason::UNHEALTHY);
            throw std::runtime_error("snapshot failed");
        });
    ReplicaSelector selector(ReplicaSelectionMode::ACTIVE, provider,
                             ReplicaSelectionDiagnostics::ENABLED);
    std::vector<Replica::Descriptor> replicas = {MakeMemory(1, "first"),
                                                 MakeMemory(2, "second")};

    const auto decision = selector.Select(replicas, {});

    EXPECT_EQ(Selected(decision, replicas)->id, 1u);
    EXPECT_EQ(decision.outcome,
              ReplicaSelectionOutcome::FALLBACK_PROVIDER_EXCEPTION);
    EXPECT_TRUE(decision.trace.hard_vetoes.empty());
    EXPECT_TRUE(decision.trace.candidate_scores.empty());
}

TEST_F(ReplicaSelectorV2Test, UnsetOutputInvalidatesWholeBatch) {
    auto provider = std::make_shared<FunctionBatchProvider>(
        [](const ReplicaSelectionContext&,
           std::span<const ReplicaScoreCandidateView>,
           std::span<ReplicaScoreVerdict> verdicts) {
            verdicts[0] = Veto(ReplicaHardVetoReason::UNHEALTHY);
            // verdicts[1] deliberately remains monostate.
        });
    ReplicaSelector selector(ReplicaSelectionMode::ACTIVE, provider,
                             ReplicaSelectionDiagnostics::ENABLED);
    std::vector<Replica::Descriptor> replicas = {MakeMemory(1, "first"),
                                                 MakeMemory(2, "second")};

    const auto decision = selector.Select(replicas, {});

    EXPECT_EQ(Selected(decision, replicas)->id, 1u);
    EXPECT_EQ(decision.outcome,
              ReplicaSelectionOutcome::FALLBACK_INVALID_RESULT);
    EXPECT_TRUE(decision.trace.hard_vetoes.empty());
}

TEST_F(ReplicaSelectorV2Test, InvalidScoresNeverApplyPartialRanking) {
    const double max = std::numeric_limits<double>::max();
    const std::vector<ReplicaScoreVerdict> invalid = {
        Score(-1.0, 0.0, 0.0),
        Score(0.0, -1.0, 0.0),
        Score(0.0, 0.0, -1.0),
        Score(std::numeric_limits<double>::quiet_NaN(), 0.0, 0.0),
        Score(0.0, std::numeric_limits<double>::infinity(), 0.0),
        Score(0.0, 0.0, -std::numeric_limits<double>::infinity()),
        Score(max, max, max),
    };

    for (const auto& invalid_score : invalid) {
        auto provider = std::make_shared<FunctionBatchProvider>(
            [invalid_score](const ReplicaSelectionContext&,
                            std::span<const ReplicaScoreCandidateView>,
                            std::span<ReplicaScoreVerdict> verdicts) {
                verdicts[0] = invalid_score;
                verdicts[1] = Score(0.0, 0.0, 0.0);
            });
        ReplicaSelector selector(ReplicaSelectionMode::ACTIVE, provider);
        std::vector<Replica::Descriptor> replicas = {MakeMemory(1, "first"),
                                                     MakeMemory(2, "second")};

        const auto decision = selector.Select(replicas, {});

        EXPECT_EQ(Selected(decision, replicas)->id, 1u);
        EXPECT_EQ(decision.outcome,
                  ReplicaSelectionOutcome::FALLBACK_INVALID_SCORE);
    }
}

TEST_F(ReplicaSelectorV2Test, HardVetoSurvivesInvalidScoreFallback) {
    auto provider = std::make_shared<FunctionBatchProvider>(
        [](const ReplicaSelectionContext&,
           std::span<const ReplicaScoreCandidateView>,
           std::span<ReplicaScoreVerdict> verdicts) {
            verdicts[0] = Veto(ReplicaHardVetoReason::UNHEALTHY);
            verdicts[1] = Score(-1.0, 0.0, 0.0);
        });
    ReplicaSelector selector(ReplicaSelectionMode::ACTIVE, provider);
    std::vector<Replica::Descriptor> replicas = {MakeMemory(1, "first"),
                                                 MakeMemory(2, "second")};

    const auto decision = selector.Select(replicas, {});

    EXPECT_EQ(Selected(decision, replicas)->id, 2u);
    EXPECT_EQ(decision.outcome,
              ReplicaSelectionOutcome::FALLBACK_INVALID_SCORE);
}

TEST_F(ReplicaSelectorV2Test, DiagnosticsAreDisabledByDefault) {
    auto provider = std::make_shared<AtomicPreferProvider>(2);
    ReplicaSelector selector(ReplicaSelectionMode::ACTIVE, provider);
    std::vector<Replica::Descriptor> replicas = {
        MakeMemory(9, "incomplete", ReplicaStatus::PROCESSING), MakeDisk(8),
        MakeMemory(1, "first"), MakeMemory(2, "second")};

    const auto decision = selector.Select(replicas, {});

    EXPECT_TRUE(decision.trace.candidate_scores.empty());
    EXPECT_TRUE(decision.trace.hard_vetoes.empty());
    EXPECT_TRUE(decision.trace.unavailable_scores.empty());
    EXPECT_TRUE(decision.trace.skipped.empty());
}

TEST_F(ReplicaSelectorV2Test, DiagnosticsCaptureCompleteAtomicDecision) {
    auto provider = std::make_shared<FunctionBatchProvider>(
        [](const ReplicaSelectionContext&,
           std::span<const ReplicaScoreCandidateView>,
           std::span<ReplicaScoreVerdict> verdicts) {
            verdicts[0] = Veto(ReplicaHardVetoReason::CIRCUIT_OPEN);
            verdicts[1] = Score(1.0, 2.0, 3.0);
        });
    ReplicaSelector selector(ReplicaSelectionMode::ACTIVE, provider,
                             ReplicaSelectionDiagnostics::ENABLED);
    std::vector<Replica::Descriptor> replicas = {
        MakeMemory(9, "incomplete", ReplicaStatus::PROCESSING), MakeDisk(8),
        MakeMemory(1, "first"), MakeMemory(2, "second")};

    const auto decision = selector.Select(replicas, {});

    ASSERT_EQ(decision.trace.hard_vetoes.size(), 1u);
    EXPECT_EQ(decision.trace.hard_vetoes[0].replica.replica_id, 1u);
    ASSERT_EQ(decision.trace.candidate_scores.size(), 1u);
    EXPECT_EQ(decision.trace.candidate_scores[0].replica.replica_id, 2u);
    EXPECT_DOUBLE_EQ(decision.trace.candidate_scores[0].total_score, 6.0);
    ASSERT_EQ(decision.trace.skipped.size(), 2u);
}

TEST_F(ReplicaSelectorV2Test, LocalDiagnosticsCaptureFastPath) {
    auto provider = std::make_shared<AtomicPreferProvider>(1);
    ReplicaSelector selector(ReplicaSelectionMode::ACTIVE, provider,
                             ReplicaSelectionDiagnostics::ENABLED);
    std::vector<Replica::Descriptor> replicas = {MakeMemory(1, "local")};

    const auto decision = selector.Select(replicas, {"local"});

    ASSERT_EQ(decision.trace.skipped.size(), 1u);
    EXPECT_EQ(decision.trace.skipped[0].reason, ReplicaSkipReason::LOCAL);
    EXPECT_EQ(provider->calls.load(std::memory_order_relaxed), 0u);
}

TEST_F(ReplicaSelectorV2Test, InlineAndOverflowCandidateCountsUseOneBatch) {
    for (const size_t count : {1u, 2u, 16u, 17u}) {
        auto provider = std::make_shared<FunctionBatchProvider>(
            [](const ReplicaSelectionContext&,
               std::span<const ReplicaScoreCandidateView> candidates,
               std::span<ReplicaScoreVerdict> verdicts) {
                for (size_t i = 0; i < candidates.size(); ++i) {
                    verdicts[i] = Score(
                        static_cast<double>(candidates.size() - i), 0.0, 0.0);
                }
            });
        ReplicaSelector selector(ReplicaSelectionMode::ACTIVE, provider);
        std::vector<Replica::Descriptor> replicas;
        replicas.reserve(count);
        for (size_t i = 0; i < count; ++i) {
            replicas.push_back(MakeMemory(static_cast<ReplicaID>(i + 1),
                                          "endpoint-" + std::to_string(i + 1)));
        }

        const auto decision = selector.Select(replicas, {});

        EXPECT_EQ(Selected(decision, replicas)->id, count);
        EXPECT_EQ(provider->calls.load(std::memory_order_relaxed), 1u);
    }
}

TEST_F(ReplicaSelectorV2Test, TwoSelectorsKeepProvidersIsolated) {
    auto first = std::make_shared<AtomicPreferProvider>(1);
    auto second = std::make_shared<AtomicPreferProvider>(2);
    const ReplicaSelector first_selector(ReplicaSelectionMode::ACTIVE, first);
    const ReplicaSelector second_selector(ReplicaSelectionMode::ACTIVE, second);
    std::vector<Replica::Descriptor> replicas = {MakeMemory(1, "first"),
                                                 MakeMemory(2, "second")};

    for (size_t i = 0; i < 100; ++i) {
        EXPECT_EQ(Selected(first_selector.Select(replicas, {}), replicas)->id,
                  1u);
        EXPECT_EQ(Selected(second_selector.Select(replicas, {}), replicas)->id,
                  2u);
    }

    EXPECT_EQ(first->calls.load(std::memory_order_relaxed), 100u);
    EXPECT_EQ(second->calls.load(std::memory_order_relaxed), 100u);
}

TEST_F(ReplicaSelectorV2Test, ConcurrentConstSelectIsRaceFree) {
    constexpr size_t kThreadCount = 8;
    constexpr size_t kIterationsPerThread = 10000;
    auto provider = std::make_shared<AtomicPreferProvider>(2);
    const ReplicaSelector selector(ReplicaSelectionMode::ACTIVE, provider);
    const std::vector<Replica::Descriptor> replicas = {MakeMemory(1, "first"),
                                                       MakeMemory(2, "second")};
    const std::unordered_set<std::string> local_endpoints;
    std::atomic<uint64_t> wrong_decisions{0};
    std::vector<std::thread> threads;

    for (size_t thread = 0; thread < kThreadCount; ++thread) {
        threads.emplace_back([&] {
            for (size_t i = 0; i < kIterationsPerThread; ++i) {
                const auto decision =
                    selector.Select(replicas, local_endpoints);
                const auto* selected = Selected(decision, replicas);
                if (!selected || selected->id != 2) {
                    wrong_decisions.fetch_add(1, std::memory_order_relaxed);
                }
            }
        });
    }
    for (auto& thread : threads) thread.join();

    EXPECT_EQ(wrong_decisions.load(std::memory_order_relaxed), 0u);
    EXPECT_EQ(provider->calls.load(std::memory_order_relaxed),
              kThreadCount * kIterationsPerThread);
}

TEST_F(ReplicaSelectorV2Test, ValueReferenceSurvivesVectorReallocation) {
    auto provider = std::make_shared<AtomicPreferProvider>(2);
    ReplicaSelector selector(ReplicaSelectionMode::ACTIVE, provider);
    std::vector<Replica::Descriptor> replicas = {MakeMemory(1, "first"),
                                                 MakeMemory(2, "second")};
    const auto decision = selector.Select(replicas, {});

    replicas.reserve(1024);

    ASSERT_NE(ResolveReplica(replicas, decision.selected), nullptr);
    EXPECT_EQ(ResolveReplica(replicas, decision.selected)->id, 2u);
}

TEST_F(ReplicaSelectorV2Test, ValueReferenceRejectsReorderAndReplacement) {
    auto provider = std::make_shared<AtomicPreferProvider>(2);
    ReplicaSelector selector(ReplicaSelectionMode::ACTIVE, provider);
    std::vector<Replica::Descriptor> replicas = {MakeMemory(1, "first"),
                                                 MakeMemory(2, "second")};
    const auto decision = selector.Select(replicas, {});

    std::swap(replicas[0], replicas[1]);
    EXPECT_EQ(ResolveReplica(replicas, decision.selected), nullptr);

    replicas[1] = MakeMemory(3, "replacement");
    EXPECT_EQ(ResolveReplica(replicas, decision.selected), nullptr);
    EXPECT_EQ(ResolveReplica(replicas, ReplicaRef{99, 2}), nullptr);
}

TEST_F(ReplicaSelectorV2Test, EmptyAndIncompleteInputsReturnNoSelection) {
    ReplicaSelector selector(ReplicaSelectionMode::ACTIVE);
    std::vector<Replica::Descriptor> empty;
    std::vector<Replica::Descriptor> incomplete = {
        MakeMemory(1, "remote", ReplicaStatus::PROCESSING)};

    const auto empty_decision = selector.Select(empty, {});
    const auto incomplete_decision = selector.Select(incomplete, {});

    EXPECT_EQ(Selected(empty_decision, empty), nullptr);
    EXPECT_EQ(Selected(incomplete_decision, incomplete), nullptr);
    EXPECT_EQ(empty_decision.outcome,
              ReplicaSelectionOutcome::NO_REMOTE_MEMORY);
    EXPECT_EQ(incomplete_decision.outcome,
              ReplicaSelectionOutcome::NO_REMOTE_MEMORY);
}

TEST_F(ReplicaSelectorV2Test, EnvironmentV1OptInDoesNotAffectActiveFallback) {
    const char* env = std::getenv("MC_STORE_REPLICA_SCORING");
    if (!env || std::string(env) != "1") {
        GTEST_SKIP() << "covered by the env-enabled CTest process";
    }
    ReplicaSelector active_selector(ReplicaSelectionMode::ACTIVE);
    ReplicaSelector shadow_selector(ReplicaSelectionMode::SHADOW);
    std::vector<Replica::Descriptor> replicas = {
        MakeMemory(1, "first", ReplicaStatus::COMPLETE, "tcp"),
        MakeMemory(2, "second", ReplicaStatus::COMPLETE, "rdma")};

    const auto active = active_selector.Select(replicas, {});
    const auto shadow = shadow_selector.Select(replicas, {});

    EXPECT_EQ(Selected(active, replicas)->id, 1u);
    EXPECT_EQ(active.outcome, ReplicaSelectionOutcome::FALLBACK_NO_PROVIDER);
    EXPECT_EQ(Selected(shadow, replicas)->id, 2u);
    EXPECT_EQ(Recommended(shadow, replicas)->id, 1u);
    EXPECT_EQ(shadow.outcome, ReplicaSelectionOutcome::FALLBACK_NO_PROVIDER);
}

}  // namespace
}  // namespace mooncake
