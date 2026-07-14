// Copyright 2026 Mooncake Authors

#include "replica_transfer_signal_collector.h"

#include <gtest/gtest.h>

#include <array>
#include <atomic>
#include <memory>
#include <thread>
#include <vector>

namespace mooncake {
namespace {

ReplicaSignalProviderConfig ProviderConfig() {
    ReplicaSignalProviderConfig config;
    config.load_source = {true, false, std::chrono::seconds(30), 1.0};
    config.health_source = {true, false, std::chrono::seconds(30), 1.0};
    return config;
}

ReplicaScoreCandidateView Candidate(std::string_view endpoint) {
    return {{0, 1}, endpoint, "tcp"};
}

const ReplicaEligibleScore* Eligible(const ReplicaScoreVerdict& verdict) {
    return std::get_if<ReplicaEligibleScore>(&verdict);
}

TEST(ReplicaTransferSignalCollectorTest,
     ReservationPublishesQueuedBytesThenCompletesExactlyOnce) {
    auto provider =
        std::make_shared<CachedReplicaScoreProvider>(ProviderConfig());
    auto collector = std::make_shared<ReplicaTransferSignalCollector>(
        provider, ReplicaTransferSignalCollectorConfig{64, 3, 0.25});
    auto reservation = collector->Reserve("source", "tcp", 32, true);
    ASSERT_NE(reservation, nullptr);
    ASSERT_EQ(collector->PublishSnapshot(),
              ReplicaSignalPublishStatus::PUBLISHED);

    const std::array candidates = {Candidate("source")};
    std::array<ReplicaScoreVerdict, 1> verdicts;
    provider->ScoreAll({}, candidates, verdicts);
    ASSERT_NE(Eligible(verdicts[0]), nullptr);
    EXPECT_DOUBLE_EQ(Eligible(verdicts[0])->breakdown.load_cost, 0.25);

    reservation->Complete(ErrorCode::OK);
    reservation->Complete(ErrorCode::TRANSFER_FAIL);
    ASSERT_EQ(collector->PublishSnapshot(),
              ReplicaSignalPublishStatus::PUBLISHED);
    provider->ScoreAll({}, candidates, verdicts);
    ASSERT_NE(Eligible(verdicts[0]), nullptr);
    EXPECT_DOUBLE_EQ(Eligible(verdicts[0])->breakdown.load_cost, 0.0);
    EXPECT_DOUBLE_EQ(Eligible(verdicts[0])->breakdown.health_cost, 0.0);
}

TEST(ReplicaTransferSignalCollectorTest,
     ObservedUnvisitedCandidateStartsIdleAndCanBeatQueuedEndpoint) {
    auto provider =
        std::make_shared<CachedReplicaScoreProvider>(ProviderConfig());
    auto collector = std::make_shared<ReplicaTransferSignalCollector>(
        provider, ReplicaTransferSignalCollectorConfig{64, 3, 0.25});
    collector->ObserveEndpoint("idle", "tcp");
    auto queued = collector->Reserve("busy", "tcp", 64, true);
    ASSERT_EQ(collector->PublishSnapshot(),
              ReplicaSignalPublishStatus::PUBLISHED);

    const std::array candidates = {Candidate("busy"), Candidate("idle")};
    std::array<ReplicaScoreVerdict, 2> verdicts;
    provider->ScoreAll({}, candidates, verdicts);
    ASSERT_NE(Eligible(verdicts[0]), nullptr);
    ASSERT_NE(Eligible(verdicts[1]), nullptr);
    EXPECT_GT(Eligible(verdicts[0])->breakdown.load_cost,
              Eligible(verdicts[1])->breakdown.load_cost);
}

TEST(ReplicaTransferSignalCollectorTest,
     AbandonedReservationReleasesLoadWithoutPoisoningHealth) {
    auto provider =
        std::make_shared<CachedReplicaScoreProvider>(ProviderConfig());
    auto collector = std::make_shared<ReplicaTransferSignalCollector>(
        provider, ReplicaTransferSignalCollectorConfig{64, 3, 0.25});
    collector->Reserve("source", "tcp", 64, true).reset();
    ASSERT_EQ(collector->PublishSnapshot(),
              ReplicaSignalPublishStatus::PUBLISHED);

    const std::array candidates = {Candidate("source")};
    std::array<ReplicaScoreVerdict, 1> verdicts;
    provider->ScoreAll({}, candidates, verdicts);
    ASSERT_NE(Eligible(verdicts[0]), nullptr);
    const auto& score = Eligible(verdicts[0])->breakdown;
    EXPECT_DOUBLE_EQ(score.load_cost + score.health_cost, 0.0);
}

TEST(ReplicaTransferSignalCollectorTest,
     ReadPathPublicationSkipsCleanStateAndCoalescesDirtyBurst) {
    auto provider =
        std::make_shared<CachedReplicaScoreProvider>(ProviderConfig());
    auto collector = std::make_shared<ReplicaTransferSignalCollector>(
        provider,
        ReplicaTransferSignalCollectorConfig{64, 3, 0.25, std::chrono::hours(1),
                                             std::chrono::hours(2)});
    collector->ObserveEndpoint("source", "tcp");
    ASSERT_EQ(collector->PublishSnapshotIfNeeded(),
              ReplicaSignalPublishStatus::PUBLISHED);
    ASSERT_EQ(provider->CurrentGeneration(), 1u);

    ASSERT_EQ(collector->PublishSnapshotIfNeeded(),
              ReplicaSignalPublishStatus::PUBLISHED);
    EXPECT_EQ(provider->CurrentGeneration(), 1u);

    auto reservation = collector->Reserve("source", "tcp", 32, true);
    ASSERT_EQ(provider->CurrentGeneration(), 2u);
    ASSERT_EQ(collector->PublishSnapshotIfNeeded(),
              ReplicaSignalPublishStatus::PUBLISHED);
    EXPECT_EQ(provider->CurrentGeneration(), 2u);

    ASSERT_EQ(collector->PublishSnapshot(),
              ReplicaSignalPublishStatus::PUBLISHED);
    ASSERT_EQ(provider->CurrentGeneration(), 3u);
    const std::array candidates = {Candidate("source")};
    std::array<ReplicaScoreVerdict, 1> verdicts;
    provider->ScoreAll({}, candidates, verdicts);
    ASSERT_NE(Eligible(verdicts[0]), nullptr);
    EXPECT_DOUBLE_EQ(Eligible(verdicts[0])->breakdown.load_cost, 0.25);
}

TEST(ReplicaTransferSignalCollectorTest,
     CleanReadPathRefreshesSnapshotAtConfiguredDeadline) {
    auto provider =
        std::make_shared<CachedReplicaScoreProvider>(ProviderConfig());
    auto collector = std::make_shared<ReplicaTransferSignalCollector>(
        provider, ReplicaTransferSignalCollectorConfig{
                      64, 3, 0.25, std::chrono::steady_clock::duration::zero(),
                      std::chrono::steady_clock::duration::zero()});
    collector->ObserveEndpoint("source", "tcp");
    ASSERT_EQ(collector->PublishSnapshotIfNeeded(),
              ReplicaSignalPublishStatus::PUBLISHED);
    ASSERT_EQ(provider->CurrentGeneration(), 1u);

    ASSERT_EQ(collector->PublishSnapshotIfNeeded(),
              ReplicaSignalPublishStatus::PUBLISHED);
    EXPECT_EQ(provider->CurrentGeneration(), 2u);
}

TEST(ReplicaTransferSignalCollectorTest,
     RepeatedAttributedFailuresProduceHardVetoAndSuccessRecovers) {
    auto provider =
        std::make_shared<CachedReplicaScoreProvider>(ProviderConfig());
    auto collector = std::make_shared<ReplicaTransferSignalCollector>(
        provider, ReplicaTransferSignalCollectorConfig{64, 3, 0.25});
    for (int i = 0; i < 3; ++i) {
        auto reservation = collector->Reserve("source", "tcp", 1, true);
        reservation->Complete(ErrorCode::TRANSFER_FAIL);
    }
    ASSERT_EQ(collector->PublishSnapshot(),
              ReplicaSignalPublishStatus::PUBLISHED);

    const std::array candidates = {Candidate("source")};
    std::array<ReplicaScoreVerdict, 1> verdicts;
    provider->ScoreAll({}, candidates, verdicts);
    ASSERT_NE(std::get_if<ReplicaHardVeto>(&verdicts[0]), nullptr);

    auto recovery = collector->Reserve("source", "tcp", 1, true);
    recovery->Complete(ErrorCode::OK);
    ASSERT_EQ(collector->PublishSnapshot(),
              ReplicaSignalPublishStatus::PUBLISHED);
    provider->ScoreAll({}, candidates, verdicts);
    ASSERT_NE(Eligible(verdicts[0]), nullptr);
    EXPECT_GT(Eligible(verdicts[0])->breakdown.health_cost, 0.0);
}

TEST(ReplicaTransferSignalCollectorTest,
     ConcurrentReserveCompleteAndPublishKeepsEveryGenerationValid) {
    auto provider =
        std::make_shared<CachedReplicaScoreProvider>(ProviderConfig());
    auto collector = std::make_shared<ReplicaTransferSignalCollector>(
        provider, ReplicaTransferSignalCollectorConfig{1024, 3, 0.25});
    collector->ObserveEndpoint("source", "tcp");
    constexpr int kThreads = 8;
    constexpr int kIterations = 2000;
    std::atomic<int> publish_failures{0};
    std::vector<std::thread> workers;
    workers.reserve(kThreads);
    for (int thread = 0; thread < kThreads; ++thread) {
        workers.emplace_back([&] {
            for (int i = 0; i < kIterations; ++i) {
                auto reservation = collector->Reserve("source", "tcp", 1, true);
                if (collector->PublishSnapshot() !=
                    ReplicaSignalPublishStatus::PUBLISHED) {
                    publish_failures.fetch_add(1, std::memory_order_relaxed);
                }
                reservation->Complete(ErrorCode::OK);
            }
        });
    }
    for (auto& worker : workers) worker.join();

    EXPECT_EQ(publish_failures.load(std::memory_order_relaxed), 0);
    EXPECT_GE(provider->CurrentGeneration(),
              static_cast<uint64_t>(kThreads * kIterations));
}

}  // namespace
}  // namespace mooncake
