#include <gtest/gtest.h>

#include <cstdint>
#include <string>
#include <vector>

#include "p2p/ha/p2p_election_readiness.h"

namespace mooncake {
namespace {

#ifdef STORE_USE_REDIS
P2PElectionPriorityCandidate MakePriorityCandidate(
    std::string instance_id, std::string master_endpoint, std::string role,
    uint64_t applied_sequence_id) {
    P2PElectionPriorityCandidate candidate;
    candidate.instance_id = std::move(instance_id);
    candidate.master_endpoint = std::move(master_endpoint);
    candidate.role = std::move(role);
    candidate.applied_sequence_id = applied_sequence_id;
    return candidate;
}
#endif

TEST(P2PElectionStandbyLagTest, WaitsForUnknownLatest) {
    auto unknown = EvaluateP2PElectionStandbyLag(
        /*latest_sequence_id_known=*/false, /*latest_sequence_id=*/200,
        /*applied_sequence_id=*/100, /*max_lag_entries=*/0);
    EXPECT_TRUE(unknown.should_wait);
    EXPECT_EQ(unknown.lag_entries, 0U);
}

TEST(P2PElectionStandbyLagTest, AllowsCaughtUpLatest) {
    auto caught_up = EvaluateP2PElectionStandbyLag(
        /*latest_sequence_id_known=*/true, /*latest_sequence_id=*/100,
        /*applied_sequence_id=*/100, /*max_lag_entries=*/0);
    EXPECT_FALSE(caught_up.should_wait);
    EXPECT_EQ(caught_up.lag_entries, 0U);

    auto ahead = EvaluateP2PElectionStandbyLag(
        /*latest_sequence_id_known=*/true, /*latest_sequence_id=*/90,
        /*applied_sequence_id=*/100, /*max_lag_entries=*/0);
    EXPECT_FALSE(ahead.should_wait);
    EXPECT_EQ(ahead.lag_entries, 0U);
}

TEST(P2PElectionStandbyLagTest, WaitsWhenLatestLagExceedsLimit) {
    auto strict = EvaluateP2PElectionStandbyLag(
        /*latest_sequence_id_known=*/true, /*latest_sequence_id=*/101,
        /*applied_sequence_id=*/100, /*max_lag_entries=*/0);
    EXPECT_TRUE(strict.should_wait);
    EXPECT_EQ(strict.lag_entries, 1U);

    auto tolerated = EvaluateP2PElectionStandbyLag(
        /*latest_sequence_id_known=*/true, /*latest_sequence_id=*/105,
        /*applied_sequence_id=*/100, /*max_lag_entries=*/5);
    EXPECT_FALSE(tolerated.should_wait);
    EXPECT_EQ(tolerated.lag_entries, 5U);
}

TEST(P2PElectionStandbyReadinessTest, BlocksFastRestartBeforeWatching) {
    auto syncing = EvaluateP2PElectionStandbyReadiness(
        StandbyState::SYNCING, /*latest_sequence_id_known=*/true,
        /*latest_sequence_id=*/100, /*applied_sequence_id=*/100,
        /*max_lag_entries=*/0);

    EXPECT_TRUE(syncing.should_wait);
    EXPECT_EQ(syncing.lag_entries, 0U);
}

TEST(P2PElectionStandbyReadinessTest, BlocksFastRestartWhenBehindLatest) {
    auto behind = EvaluateP2PElectionStandbyReadiness(
        StandbyState::WATCHING, /*latest_sequence_id_known=*/true,
        /*latest_sequence_id=*/101, /*applied_sequence_id=*/100,
        /*max_lag_entries=*/0);

    EXPECT_TRUE(behind.should_wait);
    EXPECT_EQ(behind.lag_entries, 1U);
}

TEST(P2PElectionStandbyReadinessTest, AllowsFastRestartAfterCatchUp) {
    auto caught_up = EvaluateP2PElectionStandbyReadiness(
        StandbyState::WATCHING, /*latest_sequence_id_known=*/true,
        /*latest_sequence_id=*/100, /*applied_sequence_id=*/100,
        /*max_lag_entries=*/0);

    EXPECT_FALSE(caught_up.should_wait);
    EXPECT_EQ(caught_up.lag_entries, 0U);
}

TEST(P2PElectionStandbyReadinessTest, AllowsEmptyClusterStartup) {
    auto empty_cluster = EvaluateP2PElectionStandbyReadiness(
        StandbyState::WATCHING, /*latest_sequence_id_known=*/true,
        /*latest_sequence_id=*/0, /*applied_sequence_id=*/0,
        /*max_lag_entries=*/0);

    EXPECT_FALSE(empty_cluster.should_wait);
    EXPECT_EQ(empty_cluster.lag_entries, 0U);
}

#ifdef STORE_USE_REDIS
TEST(P2PElectionPriorityTest, YieldsToHigherAppliedStandby) {
    auto decision = EvaluateP2PElectionPriority(
        {MakePriorityCandidate("standby-low", "10.0.0.2:50051", "standby", 90),
         MakePriorityCandidate("standby-high", "10.0.0.3:50051", "standby",
                               120),
         MakePriorityCandidate("primary-high", "10.0.0.4:50051", "primary",
                               200)},
        "self", "10.0.0.1:50051", 100);

    EXPECT_TRUE(decision.should_yield);
    EXPECT_EQ(decision.candidate.instance_id, "standby-high");
    EXPECT_EQ(decision.candidate.master_endpoint, "10.0.0.3:50051");
    EXPECT_EQ(decision.candidate.applied_sequence_id, 120U);
}

TEST(P2PElectionPriorityTest, IgnoresSelfAndNonStandbyCandidates) {
    auto decision = EvaluateP2PElectionPriority(
        {MakePriorityCandidate("previous-self", "10.0.0.1:50051", "standby",
                               200),
         MakePriorityCandidate("primary-high", "10.0.0.4:50051", "primary",
                               300),
         MakePriorityCandidate("standby-equal", "10.0.0.5:50051", "standby",
                               100)},
        "new-self", "10.0.0.1:50051", 100);

    EXPECT_FALSE(decision.should_yield);
}
#endif

}  // namespace
}  // namespace mooncake
