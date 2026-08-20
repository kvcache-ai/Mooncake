#include "client_liveness.h"

#include <chrono>

#include <gtest/gtest.h>

namespace mooncake::test {
namespace {

using namespace std::chrono_literals;

TEST(ClientLivenessRecordTest, SuspicionWindowStartsAtActualTransition) {
    const auto initial = ClientLivenessRecord::TimePoint{};
    ClientLivenessRecord record(initial);

    EXPECT_EQ(record.Evaluate(initial + 30s, 10s, 20s),
              ClientLivenessTransition::BECAME_SUSPECTED);
    EXPECT_EQ(record.state(), ClientLivenessState::SUSPECTED);
    EXPECT_EQ(record.Evaluate(initial + 49s, 10s, 20s),
              ClientLivenessTransition::NONE);
    EXPECT_EQ(record.Evaluate(initial + 50s, 10s, 20s),
              ClientLivenessTransition::BECAME_OFFLINE);
}

TEST(ClientLivenessRecordTest, ObservationRecoversSuspectedButNotOffline) {
    const auto initial = ClientLivenessRecord::TimePoint{};
    ClientLivenessRecord record(initial);

    ASSERT_EQ(record.Evaluate(initial + 10s, 10s, 20s),
              ClientLivenessTransition::BECAME_SUSPECTED);
    EXPECT_EQ(record.Observe(initial + 11s),
              ClientLivenessObservation::RECOVERED_ACTIVE);
    EXPECT_TRUE(record.IsServing());

    ASSERT_EQ(record.Evaluate(initial + 21s, 10s, 20s),
              ClientLivenessTransition::BECAME_SUSPECTED);
    ASSERT_EQ(record.Evaluate(initial + 41s, 10s, 20s),
              ClientLivenessTransition::BECAME_OFFLINE);
    EXPECT_EQ(record.Observe(initial + 42s),
              ClientLivenessObservation::REJECTED_OFFLINE);
    EXPECT_FALSE(record.ShouldRetainResources());
}

TEST(ClientLivenessRecordTest, AdmissionHelpersFollowState) {
    const auto initial = ClientLivenessRecord::TimePoint{};
    ClientLivenessRecord record(initial);
    int mutations = 0;

    EXPECT_TRUE(record.RunIfServing([&] { ++mutations; }));
    ASSERT_EQ(record.Evaluate(initial + 10s, 10s, 20s),
              ClientLivenessTransition::BECAME_SUSPECTED);
    EXPECT_FALSE(record.RunIfServing([&] { ++mutations; }));
    EXPECT_TRUE(record.RunUnlessOffline([&] { ++mutations; }));
    ASSERT_EQ(record.Evaluate(initial + 30s, 10s, 20s),
              ClientLivenessTransition::BECAME_OFFLINE);
    EXPECT_FALSE(record.RunUnlessOffline([&] { ++mutations; }));
    EXPECT_EQ(mutations, 2);
}

TEST(ClientLivenessRecordTest, FailedOperationWithholdsObservation) {
    const auto initial = ClientLivenessRecord::TimePoint{};
    ClientLivenessRecord record(initial);

    // A failed operation on an ACTIVE record neither refreshes the clock
    // nor changes state: the TTL keeps running from the last successful
    // signal.
    EXPECT_EQ(record.ObserveAndRun(initial + 5s, [] { return false; }),
              ClientLivenessObservation::OBSERVATION_WITHHELD);
    EXPECT_EQ(record.state(), ClientLivenessState::ACTIVE);
    EXPECT_EQ(record.Evaluate(initial + 10s, 10s, 20s),
              ClientLivenessTransition::BECAME_SUSPECTED);

    // A failed operation on a SUSPECTED record must not recover it.
    EXPECT_EQ(record.ObserveAndRun(initial + 11s, [] { return false; }),
              ClientLivenessObservation::OBSERVATION_WITHHELD);
    EXPECT_EQ(record.state(), ClientLivenessState::SUSPECTED);
    EXPECT_EQ(record.Evaluate(initial + 30s, 10s, 20s),
              ClientLivenessTransition::BECAME_OFFLINE);
}

TEST(ClientLivenessRecordTest, SuccessfulOperationCommitsObservation) {
    const auto initial = ClientLivenessRecord::TimePoint{};
    ClientLivenessRecord record(initial);

    ASSERT_EQ(record.Evaluate(initial + 10s, 10s, 20s),
              ClientLivenessTransition::BECAME_SUSPECTED);
    EXPECT_EQ(record.ObserveAndRun(initial + 11s, [] { return true; }),
              ClientLivenessObservation::RECOVERED_ACTIVE);
    EXPECT_TRUE(record.IsServing());

    // The clock was refreshed by the successful observation, so the record
    // is not yet suspicious again at +15s.
    EXPECT_EQ(record.Evaluate(initial + 15s, 10s, 20s),
              ClientLivenessTransition::NONE);

    // OFFLINE still rejects regardless of the operation's outcome.
    ASSERT_EQ(record.Evaluate(initial + 32s, 10s, 20s),
              ClientLivenessTransition::BECAME_SUSPECTED);
    ASSERT_EQ(record.Evaluate(initial + 52s, 10s, 20s),
              ClientLivenessTransition::BECAME_OFFLINE);
    EXPECT_EQ(record.ObserveAndRun(initial + 53s, [] { return true; }),
              ClientLivenessObservation::REJECTED_OFFLINE);
}

}  // namespace
}  // namespace mooncake::test
