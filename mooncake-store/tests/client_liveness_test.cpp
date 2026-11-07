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

TEST(ClientLivenessRecordTest, EveryStateChangeIsReportedToItsCaller) {
    const auto initial = ClientLivenessRecord::TimePoint{};
    ClientLivenessRecord record(initial);
    using Observation = ClientLivenessObservation;
    using Transition = ClientLivenessTransition;
    // The return value is the only notification there is, so it must tell a
    // state change from a call that changed nothing.
    EXPECT_EQ(record.Observe(initial + 1s), Observation::REFRESHED_ACTIVE);
    EXPECT_EQ(record.Evaluate(initial + 10s, 10s, 20s), Transition::NONE);
    EXPECT_EQ(record.Evaluate(initial + 11s, 10s, 20s),
              Transition::BECAME_SUSPECTED);
    EXPECT_EQ(record.Evaluate(initial + 12s, 10s, 20s), Transition::NONE);
    EXPECT_EQ(record.Observe(initial + 13s), Observation::RECOVERED_ACTIVE);
    EXPECT_EQ(record.Observe(initial + 14s), Observation::REFRESHED_ACTIVE);
    EXPECT_EQ(record.Evaluate(initial + 24s, 10s, 20s),
              Transition::BECAME_SUSPECTED);
    EXPECT_EQ(record.Evaluate(initial + 44s, 10s, 20s),
              Transition::BECAME_OFFLINE);
    EXPECT_EQ(record.Evaluate(initial + 100s, 10s, 20s), Transition::NONE);
    EXPECT_EQ(record.Observe(initial + 101s), Observation::REJECTED_OFFLINE);
}

TEST(ClientLivenessRecordTest, DelayedObservationCannotMoveHeartbeatBackwards) {
    const auto initial = ClientLivenessRecord::TimePoint{};
    ClientLivenessRecord record(initial);
    (void)record.Observe(initial + 20s);
    (void)record.Observe(initial + 1s);
    EXPECT_EQ(record.Evaluate(initial + 29s, 10s, 20s),
              ClientLivenessTransition::NONE);
    EXPECT_EQ(record.Evaluate(initial + 30s, 10s, 20s),
              ClientLivenessTransition::BECAME_SUSPECTED);
}

}  // namespace
}  // namespace mooncake::test
