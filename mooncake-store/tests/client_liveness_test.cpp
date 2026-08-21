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

}  // namespace
}  // namespace mooncake::test
