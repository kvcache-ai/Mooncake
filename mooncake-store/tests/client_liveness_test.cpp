#include "client_liveness.h"

#include <chrono>
#include <future>
#include <utility>
#include <vector>

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

TEST(ClientLivenessRecordTest, TransitionObserverReportsOnlyStateChanges) {
    const auto initial = ClientLivenessRecord::TimePoint{};
    ClientLivenessRecord record(initial);
    using State = ClientLivenessState;
    std::vector<std::pair<State, State>> transitions;
    record.SetTransitionObserver([&](State previous, State current) {
        EXPECT_EQ(record.state(), current);
        transitions.emplace_back(previous, current);
    });
    (void)record.Observe(initial + 1s);
    (void)record.Evaluate(initial + 11s, 10s, 20s);
    (void)record.Observe(initial + 13s);
    (void)record.Evaluate(initial + 23s, 10s, 20s);
    (void)record.Evaluate(initial + 43s, 10s, 20s);
    (void)record.Observe(initial + 44s);
    EXPECT_EQ(transitions, (std::vector<std::pair<State, State>>{
                               {State::ACTIVE, State::SUSPECTED},
                               {State::SUSPECTED, State::ACTIVE},
                               {State::ACTIVE, State::SUSPECTED},
                               {State::SUSPECTED, State::OFFLINE}}));
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

TEST(ClientLivenessRecordTest, StopObservingDoesNotChangeLiveness) {
    using State = ClientLivenessState;
    const auto initial = ClientLivenessRecord::TimePoint{};
    for (const auto state : {State::ACTIVE, State::SUSPECTED, State::OFFLINE}) {
        SCOPED_TRACE(toString(state));
        ClientLivenessRecord record(initial);
        if (state != State::ACTIVE) {
            (void)record.Evaluate(initial + 10s, 10s, 20s);
        }
        if (state == State::OFFLINE) {
            (void)record.Evaluate(initial + 30s, 10s, 20s);
        }
        int notifications = 0;
        record.SetTransitionObserver([&](auto, auto) { ++notifications; });
        const auto observed = record.StopObserving();
        EXPECT_EQ(observed, state);
        EXPECT_EQ(record.state(), state);
        EXPECT_EQ(record.Observe(initial + 31s),
                  state == State::OFFLINE
                      ? ClientLivenessObservation::REJECTED_OFFLINE
                  : state == State::SUSPECTED
                      ? ClientLivenessObservation::RECOVERED_ACTIVE
                      : ClientLivenessObservation::REFRESHED_ACTIVE);
        (void)record.Evaluate(initial + 41s, 10s, 20s);
        (void)record.Evaluate(initial + 61s, 10s, 20s);
        EXPECT_EQ(record.state(), State::OFFLINE);
        EXPECT_EQ(observed, state);
        EXPECT_EQ(notifications, 0);
    }
}

TEST(ClientLivenessRecordTest, StopObservingWaitsForInFlightObserver) {
    const auto initial = ClientLivenessRecord::TimePoint{};
    ClientLivenessRecord record(initial);
    std::promise<void> entered;
    auto observing = entered.get_future();
    std::promise<void> release;
    auto proceed = release.get_future();
    int notifications = 0;
    record.SetTransitionObserver([&](auto, auto) {
        ++notifications;
        entered.set_value();
        proceed.wait();
    });
    auto transition = std::async(std::launch::async, [&] {
        return record.Evaluate(initial + 10s, 10s, 20s);
    });
    EXPECT_EQ(observing.wait_for(1s), std::future_status::ready);
    std::promise<void> attempted;
    auto started = attempted.get_future();
    auto stopped = std::async(std::launch::async, [&] {
        attempted.set_value();
        return record.StopObserving();
    });
    started.wait();
    EXPECT_EQ(stopped.wait_for(20ms), std::future_status::timeout);
    release.set_value();
    EXPECT_EQ(transition.get(), ClientLivenessTransition::BECAME_SUSPECTED);
    EXPECT_EQ(stopped.get(), ClientLivenessState::SUSPECTED);
    EXPECT_EQ(record.Observe(initial + 11s),
              ClientLivenessObservation::RECOVERED_ACTIVE);
    EXPECT_EQ(notifications, 1);
}

}  // namespace
}  // namespace mooncake::test
