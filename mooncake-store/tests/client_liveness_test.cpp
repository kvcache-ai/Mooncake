#include "client_liveness.h"

#include <chrono>
#include <condition_variable>
#include <mutex>
#include <thread>
#include <type_traits>
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

TEST(ClientLivenessRecordTest, FailedOperationDoesNotRecoverOrRefresh) {
    const auto initial = ClientLivenessRecord::TimePoint{};
    ClientLivenessRecord record(initial);

    ASSERT_EQ(record.Evaluate(initial + 10s, 10s, 20s),
              ClientLivenessTransition::BECAME_SUSPECTED);
    EXPECT_EQ(record.ObserveAndRun(initial + 11s, [] { return false; }),
              ClientLivenessObservation::OBSERVATION_WITHHELD);
    EXPECT_EQ(record.state(), ClientLivenessState::SUSPECTED);
    EXPECT_EQ(record.Evaluate(initial + 30s, 10s, 20s),
              ClientLivenessTransition::BECAME_OFFLINE);
}

TEST(ClientLivenessRecordTest, RetireCallbackRunsAfterTransitionGuardRelease) {
    const auto initial = ClientLivenessRecord::TimePoint{};
    ClientLivenessRecord record(initial);
    ASSERT_EQ(record.Evaluate(initial + 10s, 10s, 20s),
              ClientLivenessTransition::BECAME_SUSPECTED);

    std::mutex completion_mutex;
    std::condition_variable completion_cv;
    bool completed = false;
    bool rejected_offline = false;
    std::thread probe;

    EXPECT_EQ(
        record.EvaluateAndRetire(
            initial + 30s, 10s, 20s,
            [&] {
                probe = std::thread([&] {
                    rejected_offline =
                        !record.TryAcquireRetainingGuard().has_value();
                    {
                        std::lock_guard<std::mutex> lock(completion_mutex);
                        completed = true;
                    }
                    completion_cv.notify_one();
                });

                std::unique_lock<std::mutex> lock(completion_mutex);
                EXPECT_TRUE(completion_cv.wait_for(lock, 1s,
                                                   [&] { return completed; }));
            }),
        ClientLivenessTransition::BECAME_OFFLINE);

    probe.join();
    EXPECT_TRUE(rejected_offline);
}

TEST(ClientLivenessRecordTest, RetirementIsReservedBeforeOfflineIsPublished) {
    const auto initial = ClientLivenessRecord::TimePoint{};
    ClientLivenessRecord record(initial);
    ASSERT_EQ(record.Evaluate(initial + 10s, 10s, 20s),
              ClientLivenessTransition::BECAME_SUSPECTED);

    bool reserved = false;
    EXPECT_EQ(record.EvaluateAndRetire(
                  initial + 30s, 10s, 20s,
                  [&] {
                      EXPECT_EQ(record.state(), ClientLivenessState::SUSPECTED);
                      reserved = true;
                  },
                  [&] {
                      EXPECT_TRUE(reserved);
                      EXPECT_EQ(record.state(), ClientLivenessState::OFFLINE);
                  }),
              ClientLivenessTransition::BECAME_OFFLINE);
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
    (void)record.ObserveAndRun(initial + 12s, [] { return false; });
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

TEST(ClientLivenessRecordTest, ObserverCanBeDisabledWhileRetainingGuardIsHeld) {
    const auto initial = ClientLivenessRecord::TimePoint{};
    ClientLivenessRecord record(initial);
    int notifications = 0;
    record.SetTransitionObserver([&](auto, auto) { ++notifications; });
    {
        auto guard = record.TryAcquireRetainingGuard();
        ASSERT_TRUE(guard);
        record.DisableTransitionObserver();
    }
    (void)record.Evaluate(initial + 10s, 10s, 20s);
    EXPECT_EQ(notifications, 0);
    record.SetTransitionObserver([&](auto, auto) { ++notifications; });
    (void)record.Observe(initial + 11s);
    EXPECT_EQ(notifications, 1);
}

TEST(ClientLivenessRecordTest, MovedRetainingGuardCanCommitObservation) {
    static_assert(
        std::is_move_constructible_v<ClientLivenessRecord::RetainingGuard>);
    static_assert(
        !std::is_move_assignable_v<ClientLivenessRecord::RetainingGuard>);
    const auto initial = ClientLivenessRecord::TimePoint{};
    ClientLivenessRecord record(initial);
    (void)record.Evaluate(initial + 10s, 10s, 20s);
    ASSERT_EQ(record.state(), ClientLivenessState::SUSPECTED);
    {
        auto guard = record.TryAcquireRetainingGuard();
        ASSERT_TRUE(guard);
        auto moved = std::move(*guard);
        EXPECT_EQ(moved.Observe(initial + 11s),
                  ClientLivenessObservation::RECOVERED_ACTIVE);
        EXPECT_EQ(record.state(), ClientLivenessState::ACTIVE);
    }
    EXPECT_EQ(record.Evaluate(initial + 20s, 10s, 20s),
              ClientLivenessTransition::NONE);
    EXPECT_EQ(record.Evaluate(initial + 21s, 10s, 20s),
              ClientLivenessTransition::BECAME_SUSPECTED);
}

}  // namespace
}  // namespace mooncake::test
