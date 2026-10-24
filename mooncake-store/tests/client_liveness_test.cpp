#include "client_liveness.h"

#include <chrono>
#include <condition_variable>
#include <future>
#include <mutex>
#include <thread>

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

TEST(ClientLivenessRecordTest, MonitorEvaluationDoesNotWaitForRetainingGuard) {
    const auto initial = ClientLivenessRecord::TimePoint{};
    ClientLivenessRecord record(initial);
    auto guard = record.TryAcquireRetainingGuard();
    ASSERT_TRUE(guard);
    bool reserved = false;
    bool retired = false;
    auto evaluation = std::async(std::launch::async, [&] {
        return record.EvaluateAndRetire(
            initial + 30s, 10s, 20s, [&] { reserved = true; },
            [&] { retired = true; },
            /*wait_for_guard=*/false);
    });
    const auto status = evaluation.wait_for(1s);
    guard.reset();
    EXPECT_EQ(evaluation.get(), ClientLivenessTransition::NONE);
    EXPECT_EQ(status, std::future_status::ready);
    EXPECT_FALSE(reserved);
    EXPECT_FALSE(retired);
    EXPECT_EQ(record.state(), ClientLivenessState::ACTIVE);
}

TEST(ClientLivenessRecordTest, HeartbeatRefreshesWhileResourcesAreRetained) {
    const auto initial = ClientLivenessRecord::TimePoint{};
    ClientLivenessRecord record(initial);
    auto guard = record.TryAcquireRetainingGuard();
    ASSERT_TRUE(guard.has_value());

    auto heartbeat = std::async(std::launch::async,
                                [&] { return record.Observe(initial + 9s); });
    const auto status = heartbeat.wait_for(1s);
    // Release the barrier before asserting so a regression cannot hang the
    // future's destructor.
    guard.reset();
    EXPECT_EQ(status, std::future_status::ready);
    EXPECT_EQ(heartbeat.get(), ClientLivenessObservation::REFRESHED_ACTIVE);
    EXPECT_EQ(record.Evaluate(initial + 10s, 10s, 20s),
              ClientLivenessTransition::NONE);
}

TEST(ClientLivenessRecordTest, AdmissionDoesNotOverwriteConcurrentHeartbeat) {
    const auto initial = ClientLivenessRecord::TimePoint{};
    ClientLivenessRecord record(initial);
    ASSERT_EQ(record.Evaluate(initial + 10s, 10s, 20s),
              ClientLivenessTransition::BECAME_SUSPECTED);

    std::promise<void> entered;
    std::promise<void> release;
    auto released = release.get_future();
    auto admission = std::async(std::launch::async, [&] {
        return record.ObserveAndRun(initial + 11s, [&] {
            entered.set_value();
            released.wait();
            return true;
        });
    });
    entered.get_future().wait();
    auto heartbeat = std::async(std::launch::async,
                                [&] { return record.Observe(initial + 12s); });
    const auto status = heartbeat.wait_for(1s);
    release.set_value();
    const auto heartbeat_result = heartbeat.get();
    const auto admission_result = admission.get();

    ASSERT_EQ(status, std::future_status::ready);
    EXPECT_EQ(heartbeat_result, ClientLivenessObservation::RECOVERED_ACTIVE);
    EXPECT_EQ(admission_result, ClientLivenessObservation::REFRESHED_ACTIVE);
    EXPECT_EQ(record.Evaluate(initial + 21s, 10s, 20s),
              ClientLivenessTransition::NONE);
    EXPECT_EQ(record.Evaluate(initial + 22s, 10s, 20s),
              ClientLivenessTransition::BECAME_SUSPECTED);
}

TEST(ClientLivenessRecordTest, HeartbeatDoesNotReleaseRetirementBarrier) {
    const auto initial = ClientLivenessRecord::TimePoint{};
    ClientLivenessRecord record(initial);
    ASSERT_EQ(record.Evaluate(initial + 10s, 10s, 20s),
              ClientLivenessTransition::BECAME_SUSPECTED);
    auto guard = record.TryAcquireRetainingGuard();
    ASSERT_TRUE(guard.has_value());
    auto retirement = std::async(std::launch::async, [&] {
        return record.Evaluate(initial + 30s, 10s, 20s);
    });
    const auto retirement_status = retirement.wait_for(100ms);
    auto heartbeat = std::async(std::launch::async,
                                [&] { return record.Observe(initial + 29s); });
    const auto heartbeat_status = heartbeat.wait_for(1s);
    guard.reset();
    const auto heartbeat_result = heartbeat.get();
    const auto retirement_result = retirement.get();

    EXPECT_EQ(retirement_status, std::future_status::timeout);
    ASSERT_EQ(heartbeat_status, std::future_status::ready);
    EXPECT_EQ(heartbeat_result, ClientLivenessObservation::RECOVERED_ACTIVE);
    EXPECT_EQ(retirement_result, ClientLivenessTransition::NONE);
    EXPECT_TRUE(record.IsServing());
}

TEST(ClientLivenessRecordTest, FailedAdmissionPreservesConcurrentHeartbeat) {
    const auto initial = ClientLivenessRecord::TimePoint{};
    ClientLivenessRecord record(initial);
    ASSERT_EQ(record.Evaluate(initial + 10s, 10s, 20s),
              ClientLivenessTransition::BECAME_SUSPECTED);

    std::promise<void> entered;
    std::promise<void> release;
    auto released = release.get_future();
    auto admission = std::async(std::launch::async, [&] {
        return record.ObserveAndRun(initial + 11s, [&] {
            entered.set_value();
            released.wait();
            return false;
        });
    });
    entered.get_future().wait();
    auto heartbeat = std::async(std::launch::async,
                                [&] { return record.Observe(initial + 12s); });
    const auto status = heartbeat.wait_for(1s);
    release.set_value();
    const auto heartbeat_result = heartbeat.get();
    const auto admission_result = admission.get();

    ASSERT_EQ(status, std::future_status::ready);
    EXPECT_EQ(heartbeat_result, ClientLivenessObservation::RECOVERED_ACTIVE);
    EXPECT_EQ(admission_result,
              ClientLivenessObservation::OBSERVATION_WITHHELD);
    EXPECT_TRUE(record.IsServing());
    EXPECT_EQ(record.Evaluate(initial + 21s, 10s, 20s),
              ClientLivenessTransition::NONE);
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

}  // namespace
}  // namespace mooncake::test
