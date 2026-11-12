#include "client_liveness.h"
#include "replica.h"

#include <chrono>
#include <condition_variable>
#include <mutex>
#include <thread>

#include <gtest/gtest.h>

namespace mooncake::test {
namespace {

using namespace std::chrono_literals;

TEST(ClientLivenessRecordTest, SuspicionWindowStartsAtActualTransition) {
    const auto initial = ClientLivenessRecord::TimePoint{};
    ClientLivenessRecord record(UUID{}, initial);

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
    ClientLivenessRecord record(UUID{}, initial);

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
    ClientLivenessRecord record(UUID{}, initial);

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
    ClientLivenessRecord record(UUID{}, initial);
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
    ClientLivenessRecord record(UUID{}, initial);
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
TEST(ClientLivenessRecordTest, ResourceIdentityDistinguishesSameUuidSessions) {
    const UUID client_id = generate_uuid();
    auto old_session = std::make_shared<ClientLivenessRecord>(
        client_id, ClientLivenessRecord::TimePoint{});
    auto new_session = std::make_shared<ClientLivenessRecord>(
        client_id, ClientLivenessRecord::TimePoint{});
    ClientSessionSharedPtr identity = old_session;
    EXPECT_EQ(identity->ClientId(), new_session->ClientId());
    EXPECT_NE(identity, new_session);
}

TEST(ClientLivenessRecordTest, LocalDiskCleanupUsesBoundIncarnation) {
    const UUID client_id = generate_uuid();
    const auto now = ClientLivenessRecord::TimePoint{};
    auto old_session = std::make_shared<ClientLivenessRecord>(client_id, now);
    auto new_session = std::make_shared<ClientLivenessRecord>(client_id, now);
    Replica old_replica(client_id, 1024, "endpoint", ReplicaStatus::COMPLETE,
                        old_session);
    Replica new_replica(client_id, 1024, "endpoint", ReplicaStatus::COMPLETE,
                        new_session);
    EXPECT_FALSE(old_replica.has_stale_local_disk_client());
    EXPECT_EQ(old_session->Evaluate(now, std::chrono::seconds::zero(),
                                    std::chrono::seconds::zero()),
              ClientLivenessTransition::BECAME_SUSPECTED);
    EXPECT_FALSE(old_replica.has_stale_local_disk_client());
    EXPECT_EQ(old_session->Evaluate(now, std::chrono::seconds::zero(),
                                    std::chrono::seconds::zero()),
              ClientLivenessTransition::BECAME_OFFLINE);
    EXPECT_TRUE(old_replica.has_stale_local_disk_client());
    EXPECT_FALSE(new_replica.has_stale_local_disk_client());
    Replica unbound(client_id, 1024, "endpoint", ReplicaStatus::COMPLETE);
    EXPECT_TRUE(unbound.has_stale_local_disk_client());
    EXPECT_FALSE(unbound.isAffiliatedWith(nullptr));
}

}  // namespace mooncake::test
