#include "client_liveness.h"
#include "replica.h"

#include <chrono>

#include <gtest/gtest.h>

namespace mooncake::test {
namespace {

using namespace std::chrono_literals;

TEST(ClientLivenessRecordTest, ClientIdSurvivesLivenessTransitions) {
    const UUID client_id{1, 2};
    const auto initial = ClientLivenessRecord::TimePoint{};
    ClientLivenessRecord record(client_id, initial);
    EXPECT_EQ(record.ClientId(), client_id);
    ASSERT_EQ(record.Evaluate(initial + 10s, 10s, 20s),
              ClientLivenessTransition::BECAME_SUSPECTED);
    EXPECT_EQ(record.ClientId(), client_id);
    ASSERT_EQ(record.Observe(initial + 11s),
              ClientLivenessObservation::RECOVERED_ACTIVE);
    EXPECT_EQ(record.ClientId(), client_id);
    ASSERT_EQ(record.Evaluate(initial + 21s, 10s, 20s),
              ClientLivenessTransition::BECAME_SUSPECTED);
    ASSERT_EQ(record.Evaluate(initial + 41s, 10s, 20s),
              ClientLivenessTransition::BECAME_OFFLINE);
    EXPECT_EQ(record.ClientId(), client_id);
}

TEST(ClientLivenessRecordTest, SuspicionWindowStartsAtActualTransition) {
    const auto initial = ClientLivenessRecord::TimePoint{};
    ClientLivenessRecord record(UUID{1, 2}, initial);

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
    ClientLivenessRecord record(UUID{1, 2}, initial);

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
    ClientLivenessRecord record(UUID{1, 2}, initial);
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
    ClientLivenessRecord record(UUID{1, 2}, initial);
    (void)record.Observe(initial + 20s);
    (void)record.Observe(initial + 1s);
    EXPECT_EQ(record.Evaluate(initial + 29s, 10s, 20s),
              ClientLivenessTransition::NONE);
    EXPECT_EQ(record.Evaluate(initial + 30s, 10s, 20s),
              ClientLivenessTransition::BECAME_SUSPECTED);
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
}

}  // namespace mooncake::test
