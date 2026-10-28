#include "dynamic_replication_controller.h"

#include <chrono>
#include <cstdint>
#include <string>
#include <vector>

#include <gtest/gtest.h>

#include "master_service/master_service_test_peer.h"

namespace mooncake {
namespace {

const TenantId kTenant = TenantId::Default();

DynamicReplicationController MakeController(const std::string& mode) {
    return DynamicReplicationController(mode, /*heat_window_seconds=*/10,
                                        /*admission_qps_threshold=*/0.2,
                                        /*max_memory_replicas=*/2);
}

ReplicaActionProposal MakeProposal(const std::string& key) {
    ReplicaActionProposal proposal;
    proposal.action = ReplicaActionType::ADD;
    proposal.proposal_id = generate_uuid();
    proposal.tenant_id = kTenant.value();
    proposal.key = key;
    return proposal;
}

DynamicReplicationController::Plan MakePlan() {
    return DynamicReplicationController::Plan{.source_segment = "source",
                                              .target_segment = "target",
                                              .target_domain = "",
                                              .source_liveness = nullptr};
}

// Issues and commits a lease for `key` the way a proposal does, leaving the
// object with a pending task bound to `task_id`.
ReplicaActionLease Admit(DynamicReplicationController& controller,
                         route::ObjectState& state, const std::string& key,
                         uint64_t version_epoch = 7) {
    const auto proposal = MakeProposal(key);
    auto lease = controller.IssueLeaseLocked(
        state, kTenant, key, proposal, MakePlan(), version_epoch,
        DynamicReplicationController::NowMs());
    lease.task_id = generate_uuid();
    controller.CommitLeaseLocked(state, kTenant, key, lease);
    return lease;
}

bool Indexed(const DynamicReplicationController& controller,
             const std::string& key) {
    for (const auto& [tenant_id, indexed_key] : controller.PendingKeys()) {
        if (tenant_id == kTenant && indexed_key == key) {
            return true;
        }
    }
    return false;
}

TEST(DynamicReplicationControllerTest, ModeGatesTheController) {
    EXPECT_FALSE(MakeController("off").Enabled());
    EXPECT_FALSE(MakeController("bogus").Enabled());
    EXPECT_TRUE(MakeController("observe").Enabled());
    EXPECT_FALSE(MakeController("observe").Enforcing());
    EXPECT_TRUE(MakeController("enforce").Enforcing());

    auto observe = MakeController("observe");
    EXPECT_EQ(observe.ValidateProposal(MakeProposal("key"), 0).error(),
              ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS);
}

TEST(DynamicReplicationControllerTest, WantsOneMoreReplicaBelowTheCap) {
    auto controller = MakeController("enforce");
    EXPECT_FALSE(controller.WantsAnotherMemoryReplica(0));
    EXPECT_TRUE(controller.WantsAnotherMemoryReplica(1));
    EXPECT_FALSE(controller.WantsAnotherMemoryReplica(2));
}

TEST(DynamicReplicationControllerTest, AccessTurnsHotAtTheThreshold) {
    auto controller = MakeController("enforce");
    // 0.2 qps over a 10s window.
    ASSERT_EQ(controller.AdmissionMinHits(), 2u);
    EXPECT_FALSE(controller.HeatAdmitted(kTenant, "key"));
    EXPECT_FALSE(controller.ObserveAccess(kTenant, "key"));
    EXPECT_TRUE(controller.ObserveAccess(kTenant, "key"));
    EXPECT_TRUE(controller.HeatAdmitted(kTenant, "key"));
    // Only the read that crosses the threshold reports it.
    EXPECT_FALSE(controller.ObserveAccess(kTenant, "key"));
    EXPECT_FALSE(controller.HeatAdmitted(kTenant, "other"));
}

TEST(DynamicReplicationControllerTest, ValidateProposalChecksShape) {
    auto controller = MakeController("enforce");
    EXPECT_TRUE(controller.ValidateProposal(MakeProposal("key"), 0));

    auto no_key = MakeProposal("");
    EXPECT_EQ(controller.ValidateProposal(no_key, 0).error(),
              ErrorCode::INVALID_PARAMS);
    auto no_id = MakeProposal("key");
    no_id.proposal_id = UUID{0, 0};
    EXPECT_EQ(controller.ValidateProposal(no_id, 0).error(),
              ErrorCode::INVALID_PARAMS);
    auto with_domain = MakeProposal("key");
    with_domain.target_domain = "rack-a";
    EXPECT_EQ(controller.ValidateProposal(with_domain, 0).error(),
              ErrorCode::INVALID_PARAMS);
    auto expired = MakeProposal("key");
    expired.expire_at_ms_epoch = 10;
    EXPECT_EQ(controller.ValidateProposal(expired, 11).error(),
              ErrorCode::INVALID_PARAMS);
}

TEST(DynamicReplicationControllerTest, CommitFilesTheLeaseAndStartsCooldown) {
    auto controller = MakeController("enforce");
    route::ObjectState state;
    const auto lease = Admit(controller, state, "key");

    ASSERT_TRUE(state.dynamic_replication_pending.has_value());
    EXPECT_EQ(state.dynamic_replication_pending->lease_id, lease.lease_id);
    EXPECT_EQ(state.dynamic_replication_pending->task_id, lease.task_id);
    EXPECT_FALSE(state.HasWorkInFlight());
    EXPECT_TRUE(Indexed(controller, "key"));
    EXPECT_TRUE(controller.HasPendingLocked(kTenant, "key", state));
    EXPECT_EQ(controller.CheckCooldownLocked(state).error(),
              ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS);

    const auto filed = controller.FindLease(kTenant, lease.proposal_id);
    ASSERT_TRUE(filed.has_value());
    EXPECT_EQ(filed->lease_id, lease.lease_id);
}

TEST(DynamicReplicationControllerTest, ReusesTheLeaseOfTheSameRequestOnly) {
    auto controller = MakeController("enforce");
    route::ObjectState state;
    const auto lease = Admit(controller, state, "key");
    const int64_t now_ms = DynamicReplicationController::NowMs();

    auto retry = MakeProposal("key");
    retry.proposal_id = lease.proposal_id;
    auto reused = controller.FindReusableLease(kTenant, "key", retry, now_ms);
    ASSERT_TRUE(reused.has_value());
    ASSERT_TRUE(reused->has_value());
    EXPECT_EQ((*reused)->lease_id, lease.lease_id);

    auto other_key =
        controller.FindReusableLease(kTenant, "other", retry, now_ms);
    ASSERT_FALSE(other_key.has_value());
    EXPECT_EQ(other_key.error(), ErrorCode::INVALID_PARAMS);

    // Past its deadline the lease is dropped instead of reused.
    auto late = controller.FindReusableLease(kTenant, "key", retry,
                                             lease.expire_at_ms_epoch + 1);
    ASSERT_TRUE(late.has_value());
    EXPECT_FALSE(late->has_value());
    EXPECT_FALSE(controller.FindLease(kTenant, lease.proposal_id).has_value());
}

TEST(DynamicReplicationControllerTest, ClearDropsPendingCooldownLeaseAndIndex) {
    auto controller = MakeController("enforce");
    route::ObjectState state;
    const auto lease = Admit(controller, state, "key");

    controller.ClearPendingLocked(kTenant, "key", state);

    EXPECT_FALSE(state.dynamic_replication_pending.has_value());
    EXPECT_TRUE(controller.CheckCooldownLocked(state));
    EXPECT_FALSE(controller.FindLease(kTenant, lease.proposal_id).has_value());
    EXPECT_FALSE(Indexed(controller, "key"));
}

TEST(DynamicReplicationControllerTest, TenantLeaseTableGoesWithItsLastLease) {
    auto controller = MakeController("enforce");
    const auto has_table = [&controller] {
        return test::MasterServiceTestPeer::HasDynamicReplicationLeaseTable(
            controller, kTenant);
    };
    EXPECT_FALSE(has_table());

    // Retracted by object teardown.
    route::ObjectState first;
    route::ObjectState second;
    Admit(controller, first, "first");
    Admit(controller, second, "second");
    controller.EraseLeasesForObject(kTenant, "first");
    EXPECT_TRUE(has_table());
    controller.EraseLeasesForObject(kTenant, "second");
    EXPECT_FALSE(has_table());

    // Expired by the sweep.
    route::ObjectState swept;
    Admit(controller, swept, "swept");
    EXPECT_TRUE(has_table());
    controller.EraseExpiredLeases(std::chrono::system_clock::now() +
                                  std::chrono::hours(1));
    EXPECT_FALSE(has_table());

    // Dropped by a retry that finds it past its deadline.
    route::ObjectState retried;
    const auto lease = Admit(controller, retried, "retried");
    auto retry = MakeProposal("retried");
    retry.proposal_id = lease.proposal_id;
    auto late = controller.FindReusableLease(kTenant, "retried", retry,
                                             lease.expire_at_ms_epoch + 1);
    ASSERT_TRUE(late.has_value());
    EXPECT_FALSE(late->has_value());
    EXPECT_FALSE(has_table());
}

TEST(DynamicReplicationControllerTest, ExpiredPendingIsClearedOnLookup) {
    auto controller = MakeController("enforce");
    route::ObjectState state;
    Admit(controller, state, "key");
    state.dynamic_replication_pending->expire_at_ms_epoch = 1;

    EXPECT_TRUE(controller.PendingExpiredLocked(
        state, DynamicReplicationController::NowMs()));
    EXPECT_FALSE(controller.HasPendingLocked(kTenant, "key", state));
    EXPECT_FALSE(state.dynamic_replication_pending.has_value());
    EXPECT_FALSE(Indexed(controller, "key"));
}

TEST(DynamicReplicationControllerTest, PlainCopyPassesOnlyWithoutPending) {
    auto controller = MakeController("enforce");
    route::ObjectState state;
    const std::vector<std::string> targets{"target"};
    EXPECT_TRUE(controller.ValidateCopyStartLocked(
        state, kTenant, "key", UUID{}, "source", 7, 0, targets));
    // A dynamic copy needs a pending task to act for.
    EXPECT_EQ(
        controller
            .ValidateCopyStartLocked(state, kTenant, "key", generate_uuid(),
                                     "source", 7, 7, targets)
            .error(),
        ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS);

    Admit(controller, state, "key");
    EXPECT_EQ(controller
                  .ValidateCopyStartLocked(state, kTenant, "key", UUID{},
                                           "source", 7, 0, targets)
                  .error(),
              ErrorCode::OBJECT_HAS_REPLICATION_TASK);
}

TEST(DynamicReplicationControllerTest, DynamicCopyMustMatchItsPendingTask) {
    auto controller = MakeController("enforce");
    route::ObjectState state;
    const auto lease = Admit(controller, state, "key", /*version_epoch=*/7);
    const std::vector<std::string> targets{"target"};

    EXPECT_TRUE(controller.ValidateCopyStartLocked(
        state, kTenant, "key", lease.lease_id, "source", 7, 7, targets));
    EXPECT_EQ(
        controller
            .ValidateCopyStartLocked(state, kTenant, "key", generate_uuid(),
                                     "source", 7, 7, targets)
            .error(),
        ErrorCode::INVALID_VERSION);
    EXPECT_EQ(
        controller
            .ValidateCopyStartLocked(state, kTenant, "key", lease.lease_id,
                                     "elsewhere", 7, 7, targets)
            .error(),
        ErrorCode::OBJECT_HAS_REPLICATION_TASK);
    // None of those gave the task up.
    EXPECT_TRUE(state.dynamic_replication_pending.has_value());

    // The object changed version since the lease: the task is dropped, with
    // its lease.
    EXPECT_EQ(
        controller
            .ValidateCopyStartLocked(state, kTenant, "key", lease.lease_id,
                                     "source", 8, 7, targets)
            .error(),
        ErrorCode::INVALID_VERSION);
    EXPECT_FALSE(state.dynamic_replication_pending.has_value());
    EXPECT_FALSE(controller.FindLease(kTenant, lease.proposal_id).has_value());
}

TEST(DynamicReplicationControllerTest, GuardGivesUpThePendingTaskUnlessKept) {
    auto controller = MakeController("enforce");
    route::ObjectState state;
    const std::string key = "key";
    Admit(controller, state, key);
    {
        DynamicReplicationController::PendingCopyGuard guard(
            controller, kTenant, key, state, /*active=*/true);
        guard.Release();
    }
    EXPECT_TRUE(state.dynamic_replication_pending.has_value());
    {
        DynamicReplicationController::PendingCopyGuard guard(
            controller, kTenant, key, state, /*active=*/true);
    }
    EXPECT_FALSE(state.dynamic_replication_pending.has_value());
}

TEST(DynamicReplicationControllerTest, UnindexForgetsOnlyThatKey) {
    auto controller = MakeController("enforce");
    route::ObjectState first;
    route::ObjectState second;
    Admit(controller, first, "first");
    Admit(controller, second, "second");

    controller.UnindexPendingLocked(kTenant, "first");
    EXPECT_FALSE(Indexed(controller, "first"));
    EXPECT_TRUE(Indexed(controller, "second"));
}

}  // namespace
}  // namespace mooncake
