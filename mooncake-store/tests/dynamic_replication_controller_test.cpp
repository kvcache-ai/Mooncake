#include "dynamic_replication_controller.h"

#include <chrono>
#include <cstdint>
#include <string>

#include <gtest/gtest.h>

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

}  // namespace
}  // namespace mooncake
