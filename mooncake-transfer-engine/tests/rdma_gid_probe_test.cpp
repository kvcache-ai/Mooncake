// Copyright 2026 KVCache.AI
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <gtest/gtest.h>

#include <set>
#include <string>
#include <type_traits>
#include <vector>

#include "transport/rdma_transport/rdma_gid_probe.h"
#include "transport/transport.h"

using namespace mooncake;

namespace {

AutoGidCandidate makeCandidate(int gid_index, uint32_t gid_type,
                               bool has_network_device, bool is_ipv4_mapped,
                               bool is_link_local_ipv6,
                               bool is_overlay_network = false,
                               bool is_overlay_ipv4 = false,
                               bool is_null_gid = false,
                               bool query_succeeded = true,
                               std::string gid = "") {
    AutoGidCandidate candidate;
    candidate.gid_index = gid_index;
    candidate.gid =
        gid.empty() ? "gid-" + std::to_string(gid_index) : std::move(gid);
    candidate.gid_type = gid_type;
    candidate.has_network_device = has_network_device;
    candidate.is_ipv4_mapped = is_ipv4_mapped;
    candidate.is_link_local_ipv6 = is_link_local_ipv6;
    candidate.is_overlay_network = is_overlay_network;
    candidate.is_overlay_ipv4 = is_overlay_ipv4;
    candidate.is_null_gid = is_null_gid;
    candidate.query_succeeded = query_succeeded;
    return candidate;
}

TEST(RdmaGidProbeTest, PrefersNetworkBackedRoutableCandidate) {
    std::vector<AutoGidCandidate> candidates = {
        makeCandidate(/*gid_index=*/0, IBV_GID_TYPE_ROCE_V2,
                      /*has_network_device=*/false,
                      /*is_ipv4_mapped=*/true,
                      /*is_link_local_ipv6=*/false),
        makeCandidate(/*gid_index=*/1, IBV_GID_TYPE_ROCE_V2,
                      /*has_network_device=*/true,
                      /*is_ipv4_mapped=*/true,
                      /*is_link_local_ipv6=*/false),
    };

    auto selection = selectBestAutoGidCandidate(candidates);
    ASSERT_TRUE(selection.has_value());
    EXPECT_EQ(selection->gid_index, 1);
    EXPECT_EQ(selection->candidate_class,
              AutoGidCandidateClass::kNetworkRoutable);
}

TEST(RdmaGidProbeTest, DemotesLinkLocalBehindRoutableNetworkCandidate) {
    std::vector<AutoGidCandidate> candidates = {
        makeCandidate(/*gid_index=*/0, IBV_GID_TYPE_ROCE_V2,
                      /*has_network_device=*/true,
                      /*is_ipv4_mapped=*/false,
                      /*is_link_local_ipv6=*/true),
        makeCandidate(/*gid_index=*/1, IBV_GID_TYPE_ROCE_V2,
                      /*has_network_device=*/true,
                      /*is_ipv4_mapped=*/true,
                      /*is_link_local_ipv6=*/false),
    };

    auto selection = selectBestAutoGidCandidate(candidates);
    ASSERT_TRUE(selection.has_value());
    EXPECT_EQ(selection->gid_index, 1);
    EXPECT_EQ(selection->candidate_class,
              AutoGidCandidateClass::kNetworkRoutable);
}

TEST(RdmaGidProbeTest, DemotesOverlayCandidateBehindNormalNetworkCandidate) {
    std::vector<AutoGidCandidate> candidates = {
        makeCandidate(/*gid_index=*/0, IBV_GID_TYPE_ROCE_V2,
                      /*has_network_device=*/true,
                      /*is_ipv4_mapped=*/true,
                      /*is_link_local_ipv6=*/false,
                      /*is_overlay_network=*/true),
        makeCandidate(/*gid_index=*/1, IBV_GID_TYPE_ROCE_V2,
                      /*has_network_device=*/true,
                      /*is_ipv4_mapped=*/true,
                      /*is_link_local_ipv6=*/false),
    };

    auto selection = selectBestAutoGidCandidate(candidates);
    ASSERT_TRUE(selection.has_value());
    EXPECT_EQ(selection->gid_index, 1);
}

TEST(RdmaGidProbeTest,
     PrefersNoNetworkRoutableOverDegradedNetworkBackedCandidate) {
    std::vector<AutoGidCandidate> candidates = {
        makeCandidate(/*gid_index=*/0, IBV_GID_TYPE_ROCE_V2,
                      /*has_network_device=*/true,
                      /*is_ipv4_mapped=*/false,
                      /*is_link_local_ipv6=*/true),
        makeCandidate(/*gid_index=*/1, IBV_GID_TYPE_ROCE_V2,
                      /*has_network_device=*/false,
                      /*is_ipv4_mapped=*/true,
                      /*is_link_local_ipv6=*/false),
    };

    auto selection = selectBestAutoGidCandidate(candidates);
    ASSERT_TRUE(selection.has_value());
    EXPECT_EQ(selection->gid_index, 1);
    EXPECT_EQ(selection->candidate_class,
              AutoGidCandidateClass::kNoNetworkRoutable);
}

TEST(RdmaGidProbeTest, KeepsNoNetworkFallbackAsLastResort) {
    std::vector<AutoGidCandidate> candidates = {
        makeCandidate(/*gid_index=*/3, IBV_GID_TYPE_ROCE_V2,
                      /*has_network_device=*/false,
                      /*is_ipv4_mapped=*/true,
                      /*is_link_local_ipv6=*/false),
    };

    auto selection = selectBestAutoGidCandidate(candidates);
    ASSERT_TRUE(selection.has_value());
    EXPECT_EQ(selection->gid_index, 3);
    EXPECT_EQ(selection->candidate_class,
              AutoGidCandidateClass::kNoNetworkRoutable);
}

TEST(RdmaGidProbeTest, FallsBackToFirstNonzeroCandidateWhenNeeded) {
    std::vector<AutoGidCandidate> candidates = {
        makeCandidate(/*gid_index=*/0, IBV_GID_TYPE_ROCE_V1,
                      /*has_network_device=*/false,
                      /*is_ipv4_mapped=*/false,
                      /*is_link_local_ipv6=*/false,
                      /*is_overlay_network=*/false,
                      /*is_overlay_ipv4=*/false,
                      /*is_null_gid=*/false),
        makeCandidate(/*gid_index=*/2, IBV_GID_TYPE_ROCE_V1,
                      /*has_network_device=*/false,
                      /*is_ipv4_mapped=*/false,
                      /*is_link_local_ipv6=*/false,
                      /*is_overlay_network=*/false,
                      /*is_overlay_ipv4=*/false,
                      /*is_null_gid=*/false),
    };

    auto selection = selectBestAutoGidCandidate(candidates);
    ASSERT_TRUE(selection.has_value());
    EXPECT_EQ(selection->gid_index, 0);
    EXPECT_EQ(selection->candidate_class,
              AutoGidCandidateClass::kFallbackNonzero);
}

TEST(RdmaGidProbeTest, DoesNotTreatIbCandidateAsLinkLocalIpv6Penalty) {
    std::vector<AutoGidCandidate> candidates = {
        makeCandidate(/*gid_index=*/0, IBV_GID_TYPE_IB,
                      /*has_network_device=*/true,
                      /*is_ipv4_mapped=*/false,
                      /*is_link_local_ipv6=*/true),
        makeCandidate(/*gid_index=*/1, IBV_GID_TYPE_ROCE_V2,
                      /*has_network_device=*/false,
                      /*is_ipv4_mapped=*/true,
                      /*is_link_local_ipv6=*/false),
    };

    auto selection = selectBestAutoGidCandidate(candidates);
    ASSERT_TRUE(selection.has_value());
    EXPECT_EQ(selection->gid_index, 0);
    EXPECT_EQ(selection->candidate_class,
              AutoGidCandidateClass::kNetworkRoutable);
}

TEST(RdmaGidProbeTest, SkipsInvalidAndNullCandidates) {
    std::vector<AutoGidCandidate> candidates = {
        makeCandidate(/*gid_index=*/0, IBV_GID_TYPE_ROCE_V2,
                      /*has_network_device=*/true,
                      /*is_ipv4_mapped=*/true,
                      /*is_link_local_ipv6=*/false,
                      /*is_overlay_network=*/false,
                      /*is_overlay_ipv4=*/false,
                      /*is_null_gid=*/false,
                      /*query_succeeded=*/false),
        makeCandidate(/*gid_index=*/1, IBV_GID_TYPE_ROCE_V2,
                      /*has_network_device=*/true,
                      /*is_ipv4_mapped=*/true,
                      /*is_link_local_ipv6=*/false,
                      /*is_overlay_network=*/false,
                      /*is_overlay_ipv4=*/false,
                      /*is_null_gid=*/true),
        makeCandidate(/*gid_index=*/2, IBV_GID_TYPE_ROCE_V2,
                      /*has_network_device=*/true,
                      /*is_ipv4_mapped=*/true,
                      /*is_link_local_ipv6=*/false),
    };

    auto selection = selectBestAutoGidCandidate(candidates);
    ASSERT_TRUE(selection.has_value());
    EXPECT_EQ(selection->gid_index, 2);
}

TEST(RdmaGidProbeTest, KeepsStableOrderingWithinSameCandidateClass) {
    std::vector<AutoGidCandidate> candidates = {
        makeCandidate(/*gid_index=*/3, IBV_GID_TYPE_ROCE_V2,
                      /*has_network_device=*/true,
                      /*is_ipv4_mapped=*/true,
                      /*is_link_local_ipv6=*/false),
        makeCandidate(/*gid_index=*/1, IBV_GID_TYPE_ROCE_V2,
                      /*has_network_device=*/true,
                      /*is_ipv4_mapped=*/true,
                      /*is_link_local_ipv6=*/false),
    };

    auto ranked = rankAutoGidCandidates(candidates);
    ASSERT_EQ(ranked.size(), 2u);
    EXPECT_EQ(ranked[0].gid_index, 1);
    EXPECT_EQ(ranked[1].gid_index, 3);
}

TEST(RdmaGidProbeTest, ReprobeStillPicksBestCandidateFromFreshSnapshot) {
    std::vector<AutoGidCandidate> candidates = {
        makeCandidate(/*gid_index=*/1, IBV_GID_TYPE_ROCE_V2,
                      /*has_network_device=*/true,
                      /*is_ipv4_mapped=*/true,
                      /*is_link_local_ipv6=*/false),
        makeCandidate(/*gid_index=*/3, IBV_GID_TYPE_ROCE_V2,
                      /*has_network_device=*/false,
                      /*is_ipv4_mapped=*/true,
                      /*is_link_local_ipv6=*/false),
    };

    auto selection = selectBestAutoGidCandidate(candidates);
    ASSERT_TRUE(selection.has_value());
    EXPECT_EQ(selection->gid_index, 1);
    EXPECT_EQ(selection->candidate_class,
              AutoGidCandidateClass::kNetworkRoutable);
}

TEST(RdmaGidProbeTest, ReprobeDetectsSameIndexGidRefresh) {
    std::vector<AutoGidCandidate> candidates = {
        makeCandidate(/*gid_index=*/1, IBV_GID_TYPE_ROCE_V2,
                      /*has_network_device=*/true,
                      /*is_ipv4_mapped=*/true,
                      /*is_link_local_ipv6=*/false,
                      /*is_overlay_network=*/false,
                      /*is_overlay_ipv4=*/false,
                      /*is_null_gid=*/false,
                      /*query_succeeded=*/true,
                      /*gid=*/"00:11:22"),
        makeCandidate(/*gid_index=*/3, IBV_GID_TYPE_ROCE_V2,
                      /*has_network_device=*/false,
                      /*is_ipv4_mapped=*/true,
                      /*is_link_local_ipv6=*/false),
    };

    auto selection = reselectAutoGidCandidate(
        candidates, /*current_gid_index=*/1, /*current_gid=*/"00:11:21");
    ASSERT_TRUE(selection.has_value());
    EXPECT_EQ(selection->gid_index, 1);
    EXPECT_EQ(selection->gid, "00:11:22");
    EXPECT_EQ(selection->candidate_class,
              AutoGidCandidateClass::kNetworkRoutable);
}

TEST(RdmaGidProbeTest, ReprobeSkipsRetryWhenBestSelectionDidNotChange) {
    std::vector<AutoGidCandidate> candidates = {
        makeCandidate(/*gid_index=*/1, IBV_GID_TYPE_ROCE_V2,
                      /*has_network_device=*/true,
                      /*is_ipv4_mapped=*/true,
                      /*is_link_local_ipv6=*/false,
                      /*is_overlay_network=*/false,
                      /*is_overlay_ipv4=*/false,
                      /*is_null_gid=*/false,
                      /*query_succeeded=*/true,
                      /*gid=*/"00:11:22"),
    };

    auto selection = reselectAutoGidCandidate(
        candidates, /*current_gid_index=*/1, /*current_gid=*/"00:11:22");
    EXPECT_FALSE(selection.has_value());
}

TEST(RdmaGidProbeTest, ReprobeSkipsAlreadyTriedCandidates) {
    std::vector<AutoGidCandidate> candidates = {
        makeCandidate(/*gid_index=*/1, IBV_GID_TYPE_ROCE_V2,
                      /*has_network_device=*/true,
                      /*is_ipv4_mapped=*/true,
                      /*is_link_local_ipv6=*/false,
                      /*is_overlay_network=*/false,
                      /*is_overlay_ipv4=*/false,
                      /*is_null_gid=*/false,
                      /*query_succeeded=*/true,
                      /*gid=*/"00:11:22"),
        makeCandidate(/*gid_index=*/3, IBV_GID_TYPE_ROCE_V2,
                      /*has_network_device=*/false,
                      /*is_ipv4_mapped=*/true,
                      /*is_link_local_ipv6=*/false,
                      /*is_overlay_network=*/false,
                      /*is_overlay_ipv4=*/false,
                      /*is_null_gid=*/false,
                      /*query_succeeded=*/true,
                      /*gid=*/"00:11:33"),
    };

    std::vector<AutoGidSelectionIdentity> tried = {
        {1, "00:11:22"},
    };
    auto selection = reselectAutoGidCandidate(
        candidates, /*current_gid_index=*/1, /*current_gid=*/"00:11:21", tried);
    ASSERT_TRUE(selection.has_value());
    EXPECT_EQ(selection->gid_index, 3);
    EXPECT_EQ(selection->gid, "00:11:33");
}

TEST(RdmaGidProbeTest, DetectsSameIndexGidByteChangesAsSelectionChanges) {
    EXPECT_TRUE(didAutoGidSelectionChange(/*previous_gid_index=*/1,
                                          /*previous_gid=*/"00:11:22",
                                          /*current_gid_index=*/1,
                                          /*current_gid=*/"00:11:23"));

    EXPECT_FALSE(didAutoGidSelectionChange(/*previous_gid_index=*/1,
                                           /*previous_gid=*/"00:11:22",
                                           /*current_gid_index=*/1,
                                           /*current_gid=*/"00:11:22"));
}

TEST(RdmaGidProbeTest, HandshakeRetryRespectsConfiguredRetryBudget) {
    EXPECT_TRUE(shouldAttemptAutoGidHandshakeRetry(
        /*auto_gid_selection_enabled=*/true,
        /*retry_count=*/0,
        /*max_retries=*/2,
        /*failure_happened_at_rtr=*/true, EINVAL));

    EXPECT_TRUE(shouldAttemptAutoGidHandshakeRetry(
        /*auto_gid_selection_enabled=*/true,
        /*retry_count=*/1,
        /*max_retries=*/2,
        /*failure_happened_at_rtr=*/true, EINVAL));

    EXPECT_FALSE(shouldAttemptAutoGidHandshakeRetry(
        /*auto_gid_selection_enabled=*/false,
        /*retry_count=*/0,
        /*max_retries=*/2,
        /*failure_happened_at_rtr=*/true, EINVAL));

    EXPECT_FALSE(shouldAttemptAutoGidHandshakeRetry(
        /*auto_gid_selection_enabled=*/true,
        /*retry_count=*/2,
        /*max_retries=*/2,
        /*failure_happened_at_rtr=*/true, EINVAL));

    EXPECT_FALSE(shouldAttemptAutoGidHandshakeRetry(
        /*auto_gid_selection_enabled=*/true,
        /*retry_count=*/0,
        /*max_retries=*/0,
        /*failure_happened_at_rtr=*/true, EINVAL));
}

TEST(RdmaGidProbeTest, HandshakeRetryOnlyTriggersForRtrEinval) {
    EXPECT_FALSE(shouldAttemptAutoGidHandshakeRetry(
        /*auto_gid_selection_enabled=*/true,
        /*retry_count=*/0,
        /*max_retries=*/2,
        /*failure_happened_at_rtr=*/false, EINVAL));

    EXPECT_FALSE(shouldAttemptAutoGidHandshakeRetry(
        /*auto_gid_selection_enabled=*/true,
        /*retry_count=*/0,
        /*max_retries=*/2,
        /*failure_happened_at_rtr=*/true, ENOENT));
}

TEST(RdmaGidProbeTest, RetryActionRequiresObservedOrReprobedChange) {
    EXPECT_EQ(decideAutoGidRetryAction(
                  /*reprobe_changed=*/false, /*previous_gid_index=*/1,
                  /*previous_gid=*/"00:11:22", /*current_gid_index=*/1,
                  /*current_gid=*/"00:11:22"),
              AutoGidRetryAction::kDoNotRetry);

    EXPECT_EQ(decideAutoGidRetryAction(
                  /*reprobe_changed=*/true, /*previous_gid_index=*/1,
                  /*previous_gid=*/"00:11:22", /*current_gid_index=*/1,
                  /*current_gid=*/"00:11:23"),
              AutoGidRetryAction::kRetryWithReprobedGid);

    EXPECT_EQ(decideAutoGidRetryAction(
                  /*reprobe_changed=*/false, /*previous_gid_index=*/1,
                  /*previous_gid=*/"00:11:22", /*current_gid_index=*/1,
                  /*current_gid=*/"00:11:23"),
              AutoGidRetryAction::kRetryWithObservedChange);
}

TEST(RdmaGidProbeTest, DataPathRetryCountsEndpointInstanceOnlyOnce) {
    PeerAutoGidStateTable peers(/*capacity=*/4, /*max_advances=*/2,
                                /*enabled=*/true);
    const AutoGidSelection initial{5, "gid-5"};
    auto selected = peers.prepare("peer-a", initial);
    AutoGidDataPathAttempt first{101, selected.selection_epoch};
    AutoGidDataPathAttempt same_endpoint{101, selected.selection_epoch};
    AutoGidDataPathAttempt fresh_endpoint{102, selected.selection_epoch};

    EXPECT_EQ(peers.recordCompletion("peer-a", first, /*success=*/false),
              AutoGidDataPathAction::kRetrySameGid);
    auto rebuilt = peers.prepare("peer-a", initial);
    EXPECT_EQ(rebuilt.selection.gid, initial.gid);
    EXPECT_EQ(rebuilt.selection_epoch, selected.selection_epoch);
    EXPECT_EQ(
        peers.recordCompletion("peer-a", same_endpoint, /*success=*/false),
        AutoGidDataPathAction::kIgnoreDuplicate);
    EXPECT_EQ(
        peers.recordCompletion("peer-a", fresh_endpoint, /*success=*/false),
        AutoGidDataPathAction::kAdvanceGid);
}

TEST(RdmaGidProbeTest, StaleOlderEndpointCannotConfirmFailure) {
    PeerAutoGidStateTable peers(/*capacity=*/4, /*max_advances=*/2,
                                /*enabled=*/true);
    auto selected = peers.prepare("peer-a", {5, "gid-5"});

    EXPECT_EQ(peers.recordCompletion("peer-a", {102, selected.selection_epoch},
                                     false),
              AutoGidDataPathAction::kRetrySameGid);
    EXPECT_EQ(peers.recordCompletion("peer-a", {101, selected.selection_epoch},
                                     false),
              AutoGidDataPathAction::kIgnoreDuplicate);
    EXPECT_EQ(peers.recordCompletion("peer-a", {103, selected.selection_epoch},
                                     false),
              AutoGidDataPathAction::kAdvanceGid);
}

TEST(RdmaGidProbeTest, PeerSelectionsUseIndependentLocalCandidateOrders) {
    PeerAutoGidStateTable left(/*capacity=*/4, /*max_advances=*/2,
                               /*enabled=*/true);
    PeerAutoGidStateTable right(/*capacity=*/4, /*max_advances=*/2,
                                /*enabled=*/true);
    const AutoGidSelection left_initial{5, "gid-shared"};
    const AutoGidSelection right_initial{3, "gid-shared"};
    auto left_selected = left.prepare("right", left_initial);
    auto right_selected = right.prepare("left", right_initial);
    ASSERT_EQ(left.recordCompletion("right", {1, left_selected.selection_epoch},
                                    false),
              AutoGidDataPathAction::kRetrySameGid);
    ASSERT_EQ(left.recordCompletion("right", {2, left_selected.selection_epoch},
                                    false),
              AutoGidDataPathAction::kAdvanceGid);
    ASSERT_EQ(right.recordCompletion(
                  "left", {11, right_selected.selection_epoch}, false),
              AutoGidDataPathAction::kRetrySameGid);
    ASSERT_EQ(right.recordCompletion(
                  "left", {12, right_selected.selection_epoch}, false),
              AutoGidDataPathAction::kAdvanceGid);

    auto left_next = left.prepare("right", left_initial,
                                  {{9, "gid-left-next"}, {5, "gid-shared"}});
    auto right_next = right.prepare(
        "left", right_initial,
        {{1, "gid-right-next"}, {7, "gid-extra"}, {3, "gid-shared"}});
    EXPECT_EQ(left_next.selection.gid, "gid-left-next");
    EXPECT_EQ(right_next.selection.gid, "gid-right-next");
}

TEST(RdmaGidProbeTest, PeerAdvanceDoesNotChangeOtherPeerOrDefault) {
    PeerAutoGidStateTable peers(/*capacity=*/4, /*max_advances=*/2,
                                /*enabled=*/true);
    const AutoGidSelection context_default{5, "gid-default"};
    auto peer_a = peers.prepare("peer-a", context_default);
    auto peer_b = peers.prepare("peer-b", context_default);
    ASSERT_EQ(
        peers.recordCompletion("peer-a", {1, peer_a.selection_epoch}, false),
        AutoGidDataPathAction::kRetrySameGid);
    ASSERT_EQ(
        peers.recordCompletion("peer-a", {2, peer_a.selection_epoch}, false),
        AutoGidDataPathAction::kAdvanceGid);

    auto advanced = peers.prepare("peer-a", context_default,
                                  {{7, "gid-next"}, {5, "gid-default"}});
    auto unchanged = peers.prepare("peer-b", context_default);
    EXPECT_EQ(advanced.selection.gid, "gid-next");
    EXPECT_TRUE(advanced.observe_success);
    EXPECT_EQ(unchanged.selection.gid, "gid-default");
    EXPECT_FALSE(unchanged.observe_success);
    EXPECT_EQ(context_default.gid, "gid-default");
}

TEST(RdmaGidProbeTest, CompletionTokenIsScalarAndEndpointIndependent) {
    static_assert(
        std::is_trivially_copyable_v<Transport::Slice::RdmaCompletionToken>);
    PeerAutoGidStateTable peers(/*capacity=*/1, /*max_advances=*/1,
                                /*enabled=*/true);
    auto selected = peers.prepare("peer", {5, "gid"});
    Transport::Slice::RdmaCompletionToken token{42, selected.selection_epoch,
                                                true, false};
    AutoGidDataPathAttempt attempt{token.endpoint_id, token.selection_epoch};
    EXPECT_EQ(peers.recordCompletion("peer", attempt, false),
              AutoGidDataPathAction::kRetrySameGid);
    EXPECT_EQ(peers.recordCompletion("peer", attempt, true),
              AutoGidDataPathAction::kIgnoreDuplicate);
}

TEST(RdmaGidProbeTest, ExhaustedRecoveryRearmsAfterDataSuccess) {
    PeerAutoGidStateTable peers(/*capacity=*/1, /*max_advances=*/0,
                                /*enabled=*/true);
    const AutoGidSelection initial{5, "gid-5"};
    auto selected = peers.prepare("peer", initial);
    ASSERT_EQ(
        peers.recordCompletion("peer", {10, selected.selection_epoch}, false),
        AutoGidDataPathAction::kRetrySameGid);
    ASSERT_EQ(
        peers.recordCompletion("peer", {11, selected.selection_epoch}, false),
        AutoGidDataPathAction::kAdvanceGid);

    auto exhausted = peers.prepare("peer", initial, {initial});
    EXPECT_TRUE(exhausted.observe_success);
    EXPECT_EQ(
        peers.recordCompletion("peer", {12, exhausted.selection_epoch}, false),
        AutoGidDataPathAction::kExhausted);
    EXPECT_EQ(
        peers.recordCompletion("peer", {12, exhausted.selection_epoch}, true),
        AutoGidDataPathAction::kIgnoreDuplicate);
    EXPECT_EQ(
        peers.recordCompletion("peer", {13, exhausted.selection_epoch}, false),
        AutoGidDataPathAction::kRetrySameGid);
}

TEST(RdmaGidProbeTest, SuccessfulRecoveryRenewsBudgetAndKeepsLiveToken) {
    PeerAutoGidStateTable peers(4, 2, true);
    const AutoGidSelection initial{5, "gid-5"};
    const std::vector<AutoGidSelection> candidates = {
        initial, {7, "gid-7"}, {3, "gid-3"}};
    auto selected = peers.prepare("peer", initial);
    uint64_t endpoint = 1;
    for (const auto* expected : {"gid-7", "gid-3"}) {
        EXPECT_EQ(peers.recordCompletion(
                      "peer", {endpoint++, selected.selection_epoch}, false),
                  AutoGidDataPathAction::kRetrySameGid);
        EXPECT_EQ(peers.recordCompletion(
                      "peer", {endpoint++, selected.selection_epoch}, false),
                  AutoGidDataPathAction::kAdvanceGid);
        selected = peers.prepare("peer", initial, candidates);
        ASSERT_EQ(selected.selection.gid, expected);
    }
    const AutoGidDataPathAttempt live{endpoint++, selected.selection_epoch};
    peers.recordCompletion("peer", live, true);
    auto recovered = peers.prepare("peer", initial);
    EXPECT_EQ(recovered.selection.gid, "gid-3");
    EXPECT_EQ(recovered.selection_epoch, live.selection_epoch);
    EXPECT_FALSE(recovered.observe_success);
    EXPECT_EQ(peers.recordCompletion("peer", live, false),
              AutoGidDataPathAction::kRetrySameGid);
    EXPECT_EQ(peers.recordCompletion("peer", {endpoint++, live.selection_epoch},
                                     false),
              AutoGidDataPathAction::kAdvanceGid);
    auto next = peers.prepare("peer", initial, candidates);
    EXPECT_EQ(next.selection.gid, "gid-5");
    EXPECT_NE(next.selection_epoch, live.selection_epoch);
    EXPECT_EQ(peers.recordCompletion("peer", live, true),
              AutoGidDataPathAction::kIgnoreDuplicate);
    EXPECT_TRUE(peers.prepare("peer", initial).observe_success);
}

TEST(RdmaGidProbeTest, CandidateRefreshRenewsExhaustedBudget) {
    for (const bool keep_current : {false, true}) {
        SCOPED_TRACE(keep_current);
        PeerAutoGidStateTable peers(4, 1, true);
        const AutoGidSelection initial{5, "gid-5"};
        const std::vector<AutoGidSelection> candidates = {initial,
                                                          {7, "gid-7"}};
        auto selected = peers.prepare("peer", initial);
        peers.recordCompletion("peer", {1, selected.selection_epoch}, false);
        peers.recordCompletion("peer", {2, selected.selection_epoch}, false);
        selected = peers.prepare("peer", initial, candidates);
        ASSERT_EQ(selected.selection.gid, "gid-7");
        peers.recordCompletion("peer", {3, selected.selection_epoch}, false);
        peers.recordCompletion("peer", {4, selected.selection_epoch}, false);
        selected = peers.prepare("peer", initial, candidates);
        const AutoGidDataPathAttempt old{5, selected.selection_epoch};
        ASSERT_EQ(peers.recordCompletion("peer", old, false),
                  AutoGidDataPathAction::kExhausted);
        EXPECT_EQ(peers.prepare("peer", initial, candidates).selection_epoch,
                  old.selection_epoch);

        peers.markCandidatesDirty();
        auto refreshed = peers.prepare(
            "peer", initial,
            keep_current ? candidates : std::vector<AutoGidSelection>{initial});
        EXPECT_NE(refreshed.selection_epoch, old.selection_epoch);
        EXPECT_EQ(peers.recordCompletion("peer", old, false),
                  AutoGidDataPathAction::kIgnoreDuplicate);
        EXPECT_EQ(peers.recordCompletion("peer", old, true),
                  AutoGidDataPathAction::kIgnoreDuplicate);
        if (keep_current) {
            EXPECT_EQ(refreshed.selection.gid, "gid-7");
            EXPECT_EQ(peers.recordCompletion(
                          "peer", {6, refreshed.selection_epoch}, false),
                      AutoGidDataPathAction::kRetrySameGid);
            EXPECT_EQ(peers.recordCompletion(
                          "peer", {7, refreshed.selection_epoch}, false),
                      AutoGidDataPathAction::kAdvanceGid);
            refreshed = peers.prepare("peer", initial, candidates);
        }
        EXPECT_EQ(refreshed.selection.gid, "gid-5");
        // Without success or another refresh, the renewed budget is bounded.
        peers.recordCompletion("peer", {8, refreshed.selection_epoch}, false);
        peers.recordCompletion("peer", {9, refreshed.selection_epoch}, false);
        auto exhausted_again = peers.prepare("peer", initial, candidates);
        EXPECT_EQ(exhausted_again.selection.gid, "gid-5");
        EXPECT_EQ(peers.recordCompletion(
                      "peer", {10, exhausted_again.selection_epoch}, false),
                  AutoGidDataPathAction::kExhausted);
    }
}

TEST(RdmaGidProbeTest, RefreshPreservesPendingAdvisoryAndBoundsEmptySearch) {
    for (const bool empty : {false, true}) {
        SCOPED_TRACE(empty);
        PeerAutoGidStateTable peers(4, 1, true);
        const AutoGidSelection initial{5, "gid-5"};
        auto selected = peers.prepare("peer", initial);
        peers.rememberPeerAdvertisedGid("peer", "remote-gid");
        peers.recordCompletion("peer", {1, selected.selection_epoch}, false);
        peers.recordCompletion("peer", {2, selected.selection_epoch}, false);
        peers.markCandidatesDirty();
        const std::vector<AutoGidSelection> candidates =
            empty ? std::vector<AutoGidSelection>{}
                  : std::vector<AutoGidSelection>{{7, "gid-7"}, initial};
        auto next = peers.prepare("peer", initial, candidates);
        EXPECT_NE(next.selection_epoch, selected.selection_epoch);
        EXPECT_EQ(next.selection.gid, empty ? "gid-5" : "gid-7");
        EXPECT_EQ(next.auto_gid_reprobe_from, empty ? "" : "remote-gid");
        EXPECT_FALSE(peers.needsCandidateScan("peer", initial));
        EXPECT_EQ(peers.prepare("peer", initial).selection_epoch,
                  next.selection_epoch);
        if (empty) {
            EXPECT_EQ(peers.recordCompletion("peer", {3, next.selection_epoch},
                                             false),
                      AutoGidDataPathAction::kExhausted);
        }
    }
}

TEST(RdmaGidProbeTest, AdvisoryAppliesOnceAndBudgetZeroKeepsSelection) {
    const AutoGidSelection initial{5, "gid-old"};
    const std::vector<AutoGidSelection> candidates = {{7, "gid-new"},
                                                      {5, "gid-old"}};
    PeerAutoGidStateTable enabled(/*capacity=*/2, /*max_advances=*/1,
                                  /*enabled=*/true);
    auto advanced = enabled.prepare("peer", initial, candidates, "gid-old");
    EXPECT_EQ(advanced.selection.gid, "gid-new");
    auto repeated = enabled.prepare("peer", initial, candidates, "gid-old");
    EXPECT_EQ(repeated.selection.gid, "gid-new");
    EXPECT_EQ(repeated.selection_epoch, advanced.selection_epoch);

    PeerAutoGidStateTable no_budget(/*capacity=*/2, /*max_advances=*/0,
                                    /*enabled=*/true);
    auto unchanged = no_budget.prepare("peer", initial, candidates, "gid-old");
    EXPECT_EQ(unchanged.selection.gid, "gid-old");

    PeerAutoGidStateTable no_candidates(/*capacity=*/2, /*max_advances=*/1,
                                        /*enabled=*/true);
    auto still_current = no_candidates.prepare(
        "peer", initial, std::vector<AutoGidSelection>{}, "gid-old");
    EXPECT_EQ(still_current.selection.gid, "gid-old");
}

TEST(RdmaGidProbeTest, LocalAdvanceAdvisesLastPeerAdvertisedGid) {
    PeerAutoGidStateTable peers(/*capacity=*/2, /*max_advances=*/1,
                                /*enabled=*/true);
    const AutoGidSelection initial{5, "local-old"};
    auto selected = peers.prepare("peer", initial);
    peers.rememberPeerAdvertisedGid("peer", "peer-old");
    ASSERT_EQ(
        peers.recordCompletion("peer", {1, selected.selection_epoch}, false),
        AutoGidDataPathAction::kRetrySameGid);
    ASSERT_EQ(
        peers.recordCompletion("peer", {2, selected.selection_epoch}, false),
        AutoGidDataPathAction::kAdvanceGid);

    auto advanced = peers.prepare("peer", initial, {{7, "local-new"}, initial});
    EXPECT_EQ(advanced.selection.gid, "local-new");
    EXPECT_EQ(advanced.auto_gid_reprobe_from, "peer-old");
}

TEST(RdmaGidProbeTest, RefreshRebindsSameGidAndOnlyAdvancesWhenMissing) {
    PeerAutoGidStateTable peers(/*capacity=*/2, /*max_advances=*/2,
                                /*enabled=*/true);
    const AutoGidSelection initial{5, "gid-stable"};
    auto first = peers.prepare("peer", initial);
    peers.markCandidatesDirty();
    auto rebound = peers.prepare("peer", initial,
                                 {{9, "gid-stable"}, {7, "gid-fallback"}});
    EXPECT_EQ(rebound.selection.gid, "gid-stable");
    EXPECT_EQ(rebound.selection.gid_index, 9);
    EXPECT_GT(rebound.selection_epoch, first.selection_epoch);

    peers.markCandidatesDirty();
    auto missing = peers.prepare("peer", initial, {{7, "gid-fallback"}});
    EXPECT_EQ(missing.selection.gid, "gid-fallback");
}

TEST(RdmaGidProbeTest, PeerStateIsBoundedAndExplicitGidBypassesRecovery) {
    PeerAutoGidStateTable bounded(/*capacity=*/2, /*max_advances=*/1,
                                  /*enabled=*/true);
    bounded.prepare("peer-a", {1, "gid-a"});
    auto peer_b = bounded.prepare("peer-b", {2, "gid-b"});
    ASSERT_EQ(
        bounded.recordCompletion("peer-b", {1, peer_b.selection_epoch}, false),
        AutoGidDataPathAction::kRetrySameGid);
    bounded.prepare("peer-a", {1, "gid-a"});
    bounded.prepare("peer-c", {3, "gid-c"});
    EXPECT_EQ(bounded.size(), 2u);
    EXPECT_TRUE(bounded.contains("peer-b"));
    EXPECT_FALSE(bounded.contains("peer-a"));

    PeerAutoGidStateTable explicit_gid(/*capacity=*/2, /*max_advances=*/2,
                                       /*enabled=*/false);
    auto selected = explicit_gid.prepare("peer", {4, "explicit"},
                                         {{8, "other"}}, "explicit");
    EXPECT_FALSE(selected.recovery_enabled);
    EXPECT_EQ(selected.selection.gid, "explicit");
    EXPECT_EQ(explicit_gid.recordCompletion(
                  "peer", {1, selected.selection_epoch}, false),
              AutoGidDataPathAction::kUnavailable);
}

TEST(RdmaGidProbeTest, RecoveryStateSurvivesCapacityChurn) {
    PeerAutoGidStateTable peers(/*capacity=*/1, /*max_advances=*/1,
                                /*enabled=*/true);
    auto peer_a = peers.prepare("peer-a", {5, "gid-a"});
    ASSERT_EQ(
        peers.recordCompletion("peer-a", {10, peer_a.selection_epoch}, false),
        AutoGidDataPathAction::kRetrySameGid);

    auto peer_b = peers.prepare("peer-b", {3, "gid-b"});
    EXPECT_EQ(peers.size(), 2u);
    EXPECT_TRUE(peers.contains("peer-a"));
    EXPECT_TRUE(peers.contains("peer-b"));
    EXPECT_TRUE(peer_b.recovery_enabled);
    EXPECT_EQ(
        peers.recordCompletion("peer-b", {20, peer_b.selection_epoch}, false),
        AutoGidDataPathAction::kRetrySameGid);
    EXPECT_EQ(
        peers.recordCompletion("peer-a", {11, peer_a.selection_epoch}, false),
        AutoGidDataPathAction::kAdvanceGid);

    auto peer_c = peers.prepare("peer-c", {7, "gid-c"});
    EXPECT_TRUE(peer_c.recovery_enabled);
    EXPECT_LE(peers.size(), 2u);
    EXPECT_TRUE(peers.contains("peer-a"));
    EXPECT_TRUE(peers.contains("peer-c"));
    EXPECT_FALSE(peers.contains("peer-b"));
}

TEST(RdmaGidProbeTest, RecoveryStateRemainsHardBoundedUnderFailureStorm) {
    PeerAutoGidStateTable peers(/*capacity=*/1, /*max_advances=*/1,
                                /*enabled=*/true);
    for (uint64_t endpoint_id = 1; endpoint_id <= 1024; ++endpoint_id) {
        const std::string peer = "peer-" + std::to_string(endpoint_id);
        auto selected = peers.prepare(peer, {5, "gid"});
        if (!selected.recovery_enabled) continue;
        EXPECT_EQ(peers.recordCompletion(
                      peer, {endpoint_id, selected.selection_epoch}, false),
                  AutoGidDataPathAction::kRetrySameGid);
        EXPECT_LE(peers.size(), 2u);
    }
    EXPECT_EQ(peers.size(), 2u);
}

TEST(RdmaGidProbeTest, EvictedPeerRejectsStaleCompletionAfterRecreation) {
    PeerAutoGidStateTable peers(/*capacity=*/1, /*max_advances=*/1,
                                /*enabled=*/true);
    const AutoGidSelection peer_a_gid{5, "gid-a"};
    auto old_peer_a = peers.prepare("peer-a", peer_a_gid);

    peers.prepare("peer-b", {3, "gid-b"});
    peers.prepare("peer-c", {7, "gid-c"});
    ASSERT_FALSE(peers.contains("peer-a"));

    auto new_peer_a = peers.prepare("peer-a", peer_a_gid);
    ASSERT_NE(new_peer_a.selection_epoch, old_peer_a.selection_epoch);
    EXPECT_EQ(peers.recordCompletion("peer-a", {10, old_peer_a.selection_epoch},
                                     false),
              AutoGidDataPathAction::kIgnoreDuplicate);
}

// Regression tests for #2729: a routable-fabric private-range IPv4 GID must
// outrank a link-local IPv6 GID instead of tying with it in the degraded
// tier (where the lowest-index tie-break used to pick fe80::).

// Exactly the GID table from the #2729 report: fe80 v1/v2 at indices 0/1,
// 10.14.x-mapped v1/v2 at indices 2/3, all on the same netdev. RoCE v1
// entries are filtered by type; index 3 (private v4, RoCE v2) must win over
// index 1 (link-local, RoCE v2).
TEST(RdmaGidProbeTest, PrefersPrivateRangeV4OverLinkLocal) {
    std::vector<AutoGidCandidate> candidates = {
        makeCandidate(/*gid_index=*/0, IBV_GID_TYPE_ROCE_V1,
                      /*has_network_device=*/true,
                      /*is_ipv4_mapped=*/false,
                      /*is_link_local_ipv6=*/true),
        makeCandidate(/*gid_index=*/1, IBV_GID_TYPE_ROCE_V2,
                      /*has_network_device=*/true,
                      /*is_ipv4_mapped=*/false,
                      /*is_link_local_ipv6=*/true),
        makeCandidate(/*gid_index=*/2, IBV_GID_TYPE_ROCE_V1,
                      /*has_network_device=*/true,
                      /*is_ipv4_mapped=*/true,
                      /*is_link_local_ipv6=*/false,
                      /*is_overlay_network=*/false,
                      /*is_overlay_ipv4=*/true),
        makeCandidate(/*gid_index=*/3, IBV_GID_TYPE_ROCE_V2,
                      /*has_network_device=*/true,
                      /*is_ipv4_mapped=*/true,
                      /*is_link_local_ipv6=*/false,
                      /*is_overlay_network=*/false,
                      /*is_overlay_ipv4=*/true),
    };

    auto selection = selectBestAutoGidCandidate(candidates);
    ASSERT_TRUE(selection.has_value());
    EXPECT_EQ(selection->gid_index, 3);
    EXPECT_EQ(selection->candidate_class,
              AutoGidCandidateClass::kNetworkPrivateV4);
}

// A genuinely routable (non-private) v4 GID still outranks a private-range
// one: the new tier sits strictly between routable and degraded.
TEST(RdmaGidProbeTest, RoutableV4StillOutranksPrivateRangeV4) {
    std::vector<AutoGidCandidate> candidates = {
        makeCandidate(/*gid_index=*/1, IBV_GID_TYPE_ROCE_V2,
                      /*has_network_device=*/true,
                      /*is_ipv4_mapped=*/true,
                      /*is_link_local_ipv6=*/false,
                      /*is_overlay_network=*/false,
                      /*is_overlay_ipv4=*/true),
        makeCandidate(/*gid_index=*/5, IBV_GID_TYPE_ROCE_V2,
                      /*has_network_device=*/true,
                      /*is_ipv4_mapped=*/true,
                      /*is_link_local_ipv6=*/false),
    };

    auto selection = selectBestAutoGidCandidate(candidates);
    ASSERT_TRUE(selection.has_value());
    EXPECT_EQ(selection->gid_index, 5);
    EXPECT_EQ(selection->candidate_class,
              AutoGidCandidateClass::kNetworkRoutable);
}

// An overlay-NAMED interface (docker0/cni/...) stays demoted below
// private-range v4 even when its address is v4-mapped: the interface-name
// heuristic remains the strongest demotion signal.
TEST(RdmaGidProbeTest, OverlayInterfaceStaysDemotedBelowPrivateRangeV4) {
    std::vector<AutoGidCandidate> candidates = {
        makeCandidate(/*gid_index=*/1, IBV_GID_TYPE_ROCE_V2,
                      /*has_network_device=*/true,
                      /*is_ipv4_mapped=*/true,
                      /*is_link_local_ipv6=*/false,
                      /*is_overlay_network=*/true,
                      /*is_overlay_ipv4=*/true),
        makeCandidate(/*gid_index=*/4, IBV_GID_TYPE_ROCE_V2,
                      /*has_network_device=*/true,
                      /*is_ipv4_mapped=*/true,
                      /*is_link_local_ipv6=*/false,
                      /*is_overlay_network=*/false,
                      /*is_overlay_ipv4=*/true),
    };

    auto selection = selectBestAutoGidCandidate(candidates);
    ASSERT_TRUE(selection.has_value());
    EXPECT_EQ(selection->gid_index, 4);
    EXPECT_EQ(selection->candidate_class,
              AutoGidCandidateClass::kNetworkPrivateV4);
}

// Cross-tier order is preserved from before the split: a link-local GID
// with a netdev still outranks a private-range v4 GID without one, exactly
// as network-degraded outranked no-network-degraded before.
TEST(RdmaGidProbeTest, NetworkLinkLocalStillOutranksNoNetworkPrivateV4) {
    std::vector<AutoGidCandidate> candidates = {
        makeCandidate(/*gid_index=*/1, IBV_GID_TYPE_ROCE_V2,
                      /*has_network_device=*/true,
                      /*is_ipv4_mapped=*/false,
                      /*is_link_local_ipv6=*/true),
        makeCandidate(/*gid_index=*/3, IBV_GID_TYPE_ROCE_V2,
                      /*has_network_device=*/false,
                      /*is_ipv4_mapped=*/true,
                      /*is_link_local_ipv6=*/false,
                      /*is_overlay_network=*/false,
                      /*is_overlay_ipv4=*/true),
    };

    auto selection = selectBestAutoGidCandidate(candidates);
    ASSERT_TRUE(selection.has_value());
    EXPECT_EQ(selection->gid_index, 1);
    EXPECT_EQ(selection->candidate_class,
              AutoGidCandidateClass::kNetworkDegraded);
}

// Completeness pin for the tier tables: every class has a distinct
// priority consistent with its enum order and a unique display name, so a
// future class insertion cannot silently create a tie or reuse a label.
TEST(RdmaGidProbeTest, ClassPriorityAndNameTablesAreComplete) {
    const AutoGidCandidateClass all[] = {
        AutoGidCandidateClass::kNetworkRoutable,
        AutoGidCandidateClass::kNoNetworkRoutable,
        AutoGidCandidateClass::kNetworkPrivateV4,
        AutoGidCandidateClass::kNetworkDegraded,
        AutoGidCandidateClass::kNoNetworkPrivateV4,
        AutoGidCandidateClass::kNoNetworkDegraded,
        AutoGidCandidateClass::kFallbackNonzero,
    };
    int expected_priority = 0;
    std::set<std::string> names;
    for (auto cls : all) {
        EXPECT_EQ(autoGidCandidateClassPriority(cls), expected_priority++);
        std::string name = autoGidCandidateClassToString(cls);
        EXPECT_NE(name, "unknown");
        EXPECT_TRUE(names.insert(name).second)
            << "duplicate class name: " << name;
    }
}

// When only link-local candidates exist, behavior is unchanged: lowest
// index wins within the tier.
TEST(RdmaGidProbeTest, LinkLocalOnlyKeepsLowestIndexTieBreak) {
    std::vector<AutoGidCandidate> candidates = {
        makeCandidate(/*gid_index=*/1, IBV_GID_TYPE_ROCE_V2,
                      /*has_network_device=*/true,
                      /*is_ipv4_mapped=*/false,
                      /*is_link_local_ipv6=*/true),
        makeCandidate(/*gid_index=*/2, IBV_GID_TYPE_ROCE_V2,
                      /*has_network_device=*/true,
                      /*is_ipv4_mapped=*/false,
                      /*is_link_local_ipv6=*/true),
    };

    auto selection = selectBestAutoGidCandidate(candidates);
    ASSERT_TRUE(selection.has_value());
    EXPECT_EQ(selection->gid_index, 1);
    EXPECT_EQ(selection->candidate_class,
              AutoGidCandidateClass::kNetworkDegraded);
}

}  // namespace
