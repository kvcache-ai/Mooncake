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

TEST(RdmaGidProbeTest, CursorAdvancesInRankOrderAndWraps) {
    const std::vector<AutoGidSelection> candidates = {
        {1, "gid-best"}, {3, "gid-middle"}, {5, "gid-last"}};
    ASSERT_NE(reselectAutoGidCandidate(candidates, "gid-middle"), nullptr);
    EXPECT_EQ(reselectAutoGidCandidate(candidates, "gid-middle")->gid,
              "gid-last");
    EXPECT_EQ(reselectAutoGidCandidate(candidates, "gid-last")->gid,
              "gid-best");
    EXPECT_EQ(reselectAutoGidCandidate(candidates, "gid-missing")->gid,
              "gid-best");
    EXPECT_EQ(reselectAutoGidCandidate({{1, "gid-only"}}, "gid-only"), nullptr);
    EXPECT_EQ(reselectAutoGidCandidate({}, "gid-missing"), nullptr);
}

TEST(RdmaGidProbeTest, RankingKeepsPreferredIndexForDuplicateGid) {
    auto low = makeCandidate(1, IBV_GID_TYPE_ROCE_V2, false, true, false);
    auto high = makeCandidate(3, IBV_GID_TYPE_ROCE_V2, true, true, false);
    low.gid = high.gid = "gid-shared";
    auto distinct = makeCandidate(5, IBV_GID_TYPE_ROCE_V2, true, true, false);
    const auto ranked = rankAutoGidCandidates({low, distinct, high});
    ASSERT_EQ(ranked.size(), 2u);
    EXPECT_EQ(ranked[0].gid, "gid-shared");
    EXPECT_EQ(ranked[0].gid_index, 3);
    EXPECT_EQ(ranked[1].gid_index, 5);
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

TEST(RdmaGidProbeTest, DataPathRetryCountsEndpointInstanceOnlyOnce) {
    PeerAutoGidStateTable peers(/*max_advances=*/2, /*enabled=*/true);
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
    PeerAutoGidStateTable peers(/*max_advances=*/2, /*enabled=*/true);
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
    PeerAutoGidStateTable left(/*max_advances=*/2, /*enabled=*/true);
    PeerAutoGidStateTable right(/*max_advances=*/2, /*enabled=*/true);
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
    PeerAutoGidStateTable peers(/*max_advances=*/2, /*enabled=*/true);
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
    PeerAutoGidStateTable peers(/*max_advances=*/1, /*enabled=*/true);
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
    PeerAutoGidStateTable peers(/*max_advances=*/0, /*enabled=*/true);
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
    PeerAutoGidStateTable peers(2, true);
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

TEST(RdmaGidProbeTest, UnchangedCandidateRefreshPreservesRecoveryBudget) {
    PeerAutoGidStateTable peers(1, true);
    const AutoGidSelection initial{5, "gid-5"};
    const std::vector<AutoGidSelection> candidates = {initial, {7, "gid-7"}};
    peers.requestAdvance("peer", initial);
    auto selected = peers.prepare("peer", initial, candidates);
    ASSERT_EQ(selected.selection.gid, "gid-7");

    EXPECT_TRUE(peers.refreshCandidates(candidates).empty());
    peers.requestAdvance("peer", initial);
    auto exhausted = peers.prepare("peer", initial, candidates);
    EXPECT_EQ(exhausted.selection.gid, "gid-7");
    EXPECT_EQ(exhausted.selection_epoch, selected.selection_epoch);
    ASSERT_EQ(
        peers.recordCompletion("peer", {1, selected.selection_epoch}, false),
        AutoGidDataPathAction::kExhausted);

    EXPECT_TRUE(peers.refreshCandidates(candidates).empty());
    EXPECT_FALSE(peers.needsCandidateScan("peer", initial));
    EXPECT_EQ(
        peers.recordCompletion("peer", {2, selected.selection_epoch}, false),
        AutoGidDataPathAction::kExhausted);
    auto unchanged = peers.prepare("peer", initial, candidates);
    EXPECT_EQ(unchanged.selection_epoch, selected.selection_epoch);
    EXPECT_TRUE(unchanged.observe_success);
}

TEST(RdmaGidProbeTest, UnchangedCandidateRefreshPreservesFirstFailedEndpoint) {
    PeerAutoGidStateTable peers(1, true);
    const AutoGidSelection initial{5, "gid-5"};
    const std::vector<AutoGidSelection> candidates = {initial, {7, "gid-7"}};
    auto selected = peers.prepare("peer", initial);
    ASSERT_EQ(
        peers.recordCompletion("peer", {10, selected.selection_epoch}, false),
        AutoGidDataPathAction::kRetrySameGid);
    EXPECT_TRUE(peers.refreshCandidates(candidates).empty());
    EXPECT_EQ(peers.prepare("peer", initial).selection_epoch,
              selected.selection_epoch);
    EXPECT_EQ(
        peers.recordCompletion("peer", {10, selected.selection_epoch}, false),
        AutoGidDataPathAction::kIgnoreDuplicate);
    EXPECT_EQ(
        peers.recordCompletion("peer", {11, selected.selection_epoch}, false),
        AutoGidDataPathAction::kAdvanceGid);
    EXPECT_EQ(peers.prepare("peer", initial, candidates).selection.gid,
              "gid-7");
}

TEST(RdmaGidProbeTest, ChangedBindingRefreshRenewsExhaustedBudget) {
    for (const bool moved : {false, true}) {
        SCOPED_TRACE(moved);
        PeerAutoGidStateTable peers(1, true);
        const AutoGidSelection initial{5, "gid-5"};
        const std::vector<AutoGidSelection> candidates = {initial,
                                                          {7, "gid-7"}};
        peers.requestAdvance("peer", initial);
        auto selected = peers.prepare("peer", initial, candidates);
        ASSERT_EQ(selected.selection.gid, "gid-7");
        peers.requestAdvance("peer", initial);
        peers.prepare("peer", initial, candidates);
        const AutoGidDataPathAttempt old{1, selected.selection_epoch};
        ASSERT_EQ(peers.recordCompletion("peer", old, false),
                  AutoGidDataPathAction::kExhausted);

        const std::vector<AutoGidSelection> refreshed_candidates =
            moved ? std::vector<AutoGidSelection>{initial, {9, "gid-7"}}
                  : std::vector<AutoGidSelection>{initial};
        EXPECT_EQ(peers.refreshCandidates(refreshed_candidates),
                  std::vector<std::string>{"peer"});
        auto refreshed = peers.prepare("peer", initial, refreshed_candidates);
        EXPECT_NE(refreshed.selection_epoch, old.selection_epoch);
        EXPECT_EQ(peers.recordCompletion("peer", old, false),
                  AutoGidDataPathAction::kIgnoreDuplicate);
        EXPECT_EQ(peers.recordCompletion("peer", old, true),
                  AutoGidDataPathAction::kIgnoreDuplicate);
        if (moved) {
            EXPECT_EQ(refreshed.selection.gid, "gid-7");
            EXPECT_EQ(refreshed.selection.gid_index, 9);
            peers.requestAdvance("peer", initial);
            refreshed = peers.prepare("peer", initial, refreshed_candidates);
        }
        EXPECT_EQ(refreshed.selection.gid, "gid-5");
        peers.requestAdvance("peer", initial);
        auto exhausted_again = peers.prepare("peer", initial, candidates);
        EXPECT_EQ(exhausted_again.selection.gid, "gid-5");
        EXPECT_EQ(peers.recordCompletion(
                      "peer", {2, exhausted_again.selection_epoch}, false),
                  AutoGidDataPathAction::kExhausted);
    }
}

TEST(RdmaGidProbeTest, EmptyCandidateScansPreservePendingRecoveryAndBudget) {
    PeerAutoGidStateTable peers(2, true);
    const AutoGidSelection initial{5, "gid-5"};
    const std::vector<AutoGidSelection> candidates = {
        initial, {7, "gid-7"}, {9, "gid-9"}};
    auto selected = peers.prepare("peer", initial);
    peers.rememberPeerAdvertisedGid("peer", "remote-gid");
    ASSERT_EQ(
        peers.recordCompletion("peer", {1, selected.selection_epoch}, false),
        AutoGidDataPathAction::kRetrySameGid);
    ASSERT_EQ(
        peers.recordCompletion("peer", {2, selected.selection_epoch}, false),
        AutoGidDataPathAction::kAdvanceGid);
    for (int scan = 0; scan < 3; ++scan) {
        auto empty = peers.prepare("peer", initial, {});
        EXPECT_EQ(empty.selection.gid, initial.gid);
        EXPECT_EQ(empty.selection_epoch, selected.selection_epoch);
        EXPECT_TRUE(empty.observe_success);
        EXPECT_TRUE(empty.auto_gid_reprobe_from.empty());
        EXPECT_TRUE(peers.needsCandidateScan("peer", initial));
        EXPECT_EQ(peers.recordCompletion("peer", {3, selected.selection_epoch},
                                         false),
                  AutoGidDataPathAction::kIgnoreDuplicate);
    }
    auto next = peers.prepare("peer", initial, candidates);
    EXPECT_EQ(next.selection.gid, "gid-7");
    EXPECT_NE(next.selection_epoch, selected.selection_epoch);
    EXPECT_EQ(next.auto_gid_reprobe_from, "remote-gid");
    peers.requestAdvance("peer", initial);
    next = peers.prepare("peer", initial, candidates);
    EXPECT_EQ(next.selection.gid, "gid-9");
    peers.requestAdvance("peer", initial);
    auto exhausted = peers.prepare("peer", initial, candidates);
    EXPECT_EQ(exhausted.selection_epoch, next.selection_epoch);
    EXPECT_EQ(peers.recordCompletion("peer", {4, next.selection_epoch}, false),
              AutoGidDataPathAction::kExhausted);
}

TEST(RdmaGidProbeTest, MatchingAdvisoryGrantsOneStepAfterBudgetIsSpent) {
    for (const bool exhaust_before_advisory : {false, true}) {
        SCOPED_TRACE(exhaust_before_advisory);
        PeerAutoGidStateTable peers(1, true);
        const AutoGidSelection initial{5, "gid-5"};
        const std::vector<AutoGidSelection> candidates = {
            initial, {7, "gid-7"}, {9, "gid-9"}};
        peers.requestAdvance("peer", initial);
        auto selected = peers.prepare("peer", initial, candidates);
        ASSERT_EQ(selected.selection.gid, "gid-7");
        peers.rememberPeerAdvertisedGid("peer", "remote-gid");
        if (exhaust_before_advisory) {
            peers.requestAdvance("peer", initial);
            peers.prepare("peer", initial, candidates);
            ASSERT_EQ(peers.recordCompletion(
                          "peer", {1, selected.selection_epoch}, false),
                      AutoGidDataPathAction::kExhausted);
        }
        EXPECT_FALSE(peers.needsCandidateScan("peer", initial, "gid-5"));
        auto mismatch = peers.prepare("peer", initial, candidates, "gid-5");
        EXPECT_EQ(mismatch.selection_epoch, selected.selection_epoch);
        EXPECT_TRUE(peers.needsCandidateScan("peer", initial, "gid-7"));
        auto empty = peers.prepare("peer", initial, {}, "gid-7");
        EXPECT_EQ(empty.selection_epoch, selected.selection_epoch);
        if (exhaust_before_advisory) {
            EXPECT_EQ(peers.recordCompletion(
                          "peer", {2, selected.selection_epoch}, false),
                      AutoGidDataPathAction::kExhausted);
        }
        auto advanced = peers.prepare("peer", initial, candidates, "gid-7");
        EXPECT_EQ(advanced.selection.gid, "gid-9");
        EXPECT_NE(advanced.selection_epoch, selected.selection_epoch);
        EXPECT_TRUE(advanced.auto_gid_reprobe_from.empty());
        EXPECT_FALSE(peers.needsCandidateScan("peer", initial, "gid-7"));
        auto repeated = peers.prepare("peer", initial, candidates, "gid-7");
        EXPECT_EQ(repeated.selection.gid, "gid-9");
        EXPECT_EQ(repeated.selection_epoch, advanced.selection_epoch);
        peers.requestAdvance("peer", initial);
        auto exhausted = peers.prepare("peer", initial, candidates);
        EXPECT_EQ(exhausted.selection_epoch, advanced.selection_epoch);
        EXPECT_EQ(peers.recordCompletion("peer", {3, exhausted.selection_epoch},
                                         false),
                  AutoGidDataPathAction::kExhausted);
    }
}

TEST(RdmaGidProbeTest, MatchingAdvisoryWithSingleCandidateStaysBounded) {
    PeerAutoGidStateTable peers(1, true);
    const AutoGidSelection initial{5, "gid-only"};
    auto selected = peers.prepare("peer", initial);
    peers.rememberPeerAdvertisedGid("peer", "remote-gid");
    for (int request = 0; request < 3; ++request) {
        auto unchanged = peers.prepare("peer", initial, {initial}, initial.gid);
        EXPECT_EQ(unchanged.selection.gid, initial.gid);
        EXPECT_EQ(unchanged.selection_epoch, selected.selection_epoch);
        EXPECT_TRUE(unchanged.auto_gid_reprobe_from.empty());
        EXPECT_FALSE(peers.needsCandidateScan("peer", initial));
        EXPECT_EQ(peers.recordCompletion("peer", {1, selected.selection_epoch},
                                         false),
                  AutoGidDataPathAction::kExhausted);
    }
}

TEST(RdmaGidProbeTest, LocalAdvanceAdvisesLastPeerAdvertisedGid) {
    PeerAutoGidStateTable peers(/*max_advances=*/1, /*enabled=*/true);
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
    EXPECT_TRUE(peers.refreshCandidates({{7, "local-new"}, initial}).empty());
    auto refreshed = peers.prepare("peer", initial);
    EXPECT_EQ(refreshed.selection_epoch, advanced.selection_epoch);
    EXPECT_EQ(refreshed.auto_gid_reprobe_from, "peer-old");
    peers.rememberPeerAdvertisedGid("peer", "peer-new");
    EXPECT_TRUE(peers.prepare("peer", initial).auto_gid_reprobe_from.empty());
}

TEST(RdmaGidProbeTest, RefreshRebindsSameGidAndOnlyAdvancesWhenMissing) {
    PeerAutoGidStateTable peers(/*max_advances=*/2, /*enabled=*/true);
    const AutoGidSelection initial{5, "gid-stable"};
    auto first = peers.prepare("peer", initial);
    EXPECT_EQ(peers.refreshCandidates({{9, "gid-stable"}, {7, "gid-fallback"}}),
              std::vector<std::string>{"peer"});
    auto rebound = peers.prepare("peer", initial);
    EXPECT_EQ(rebound.selection.gid, "gid-stable");
    EXPECT_EQ(rebound.selection.gid_index, 9);
    EXPECT_GT(rebound.selection_epoch, first.selection_epoch);

    EXPECT_EQ(peers.refreshCandidates({{7, "gid-fallback"}}),
              std::vector<std::string>{"peer"});
    EXPECT_GT(peers.prepare("peer", initial).selection_epoch,
              rebound.selection_epoch);
    EXPECT_EQ(
        peers.recordCompletion("peer", {1, rebound.selection_epoch}, false),
        AutoGidDataPathAction::kIgnoreDuplicate);
    auto missing = peers.prepare("peer", initial, {{7, "gid-fallback"}});
    EXPECT_EQ(missing.selection.gid, "gid-fallback");
}

TEST(RdmaGidProbeTest, RefreshKeepsValidLiveEpochAndInvalidatesOnlyMovedPeer) {
    PeerAutoGidStateTable peers(2, true);
    const AutoGidSelection stable{5, "gid-stable"};
    const AutoGidSelection moved{7, "gid-moved"};
    auto stable_before = peers.prepare("stable-peer", stable);
    auto moved_before = peers.prepare("moved-peer", moved);
    auto invalidated = peers.refreshCandidates({stable, {9, "gid-moved"}});
    EXPECT_EQ(invalidated, std::vector<std::string>{"moved-peer"});
    EXPECT_EQ(peers.prepare("stable-peer", stable).selection_epoch,
              stable_before.selection_epoch);
    EXPECT_EQ(peers.recordCompletion("stable-peer",
                                     {1, stable_before.selection_epoch}, false),
              AutoGidDataPathAction::kRetrySameGid);
    EXPECT_TRUE(peers.prepare("stable-peer", stable).observe_success);
    EXPECT_EQ(peers.recordCompletion("stable-peer",
                                     {2, stable_before.selection_epoch}, true),
              AutoGidDataPathAction::kIgnoreDuplicate);
    EXPECT_FALSE(peers.prepare("stable-peer", stable).observe_success);
    EXPECT_EQ(peers.recordCompletion("moved-peer",
                                     {3, moved_before.selection_epoch}, false),
              AutoGidDataPathAction::kIgnoreDuplicate);
}

TEST(RdmaGidProbeTest, ExplicitGidBypassesRecovery) {
    PeerAutoGidStateTable explicit_gid(/*max_advances=*/2, /*enabled=*/false);
    auto selected = explicit_gid.prepare("peer", {4, "explicit"},
                                         {{8, "other"}}, "explicit");
    EXPECT_FALSE(selected.recovery_enabled);
    EXPECT_EQ(selected.selection.gid, "explicit");
    EXPECT_EQ(explicit_gid.size(), 0u);
    EXPECT_EQ(explicit_gid.recordCompletion(
                  "peer", {1, selected.selection_epoch}, false),
              AutoGidDataPathAction::kUnavailable);
}

TEST(RdmaGidProbeTest, RecoveryStateSurvivesPeerAndEndpointChurn) {
    PeerAutoGidStateTable peers(/*max_advances=*/1, /*enabled=*/true);
    const AutoGidSelection initial{5, "gid-a"};
    auto peer_a = peers.prepare("peer-a", initial);
    ASSERT_EQ(
        peers.recordCompletion("peer-a", {10, peer_a.selection_epoch}, false),
        AutoGidDataPathAction::kRetrySameGid);
    for (int i = 0; i < 1024; ++i) {
        const auto peer = "other-" + std::to_string(i);
        auto selected = peers.prepare(peer, initial);
        EXPECT_EQ(
            peers.recordCompletion(peer, {20, selected.selection_epoch}, false),
            AutoGidDataPathAction::kRetrySameGid);
    }
    EXPECT_EQ(peers.size(), 1025u);
    EXPECT_EQ(peers.prepare("peer-a", initial).selection_epoch,
              peer_a.selection_epoch);
    EXPECT_EQ(
        peers.recordCompletion("peer-a", {11, peer_a.selection_epoch}, false),
        AutoGidDataPathAction::kAdvanceGid);
    auto next = peers.prepare("peer-a", initial, {initial, {7, "gid-next"}});
    EXPECT_EQ(next.selection.gid, "gid-next");
    peers.configure(1, true);
    EXPECT_EQ(peers.size(), 0u);
    auto recreated = peers.prepare("peer-a", initial);
    EXPECT_NE(recreated.selection_epoch, peer_a.selection_epoch);
    EXPECT_EQ(
        peers.recordCompletion("peer-a", {12, peer_a.selection_epoch}, false),
        AutoGidDataPathAction::kIgnoreDuplicate);
}

TEST(RdmaGidProbeTest, CursorVisitsEachAlternativeOnceBeforeExhaustion) {
    const std::vector<AutoGidSelection> candidates = {
        {1, "gid-first"}, {3, "gid-middle"}, {5, "gid-last"}};
    PeerAutoGidStateTable peers(/*max_advances=*/10, /*enabled=*/true);
    auto selected = peers.prepare("peer", candidates[1]);
    for (const auto* expected : {"gid-last", "gid-first"}) {
        peers.requestAdvance("peer", candidates[1]);
        selected = peers.prepare("peer", candidates[1], candidates);
        EXPECT_EQ(selected.selection.gid, expected);
    }
    peers.requestAdvance("peer", candidates[1]);
    auto exhausted = peers.prepare("peer", candidates[1], candidates);
    EXPECT_EQ(exhausted.selection.gid, "gid-first");
    EXPECT_EQ(exhausted.selection_epoch, selected.selection_epoch);
    EXPECT_EQ(
        peers.recordCompletion("peer", {1, exhausted.selection_epoch}, false),
        AutoGidDataPathAction::kExhausted);
}

TEST(RdmaGidProbeTest, VanishedCursorVisitsAllFreshCandidatesOnce) {
    const AutoGidSelection initial{9, "gid-missing"};
    const std::vector<AutoGidSelection> candidates = {
        {1, "gid-first"}, {3, "gid-middle"}, {5, "gid-last"}};
    PeerAutoGidStateTable peers(/*max_advances=*/10, /*enabled=*/true);
    peers.prepare("peer", initial);
    ASSERT_EQ(peers.refreshCandidates(candidates),
              std::vector<std::string>{"peer"});
    for (const auto& expected : candidates) {
        peers.requestAdvance("peer", initial);
        EXPECT_EQ(peers.prepare("peer", initial, candidates).selection.gid,
                  expected.gid);
    }
    peers.requestAdvance("peer", initial);
    auto exhausted = peers.prepare("peer", initial, candidates);
    EXPECT_EQ(exhausted.selection.gid, "gid-last");
    EXPECT_EQ(
        peers.recordCompletion("peer", {1, exhausted.selection_epoch}, false),
        AutoGidDataPathAction::kExhausted);
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
