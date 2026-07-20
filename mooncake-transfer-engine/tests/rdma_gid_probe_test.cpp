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

#include <vector>

#include "transport/rdma_transport/rdma_gid_probe.h"

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

}  // namespace
