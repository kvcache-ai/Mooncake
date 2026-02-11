#include "allocation_strategy.h"

#include <gtest/gtest.h>
#include <set>
#include <string>
#include <vector>

#include "types.h"

namespace mooncake {

class LocalityScoringTest : public ::testing::Test {
   protected:
    UUID node_a_{1, 0};
    UUID node_b_{2, 0};
    UUID node_c_{3, 0};
};

TEST_F(LocalityScoringTest, ScoreSegmentLocalitySameNodePreferred) {
    SegmentLocalityInfo same_node{"seg2", node_a_, 0.5};
    SegmentLocalityInfo diff_node{"seg3", node_b_, 0.0};

    double score_same = ScoreSegmentLocality("seg1", node_a_, same_node);
    double score_diff = ScoreSegmentLocality("seg1", node_a_, diff_node);

    EXPECT_GT(score_same, score_diff);
    EXPECT_EQ(score_same, 1000.0 - 5.0);
    EXPECT_EQ(score_diff, 100.0);
}

TEST_F(LocalityScoringTest, ScoreSegmentLocalityCurrentSegmentExcluded) {
    SegmentLocalityInfo current{"seg1", node_a_, 0.2};
    double score = ScoreSegmentLocality("seg1", node_a_, current);
    EXPECT_LT(score, -1e8);
}

TEST_F(LocalityScoringTest, ScoreSegmentLocalityLowerUtilizationPreferred) {
    SegmentLocalityInfo low_util{"seg2", node_b_, 0.1};
    SegmentLocalityInfo high_util{"seg3", node_b_, 0.9};

    double score_low = ScoreSegmentLocality("seg1", node_a_, low_util);
    double score_high = ScoreSegmentLocality("seg1", node_a_, high_util);
    EXPECT_GT(score_low, score_high);
}

TEST_F(LocalityScoringTest, SelectBestTargetSegmentPicksSameNode) {
    std::vector<SegmentLocalityInfo> infos = {
        {"seg1", node_a_, 0.5},
        {"seg2", node_a_, 0.3},
        {"seg3", node_b_, 0.0},
    };
    auto best = SelectBestTargetSegment("seg0", node_a_, infos);
    ASSERT_TRUE(best.has_value());
    EXPECT_EQ(*best, "seg2");
}

TEST_F(LocalityScoringTest, SelectBestTargetSegmentExcludesCurrent) {
    std::vector<SegmentLocalityInfo> infos = {
        {"seg1", node_a_, 0.5},
        {"seg2", node_a_, 0.3},
    };
    auto best = SelectBestTargetSegment("seg1", node_a_, infos);
    ASSERT_TRUE(best.has_value());
    EXPECT_EQ(*best, "seg2");
}

TEST_F(LocalityScoringTest, SelectBestTargetSegmentRespectsExcluded) {
    std::vector<SegmentLocalityInfo> infos = {
        {"seg1", node_a_, 0.5},
        {"seg2", node_a_, 0.3},
    };
    std::set<std::string> excluded = {"seg2"};
    auto best = SelectBestTargetSegment("seg0", node_a_, infos, excluded);
    ASSERT_TRUE(best.has_value());
    EXPECT_EQ(*best, "seg1");
}

TEST_F(LocalityScoringTest, SelectBestTargetSegmentSingleCandidateExcluded) {
    // Only candidate is current segment (seg1); should return no target.
    std::vector<SegmentLocalityInfo> infos = {
        {"seg1", node_a_, 0.5},
    };
    auto best = SelectBestTargetSegment("seg1", node_a_, infos);
    EXPECT_FALSE(best.has_value())
        << "Current segment must not be selected as target";
}

TEST_F(LocalityScoringTest, PlanLocalityRebalanceSuggestsMoves) {
    std::vector<ReplicaPlacement> placements = {
        {"seg1", node_a_},
        {"seg2", node_b_},
    };
    std::vector<SegmentLocalityInfo> segment_infos = {
        {"seg1", node_a_, 0.9},
        {"seg2", node_b_, 0.9},
        {"seg3", node_a_, 0.1},
        {"seg4", node_b_, 0.1},
    };

    auto moves = PlanLocalityRebalance(placements, segment_infos);
    EXPECT_GE(moves.size(), 1u);
    bool has_move_from_seg1 = false;
    bool has_move_from_seg2 = false;
    for (const auto& m : moves) {
        if (m.src_segment == "seg1") {
            has_move_from_seg1 = true;
            EXPECT_TRUE(m.target_segment == "seg3");
        }
        if (m.src_segment == "seg2") {
            has_move_from_seg2 = true;
            EXPECT_TRUE(m.target_segment == "seg4");
        }
    }
    EXPECT_TRUE(has_move_from_seg1);
    EXPECT_TRUE(has_move_from_seg2);
}

TEST_F(LocalityScoringTest, PlanLocalityRebalanceNoDuplicateMoves) {
    std::vector<ReplicaPlacement> placements = {
        {"seg1", node_a_},
        {"seg1", node_a_},
    };
    std::vector<SegmentLocalityInfo> segment_infos = {
        {"seg1", node_a_, 0.9},
        {"seg2", node_a_, 0.1},
    };
    auto moves = PlanLocalityRebalance(placements, segment_infos);
    EXPECT_EQ(moves.size(), 1u);
    EXPECT_EQ(moves[0].src_segment, "seg1");
    EXPECT_EQ(moves[0].target_segment, "seg2");
}

TEST_F(LocalityScoringTest, PlanLocalityRebalanceEmptyInput) {
    std::vector<RebalanceMove> moves = PlanLocalityRebalance({}, {});
    EXPECT_TRUE(moves.empty());
}

}  // namespace mooncake
