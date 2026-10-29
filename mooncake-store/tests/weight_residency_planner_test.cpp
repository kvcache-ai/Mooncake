#include <gtest/gtest.h>

#include <string>
#include <vector>

#include "weight_residency_planner.h"

namespace mooncake::test {
namespace {

TEST(WeightResidencyPlannerTest, PicksCompleteAffinitiesClosestToMixedTarget) {
    const std::vector<WeightAffinityUnit> units{
        {.affinity_id = "a", .logical_bytes = 500},
        {.affinity_id = "b", .logical_bytes = 400},
        {.affinity_id = "c", .logical_bytes = 100},
    };

    auto plan = PlanMixedWeightResidency(units, 0.5);

    ASSERT_TRUE(plan.has_value());
    EXPECT_EQ(500, plan->hot_logical_bytes);
    EXPECT_EQ(std::vector<std::string>{"a"}, plan->hot_affinity_ids);
}

TEST(WeightResidencyPlannerTest, IsDeterministicAcrossInputOrder) {
    const std::vector<WeightAffinityUnit> first{
        {.affinity_id = "a", .logical_bytes = 600},
        {.affinity_id = "b", .logical_bytes = 400},
        {.affinity_id = "c", .logical_bytes = 200},
    };
    const std::vector<WeightAffinityUnit> second{first.rbegin(), first.rend()};

    auto first_plan = PlanMixedWeightResidency(first, 0.5);
    auto second_plan = PlanMixedWeightResidency(second, 0.5);

    ASSERT_TRUE(first_plan.has_value());
    ASSERT_TRUE(second_plan.has_value());
    EXPECT_EQ(*first_plan, *second_plan);
}

TEST(WeightResidencyPlannerTest, ImprovesBySwappingWholeAffinities) {
    const std::vector<WeightAffinityUnit> units{
        {.affinity_id = "two", .logical_bytes = 2},
        {.affinity_id = "three", .logical_bytes = 3},
        {.affinity_id = "four", .logical_bytes = 4},
    };

    auto plan = PlanMixedWeightResidency(units, 0.6);

    ASSERT_TRUE(plan.has_value());
    EXPECT_EQ(5, plan->hot_logical_bytes);
    EXPECT_EQ((std::vector<std::string>{"three", "two"}),
              plan->hot_affinity_ids);
}

TEST(WeightResidencyPlannerTest, RejectsMixedForSingleAffinity) {
    auto plan = PlanMixedWeightResidency(
        {{.affinity_id = "only", .logical_bytes = 1024}}, 0.5);

    ASSERT_FALSE(plan.has_value());
    EXPECT_EQ(WeightManagementError::POLICY_UNSATISFIABLE, plan.error());
}

}  // namespace
}  // namespace mooncake::test
