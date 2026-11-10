#include <gtest/gtest.h>

#include <algorithm>
#include <cmath>
#include <limits>
#include <string>
#include <vector>

#include "weight_residency_planner.h"

namespace mooncake::test {
namespace {

TEST(WeightResidencyPlannerTest, ReplacesOneLargeAffinityWithTwoSmallOnes) {
    auto plan =
        PlanMixedWeightResidency({{.affinity_id = "a", .logical_bytes = 3},
                                  {.affinity_id = "b", .logical_bytes = 1},
                                  {.affinity_id = "c", .logical_bytes = 1},
                                  {.affinity_id = "d", .logical_bytes = 1}},
                                 0.4);
    ASSERT_TRUE(plan.has_value());
    EXPECT_EQ(2, plan->hot_logical_bytes);
}

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

TEST(WeightResidencyPlannerTest, ExactSearchMatchesExhaustiveOracle) {
    for (uint64_t seed = 1; seed <= 40; ++seed) {
        std::vector<WeightAffinityUnit> units;
        uint64_t total = 0;
        for (size_t i = 0; i < 9; ++i) {
            const uint64_t bytes = 1 + (seed * (i + 3) * (i + 7)) % 97;
            units.push_back({std::to_string(i), bytes});
            total += bytes;
        }
        const double ratio = static_cast<double>(seed) / 41;
        const long double target = static_cast<long double>(total) * ratio;
        long double optimum = std::numeric_limits<long double>::infinity();
        for (size_t mask = 1; mask + 1 < (size_t{1} << units.size()); ++mask) {
            uint64_t bytes = 0;
            for (size_t i = 0; i < units.size(); ++i) {
                if (mask & (size_t{1} << i)) bytes += units[i].logical_bytes;
            }
            optimum = std::min(
                optimum, std::fabs(static_cast<long double>(bytes) - target));
        }
        const auto plan = PlanMixedWeightResidency(units, ratio);
        ASSERT_TRUE(plan.has_value());
        EXPECT_EQ(optimum,
                  std::fabs(static_cast<long double>(plan->hot_logical_bytes) -
                            target));
    }
}

TEST(WeightResidencyPlannerTest, LargeSearchHasDeterministicGranularityBound) {
    std::vector<WeightAffinityUnit> units;
    uint64_t total = 0;
    for (size_t i = 0; i < 10000; ++i) {
        const uint64_t bytes = 1 + i % 101;
        units.push_back({std::to_string(i), bytes});
        total += bytes;
    }
    for (const double ratio : {0.0000001, 0.1, 0.4, 0.5, 0.9, 0.9999999}) {
        const auto plan = PlanMixedWeightResidency(units, ratio);
        ASSERT_TRUE(plan.has_value());
        const long double target = static_cast<long double>(total) * ratio;
        const auto bound =
            std::max({50.5L, 1.0L - target, target - (total - 1)});
        EXPECT_LE(std::fabs(static_cast<long double>(plan->hot_logical_bytes) -
                            target),
                  bound);
        EXPECT_GT(plan->hot_logical_bytes, 0);
        EXPECT_LT(plan->hot_logical_bytes, total);
        std::reverse(units.begin(), units.end());
        EXPECT_EQ(plan, PlanMixedWeightResidency(units, ratio));
    }
}

TEST(WeightResidencyPlannerTest, ExactLimitHandlesLargeByteMagnitudes) {
    std::vector<WeightAffinityUnit> units;
    const auto size = std::numeric_limits<uint64_t>::max() / 32;
    for (size_t i = 0; i < 32; ++i) {
        units.push_back({std::to_string(i), size});
    }
    const auto plan = PlanMixedWeightResidency(units, 0.5);
    ASSERT_TRUE(plan.has_value());
    EXPECT_EQ(size * 16, plan->hot_logical_bytes);
    units.push_back({"overflow", 32});
    EXPECT_FALSE(PlanMixedWeightResidency(units, 0.5).has_value());
}

TEST(WeightResidencyPlannerTest, LargeSearchPreservesNonemptyEndpoints) {
    std::vector<WeightAffinityUnit> units;
    const auto size = std::numeric_limits<uint64_t>::max() / 33;
    for (size_t i = 0; i < 33; ++i) {
        units.push_back({std::to_string(i), size});
    }
    const auto low = PlanMixedWeightResidency(units, 0.000001);
    const auto high = PlanMixedWeightResidency(units, 0.999999);
    ASSERT_TRUE(low.has_value());
    ASSERT_TRUE(high.has_value());
    EXPECT_EQ(size, low->hot_logical_bytes);
    EXPECT_EQ(size * 32, high->hot_logical_bytes);
    EXPECT_EQ(1, low->hot_affinity_ids.size());
    EXPECT_EQ(32, high->hot_affinity_ids.size());
}

TEST(WeightResidencyPlannerTest, RejectsInvalidInputs) {
    const std::vector<WeightAffinityUnit> units{{"a", 1}, {"b", 2}};
    for (const double ratio :
         {0.0, 1.0, -1.0, std::numeric_limits<double>::infinity(),
          std::numeric_limits<double>::quiet_NaN()}) {
        EXPECT_FALSE(PlanMixedWeightResidency(units, ratio).has_value());
    }
    EXPECT_FALSE(
        PlanMixedWeightResidency({{"a", 1}, {"a", 2}}, 0.5).has_value());
    EXPECT_FALSE(
        PlanMixedWeightResidency({{"a", 0}, {"b", 2}}, 0.5).has_value());
    EXPECT_FALSE(
        PlanMixedWeightResidency({{"", 1}, {"b", 2}}, 0.5).has_value());
}

}  // namespace
}  // namespace mooncake::test
