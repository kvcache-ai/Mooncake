#include "storage/distributed/kvcs/kvcs_driver.h"
#include "storage/object_layout.h"

#include <limits>
#include <string>
#include <vector>

#include <gtest/gtest.h>

namespace mooncake {
namespace {

TEST(ObjectLayoutTest, SplitsAcrossSliceBoundaries) {
    std::string first = "abc";
    std::string second = "defgh";
    const std::vector<Slice> slices{{first.data(), first.size()},
                                    {second.data(), second.size()}};

    auto plan =
        PlanObjectShards(slices, {.max_value_size = 4, .inline_value_size = 4});
    ASSERT_TRUE(plan);
    EXPECT_EQ(plan->total_size, 8);
    EXPECT_EQ(plan->total_shard, 2);
    ASSERT_EQ(plan->shards.size(), 2);
    EXPECT_EQ(plan->shards[0].offset, 0);
    EXPECT_EQ(plan->shards[0].size, 4);
    EXPECT_EQ(plan->shards[1].offset, 4);
    EXPECT_EQ(plan->shards[1].size, 4);

    auto first_shard = SliceObjectShard(slices, plan->shards[0]);
    ASSERT_TRUE(first_shard);
    ASSERT_EQ(first_shard->size(), 2);
    EXPECT_EQ(std::string(static_cast<const char*>((*first_shard)[0].ptr),
                          (*first_shard)[0].size),
              "abc");
    EXPECT_EQ(std::string(static_cast<const char*>((*first_shard)[1].ptr),
                          (*first_shard)[1].size),
              "d");

    auto second_shard = SliceObjectShard(slices, plan->shards[1]);
    ASSERT_TRUE(second_shard);
    ASSERT_EQ(second_shard->size(), 1);
    EXPECT_EQ(std::string(static_cast<const char*>((*second_shard)[0].ptr),
                          (*second_shard)[0].size),
              "efgh");

    auto put = BuildKvcsShardPutRequests("key", slices, 4);
    ASSERT_TRUE(put);
    ASSERT_EQ(put->size(), 2u);
    EXPECT_EQ((*put)[0].total_shard, 2u);
    EXPECT_EQ((*put)[0].shard_id, 0u);
    EXPECT_EQ((*put)[1].shard_id, 1u);
}

TEST(ObjectLayoutTest, UsesOneShardForEmptyAndExactValues) {
    auto empty =
        PlanObjectShards(0, {.max_value_size = 4, .inline_value_size = 4});
    ASSERT_TRUE(empty);
    EXPECT_EQ(empty->total_shard, 1);
    ASSERT_EQ(empty->shards.size(), 1);
    EXPECT_EQ(empty->shards[0].size, 0);

    auto exact =
        PlanObjectShards(8, {.max_value_size = 4, .inline_value_size = 4});
    ASSERT_TRUE(exact);
    EXPECT_EQ(exact->total_shard, 2);
    EXPECT_EQ(exact->shards.back().size, 4);
}

TEST(ObjectLayoutTest, ConfiguresInlineThresholdSeparatelyFromShardSize) {
    auto inline_plan =
        PlanObjectShards(4, {.max_value_size = 8, .inline_value_size = 4});
    ASSERT_TRUE(inline_plan);
    EXPECT_TRUE(inline_plan->inline_value);

    auto chunked_plan =
        PlanObjectShards(5, {.max_value_size = 8, .inline_value_size = 4});
    ASSERT_TRUE(chunked_plan);
    EXPECT_FALSE(chunked_plan->inline_value);
    ASSERT_EQ(chunked_plan->shards.size(), 1u);
    EXPECT_EQ(chunked_plan->shards[0].size, 5u);
}

TEST(ObjectLayoutTest, KeepsLowLevelFourGiBValueOnFastPath) {
    constexpr uint64_t kFourGiB = 4ULL * 1024 * 1024 * 1024;

    auto exact = PlanObjectShards(
        kFourGiB, {.max_value_size = kFourGiB, .inline_value_size = kFourGiB});
    ASSERT_TRUE(exact);
    ASSERT_EQ(exact->shards.size(), 1u);
    EXPECT_EQ(exact->total_shard, 1u);
    EXPECT_EQ(exact->shards[0].offset, 0u);
    EXPECT_EQ(exact->shards[0].size, kFourGiB);

    auto split = PlanObjectShards(
        kFourGiB + 1,
        {.max_value_size = kFourGiB, .inline_value_size = kFourGiB});
    ASSERT_TRUE(split);
    ASSERT_EQ(split->shards.size(), 2u);
    EXPECT_EQ(split->shards[0].size, kFourGiB);
    EXPECT_EQ(split->shards[1].offset, kFourGiB);
    EXPECT_EQ(split->shards[1].size, 1u);
}

TEST(ObjectLayoutTest, RejectsInvalidInputs) {
    EXPECT_FALSE(
        PlanObjectShards(1, {.max_value_size = 0, .inline_value_size = 0}));
    EXPECT_FALSE(PlanObjectShards(
        std::numeric_limits<uint64_t>::max(),
        {.max_value_size = std::numeric_limits<uint32_t>::max(),
         .inline_value_size = std::numeric_limits<uint32_t>::max()}));

    const std::vector<Slice> invalid{{nullptr, 1}};
    EXPECT_FALSE(PlanObjectShards(
        invalid, {.max_value_size = 4, .inline_value_size = 4}));

    const auto valid =
        PlanObjectShards(4, {.max_value_size = 4, .inline_value_size = 4});
    ASSERT_TRUE(valid);
    ObjectShard invalid_shard = valid->shards[0];
    invalid_shard.shard_id = invalid_shard.total_shard;
    EXPECT_FALSE(SliceObjectShard(std::span<const Slice>{}, invalid_shard));
}

}  // namespace
}  // namespace mooncake
