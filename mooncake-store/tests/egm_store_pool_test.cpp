// Copyright 2024 KVCache.AI

#include <gtest/gtest.h>

#include <limits>
#include <string>
#include <vector>

#include "../src/config/egm_store_pool_config.h"

namespace mooncake {
namespace {

TEST(EgmStorePoolConfigTest, DefaultOffIgnoresDependentSettings) {
    const ConfigDict config{{CONFIG_KEY_EGM_NUMA_NODES, "invalid"}};
    const auto parsed = ParseEgmStorePoolConfig(config, "tcp", 0, 1);
    ASSERT_TRUE(parsed);
    EXPECT_FALSE(parsed->enabled);
    EXPECT_TRUE(parsed->auto_numa_nodes);
    EXPECT_TRUE(parsed->numa_nodes.empty());
}

TEST(EgmStorePoolConfigTest, AcceptsNarrowCommonBooleanSyntax) {
    for (const auto* value : {"true", " \tTrUe\r\n", "1"}) {
        SCOPED_TRACE(value);
        const ConfigDict config{{CONFIG_KEY_ENABLE_EGM_STORE_POOL, value}};
        const auto parsed = ParseEgmStorePoolConfig(config, "nvlink", 1, 0);
        ASSERT_TRUE(parsed);
        EXPECT_TRUE(parsed->enabled);
    }
    for (const auto* value : {"false", " \tFaLsE\r\n", "0"}) {
        SCOPED_TRACE(value);
        const ConfigDict config{{CONFIG_KEY_ENABLE_EGM_STORE_POOL, value},
                                {CONFIG_KEY_EGM_NUMA_NODES, "invalid"}};
        const auto parsed = ParseEgmStorePoolConfig(config, "tcp", 0, 1);
        ASSERT_TRUE(parsed);
        EXPECT_FALSE(parsed->enabled);
    }
}

TEST(EgmStorePoolConfigTest, RejectsOtherBooleanValues) {
    for (const auto* value : {"", " ", "2", "yes", "no", "on", "off", "enable",
                              "disable", "truex"}) {
        SCOPED_TRACE(value);
        const ConfigDict config{{CONFIG_KEY_ENABLE_EGM_STORE_POOL, value}};
        const auto parsed = ParseEgmStorePoolConfig(config, "nvlink", 1, 0);
        ASSERT_FALSE(parsed);
        EXPECT_EQ(parsed.error(), ErrorCode::INVALID_PARAMS);
    }
}

TEST(EgmStorePoolConfigTest, AutoNodesAreDefaultAndCaseSensitive) {
    ConfigDict config{{CONFIG_KEY_ENABLE_EGM_STORE_POOL, "true"}};
    auto parsed = ParseEgmStorePoolConfig(config, "nvlink", 1, 0);
    ASSERT_TRUE(parsed);
    EXPECT_TRUE(parsed->auto_numa_nodes);
    EXPECT_TRUE(parsed->numa_nodes.empty());

    config[CONFIG_KEY_EGM_NUMA_NODES] = " \tauto\r\n";
    parsed = ParseEgmStorePoolConfig(config, "nvlink", 1, 0);
    ASSERT_TRUE(parsed);
    EXPECT_TRUE(parsed->auto_numa_nodes);
    EXPECT_TRUE(parsed->numa_nodes.empty());

    config[CONFIG_KEY_EGM_NUMA_NODES] = "AUTO";
    EXPECT_FALSE(ParseEgmStorePoolConfig(config, "nvlink", 1, 0));
}

TEST(EgmStorePoolConfigTest, ExplicitNodesAreTrimmedSortedAndDeduplicated) {
    const ConfigDict config{
        {CONFIG_KEY_ENABLE_EGM_STORE_POOL, "true"},
        {CONFIG_KEY_EGM_NUMA_NODES, " \t3, 1,3,-0,2147483647\r\n"}};
    const auto parsed = ParseEgmStorePoolConfig(config, "nvlink", 1, 0);
    ASSERT_TRUE(parsed);
    EXPECT_FALSE(parsed->auto_numa_nodes);
    EXPECT_EQ(parsed->numa_nodes,
              (std::vector<int>{0, 1, 3, std::numeric_limits<int>::max()}));
}

TEST(EgmStorePoolConfigTest, RejectsInvalidNodeLists) {
    for (const auto* value : {"", " ", "1,,3", "1,", ",1", "-1", "+1", "1x",
                              "1 2", "2147483648", "4294967296"}) {
        SCOPED_TRACE(value);
        const ConfigDict config{{CONFIG_KEY_ENABLE_EGM_STORE_POOL, "true"},
                                {CONFIG_KEY_EGM_NUMA_NODES, value}};
        const auto parsed = ParseEgmStorePoolConfig(config, "nvlink", 1, 0);
        ASSERT_FALSE(parsed);
        EXPECT_EQ(parsed.error(), ErrorCode::INVALID_PARAMS);
    }
}

TEST(EgmStorePoolConfigTest, EnabledRequiresNvlinkCapacityAndNoLocalBuffer) {
    const ConfigDict config{{CONFIG_KEY_ENABLE_EGM_STORE_POOL, "true"}};
    EXPECT_TRUE(ParseEgmStorePoolConfig(config, "nvlink", 1, 0));
    EXPECT_FALSE(ParseEgmStorePoolConfig(config, "rdma", 1, 0));
    EXPECT_FALSE(ParseEgmStorePoolConfig(config, "nvlink", 0, 0));
    EXPECT_FALSE(ParseEgmStorePoolConfig(config, "nvlink", 1, 1));
}

TEST(EgmStorePoolCapacityTest, RoundsDownAndSplitsAlignedUnitsInInputOrder) {
    const auto capacity = CalculateEgmStorePoolCapacity(57, {4, 8}, 4, 20);
    ASSERT_TRUE(capacity);
    EXPECT_EQ(capacity->alignment, 8);
    EXPECT_EQ(capacity->max_chunk_size, 16);
    EXPECT_EQ(capacity->node_sizes, (std::vector<size_t>{32, 24}));
}

TEST(EgmStorePoolCapacityTest, UsesLargestPowerOfTwoGranularity) {
    const auto capacity = CalculateEgmStorePoolCapacity(143, {4, 16, 8}, 8, 63);
    ASSERT_TRUE(capacity);
    EXPECT_EQ(capacity->alignment, 16);
    EXPECT_EQ(capacity->max_chunk_size, 48);
    EXPECT_EQ(capacity->node_sizes, (std::vector<size_t>{48, 48, 32}));
}

TEST(EgmStorePoolCapacityTest, IncludesStoreSlabAlignment) {
    const auto capacity = CalculateEgmStorePoolCapacity(17, {2, 4}, 8, 16);
    ASSERT_TRUE(capacity);
    EXPECT_EQ(capacity->alignment, 8);
    EXPECT_EQ(capacity->node_sizes, (std::vector<size_t>{8, 8}));
}

TEST(EgmStorePoolCapacityTest, RejectsMissingNodesOrInvalidAlignment) {
    EXPECT_FALSE(CalculateEgmStorePoolCapacity(64, {}, 4, 64));
    for (const size_t invalid : {0, 3, 12}) {
        SCOPED_TRACE(invalid);
        EXPECT_FALSE(CalculateEgmStorePoolCapacity(64, {4}, invalid, 64));
        const auto capacity =
            CalculateEgmStorePoolCapacity(64, {4, invalid}, 4, 64);
        ASSERT_FALSE(capacity);
        EXPECT_EQ(capacity.error(), ErrorCode::INVALID_PARAMS);
    }
}

TEST(EgmStorePoolCapacityTest, RequiresOneUnitPerNodeAndAnAlignedMrCap) {
    for (const size_t requested : {0, 7, 15}) {
        SCOPED_TRACE(requested);
        EXPECT_FALSE(CalculateEgmStorePoolCapacity(requested, {8, 4}, 4, 64));
    }
    EXPECT_FALSE(CalculateEgmStorePoolCapacity(64, {4, 8}, 4, 0));
    EXPECT_FALSE(CalculateEgmStorePoolCapacity(64, {4, 8}, 4, 7));
    EXPECT_TRUE(CalculateEgmStorePoolCapacity(16, {4, 8}, 4, 8));
}

TEST(EgmStorePoolCapacityTest, MaximumSizeRoundsDownWithoutOverflow) {
    const size_t maximum = std::numeric_limits<size_t>::max();
    const auto capacity =
        CalculateEgmStorePoolCapacity(maximum, {8, 4}, 4, maximum);
    ASSERT_TRUE(capacity);
    ASSERT_EQ(capacity->node_sizes.size(), 2);
    EXPECT_EQ(capacity->max_chunk_size, maximum - maximum % 8);
    EXPECT_EQ(capacity->node_sizes[0] + capacity->node_sizes[1],
              maximum - maximum % 8);
    EXPECT_EQ(capacity->node_sizes[0] - capacity->node_sizes[1], 8);
}

}  // namespace
}  // namespace mooncake
