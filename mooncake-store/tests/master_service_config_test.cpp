#include "master_config.h"

#include <limits>

#include <gtest/gtest.h>

namespace mooncake::test {

TEST(MasterServiceConfigTest, OplogBatchMaxEntriesDefaultsTo1024) {
    MasterConfig master_config;
    EXPECT_EQ(1024u, master_config.oplog_batch_max_entries);

    MasterServiceConfig service_config;
    EXPECT_EQ(1024u, service_config.oplog_batch_max_entries);
}

TEST(MasterServiceConfigTest, OplogIsDisabledByDefault) {
    MasterConfig master_config;
    EXPECT_FALSE(master_config.enable_oplog);

    MasterServiceConfig service_config;
    EXPECT_FALSE(service_config.enable_oplog);
}

TEST(MasterServiceConfigTest, OplogBuilderOverrideIsRespected) {
    auto config = MasterServiceConfig::builder().set_enable_oplog(true).build();

    EXPECT_TRUE(config.enable_oplog);
}

TEST(MasterServiceConfigTest, OplogEnablementPropagatesToServingConfig) {
    MasterConfig master_config{};
    master_config.enable_oplog = true;
    MasterServiceSupervisorConfig supervisor_config(master_config);

    WrappedMasterServiceConfig wrapped_config(supervisor_config, 1);
    MasterServiceConfig service_config(wrapped_config);

    EXPECT_TRUE(supervisor_config.enable_oplog);
    EXPECT_TRUE(wrapped_config.enable_oplog);
    EXPECT_TRUE(service_config.enable_oplog);
}

TEST(MasterServiceConfigTest, SizeClassAwareStrategyIsParsed) {
    MasterConfig master_config{};
    master_config.allocation_strategy = "size_class_aware";
    master_config.size_class_free_ratio_weight = 0.75;
    master_config.size_class_matching_share_weight = 0.25;

    MasterServiceSupervisorConfig supervisor_config(master_config);
    WrappedMasterServiceConfig wrapped_config(supervisor_config, 1);
    MasterServiceConfig service_config(wrapped_config);

    EXPECT_EQ(supervisor_config.allocation_strategy_type,
              AllocationStrategyType::SIZE_CLASS_AWARE);
    EXPECT_EQ(wrapped_config.allocation_strategy_type,
              AllocationStrategyType::SIZE_CLASS_AWARE);
    EXPECT_DOUBLE_EQ(supervisor_config.size_class_free_ratio_weight, 0.75);
    EXPECT_DOUBLE_EQ(supervisor_config.size_class_matching_share_weight, 0.25);
    EXPECT_DOUBLE_EQ(wrapped_config.size_class_free_ratio_weight, 0.75);
    EXPECT_DOUBLE_EQ(wrapped_config.size_class_matching_share_weight, 0.25);
    EXPECT_DOUBLE_EQ(service_config.size_class_free_ratio_weight, 0.75);
    EXPECT_DOUBLE_EQ(service_config.size_class_matching_share_weight, 0.25);
}

TEST(MasterServiceConfigTest, SizeClassWeightsBuilderOverrideRespected) {
    auto config =
        MasterServiceConfig::builder().set_size_class_weights(0.6, 0.4).build();

    EXPECT_DOUBLE_EQ(config.size_class_free_ratio_weight, 0.6);
    EXPECT_DOUBLE_EQ(config.size_class_matching_share_weight, 0.4);
}

TEST(MasterServiceConfigTest, RejectsInvalidSizeClassWeights) {
    MasterConfig master_config{};

    master_config.size_class_free_ratio_weight = 0.0;
    EXPECT_THROW((void)MasterServiceSupervisorConfig{master_config},
                 std::runtime_error);

    master_config.size_class_free_ratio_weight = 1.0;
    master_config.size_class_matching_share_weight = -0.1;
    EXPECT_THROW((void)MasterServiceSupervisorConfig{master_config},
                 std::runtime_error);

    master_config.size_class_matching_share_weight =
        std::numeric_limits<double>::infinity();
    EXPECT_THROW((void)MasterServiceSupervisorConfig{master_config},
                 std::runtime_error);
}

TEST(MasterServiceConfigTest, OplogBatchMaxEntriesBuilderOverrideRespected) {
    auto config =
        MasterServiceConfig::builder().set_oplog_batch_max_entries(17).build();

    EXPECT_EQ(17u, config.oplog_batch_max_entries);
}

}  // namespace mooncake::test
