#include "master_config.h"
#include "ha/snapshot/batch_oplog/config.h"

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

TEST(MasterServiceConfigTest, OplogSnapshotDefaultsAndOverridesPropagate) {
    MasterConfig master_config{};
    EXPECT_FALSE(master_config.enable_oplog_snapshot);
    EXPECT_EQ(1000000u, master_config.snapshot_chunk_object_count);

    master_config.enable_oplog_snapshot = true;
    master_config.snapshot_chunk_object_count = 17;
    MasterServiceSupervisorConfig supervisor_config(master_config);
    WrappedMasterServiceConfig wrapped_config(supervisor_config, 1);
    MasterServiceConfig service_config(wrapped_config);

    EXPECT_TRUE(service_config.enable_oplog_snapshot);
    EXPECT_EQ(17u, service_config.snapshot_chunk_object_count);
}

TEST(MasterServiceConfigTest, ValidatesOplogSnapshotConfiguration) {
    MasterConfig config{};
    EXPECT_FALSE(ValidateBatchOpLogSnapshotConfig(config).has_value());

    config.enable_oplog_snapshot = true;
    EXPECT_TRUE(ValidateBatchOpLogSnapshotConfig(config).has_value());
    config.enable_oplog = true;
    config.enable_ha = true;
    config.ha_backend_type = "etcd";
    config.cluster_id = "snapshot-config-test";
    config.snapshot_object_store_type = "local";
    EXPECT_FALSE(ValidateBatchOpLogSnapshotConfig(config).has_value());

    config.snapshot_chunk_object_count = 0;
    EXPECT_TRUE(ValidateBatchOpLogSnapshotConfig(config).has_value());
    config.snapshot_chunk_object_count = 1;
    config.snapshot_object_store_type.clear();
    EXPECT_TRUE(ValidateBatchOpLogSnapshotConfig(config).has_value());
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

TEST(MasterServiceConfigTest, OplogBatchMaxEntriesBuilderOverrideRespected) {
    auto config =
        MasterServiceConfig::builder().set_oplog_batch_max_entries(17).build();

    EXPECT_EQ(17u, config.oplog_batch_max_entries);
}

}  // namespace mooncake::test
