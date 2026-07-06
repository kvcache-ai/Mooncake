#include "master_config.h"

#include <gtest/gtest.h>

namespace mooncake::test {

TEST(MasterServiceConfigTest, OplogBatchMaxEntriesDefaultsTo1024) {
    MasterConfig master_config;
    EXPECT_EQ(1024u, master_config.oplog_batch_max_entries);

    MasterServiceConfig service_config;
    EXPECT_EQ(1024u, service_config.oplog_batch_max_entries);
}

TEST(MasterServiceConfigTest, OplogBatchMaxEntriesBuilderOverrideRespected) {
    auto config =
        MasterServiceConfig::builder().set_oplog_batch_max_entries(17).build();

    EXPECT_EQ(17u, config.oplog_batch_max_entries);
}

}  // namespace mooncake::test
