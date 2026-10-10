#include "../src/config/master_metadata_config.h"

#include <gtest/gtest.h>

#include "environ.h"

namespace mooncake::test {
namespace {

class MasterMetadataConfigTest : public ::testing::Test {
   protected:
    MasterMetadataConfig Load() const {
        return MasterMetadataConfig::FromEnvironment(Environ(source_));
    }

    MapEnvironSource source_;
};

TEST_F(MasterMetadataConfigTest, UsesDefaultPrefixWhenUnsetOrEmpty) {
    const auto config = Load();
    EXPECT_TRUE(config.cluster_id.empty());
    EXPECT_EQ(config.HttpMetadataPrefix(), "mooncake/");

    source_.Set("MC_METADATA_CLUSTER_ID", "");
    EXPECT_EQ(Load().HttpMetadataPrefix(), "mooncake/");
}

TEST_F(MasterMetadataConfigTest, PreservesClusterIdAndTrailingSlash) {
    source_.Set("MC_METADATA_CLUSTER_ID", "team-a");
    EXPECT_EQ(Load().HttpMetadataPrefix(), "mooncake/team-a/");

    source_.Set("MC_METADATA_CLUSTER_ID", "team-a/");
    EXPECT_EQ(Load().HttpMetadataPrefix(), "mooncake/team-a/");
}

}  // namespace
}  // namespace mooncake::test
