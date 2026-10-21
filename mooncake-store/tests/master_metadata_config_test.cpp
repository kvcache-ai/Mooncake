#include "../src/config/master_metadata_config.h"

#include <gtest/gtest.h>

#include <cstdlib>
#include <optional>
#include <string>

namespace mooncake::test {
namespace {

class MasterMetadataConfigTest : public ::testing::Test {
   protected:
    void SetUp() override {
        if (const char* value = std::getenv("MC_METADATA_CLUSTER_ID")) {
            original_ = value;
        }
        ASSERT_EQ(unsetenv("MC_METADATA_CLUSTER_ID"), 0);
    }

    void TearDown() override {
        if (original_.has_value()) {
            EXPECT_EQ(setenv("MC_METADATA_CLUSTER_ID", original_->c_str(), 1),
                      0);
        } else {
            EXPECT_EQ(unsetenv("MC_METADATA_CLUSTER_ID"), 0);
        }
    }

    std::optional<std::string> original_;
};

TEST_F(MasterMetadataConfigTest, UsesDefaultPrefixWhenUnsetOrEmpty) {
    const auto config = MasterMetadataConfig::FromEnvironment();
    EXPECT_TRUE(config.cluster_id.empty());
    EXPECT_EQ(config.HttpMetadataPrefix(), "mooncake/");

    ASSERT_EQ(setenv("MC_METADATA_CLUSTER_ID", "", 1), 0);
    EXPECT_EQ(MasterMetadataConfig::FromEnvironment().HttpMetadataPrefix(),
              "mooncake/");
}

TEST_F(MasterMetadataConfigTest, PreservesClusterIdAndTrailingSlash) {
    ASSERT_EQ(setenv("MC_METADATA_CLUSTER_ID", "team-a", 1), 0);
    EXPECT_EQ(MasterMetadataConfig::FromEnvironment().HttpMetadataPrefix(),
              "mooncake/team-a/");

    ASSERT_EQ(setenv("MC_METADATA_CLUSTER_ID", "team-a/", 1), 0);
    EXPECT_EQ(MasterMetadataConfig::FromEnvironment().HttpMetadataPrefix(),
              "mooncake/team-a/");
}

}  // namespace
}  // namespace mooncake::test
