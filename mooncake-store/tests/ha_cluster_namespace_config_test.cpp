#include <gtest/gtest.h>

#include <cstdlib>
#include <optional>
#include <string>

#include "../master/config/ha_cluster_namespace_config.h"

namespace mooncake {
namespace {

class HaClusterNamespaceConfigTest : public ::testing::Test {
   protected:
    void SetUp() override {
        if (const char* original = std::getenv("MC_STORE_CLUSTER_ID")) {
            original_ = original;
        }
        ASSERT_EQ(unsetenv("MC_STORE_CLUSTER_ID"), 0);
    }

    void TearDown() override {
        if (original_) {
            EXPECT_EQ(setenv("MC_STORE_CLUSTER_ID", original_->c_str(), 1), 0);
        } else {
            EXPECT_EQ(unsetenv("MC_STORE_CLUSTER_ID"), 0);
        }
    }

   private:
    std::optional<std::string> original_;
};

TEST_F(HaClusterNamespaceConfigTest, FallsBackWhenUnsetOrEmpty) {
    EXPECT_EQ(HaClusterNamespaceConfig::FromEnvironment().cluster_namespace,
              "mooncake_cluster");

    ASSERT_EQ(setenv("MC_STORE_CLUSTER_ID", "", 1), 0);
    EXPECT_EQ(HaClusterNamespaceConfig::FromEnvironment().cluster_namespace,
              "mooncake_cluster");
}

TEST_F(HaClusterNamespaceConfigTest, PreservesValueAndReadsEachConstruction) {
    ASSERT_EQ(setenv("MC_STORE_CLUSTER_ID", "  team a  ", 1), 0);
    const auto first = HaClusterNamespaceConfig::FromEnvironment();
    EXPECT_EQ(first.cluster_namespace, "  team a  ");

    ASSERT_EQ(setenv("MC_STORE_CLUSTER_ID", "team b", 1), 0);
    EXPECT_EQ(HaClusterNamespaceConfig::FromEnvironment().cluster_namespace,
              "team b");
    EXPECT_EQ(first.cluster_namespace, "  team a  ");
}

}  // namespace
}  // namespace mooncake
