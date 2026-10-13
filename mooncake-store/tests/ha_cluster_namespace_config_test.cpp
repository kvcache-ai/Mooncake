#include <gtest/gtest.h>

#include <string>

#include "../src/config/ha_cluster_namespace_config.h"
#include "environ.h"

namespace mooncake {
namespace {

class HaClusterNamespaceConfigTest : public ::testing::Test {
   protected:
    HaClusterNamespaceConfig Load() const {
        return HaClusterNamespaceConfig::FromEnvironment(Environ(source_));
    }

    MapEnvironSource source_;
};

TEST_F(HaClusterNamespaceConfigTest, FallsBackWhenUnsetOrEmpty) {
    EXPECT_EQ(Load().cluster_namespace, "mooncake_cluster");

    source_.Set("MC_STORE_CLUSTER_ID", "");
    EXPECT_EQ(Load().cluster_namespace, "mooncake_cluster");
}

TEST_F(HaClusterNamespaceConfigTest, PreservesValueAndReadsEachConstruction) {
    source_.Set("MC_STORE_CLUSTER_ID", "  team a  ");
    const auto first = Load();
    EXPECT_EQ(first.cluster_namespace, "  team a  ");

    source_.Set("MC_STORE_CLUSTER_ID", "team b");
    EXPECT_EQ(Load().cluster_namespace, "team b");
    EXPECT_EQ(first.cluster_namespace, "  team a  ");
}

}  // namespace
}  // namespace mooncake
