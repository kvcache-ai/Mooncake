#include "storage/distributed/kvcs/kvcs_efc_topology.h"

#include <gtest/gtest.h>

namespace mooncake {
namespace {

TEST(KvcsEfcTopologyTest, LocalBackendIsOneEfcOwnedRoute) {
    auto routes = ResolveKvcsEfcTopology({.backend = "disk"});
    ASSERT_TRUE(routes);
    ASSERT_EQ(routes->size(), 1);
    EXPECT_EQ((*routes)[0].kind, KvcsEfcRouteKind::kLocal);
    EXPECT_EQ((*routes)[0].mountpoint_index, 0);
}

TEST(KvcsEfcTopologyTest, MixedPoolExposesLocalAndRemoteRoutes) {
    KvcsEfcDeployment deployment{
        .backend = "disk",
        .extra_backends = {"kvcachestore"},
        .mountpoints = {{.id = "remote-a", .index = 1}},
    };
    auto routes = ResolveKvcsEfcTopology(deployment);
    ASSERT_TRUE(routes);
    ASSERT_EQ(routes->size(), 2);
    EXPECT_EQ((*routes)[0].id, "local");
    EXPECT_EQ((*routes)[0].kind, KvcsEfcRouteKind::kLocal);
    EXPECT_EQ((*routes)[0].mountpoint_index, 0);
    EXPECT_EQ((*routes)[1].id, "remote-a");
    EXPECT_EQ((*routes)[1].kind, KvcsEfcRouteKind::kKvCacheStore);
    EXPECT_EQ((*routes)[1].mountpoint_index, 1);
}

TEST(KvcsEfcTopologyTest, DirectKvCacheStoreExposesEveryMountpoint) {
    KvcsEfcDeployment deployment{
        .backend = "kvcachestore",
        .mountpoints = {{.id = "remote-a", .index = 1, .is_default = true},
                        {.id = "remote-b", .index = 2}},
        .require_single_default = true,
    };
    auto routes = ResolveKvcsEfcTopology(deployment);
    ASSERT_TRUE(routes);
    ASSERT_EQ(routes->size(), 2);
    EXPECT_EQ((*routes)[0].kind, KvcsEfcRouteKind::kKvCacheStore);
    EXPECT_EQ((*routes)[0].mountpoint_index, 1);
    EXPECT_EQ((*routes)[1].mountpoint_index, 2);
}

TEST(KvcsEfcTopologyTest, DisklessWithoutKvCacheStoreIsRejected) {
    auto routes = ResolveKvcsEfcTopology({.backend = "diskless"});
    ASSERT_FALSE(routes);
    EXPECT_EQ(routes.error(), ErrorCode::INVALID_PARAMS);
}

TEST(KvcsEfcTopologyTest, DirectMountpointsMustBeUnique) {
    KvcsEfcDeployment deployment{
        .backend = "kvcachestore",
        .mountpoints = {{.id = "remote-a", .index = 1, .is_default = true},
                        {.id = "remote-a", .index = 2}},
        .require_single_default = true,
    };
    EXPECT_FALSE(ResolveKvcsEfcTopology(deployment));
}

}  // namespace
}  // namespace mooncake
