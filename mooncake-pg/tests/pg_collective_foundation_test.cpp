#include <gtest/gtest.h>

#include "control_plane/control_types.h"
#include "control_plane/host_identity.h"

namespace mooncake {
namespace {

TEST(PgHostIdentityTest, OverrideTakesPrecedence) {
    EXPECT_EQ(makePGHostId("node-a", "boot-a", "container-a"),
              "override:node-a");
    EXPECT_EQ(makePGHostId("node-a", {}, {}), "override:node-a");
}

TEST(PgHostIdentityTest, DistinguishesBootsAndHostnames) {
    const auto id = makePGHostId({}, "boot-a", "container-a");
    EXPECT_EQ(id, "boot:boot-a:container-a");
    EXPECT_NE(id, makePGHostId({}, "boot-b", "container-a"));
    EXPECT_NE(id, makePGHostId({}, "boot-a", "container-b"));
}

TEST(PgHostIdentityTest, MissingIdentityIsEmpty) {
    EXPECT_TRUE(makePGHostId({}, {}, "container-a").empty());
    EXPECT_TRUE(makePGHostId({}, "boot-a", {}).empty());
}

TEST(PgLLPeerScopeTest, RequiresDirectAccessAndNativeAtomics) {
    for (const bool direct : {false, true}) {
        for (const bool atomics : {false, true}) {
            const DeviceTransferRouteInfo route{
                .reachable = true,
                .directly_addressable = direct,
                .native_atomics = atomics,
            };
            EXPECT_EQ(route.supportsDirectLl(), direct && atomics);
        }
    }
}

}  // namespace
}  // namespace mooncake
