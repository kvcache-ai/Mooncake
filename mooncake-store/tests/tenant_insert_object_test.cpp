#include "tenant/tenant_store.h"

#include <memory>
#include <string>

#include <gtest/gtest.h>

namespace mooncake {
namespace tenant {
namespace {

TEST(TenantInsertObjectTest, InsertObjectWiresSharedLeaseAndJoinsGroup) {
    TenantStore store;

    // A grouped object: InsertObject should wire the group's shared Lease into
    // the entry's lease slot AND register it as a group member.
    auto member = std::make_shared<ObjectEntry>("k1", "g1");
    EXPECT_TRUE(store.InsertObject("k1", member));

    EXPECT_EQ(store.ObjectCount(), 1u);
    EXPECT_EQ(store.GroupCount(), 1u);
    EXPECT_TRUE(store.HasGroup("g1"));
    EXPECT_EQ(store.Members("g1").size(), 1u);
    ASSERT_NE(member->lease(), nullptr);  // shared lease wired
    EXPECT_EQ(member->lease().get(),
              store.LeaseFor("g1").get());  // same single shared lease
}

TEST(TenantInsertObjectTest, InsertObjectDoesNotJoinForSingleton) {
    TenantStore store;

    auto singleton = std::make_shared<ObjectEntry>("k1", "");
    EXPECT_TRUE(store.InsertObject("k1", singleton));

    EXPECT_EQ(store.ObjectCount(), 1u);
    EXPECT_EQ(store.GroupCount(), 0u);   // singleton adds no group
    EXPECT_EQ(store.Members("g1").size(), 0u);
    EXPECT_EQ(singleton->lease(), nullptr);  // own lease wired by caller, not here
}

TEST(TenantInsertObjectTest, InsertObjectRejectsDuplicateKey) {
    TenantStore store;
    store.InsertObject("k1", std::make_shared<ObjectEntry>("k1", "g1"));
    // Second insert for the same key is rejected; the original is intact.
    EXPECT_FALSE(store.InsertObject("k1", std::make_shared<ObjectEntry>("k1", "g2")));
    EXPECT_EQ(store.ObjectCount(), 1u);
    EXPECT_EQ(store.HasGroup("g1"), true);
    EXPECT_EQ(store.HasGroup("g2"), false);
}

}  // namespace
}  // namespace tenant
}  // namespace mooncake
