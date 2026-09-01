#include "tenant/tenant_store.h"

#include <memory>
#include <string>
#include <vector>

#include <gtest/gtest.h>

namespace mooncake {
namespace tenant {
namespace {

TEST(TenantRouteTest, InsertPinEraseContainsObjectCount) {
    TenantStore store;
    EXPECT_EQ(store.ObjectCount(), 0u);

    auto e1 = std::make_shared<ObjectEntry>("k1", "");
    EXPECT_TRUE(store.Insert("k1", e1));
    EXPECT_TRUE(store.Contains("k1"));
    EXPECT_EQ(store.ObjectCount(), 1u);

    auto pinned = store.Pin("k1");
    ASSERT_NE(pinned, nullptr);
    EXPECT_EQ(pinned->key(), "k1");
    EXPECT_EQ(pinned.get(), e1.get());  // same underlying entry

    EXPECT_EQ(store.Pin("missing"), nullptr);
    EXPECT_TRUE(store.Erase("k1"));
    EXPECT_FALSE(store.Contains("k1"));
    EXPECT_EQ(store.ObjectCount(), 0u);
}

TEST(TenantRouteTest, DuplicateInsertIsRejected) {
    TenantStore store;
    store.Insert("k1", std::make_shared<ObjectEntry>("k1", ""));
    // Second insert for the same key must not clobber the original.
    EXPECT_FALSE(store.Insert("k1", std::make_shared<ObjectEntry>("k1", "")));
    EXPECT_EQ(store.ObjectCount(), 1u);
    ASSERT_NE(store.Pin("k1"), nullptr);
    EXPECT_EQ(store.Pin("k1")->key(), "k1");
}

TEST(TenantRouteTest, VisitObjectsEnumeratesEveryEntry) {
    TenantStore store;
    store.Insert("k1", std::make_shared<ObjectEntry>("k1", ""));
    store.Insert("k2", std::make_shared<ObjectEntry>("k2", "g1"));
    store.Insert("k3", std::make_shared<ObjectEntry>("k3", "g1"));

    std::vector<std::string> keys;
    store.VisitObjects([&](const std::shared_ptr<ObjectEntry>& entry) {
        keys.push_back(entry->key());
    });
    EXPECT_EQ(keys.size(), 3u);
    EXPECT_TRUE(std::find(keys.begin(), keys.end(), "k1") != keys.end());
    EXPECT_TRUE(std::find(keys.begin(), keys.end(), "k2") != keys.end());
    EXPECT_TRUE(std::find(keys.begin(), keys.end(), "k3") != keys.end());
}

TEST(TenantRouteTest, ObjectRouteAndGroupMembershipAreIndependentFlatStructures) {
    TenantStore store;
    // A grouped member is just a flat route entry with a group_id annotation.
    auto member = std::make_shared<ObjectEntry>("k2", "g1");
    store.Insert("k2", member);
    store.LeaseFor("g1");
    store.AddMember("g1", "k2");

    EXPECT_EQ(store.ObjectCount(), 1u);
    EXPECT_EQ(store.GroupCount(), 1u);
    EXPECT_EQ(store.Members("g1").size(), 1u);

    // Erasing the object does not mutate group membership in the flat model
    // (membership is a parallel structure; cleanup is the caller's concern).
    store.Erase("k2");
    EXPECT_EQ(store.ObjectCount(), 0u);
    EXPECT_EQ(store.GroupCount(), 1u);
    EXPECT_TRUE(store.HasGroup("g1"));
}

}  // namespace
}  // namespace tenant
}  // namespace mooncake
