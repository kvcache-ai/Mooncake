#include "tenant/tenant_catalog.h"

#include <chrono>
#include <memory>
#include <string>
#include <vector>

#include <gtest/gtest.h>

namespace mooncake {
namespace tenant {
namespace {

std::shared_ptr<ObjectEntry> MakeEntry(const std::string& key,
                                       const std::string& group_id) {
    return std::make_shared<ObjectEntry>(
        std::make_unique<ObjectMetadata>(
            UUID{1, 2}, std::chrono::system_clock::now(), 128,
            std::vector<Replica>{}, std::nullopt, false,
            ObjectDataType::UNKNOWN, group_id, TenantId(), key));
}


// --- InsertObject (route + group wiring) ---

TEST(TenantCatalogTest, InsertObjectWiresSharedLeaseAndJoinsGroup) {
    TenantCatalog catalog;

    // A grouped object: InsertObject should wire the group's shared Lease into
    // the entry's lease slot AND register it as a group member.
    auto member = MakeEntry("k1", "g1");
    EXPECT_TRUE(catalog.InsertObject("k1", member));

    EXPECT_EQ(catalog.ObjectCount(), 1u);
    EXPECT_EQ(catalog.group_index.Members("g1").size(), 1u);
    ASSERT_NE(member->metadata().lease(), nullptr);  // shared lease wired
    EXPECT_EQ(member->metadata().lease().get(),
              catalog.group_index.LeaseFor("g1").get());  // same single shared lease
}

TEST(TenantCatalogTest, InsertObjectDoesNotJoinForSingleton) {
    TenantCatalog catalog;

    auto singleton = MakeEntry("k1", "");
    EXPECT_TRUE(catalog.InsertObject("k1", singleton));

    EXPECT_EQ(catalog.ObjectCount(), 1u);
    EXPECT_TRUE(catalog.group_index.Members("g1").empty());  // singleton adds no group
    // A singleton keeps the envelope's own never-granted lease: non-null but
    // expired, and distinct from any group's shared lease.
    EXPECT_NE(singleton->metadata().lease(), nullptr);
    EXPECT_TRUE(singleton->metadata().IsLeaseExpired());
}

TEST(TenantCatalogTest, InsertObjectRejectsDuplicateKey) {
    TenantCatalog catalog;
    catalog.InsertObject("k1", MakeEntry("k1", "g1"));
    // Second insert for the same key is rejected; the original is intact.
    EXPECT_FALSE(
        catalog.InsertObject("k1", MakeEntry("k1", "g2")));
    EXPECT_EQ(catalog.ObjectCount(), 1u);
    EXPECT_EQ(catalog.group_index.Members("g1").size(), 1u);
    EXPECT_TRUE(catalog.group_index.Members("g2").empty());
}

TEST(TenantCatalogTest, EmptyTracksRouteGroupsAndLeases) {
    TenantCatalog catalog;
    EXPECT_TRUE(catalog.Empty());

    // A routed object makes the aggregate non-empty.
    catalog.InsertObject("k1", MakeEntry("k1", ""));
    EXPECT_FALSE(catalog.Empty());
    auto handle = catalog.Pin("k1");
    ASSERT_NE(handle, nullptr);
    ASSERT_TRUE(catalog.EraseObjectIf("k1", handle.get()));
    EXPECT_TRUE(catalog.Empty());

    // Group membership also counts.
    catalog.group_index.LeaseFor("g1");
    catalog.group_index.AddMember("g1", "k1");
    EXPECT_FALSE(catalog.Empty());
    catalog.group_index.RemoveMember("g1", "k1");
    EXPECT_TRUE(catalog.Empty());

    // A tenant-scoped dynamic-replication lease also counts.
    catalog.object_index.PutDynamicReplicationLease(UUID{7, 8},
                                                    ReplicaActionLease{});
    EXPECT_FALSE(catalog.Empty());
    catalog.object_index.RemoveDynamicReplicationLease(UUID{7, 8});
    EXPECT_TRUE(catalog.Empty());
}

TEST(TenantCatalogTest,
     ObjectRouteAndGroupMembershipAreIndependentFlatStructures) {
    TenantCatalog catalog;
    // A grouped member is just a flat route entry with a group_id annotation.
    auto member = MakeEntry("k2", "g1");
    catalog.object_index.Insert("k2", member);
    catalog.group_index.LeaseFor("g1");
    catalog.group_index.AddMember("g1", "k2");

    EXPECT_EQ(catalog.ObjectCount(), 1u);
    EXPECT_EQ(catalog.group_index.Members("g1").size(), 1u);

    // Erasing the object does not mutate group membership in the flat model
    // (membership is a parallel structure; cleanup is the caller's concern).
    auto member_handle = catalog.Pin("k2");
    ASSERT_NE(member_handle, nullptr);
    catalog.EraseObjectIf("k2", member_handle.get());
    EXPECT_EQ(catalog.ObjectCount(), 0u);
    EXPECT_EQ(catalog.group_index.Members("g1").size(), 1u);
}

}  // namespace
}  // namespace tenant
}  // namespace mooncake
