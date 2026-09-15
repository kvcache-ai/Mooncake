#include "tenant/tenant_catalog.h"

#include <chrono>
#include <memory>
#include <string>
#include <vector>

#include <gtest/gtest.h>

namespace mooncake {
namespace metadata {
namespace {

std::shared_ptr<ObjectEntry> MakeEntry(const std::string& key,
                                       const std::string& group_id) {
    return std::make_shared<ObjectEntry>(std::make_unique<ObjectMetadata>(
        UUID{1, 2}, std::chrono::system_clock::now(), 128,
        std::vector<Replica>{}, std::nullopt, false, ObjectDataType::UNKNOWN,
        group_id, TenantId(), key));
}

// --- InsertObject (route + group wiring) ---

TEST(TenantCatalogTest, InsertObjectWiresSharedLeaseAndJoinsGroup) {
    TenantCatalog catalog;

    // A grouped object: InsertObject should wire the group's shared Lease into
    // the entry's lease slot AND register it as a group member.
    auto member = MakeEntry("k1", "g1");
    EXPECT_TRUE(catalog.InsertObject("k1", member));

    EXPECT_EQ(catalog.ObjectCount(), 1u);
    EXPECT_EQ(catalog.GroupMembers("g1").size(), 1u);
    ASSERT_NE(member->metadata().lease(), nullptr);  // shared lease wired
    EXPECT_EQ(member->metadata().lease().get(),
              catalog.LeaseForTest("g1").get());  // single shared lease
}

TEST(TenantCatalogTest, InsertObjectDoesNotJoinForSingleton) {
    TenantCatalog catalog;

    auto singleton = MakeEntry("k1", "");
    EXPECT_TRUE(catalog.InsertObject("k1", singleton));

    EXPECT_EQ(catalog.ObjectCount(), 1u);
    EXPECT_TRUE(catalog.GroupMembers("g1").empty());  // singleton adds no group
    // A singleton keeps the envelope's own never-granted lease: non-null but
    // expired, and distinct from any group's shared lease.
    EXPECT_NE(singleton->metadata().lease(), nullptr);
    EXPECT_TRUE(singleton->metadata().IsLeaseExpired());
}

TEST(TenantCatalogTest, InsertObjectRejectsDuplicateKey) {
    TenantCatalog catalog;
    catalog.InsertObject("k1", MakeEntry("k1", "g1"));
    // Second insert for the same key is rejected; the original is intact.
    EXPECT_FALSE(catalog.InsertObject("k1", MakeEntry("k1", "g2")));
    EXPECT_EQ(catalog.ObjectCount(), 1u);
    EXPECT_EQ(catalog.GroupMembers("g1").size(), 1u);
    EXPECT_TRUE(catalog.GroupMembers("g2").empty());
}

TEST(TenantCatalogTest, EmptyTracksRouteGroupsAndLeases) {
    TenantCatalog catalog;
    EXPECT_TRUE(catalog.Empty());

    // A routed object makes the aggregate non-empty.
    catalog.InsertObject("k1", MakeEntry("k1", ""));
    EXPECT_FALSE(catalog.Empty());
    auto handle = catalog.Get("k1");
    ASSERT_NE(handle, nullptr);
    ASSERT_TRUE(catalog.EraseObjectIf("k1", handle.get()));
    EXPECT_TRUE(catalog.Empty());

    // Group membership alone also counts: erasing the route slot does not
    // unregister membership, so the tenant stays non-empty until the
    // membership is dropped too.
    auto grouped = MakeEntry("k1", "g1");
    ASSERT_TRUE(catalog.InsertObject("k1", grouped));
    ASSERT_TRUE(catalog.EraseObjectIf("k1", grouped.get()));
    EXPECT_FALSE(catalog.Empty());
    catalog.UnregisterGroupMember("k1", "g1");
    EXPECT_TRUE(catalog.Empty());

    // A tenant-scoped dynamic-replication lease also counts.
    catalog.PutDynamicReplicationLease(UUID{7, 8}, ReplicaActionLease{});
    EXPECT_FALSE(catalog.Empty());
    catalog.RemoveDynamicReplicationLease(UUID{7, 8});
    EXPECT_TRUE(catalog.Empty());
}

}  // namespace
}  // namespace metadata
}  // namespace mooncake
