#include "tenant/tenant_catalog.h"
#include "object_test_helpers.h"

#include <chrono>
#include <memory>
#include <string>
#include <vector>

#include <gtest/gtest.h>

namespace mooncake {
namespace metadata {
namespace {

// The lease the entry currently holds, read through the entry's own lock.
std::shared_ptr<Lease> LeaseOf(const std::shared_ptr<ObjectEntry>& entry) {
    return entry->WithSharedAccess(
        [](const ObjectMetadata& metadata, const ObjectEntry::State&) {
            return metadata.lease_;
        });
}

TEST(TenantCatalogTest, InsertObjectWiresTheGroupLeaseAndJoinsTheGroup) {
    TenantCatalog catalog;

    auto first = test::MakeObjectEntry("k1", "g1");
    ASSERT_TRUE(catalog.InsertObject(first));
    auto second = test::MakeObjectEntry("k2", "g1");
    ASSERT_TRUE(catalog.InsertObject(second));

    EXPECT_EQ(catalog.ObjectCount(), 2u);
    EXPECT_EQ(catalog.GroupMembers("g1").size(), 2u);

    // Both members of the group point at the one shared lease, so the group has
    // a single deadline.
    auto first_lease = LeaseOf(first);
    auto second_lease = LeaseOf(second);
    ASSERT_NE(first_lease, nullptr);
    EXPECT_EQ(first_lease.get(), second_lease.get());
}

TEST(TenantCatalogTest, InsertObjectLeavesAnUngroupedEntryOnItsOwnLease) {
    TenantCatalog catalog;

    auto singleton = test::MakeObjectEntry("k1");
    ASSERT_TRUE(catalog.InsertObject(singleton));

    EXPECT_EQ(catalog.ObjectCount(), 1u);
    EXPECT_TRUE(catalog.GroupMembers("g1").empty());

    // An ungrouped entry keeps the never-granted lease the envelope was
    // constructed with: present, and expired.
    ASSERT_NE(LeaseOf(singleton), nullptr);
    EXPECT_TRUE(singleton->WithSharedAccess(
        [](const ObjectMetadata& metadata, const ObjectEntry::State&) {
            return metadata.IsLeaseExpired();
        }));
}

TEST(TenantCatalogTest, InsertObjectRejectsAKeyThatIsAlreadyRouted) {
    TenantCatalog catalog;
    ASSERT_TRUE(catalog.InsertObject(test::MakeObjectEntry("k1", "g1")));

    // The second insert is rejected and registers nothing: no new route slot,
    // and no membership in its own group.
    EXPECT_FALSE(catalog.InsertObject(test::MakeObjectEntry("k1", "g2")));
    EXPECT_EQ(catalog.ObjectCount(), 1u);
    EXPECT_EQ(catalog.GroupMembers("g1").size(), 1u);
    EXPECT_TRUE(catalog.GroupMembers("g2").empty());
}

TEST(TenantCatalogTest, EraseObjectIfHonoursTheEntryIdentity) {
    TenantCatalog catalog;
    auto entry = test::MakeObjectEntry("k1", "");
    ASSERT_TRUE(catalog.InsertObject(entry));
    EXPECT_EQ(catalog.Get("k1"), entry);

    // A different handle for the same key does not erase the routed entry.
    EXPECT_FALSE(catalog.EraseObjectIf(test::MakeObjectEntry("k1")));
    EXPECT_TRUE(catalog.ContainsObject("k1"));

    EXPECT_TRUE(catalog.EraseObjectIf(entry));
    EXPECT_FALSE(catalog.ContainsObject("k1"));
    EXPECT_EQ(catalog.Get("k1"), nullptr);
    EXPECT_FALSE(catalog.EraseObjectIf(entry));
}

TEST(TenantCatalogTest, EmptyTracksObjectsGroupsAndLeases) {
    TenantCatalog catalog;
    EXPECT_TRUE(catalog.Empty());

    auto grouped = test::MakeObjectEntry("k1", "g1");
    ASSERT_TRUE(catalog.InsertObject(grouped));
    EXPECT_FALSE(catalog.Empty());

    // Erasing the route slot leaves the membership behind, so the tenant is
    // still non-empty until that is dropped too.
    ASSERT_TRUE(catalog.EraseObjectIf(grouped));
    EXPECT_FALSE(catalog.Empty());
    catalog.UnregisterGroupMember(grouped);
    EXPECT_TRUE(catalog.Empty());
    EXPECT_TRUE(catalog.GroupMembers("g1").empty());

    // A lease in flight is state of its own.
    catalog.PutDynamicReplicationLease(UUID{7, 8}, ReplicaActionLease{});
    EXPECT_FALSE(catalog.Empty());
    EXPECT_TRUE(catalog.RemoveDynamicReplicationLease(UUID{7, 8}));
    EXPECT_TRUE(catalog.Empty());
}

TEST(TenantCatalogTest, RebuildGroupStateRegroupsTheSameMembers) {
    TenantCatalog catalog;
    auto first = test::MakeObjectEntry("k1", "g1");
    auto second = test::MakeObjectEntry("k2", "g1");
    ASSERT_TRUE(catalog.InsertObject(first));
    ASSERT_TRUE(catalog.InsertObject(second));
    // A restored tenant starts without membership, so the rebuild is what
    // re-registers these members.
    catalog.UnregisterGroupMember(first);
    catalog.UnregisterGroupMember(second);
    ASSERT_TRUE(catalog.GroupMembers("g1").empty());

    catalog.RebuildGroupState();

    // Membership is back, and the rebuilt members share one lease again.
    EXPECT_EQ(catalog.GroupMembers("g1").size(), 2u);
    auto rebuilt_first = LeaseOf(first);
    auto rebuilt_second = LeaseOf(second);
    ASSERT_NE(rebuilt_first, nullptr);
    EXPECT_EQ(rebuilt_first.get(), rebuilt_second.get());
}

TEST(TenantCatalogTest, PromotionCandidateKeysTrackWhatWasIndexed) {
    TenantCatalog catalog;
    EXPECT_TRUE(catalog.PromotionCandidateKeys().empty());

    catalog.IndexPromotionCandidate("k1");
    catalog.IndexPromotionCandidate("k2");
    catalog.IndexPromotionCandidate("k1");
    EXPECT_EQ(catalog.PromotionCandidateKeys().size(), 2u);

    catalog.UnindexPromotionCandidate("k1");
    auto keys = catalog.PromotionCandidateKeys();
    ASSERT_EQ(keys.size(), 1u);
    EXPECT_EQ(keys[0], "k2");
}

}  // namespace
}  // namespace metadata
}  // namespace mooncake
