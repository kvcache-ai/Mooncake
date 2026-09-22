#include "metadata/tenant.h"
#include "object_test_helpers.h"

#include <chrono>
#include <memory>

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

TEST(TenantTest, InsertObjectWiresTheGroupLeaseAndJoinsTheGroup) {
    Tenant tenant;

    auto first = test::MakeObjectEntry("k1", "g1");
    ASSERT_TRUE(tenant.InsertObject(first));
    auto second = test::MakeObjectEntry("k2", "g1");
    ASSERT_TRUE(tenant.InsertObject(second));

    EXPECT_EQ(tenant.ObjectCount(), 2u);
    EXPECT_EQ(tenant.GroupMembers("g1").size(), 2u);

    // Both members of the group point at the one shared lease, so the group has
    // a single deadline.
    auto first_lease = LeaseOf(first);
    auto second_lease = LeaseOf(second);
    ASSERT_NE(first_lease, nullptr);
    EXPECT_EQ(first_lease.get(), second_lease.get());
}

TEST(TenantTest, InsertObjectLeavesAnUngroupedEntryOnItsOwnLease) {
    Tenant tenant;

    auto singleton = test::MakeObjectEntry("k1");
    ASSERT_TRUE(tenant.InsertObject(singleton));

    EXPECT_EQ(tenant.ObjectCount(), 1u);
    EXPECT_TRUE(tenant.GroupMembers("g1").empty());

    // An ungrouped entry keeps the never-granted lease the envelope was
    // constructed with: present, and expired.
    ASSERT_NE(LeaseOf(singleton), nullptr);
    EXPECT_TRUE(singleton->WithSharedAccess(
        [](const ObjectMetadata& metadata, const ObjectEntry::State&) {
            return metadata.IsLeaseExpired();
        }));
}

TEST(TenantTest, InsertObjectRejectsAKeyThatIsAlreadyRouted) {
    Tenant tenant;
    ASSERT_TRUE(tenant.InsertObject(test::MakeObjectEntry("k1", "g1")));

    // The second insert is rejected and registers nothing: no new route slot,
    // and no membership in its own group.
    EXPECT_FALSE(tenant.InsertObject(test::MakeObjectEntry("k1", "g2")));
    EXPECT_EQ(tenant.ObjectCount(), 1u);
    EXPECT_EQ(tenant.GroupMembers("g1").size(), 1u);
    EXPECT_TRUE(tenant.GroupMembers("g2").empty());
}

TEST(TenantTest, EraseObjectIfHonoursTheEntryIdentity) {
    Tenant tenant;
    auto entry = test::MakeObjectEntry("k1", "");
    ASSERT_TRUE(tenant.InsertObject(entry));
    EXPECT_EQ(tenant.Get("k1"), entry);

    // A different handle for the same key does not erase the routed entry.
    EXPECT_FALSE(tenant.EraseObjectIf(test::MakeObjectEntry("k1")));
    EXPECT_TRUE(tenant.ContainsObject("k1"));

    EXPECT_TRUE(tenant.EraseObjectIf(entry));
    EXPECT_FALSE(tenant.ContainsObject("k1"));
    EXPECT_EQ(tenant.Get("k1"), nullptr);
    EXPECT_FALSE(tenant.EraseObjectIf(entry));
}

TEST(TenantTest, RemoveObjectDropsRouteGroupAndLeasesTogether) {
    Tenant tenant;
    auto entry = test::MakeObjectEntry("k1", "g1");
    ASSERT_TRUE(tenant.InsertObject(entry));
    ReplicaActionLease lease;
    lease.key = "k1";
    tenant.PutDynamicReplicationLease(UUID{7, 8}, lease);
    ASSERT_FALSE(tenant.Empty());

    EXPECT_TRUE(tenant.RemoveObject(entry));

    // One call drops what a teardown otherwise drops by hand: the route slot,
    // the group membership and the leases still in flight for that key.
    EXPECT_FALSE(tenant.ContainsObject("k1"));
    EXPECT_TRUE(tenant.GroupMembers("g1").empty());
    EXPECT_FALSE(tenant.FindDynamicReplicationLease(UUID{7, 8}).has_value());
    EXPECT_TRUE(tenant.Empty());
}

TEST(TenantTest, RemoveObjectRequiresTheEntryStillOnTheRoute) {
    Tenant tenant;
    auto published = test::MakeObjectEntry("k1", "g1");
    ASSERT_TRUE(tenant.InsertObject(published));

    // A different handle for the same key is not what the route publishes, so
    // the slot survives and the caller learns nothing was removed.
    EXPECT_FALSE(tenant.RemoveObject(test::MakeObjectEntry("k1", "g1")));
    EXPECT_TRUE(tenant.ContainsObject("k1"));
    EXPECT_EQ(tenant.Get("k1"), published);

    // The published entry itself is removed.
    EXPECT_TRUE(tenant.RemoveObject(published));
    EXPECT_FALSE(tenant.ContainsObject("k1"));

    // A null handle changes nothing.
    EXPECT_FALSE(tenant.RemoveObject(std::shared_ptr<ObjectEntry>{}));
}

TEST(TenantTest, EmptyTracksObjectsGroupsAndLeases) {
    Tenant tenant;
    EXPECT_TRUE(tenant.Empty());

    auto grouped = test::MakeObjectEntry("k1", "g1");
    ASSERT_TRUE(tenant.InsertObject(grouped));
    EXPECT_FALSE(tenant.Empty());

    // Erasing the route slot leaves the membership behind, so the tenant is
    // still non-empty until that is dropped too.
    ASSERT_TRUE(tenant.EraseObjectIf(grouped));
    EXPECT_FALSE(tenant.Empty());
    tenant.UnregisterGroupMember(grouped);
    EXPECT_TRUE(tenant.Empty());
    EXPECT_TRUE(tenant.GroupMembers("g1").empty());

    // A lease in flight is state of its own.
    tenant.PutDynamicReplicationLease(UUID{7, 8}, ReplicaActionLease{});
    EXPECT_FALSE(tenant.Empty());
    EXPECT_TRUE(tenant.RemoveDynamicReplicationLease(UUID{7, 8}));
    EXPECT_TRUE(tenant.Empty());
}

TEST(TenantTest, RebuildGroupStateRegroupsTheSameMembers) {
    Tenant tenant;
    auto first = test::MakeObjectEntry("k1", "g1");
    auto second = test::MakeObjectEntry("k2", "g1");
    ASSERT_TRUE(tenant.InsertObject(first));
    ASSERT_TRUE(tenant.InsertObject(second));
    // A restored tenant starts without membership, so the rebuild is what
    // re-registers these members.
    tenant.UnregisterGroupMember(first);
    tenant.UnregisterGroupMember(second);
    ASSERT_TRUE(tenant.GroupMembers("g1").empty());

    tenant.RebuildGroupState();

    // Membership is back, and the rebuilt members share one lease again.
    EXPECT_EQ(tenant.GroupMembers("g1").size(), 2u);
    auto rebuilt_first = LeaseOf(first);
    auto rebuilt_second = LeaseOf(second);
    ASSERT_NE(rebuilt_first, nullptr);
    EXPECT_EQ(rebuilt_first.get(), rebuilt_second.get());
}

TEST(TenantTest, PromotionCandidateKeysTrackWhatWasIndexed) {
    Tenant tenant;
    EXPECT_TRUE(tenant.PromotionCandidateKeys().empty());

    tenant.IndexPromotionCandidate("k1");
    tenant.IndexPromotionCandidate("k2");
    tenant.IndexPromotionCandidate("k1");
    EXPECT_EQ(tenant.PromotionCandidateKeys().size(), 2u);

    tenant.UnindexPromotionCandidate("k1");
    auto keys = tenant.PromotionCandidateKeys();
    ASSERT_EQ(keys.size(), 1u);
    EXPECT_EQ(keys[0], "k2");
}

}  // namespace
}  // namespace metadata
}  // namespace mooncake
