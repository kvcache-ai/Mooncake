#include "tenant/object_index.h"

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include <gtest/gtest.h>

namespace mooncake {
namespace tenant {
namespace {

// Build an entry with a minimal (128 B, replica-less) envelope; the store
// tests exercise routing/membership, not replica validity.
std::shared_ptr<ObjectEntry> MakeEntry(const std::string& key,
                                       const std::string& group_id) {
    return std::make_shared<ObjectEntry>(
        std::make_unique<ObjectMetadata>(
            UUID{1, 2}, std::chrono::system_clock::now(), 128,
            std::vector<Replica>{}, std::nullopt, false,
            ObjectDataType::UNKNOWN, group_id, TenantId(), key));
}

// --- Group membership ---

TEST(ObjectIndexTest, StartsWithNoGroups) {
    ObjectIndex store;
    EXPECT_TRUE(store.Members("g1").empty());
}

TEST(ObjectIndexTest, LeaseForCreatesAndSharesOneLeasePerGroup) {
    ObjectIndex store;

    auto a1 = store.LeaseFor("g1");
    auto a2 = store.LeaseFor("g1");
    auto b = store.LeaseFor("g2");

    ASSERT_NE(a1, nullptr);
    EXPECT_EQ(a1.get(), a2.get());  // same group -> same shared Lease
    EXPECT_NE(a1.get(), b.get());   // different group -> distinct Lease
}

TEST(ObjectIndexTest, AddRemoveGroupMembers) {
    ObjectIndex store;
    store.LeaseFor("g1");

    EXPECT_TRUE(store.AddMember("g1", "k1"));
    EXPECT_TRUE(store.AddMember("g1", "k2"));

    auto members = store.Members("g1");
    EXPECT_EQ(members.size(), 2u);

    EXPECT_TRUE(store.RemoveMember("g1", "k1"));
    auto after = store.Members("g1");
    ASSERT_EQ(after.size(), 1u);
    EXPECT_EQ(after[0], "k2");
}

TEST(ObjectIndexTest, AddingToUndefinedGroupIsRejected) {
    ObjectIndex store;
    // Group must be materialized via LeaseFor before members are registered.
    EXPECT_FALSE(store.AddMember("nope", "k1"));
}

TEST(ObjectIndexTest, EmptyGroupIsDroppedOnLastMemberRemoved) {
    ObjectIndex store;
    store.LeaseFor("g1");
    store.AddMember("g1", "k1");

    EXPECT_EQ(store.Members("g1").size(), 1u);
    EXPECT_TRUE(store.RemoveMember("g1", "k1"));
    // Last member gone -> the group (and its membership) is dropped.
    EXPECT_TRUE(store.Members("g1").empty());
}

TEST(ObjectIndexTest, SharedLeaseWiresGroupAllOrNoneExpiry) {
    ObjectIndex store;
    store.LeaseFor("g1");
    store.AddMember("g1", "k1");
    store.AddMember("g1", "k2");

    // Distinct groups get independent shared leases.
    auto g2 = store.LeaseFor("g2");
    ASSERT_NE(g2, nullptr);
    EXPECT_NE(store.LeaseFor("g1").get(), g2.get());

    // All-or-none: every member of the group shares the one Lease, so a live
    // shared lease protects the whole group and one deadline expires it all.
    const auto now = std::chrono::system_clock::now();

    auto shared = store.LeaseFor("g1");
    shared->GrantReadLease(std::chrono::milliseconds(10'000));
    EXPECT_FALSE(shared->IsExpired(now));

    shared->SetDeadline(now);
    EXPECT_TRUE(shared->IsExpired(now));
}

// --- Object route ---

TEST(ObjectIndexTest, InsertPinEraseContainsObjectCount) {
    ObjectIndex store;
    EXPECT_EQ(store.ObjectCount(), 0u);

    auto e1 = MakeEntry("k1", "");
    EXPECT_TRUE(store.Insert("k1", e1));
    EXPECT_TRUE(store.Contains("k1"));
    EXPECT_EQ(store.ObjectCount(), 1u);

    auto pinned = store.Pin("k1");
    ASSERT_NE(pinned, nullptr);
    EXPECT_EQ(pinned->key(), "k1");
    EXPECT_EQ(pinned.get(), e1.get());  // same underlying entry

    EXPECT_EQ(store.Pin("missing"), nullptr);
    // Identity-checked erase: only the pinned entry's slot may go, and the
    // second call is a no-op (slot already gone).
    EXPECT_TRUE(store.EraseIf("k1", e1.get()));
    EXPECT_FALSE(store.EraseIf("k1", e1.get()));
    EXPECT_FALSE(store.Contains("k1"));
    EXPECT_EQ(store.ObjectCount(), 0u);
}

TEST(ObjectIndexTest, InsertAssignsGenerationsAndIsCurrentTracksReplacement) {
    ObjectIndex store;
    auto e1 = MakeEntry("k1", "");
    ASSERT_TRUE(store.Insert("k1", e1));
    EXPECT_GT(e1->generation(), 0u);  // publication assigns a generation
    EXPECT_TRUE(store.IsCurrent("k1", e1.get()));

    EXPECT_TRUE(store.EraseIf("k1", e1.get()));
    EXPECT_FALSE(store.IsCurrent("k1", e1.get()));

    // A replacement of the same key gets a fresh, higher generation; the
    // stale instance is never current again.
    auto e2 = MakeEntry("k1", "");
    EXPECT_EQ(e2->generation(), 0u);  // unpublished
    ASSERT_TRUE(store.Insert("k1", e2));
    EXPECT_GT(e2->generation(), e1->generation());
    EXPECT_TRUE(store.IsCurrent("k1", e2.get()));
    EXPECT_FALSE(store.IsCurrent("k1", e1.get()));
}

TEST(ObjectIndexTest, DuplicateInsertIsRejected) {
    ObjectIndex store;
    store.Insert("k1", MakeEntry("k1", ""));
    // Second insert for the same key must not clobber the original.
    EXPECT_FALSE(store.Insert("k1", MakeEntry("k1", "")));
    EXPECT_EQ(store.ObjectCount(), 1u);
    ASSERT_NE(store.Pin("k1"), nullptr);
    EXPECT_EQ(store.Pin("k1")->key(), "k1");
}

TEST(ObjectIndexTest, SnapshotObjectsEnumeratesEveryEntry) {
    ObjectIndex store;
    store.Insert("k1", MakeEntry("k1", ""));
    store.Insert("k2", MakeEntry("k2", "g1"));
    store.Insert("k3", MakeEntry("k3", "g1"));

    std::vector<std::string> keys;
    for (const auto& entry : store.SnapshotObjects()) {
        keys.push_back(entry->key());
    }
    EXPECT_EQ(keys.size(), 3u);
    EXPECT_TRUE(std::find(keys.begin(), keys.end(), "k1") != keys.end());
    EXPECT_TRUE(std::find(keys.begin(), keys.end(), "k2") != keys.end());
    EXPECT_TRUE(std::find(keys.begin(), keys.end(), "k3") != keys.end());
}

TEST(ObjectIndexTest,
     ObjectRouteAndGroupMembershipAreIndependentFlatStructures) {
    ObjectIndex store;
    // A grouped member is just a flat route entry with a group_id annotation.
    auto member = MakeEntry("k2", "g1");
    store.Insert("k2", member);
    store.LeaseFor("g1");
    store.AddMember("g1", "k2");

    EXPECT_EQ(store.ObjectCount(), 1u);
    EXPECT_EQ(store.Members("g1").size(), 1u);

    // Erasing the object does not mutate group membership in the flat model
    // (membership is a parallel structure; cleanup is the caller's concern).
    auto member_handle = store.Pin("k2");
    ASSERT_NE(member_handle, nullptr);
    store.EraseIf("k2", member_handle.get());
    EXPECT_EQ(store.ObjectCount(), 0u);
    EXPECT_EQ(store.Members("g1").size(), 1u);
}

// --- InsertObject (route + group wiring) ---

TEST(ObjectIndexTest, InsertObjectWiresSharedLeaseAndJoinsGroup) {
    ObjectIndex store;

    // A grouped object: InsertObject should wire the group's shared Lease into
    // the entry's lease slot AND register it as a group member.
    auto member = MakeEntry("k1", "g1");
    EXPECT_TRUE(store.InsertObject("k1", member));

    EXPECT_EQ(store.ObjectCount(), 1u);
    EXPECT_EQ(store.Members("g1").size(), 1u);
    ASSERT_NE(member->metadata().lease(), nullptr);  // shared lease wired
    EXPECT_EQ(member->metadata().lease().get(),
              store.LeaseFor("g1").get());  // same single shared lease
}

TEST(ObjectIndexTest, InsertObjectDoesNotJoinForSingleton) {
    ObjectIndex store;

    auto singleton = MakeEntry("k1", "");
    EXPECT_TRUE(store.InsertObject("k1", singleton));

    EXPECT_EQ(store.ObjectCount(), 1u);
    EXPECT_TRUE(store.Members("g1").empty());  // singleton adds no group
    // A singleton keeps the envelope's own never-granted lease: non-null but
    // expired, and distinct from any group's shared lease.
    EXPECT_NE(singleton->metadata().lease(), nullptr);
    EXPECT_TRUE(singleton->metadata().IsLeaseExpired());
}

TEST(ObjectIndexTest, InsertObjectRejectsDuplicateKey) {
    ObjectIndex store;
    store.InsertObject("k1", MakeEntry("k1", "g1"));
    // Second insert for the same key is rejected; the original is intact.
    EXPECT_FALSE(
        store.InsertObject("k1", MakeEntry("k1", "g2")));
    EXPECT_EQ(store.ObjectCount(), 1u);
    EXPECT_EQ(store.Members("g1").size(), 1u);
    EXPECT_TRUE(store.Members("g2").empty());
}

// --- Accessors ---

TEST(ObjectIndexTest, WithObjectScopeRespectsPresenceAndAbsence) {
    ObjectIndex store;
    auto singleton = MakeEntry("k1", "");
    store.Insert("k1", singleton);
    auto& raw = singleton->metadata();

    // Present key -> WithObject reaches the envelope under the per-object
    // lock. Absent key -> callback is not invoked.
    bool called = false;
    store.WithObject("missing", [&](ObjectMetadata&) { FAIL(); });

    called = false;
    store.WithObject("k1", [&](ObjectMetadata& m) {
        called = true;
        EXPECT_EQ(&m, &raw);
    });
    EXPECT_TRUE(called);
}

TEST(ObjectIndexTest, EmptyTracksRouteGroupsAndLeases) {
    ObjectIndex store;
    EXPECT_TRUE(store.Empty());

    // A routed object makes the container non-empty.
    store.Insert("k1", MakeEntry("k1", ""));
    EXPECT_FALSE(store.Empty());
    ASSERT_TRUE(store.EraseIf("k1", store.Pin("k1").get()));
    EXPECT_TRUE(store.Empty());

    // Group membership also counts.
    store.LeaseFor("g1");
    store.AddMember("g1", "k1");
    EXPECT_FALSE(store.Empty());
    store.RemoveMember("g1", "k1");
    EXPECT_TRUE(store.Empty());

    // A tenant-scoped dynamic-replication lease also counts.
    store.PutDynamicReplicationLease(UUID{7, 8}, ReplicaActionLease{});
    EXPECT_FALSE(store.Empty());
    store.RemoveDynamicReplicationLease(UUID{7, 8});
    EXPECT_TRUE(store.Empty());
}

}  // namespace
}  // namespace tenant
}  // namespace mooncake
