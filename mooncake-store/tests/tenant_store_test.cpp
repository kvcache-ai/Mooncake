#include "tenant/tenant_store.h"

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

// --- Group membership ---

TEST(TenantStoreTest, StartsWithNoGroups) {
    TenantStore store;
    EXPECT_TRUE(store.Members("g1").empty());
}

TEST(TenantStoreTest, LeaseForCreatesAndSharesOneLeasePerGroup) {
    TenantStore store;

    auto a1 = store.LeaseFor("g1");
    auto a2 = store.LeaseFor("g1");
    auto b = store.LeaseFor("g2");

    ASSERT_NE(a1, nullptr);
    EXPECT_EQ(a1.get(), a2.get());   // same group -> same shared Lease
    EXPECT_NE(a1.get(), b.get());    // different group -> distinct Lease
}

TEST(TenantStoreTest, AddRemoveGroupMembers) {
    TenantStore store;
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

TEST(TenantStoreTest, AddingToUndefinedGroupIsRejected) {
    TenantStore store;
    // Group must be materialized via LeaseFor before members are registered.
    EXPECT_FALSE(store.AddMember("nope", "k1"));
}

TEST(TenantStoreTest, EmptyGroupIsDroppedOnLastMemberRemoved) {
    TenantStore store;
    store.LeaseFor("g1");
    store.AddMember("g1", "k1");

    EXPECT_EQ(store.Members("g1").size(), 1u);
    EXPECT_TRUE(store.RemoveMember("g1", "k1"));
    // Last member gone -> the group (and its membership) is dropped.
    EXPECT_TRUE(store.Members("g1").empty());
}

TEST(TenantStoreTest, SharedLeaseWiresGroupAllOrNoneExpiry) {
    TenantStore store;
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

TEST(TenantStoreTest, InsertPinEraseContainsObjectCount) {
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

TEST(TenantStoreTest, DuplicateInsertIsRejected) {
    TenantStore store;
    store.Insert("k1", std::make_shared<ObjectEntry>("k1", ""));
    // Second insert for the same key must not clobber the original.
    EXPECT_FALSE(store.Insert("k1", std::make_shared<ObjectEntry>("k1", "")));
    EXPECT_EQ(store.ObjectCount(), 1u);
    ASSERT_NE(store.Pin("k1"), nullptr);
    EXPECT_EQ(store.Pin("k1")->key(), "k1");
}

TEST(TenantStoreTest, VisitObjectsEnumeratesEveryEntry) {
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

TEST(TenantStoreTest, ObjectRouteAndGroupMembershipAreIndependentFlatStructures) {
    TenantStore store;
    // A grouped member is just a flat route entry with a group_id annotation.
    auto member = std::make_shared<ObjectEntry>("k2", "g1");
    store.Insert("k2", member);
    store.LeaseFor("g1");
    store.AddMember("g1", "k2");

    EXPECT_EQ(store.ObjectCount(), 1u);
    EXPECT_EQ(store.Members("g1").size(), 1u);

    // Erasing the object does not mutate group membership in the flat model
    // (membership is a parallel structure; cleanup is the caller's concern).
    store.Erase("k2");
    EXPECT_EQ(store.ObjectCount(), 0u);
    EXPECT_EQ(store.Members("g1").size(), 1u);
}

// --- InsertObject (route + group wiring) ---

TEST(TenantStoreTest, InsertObjectWiresSharedLeaseAndJoinsGroup) {
    TenantStore store;

    // A grouped object: InsertObject should wire the group's shared Lease into
    // the entry's lease slot AND register it as a group member.
    auto member = std::make_shared<ObjectEntry>("k1", "g1");
    EXPECT_TRUE(store.InsertObject("k1", member));

    EXPECT_EQ(store.ObjectCount(), 1u);
    EXPECT_EQ(store.Members("g1").size(), 1u);
    ASSERT_NE(member->lease(), nullptr);  // shared lease wired
    EXPECT_EQ(member->lease().get(),
              store.LeaseFor("g1").get());  // same single shared lease
}

TEST(TenantStoreTest, InsertObjectDoesNotJoinForSingleton) {
    TenantStore store;

    auto singleton = std::make_shared<ObjectEntry>("k1", "");
    EXPECT_TRUE(store.InsertObject("k1", singleton));

    EXPECT_EQ(store.ObjectCount(), 1u);
    EXPECT_TRUE(store.Members("g1").empty());  // singleton adds no group
    EXPECT_EQ(singleton->lease(),
              nullptr);  // own lease wired by caller, not here
}

TEST(TenantStoreTest, InsertObjectRejectsDuplicateKey) {
    TenantStore store;
    store.InsertObject("k1", std::make_shared<ObjectEntry>("k1", "g1"));
    // Second insert for the same key is rejected; the original is intact.
    EXPECT_FALSE(store.InsertObject("k1", std::make_shared<ObjectEntry>("k1", "g2")));
    EXPECT_EQ(store.ObjectCount(), 1u);
    EXPECT_EQ(store.Members("g1").size(), 1u);
    EXPECT_TRUE(store.Members("g2").empty());
}


// --- Accessors ---

TEST(TenantStoreTest, WithObjectScopeRespectsPresenceAndAbsence) {
    TenantStore store;
    auto singleton = std::make_shared<ObjectEntry>("k1", "");
    store.Insert("k1", singleton);

    // No metadata wired yet -> callback is not invoked.
    bool called = false;
    store.WithObject("k1", [&](ObjectMetadata&) { called = true; });
    EXPECT_FALSE(called);

    // Absent key -> callback is not invoked.
    store.WithObject("missing", [&](ObjectMetadata&) { FAIL(); });

    // After wiring metadata into the entry, WithObject reaches it under the
    // per-object lock.
    singleton->SetMetadata(std::make_unique<ObjectMetadata>(
        UUID{1, 2}, std::chrono::system_clock::now(), 64,
        std::vector<Replica>{}, std::nullopt, false,
        ObjectDataType::UNKNOWN, std::string{}, TenantId(), "k1"));
    const auto* raw = singleton->metadata();

    called = false;
    store.WithObject("k1", [&](ObjectMetadata& m) {
        called = true;
        EXPECT_EQ(&m, raw);
    });
    EXPECT_TRUE(called);
}

TEST(TenantStoreTest, EmptyTracksRouteGroupsAndLeases) {
    TenantStore store;
    EXPECT_TRUE(store.Empty());

    // A routed object makes the container non-empty.
    store.Insert("k1", std::make_shared<ObjectEntry>("k1", ""));
    EXPECT_FALSE(store.Empty());
    store.Erase("k1");
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
