#include "tenant/tenant_store.h"

#include <chrono>
#include <memory>
#include <string>
#include <vector>

#include <gtest/gtest.h>

namespace mooncake {
namespace tenant {
namespace {

TEST(TenantStoreTest, StartsWithNoGroups) {
    TenantStore store;
    EXPECT_EQ(store.GroupCount(), 0u);
    EXPECT_FALSE(store.HasGroup("g1"));
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
    EXPECT_TRUE(store.HasGroup("g1"));
    EXPECT_EQ(store.GroupCount(), 1u);

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

    EXPECT_TRUE(store.HasGroup("g1"));
    EXPECT_TRUE(store.RemoveMember("g1", "k1"));
    // Last member gone -> the group (and its membership) is dropped.
    EXPECT_FALSE(store.HasGroup("g1"));
    EXPECT_EQ(store.GroupCount(), 0u);
}

TEST(TenantStoreTest, SharedLeaseDrivesAllOrNoneExpiry) {
    TenantStore store;
    store.LeaseFor("g1");
    store.AddMember("g1", "k1");
    store.AddMember("g1", "k2");

    const auto now = std::chrono::system_clock::now();
    // A live shared lease keeps the whole group unexpired (all-or-none protect).
    auto shared = store.LeaseFor("g1");
    shared->GrantReadLease(std::chrono::milliseconds(10'000));
    EXPECT_FALSE(store.AllExpired("g1", now));

    // Forcing the shared deadline into the past expires the whole group at once.
    shared->SetDeadline(now);
    EXPECT_TRUE(store.AllExpired("g1", now));
}

TEST(TenantStoreTest, DistinctGroupsHaveIndependentMembershipAndLease) {
    TenantStore store;
    store.LeaseFor("g1");
    store.LeaseFor("g2");
    store.AddMember("g1", "k1");
    store.AddMember("g2", "k2");

    EXPECT_EQ(store.Members("g1").size(), 1u);
    EXPECT_EQ(store.Members("g2").size(), 1u);

    store.RemoveMember("g1", "k1");
    EXPECT_FALSE(store.HasGroup("g1"));
    EXPECT_TRUE(store.HasGroup("g2"));
    EXPECT_EQ(store.GroupCount(), 1u);
}

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
    store.dynamic_replication_leases[UUID{7, 8}] = ReplicaActionLease{};
    EXPECT_FALSE(store.Empty());
    store.dynamic_replication_leases.clear();
    EXPECT_TRUE(store.Empty());
}

}  // namespace
}  // namespace tenant
}  // namespace mooncake
