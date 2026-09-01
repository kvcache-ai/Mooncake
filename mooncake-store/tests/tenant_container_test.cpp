#include "tenant/tenant_dir.h"
#include "tenant/tenant_store.h"

#include <memory>
#include <string>

#include <gtest/gtest.h>

namespace mooncake {
namespace tenant {
namespace {

// The per-tenant container model, composed at the module level: a read-
// optimized TenantDirectory owns one TenantStore per tenant, and each store
// owns that tenant's flat group membership + shared leases. MasterService will
// orchestrate through these two components rather than nesting tenant state.
using TenantStorage = TenantDirectory<std::shared_ptr<TenantStore>>;

TEST(TenantContainerTest, DirectoryOwnsPerTenantStores) {
    TenantStorage dir;
    const TenantId tenant("tenant-a");

    auto store = std::make_shared<TenantStore>();
    store->LeaseFor("g1");
    store->AddMember("g1", "k1");
    dir.Upsert(tenant, store);

    auto found = dir.Lookup(tenant);
    ASSERT_NE(found, nullptr);
    EXPECT_EQ(found->GroupCount(), 1u);
    EXPECT_TRUE(found->HasGroup("g1"));
    EXPECT_EQ(found->Members("g1").size(), 1u);

    // A distinct tenant owns an independent store (no cross-tenant leakage).
    auto store_b = std::make_shared<TenantStore>();
    dir.Upsert(TenantId("tenant-b"), store_b);
    EXPECT_EQ(dir.Lookup(TenantId("tenant-b"))->GroupCount(), 0u);
    EXPECT_EQ(dir.Size(), 2u);
}

TEST(TenantContainerTest, CowRemoveKeepsTenantHandleUsable) {
    TenantStorage dir;
    const TenantId tenant("tenant-a");

    auto store = std::make_shared<TenantStore>();
    store->LeaseFor("g1");
    store->AddMember("g1", "k1");
    dir.Upsert(tenant, store);

    auto escaped = dir.Lookup(tenant);  // a handle that "escaped" the snapshot
    dir.Remove(tenant);                 // the directory forgets the tenant

    EXPECT_EQ(dir.Lookup(tenant), nullptr);
    ASSERT_NE(escaped, nullptr);        // outstanding handle keeps old store alive
    EXPECT_EQ(escaped->GroupCount(), 1u);
    EXPECT_EQ(escaped->Members("g1").size(), 1u);
}

TEST(TenantContainerTest, RecreatedTenantHasFreshStore) {
    TenantStorage dir;
    const TenantId tenant("tenant-a");

    auto store = std::make_shared<TenantStore>();
    store->LeaseFor("g1");
    store->AddMember("g1", "k1");
    dir.Upsert(tenant, store);

    // Teardown + recreate: the new tenant gets a brand-new store; the old store
    // (if observed earlier) is unaffected.
    dir.Remove(tenant);
    dir.Upsert(tenant, std::make_shared<TenantStore>());

    auto fresh = dir.Lookup(tenant);
    ASSERT_NE(fresh, nullptr);
    EXPECT_EQ(fresh->GroupCount(), 0u);
}

TEST(TenantContainerTest, PerTenantStoreCarriesRouteGroupsQuotaAndLeases) {
    TenantStorage dir;
    const TenantId tenant("tenant-a");

    // A fully-populated per-tenant container: object route + flat group + the
    // tenant-scoped bookkeeping that does NOT fold into ObjectEntry.
    auto store = std::make_shared<TenantStore>();
    store->InsertObject("k1", std::make_shared<ObjectEntry>("k1", "g1"));
    store->InsertObject("k2", std::make_shared<ObjectEntry>("k2", ""));
    store->quota_account = nullptr;
    store->disk_object_count = 1;
    store->dynamic_replication_leases[UUID{1, 2}] = ReplicaActionLease{};
    EXPECT_FALSE(store->Empty());
    EXPECT_EQ(store->ObjectCount(), 2u);
    EXPECT_EQ(store->GroupCount(), 1u);

    dir.Upsert(tenant, store);

    // The same handle is reachable from the directory; it still owns everything.
    auto found = dir.Lookup(tenant);
    ASSERT_NE(found, nullptr);
    EXPECT_EQ(found->ObjectCount(), 2u);
    EXPECT_EQ(found->GroupCount(), 1u);
    EXPECT_EQ(found->disk_object_count, 1);

    // A sibling tenant stays isolated.
    dir.Upsert(TenantId("tenant-b"), std::make_shared<TenantStore>());
    EXPECT_TRUE(dir.Lookup(TenantId("tenant-b"))->Empty());
    EXPECT_FALSE(dir.Lookup(tenant)->Empty());
    EXPECT_EQ(dir.Size(), 2u);
}

}  // namespace
}  // namespace tenant
}  // namespace mooncake
