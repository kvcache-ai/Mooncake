#include "tenant/metadata_catalog.h"
#include "object_test_helpers.h"

#include <atomic>
#include <memory>
#include <thread>
#include <vector>

#include <gtest/gtest.h>

namespace mooncake {
namespace metadata {
namespace {

// Every tenant of one registry is built through the factory it was constructed
// with, so its tests can pass a plain one.
std::shared_ptr<TenantCatalog> MakeTenant(const TenantId&) {
    return std::make_shared<TenantCatalog>();
}

TEST(MetadataCatalogTest, GetOrCreateTenantBuildsOneCatalogPerTenant) {
    size_t builds = 0;
    MetadataCatalog catalog([&builds](const TenantId&) {
        ++builds;
        return std::make_shared<TenantCatalog>();
    });
    const TenantId tenant("tenant-a");
    EXPECT_EQ(catalog.Lookup(tenant), nullptr);

    auto created = catalog.GetOrCreateTenant(tenant);
    ASSERT_NE(created, nullptr);
    EXPECT_EQ(builds, 1u);
    EXPECT_EQ(catalog.Lookup(tenant).get(), created.get());

    // A second call finds the published catalog instead of building another.
    auto found = catalog.GetOrCreateTenant(tenant);
    EXPECT_EQ(found.get(), created.get());
    EXPECT_EQ(builds, 1u);
}

TEST(MetadataCatalogTest, ConcurrentCreationPublishesOneWinningCatalog) {
    std::atomic<size_t> builds{0};
    MetadataCatalog catalog([&builds](const TenantId&) {
        builds.fetch_add(1, std::memory_order_relaxed);
        return std::make_shared<TenantCatalog>();
    });
    const TenantId tenant("tenant-race");

    constexpr int kRacers = 16;
    std::vector<std::shared_ptr<TenantCatalog>> observed(kRacers);
    std::atomic<size_t> ready{0};
    std::atomic<bool> start{false};
    std::vector<std::thread> threads;
    threads.reserve(kRacers);
    for (int i = 0; i < kRacers; ++i) {
        threads.emplace_back([&, i] {
            ready.fetch_add(1, std::memory_order_acq_rel);
            while (!start.load(std::memory_order_acquire)) {
                std::this_thread::yield();
            }
            observed[i] = catalog.GetOrCreateTenant(tenant);
        });
    }
    while (ready.load(std::memory_order_acquire) < kRacers) {
        std::this_thread::yield();
    }
    start.store(true, std::memory_order_release);
    for (auto& thread : threads) {
        thread.join();
    }

    for (int i = 0; i < kRacers; ++i) {
        ASSERT_NE(observed[i], nullptr) << "racer " << i;
        EXPECT_EQ(observed[i].get(), observed[0].get())
            << "racer " << i << " kept a losing catalog";
    }
    EXPECT_EQ(catalog.Lookup(tenant).get(), observed[0].get());
    // The factory runs for the winner only, so a race leaves no orphan catalog
    // (and no orphan quota account) behind.
    EXPECT_EQ(builds.load(std::memory_order_relaxed), 1u);
}

TEST(MetadataCatalogTest, RemoveDropsTheTenantButNotTheHandle) {
    MetadataCatalog catalog(MakeTenant);
    ASSERT_NE(catalog.GetOrCreateTenant(TenantId("tenant-a")), nullptr);
    ASSERT_NE(catalog.GetOrCreateTenant(TenantId("tenant-b")), nullptr);

    // The removed handle still addresses the tenant it resolved, while the
    // registry creates a fresh one for the same id.
    auto removed = catalog.Lookup(TenantId("tenant-a"));
    ASSERT_NE(removed, nullptr);
    catalog.Remove(TenantId("tenant-a"));
    EXPECT_EQ(catalog.Lookup(TenantId("tenant-a")), nullptr);
    ASSERT_TRUE(removed->InsertObject(test::MakeObjectEntry("k1")));
    EXPECT_FALSE(removed->Empty());

    auto recreated = catalog.GetOrCreateTenant(TenantId("tenant-a"));
    ASSERT_NE(recreated, nullptr);
    EXPECT_NE(recreated.get(), removed.get());
    EXPECT_TRUE(recreated->Empty());
    ASSERT_NE(catalog.Lookup(TenantId("tenant-b")), nullptr);
}

TEST(MetadataCatalogTest, RebuildGroupStateReachesEveryTenant) {
    MetadataCatalog catalog(MakeTenant);
    std::vector<std::shared_ptr<TenantCatalog>> tenants;
    for (const auto* name : {"tenant-a", "tenant-b"}) {
        tenants.push_back(catalog.GetOrCreateTenant(TenantId(name)));
    }
    for (auto& tenant : tenants) {
        auto first = test::MakeObjectEntry("k1", "g1");
        auto second = test::MakeObjectEntry("k2", "g1");
        ASSERT_TRUE(tenant->InsertObject(first));
        ASSERT_TRUE(tenant->InsertObject(second));
        // A restored tenant starts without membership.
        tenant->UnregisterGroupMember(first);
        tenant->UnregisterGroupMember(second);
        ASSERT_TRUE(tenant->GroupMembers("g1").empty());
    }

    catalog.RebuildGroupState();

    for (const auto& tenant : tenants) {
        EXPECT_EQ(tenant->GroupMembers("g1").size(), 2u);
    }
}

}  // namespace
}  // namespace metadata
}  // namespace mooncake
