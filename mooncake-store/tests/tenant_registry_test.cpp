#include "tenant/tenant_registry.h"
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
std::shared_ptr<TenantMetadata> MakeTenant(const TenantId&) {
    return std::make_shared<TenantMetadata>();
}

TEST(TenantRegistryTest, GetOrCreateTenantBuildsOncePerTenantId) {
    size_t builds = 0;
    TenantRegistry registry([&builds](const TenantId&) {
        ++builds;
        return std::make_shared<TenantMetadata>();
    });
    const TenantId tenant("tenant-a");
    EXPECT_EQ(registry.Lookup(tenant), nullptr);

    auto created = registry.GetOrCreateTenant(tenant);
    ASSERT_NE(created, nullptr);
    EXPECT_EQ(builds, 1u);
    EXPECT_EQ(registry.Lookup(tenant).get(), created.get());

    // A second call finds the published tenant instead of building another.
    auto found = registry.GetOrCreateTenant(tenant);
    EXPECT_EQ(found.get(), created.get());
    EXPECT_EQ(builds, 1u);
}

TEST(TenantRegistryTest, ConcurrentCreationPublishesOneWinningTenant) {
    std::atomic<size_t> builds{0};
    TenantRegistry registry([&builds](const TenantId&) {
        builds.fetch_add(1, std::memory_order_relaxed);
        return std::make_shared<TenantMetadata>();
    });
    const TenantId tenant("tenant-race");

    constexpr int kRacers = 16;
    std::vector<std::shared_ptr<TenantMetadata>> observed(kRacers);
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
            observed[i] = registry.GetOrCreateTenant(tenant);
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
            << "racer " << i << " kept a losing handle";
    }
    EXPECT_EQ(registry.Lookup(tenant).get(), observed[0].get());
    // The factory runs for the winner only, so a race leaves no orphan tenant
    // behind.
    EXPECT_EQ(builds.load(std::memory_order_relaxed), 1u);
}

TEST(TenantRegistryTest, RemoveDropsTheTenantButNotTheHandle) {
    TenantRegistry registry(MakeTenant);
    ASSERT_NE(registry.GetOrCreateTenant(TenantId("tenant-a")), nullptr);
    ASSERT_NE(registry.GetOrCreateTenant(TenantId("tenant-b")), nullptr);

    // The removed handle still addresses the tenant it resolved, while the
    // registry creates a fresh one for the same id.
    auto removed = registry.Lookup(TenantId("tenant-a"));
    ASSERT_NE(removed, nullptr);
    registry.Remove(TenantId("tenant-a"));
    EXPECT_EQ(registry.Lookup(TenantId("tenant-a")), nullptr);
    ASSERT_TRUE(removed->InsertObject(test::MakeObjectEntry("k1")));
    EXPECT_FALSE(removed->Empty());

    auto recreated = registry.GetOrCreateTenant(TenantId("tenant-a"));
    ASSERT_NE(recreated, nullptr);
    EXPECT_NE(recreated.get(), removed.get());
    EXPECT_TRUE(recreated->Empty());
    ASSERT_NE(registry.Lookup(TenantId("tenant-b")), nullptr);
}

TEST(TenantRegistryTest, VisitReachesEveryTenantAndCarriesABroadcast) {
    TenantRegistry registry(MakeTenant);
    std::vector<std::shared_ptr<TenantMetadata>> tenants;
    for (const auto* name : {"tenant-a", "tenant-b"}) {
        tenants.push_back(registry.GetOrCreateTenant(TenantId(name)));
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

    // The broadcast a snapshot restore needs: one walk reaches every tenant,
    // and the callback receives the handle the registry publishes.
    size_t visited = 0;
    registry.Visit([&](const TenantId& tenant_id,
                       const std::shared_ptr<TenantMetadata>& tenant) {
        EXPECT_NE(tenant, nullptr) << tenant_id.value();
        ++visited;
        tenant->RebuildGroupState();
    });

    EXPECT_EQ(visited, tenants.size());
    for (const auto& tenant : tenants) {
        EXPECT_EQ(tenant->GroupMembers("g1").size(), 2u);
    }
}

}  // namespace
}  // namespace metadata
}  // namespace mooncake
