#include "metadata/tenant_registry.h"
#include "object_test_helpers.h"

#include <algorithm>
#include <atomic>
#include <cassert>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include <gtest/gtest.h>

namespace mooncake {
namespace metadata {
namespace {

// Every tenant of one registry is built through the factory it was constructed
// with, so its tests can pass a plain one.
std::shared_ptr<Tenant> MakeTenant(const TenantId&) {
    return std::make_shared<Tenant>();
}

TEST(TenantRegistryTest, GetOrCreateTenantBuildsOncePerTenantId) {
    size_t builds = 0;
    TenantRegistry registry([&builds](const TenantId&) {
        ++builds;
        return std::make_shared<Tenant>();
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
        return std::make_shared<Tenant>();
    });
    const TenantId tenant("tenant-race");

    constexpr int kRacers = 16;
    std::vector<std::shared_ptr<Tenant>> observed(kRacers);
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
    std::vector<std::shared_ptr<Tenant>> tenants;
    for (const auto* name : {"tenant-a", "tenant-b"}) {
        tenants.push_back(registry.GetOrCreateTenant(TenantId(name)));
    }
    for (auto& tenant : tenants) {
        auto first = test::MakeObjectEntry("k1", "g1");
        auto second = test::MakeObjectEntry("k2", "g1");
        ASSERT_TRUE(tenant->InsertObject(first));
        ASSERT_TRUE(tenant->InsertObject(second));
        // A restored tenant starts without membership.
        tenant->UnregisterGroupMember(first, first->generation());
        tenant->UnregisterGroupMember(second, second->generation());
        ASSERT_TRUE(tenant->GroupMembers("g1").empty());
    }

    // The broadcast a snapshot restore needs: one walk reaches every tenant,
    // and the callback receives the handle the registry publishes.
    size_t visited = 0;
    registry.Visit(
        [&](const TenantId& tenant_id, const std::shared_ptr<Tenant>& tenant) {
            EXPECT_NE(tenant, nullptr) << tenant_id.value();
            ++visited;
            tenant->RebuildGroupState();
        });

    EXPECT_EQ(visited, tenants.size());
    for (const auto& tenant : tenants) {
        EXPECT_EQ(tenant->GroupMembers("g1").size(), 2u);
    }
}

TEST(TenantRegistryTest, VisitWalksTheFrameItLoaded) {
    TenantRegistry registry(MakeTenant);
    ASSERT_NE(registry.GetOrCreateTenant(TenantId("tenant-a")), nullptr);
    ASSERT_NE(registry.GetOrCreateTenant(TenantId("tenant-b")), nullptr);

    // Publishing from inside the walk is allowed and must not change what that
    // walk sees: it iterates the frame it loaded, not the registry's current
    // one.
    std::vector<std::string> seen;
    bool published = false;
    registry.Visit(
        [&](const TenantId& tenant_id, const std::shared_ptr<Tenant>& tenant) {
            seen.push_back(tenant_id.value());
            EXPECT_NE(tenant, nullptr);
            if (!published) {
                published = true;
                EXPECT_NE(registry.GetOrCreateTenant(TenantId("tenant-c")),
                          nullptr);
                registry.Remove(TenantId("tenant-a"));
            }
        });

    EXPECT_EQ(seen.size(), 2u);
    EXPECT_NE(std::find(seen.begin(), seen.end(), "tenant-a"), seen.end());
    EXPECT_NE(std::find(seen.begin(), seen.end(), "tenant-b"), seen.end());
    EXPECT_EQ(std::find(seen.begin(), seen.end(), "tenant-c"), seen.end());

    // The published frame is what the next walk sees.
    std::vector<std::string> after;
    registry.Visit(
        [&](const TenantId& tenant_id, const std::shared_ptr<Tenant>&) {
            after.push_back(tenant_id.value());
        });
    ASSERT_EQ(after.size(), 2u);
    EXPECT_NE(std::find(after.begin(), after.end(), "tenant-b"), after.end());
    EXPECT_NE(std::find(after.begin(), after.end(), "tenant-c"), after.end());
}

TEST(TenantRegistryTest, LookupsDuringWritesSeeOneWholePublish) {
    // Every tenant this registry builds arrives with one object already
    // inserted, so a reader that finds a tenant without it saw a frame that was
    // published before the tenant it names had finished being built.
    TenantRegistry registry([](const TenantId&) {
        auto tenant = std::make_shared<Tenant>();
        [[maybe_unused]] const bool inserted =
            tenant->InsertObject(test::MakeObjectEntry("k1"));
        assert(inserted);
        return tenant;
    });
    const TenantId tenant_id("tenant-a");
    ASSERT_NE(registry.GetOrCreateTenant(tenant_id), nullptr);

    constexpr int kWriterIterations = 50'000;
    std::atomic<bool> readers_stopped{false};
    std::atomic<int> torn_reads{0};

    const auto reader = [&]() {
        while (!readers_stopped.load(std::memory_order_relaxed)) {
            auto tenant = registry.Lookup(tenant_id);
            if (tenant != nullptr && tenant->ObjectCount() != 1) {
                torn_reads.fetch_add(1, std::memory_order_relaxed);
            }
        }
    };

    std::vector<std::thread> readers;
    for (int i = 0; i < 8; ++i) {
        readers.emplace_back(reader);
    }

    for (int i = 0; i < kWriterIterations; ++i) {
        registry.Remove(tenant_id);
        ASSERT_NE(registry.GetOrCreateTenant(tenant_id), nullptr);
    }

    readers_stopped.store(true, std::memory_order_relaxed);
    for (auto& thread : readers) {
        thread.join();
    }

    EXPECT_EQ(torn_reads.load(std::memory_order_relaxed), 0);
    auto final_tenant = registry.Lookup(tenant_id);
    ASSERT_NE(final_tenant, nullptr);
    EXPECT_EQ(final_tenant->ObjectCount(), 1u);
}

}  // namespace
}  // namespace metadata
}  // namespace mooncake
