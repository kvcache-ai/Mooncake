#include "metadata/tenant_registry.h"
#include "object_test_helpers.h"

#include <algorithm>
#include <atomic>
#include <cassert>
#include <cstdint>
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
    // Racers that missed the lookup may each build, but only one build is
    // published and every racer holds it.
    EXPECT_GE(builds.load(std::memory_order_relaxed), 1u);
    EXPECT_LE(builds.load(std::memory_order_relaxed),
              static_cast<size_t>(kRacers));
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
        tenant->UnregisterGroupMember(first);
        tenant->UnregisterGroupMember(second);
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

TEST(TenantRegistryTest, VisitWalksTheTenantsPresentWhenItStarted) {
    TenantRegistry registry(MakeTenant);
    ASSERT_NE(registry.GetOrCreateTenant(TenantId("tenant-a")), nullptr);
    ASSERT_NE(registry.GetOrCreateTenant(TenantId("tenant-b")), nullptr);

    // Creating and removing tenants from inside the walk is allowed and must
    // not change what that walk sees: it iterates the tenants present when it
    // started, not the registry's current ones.
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

    // The next walk sees the changes.
    std::vector<std::string> after;
    registry.Visit(
        [&](const TenantId& tenant_id, const std::shared_ptr<Tenant>&) {
            after.push_back(tenant_id.value());
        });
    ASSERT_EQ(after.size(), 2u);
    EXPECT_NE(std::find(after.begin(), after.end(), "tenant-b"), after.end());
    EXPECT_NE(std::find(after.begin(), after.end(), "tenant-c"), after.end());
}

TEST(TenantRegistryTest, MixedLookupCreateRemoveAndVisitStayConsistent) {
    // Every tenant this registry builds arrives with one object already
    // inserted, so a reader that finds a tenant without it saw the tenant
    // published before it had finished being built.
    TenantRegistry registry([](const TenantId&) {
        auto tenant = std::make_shared<Tenant>();
        [[maybe_unused]] const bool inserted =
            tenant->InsertObject(test::MakeObjectEntry("k1"));
        assert(inserted);
        return tenant;
    });
    const std::vector<TenantId> ids = {
        TenantId("tenant-a"), TenantId("tenant-b"), TenantId("tenant-c"),
        TenantId("tenant-d")};
    constexpr int kRemoverRounds = 20000;
    // The other threads yield between rounds: std::shared_mutex prefers
    // readers on glibc, and readers that never pause starve the removers on a
    // machine with few cores. The caps only guard against a hang.
    constexpr uint64_t kOtherRounds = 200000;
    constexpr uint64_t kWalks = 20000;

    std::atomic<bool> removers_done{false};
    std::atomic<int> violations{0};
    std::atomic<uint64_t> walks{0};
    const auto violation = [&] {
        violations.fetch_add(1, std::memory_order_relaxed);
    };
    const auto whole = [](const std::shared_ptr<Tenant>& tenant) {
        return tenant != nullptr && tenant->ObjectCount() == 1;
    };

    std::vector<std::thread> threads;
    // Removers first, so the others can stop when they are done.
    for (int r = 0; r < 2; ++r) {
        threads.emplace_back([&, r] {
            for (int round = 0; round < kRemoverRounds; ++round) {
                registry.Remove(ids[(round + r) % ids.size()]);
            }
        });
    }
    for (int c = 0; c < 2; ++c) {
        threads.emplace_back([&, c] {
            for (uint64_t i = c; i < kOtherRounds &&
                                 !removers_done.load(std::memory_order_relaxed);
                 ++i) {
                std::this_thread::yield();
                if (!whole(registry.GetOrCreateTenant(ids[i % ids.size()]))) {
                    violation();
                }
            }
        });
    }
    for (int l = 0; l < 4; ++l) {
        threads.emplace_back([&, l] {
            for (uint64_t i = l; i < kOtherRounds &&
                                 !removers_done.load(std::memory_order_relaxed);
                 ++i) {
                std::this_thread::yield();
                const auto tenant = registry.Lookup(ids[i % ids.size()]);
                if (tenant != nullptr && !whole(tenant)) {
                    violation();
                }
            }
        });
    }
    // A walker creates and removes tenants from inside its own callback, which
    // must neither deadlock nor disturb the walk it is part of.
    threads.emplace_back([&] {
        uint64_t calls = 0;
        for (uint64_t walk = 0;
             walk < kWalks && !removers_done.load(std::memory_order_relaxed);
             ++walk) {
            std::this_thread::yield();
            std::vector<std::string> seen;
            registry.Visit([&](const TenantId& tenant_id,
                               const std::shared_ptr<Tenant>& tenant) {
                if (std::find(seen.begin(), seen.end(), tenant_id.value()) !=
                        seen.end() ||
                    !whole(tenant)) {
                    violation();
                }
                seen.push_back(tenant_id.value());
                const auto& other = ids[++calls % ids.size()];
                if (calls % 2 == 0) {
                    (void)registry.GetOrCreateTenant(other);
                } else {
                    registry.Remove(other);
                }
            });
            walks.fetch_add(1, std::memory_order_relaxed);
        }
    });

    threads[0].join();
    threads[1].join();
    removers_done.store(true, std::memory_order_relaxed);
    for (size_t t = 2; t < threads.size(); ++t) {
        threads[t].join();
    }

    EXPECT_EQ(violations.load(), 0);
    EXPECT_GT(walks.load(), 0u);
    for (const auto& tenant_id : ids) {
        const auto tenant = registry.Lookup(tenant_id);
        EXPECT_TRUE(tenant == nullptr || whole(tenant)) << tenant_id.value();
    }
}

}  // namespace
}  // namespace metadata
}  // namespace mooncake
