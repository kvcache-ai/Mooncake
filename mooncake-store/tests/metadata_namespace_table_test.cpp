#include "metadata/namespace_table.h"
#include "object_test_helpers.h"

#include <algorithm>
#include <array>
#include <atomic>
#include <cassert>
#include <cstdint>
#include <memory>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include <gtest/gtest.h>

namespace mooncake {
namespace metadata {
namespace {

// Every namespace of one table is built through the factory it was
// constructed with, so its tests can pass a plain one.
std::unique_ptr<Namespace> MakeNamespace(const TenantId&) {
    return std::make_unique<Namespace>();
}

class NamespaceTableCreationTest : public ::testing::TestWithParam<TenantId> {};

TEST_P(NamespaceTableCreationTest, GetOrCreateBuildsOncePerId) {
    size_t builds = 0;
    NamespaceTable table([&builds](const TenantId&) {
        ++builds;
        return std::make_unique<Namespace>();
    });
    const TenantId& id = GetParam();
    EXPECT_EQ(table.Lookup(id), nullptr);

    Namespace& created = table.GetOrCreate(id);
    EXPECT_EQ(builds, 1u);
    EXPECT_EQ(table.Lookup(id), &created);

    // A second call finds the published namespace instead of building another.
    EXPECT_EQ(&table.GetOrCreate(id), &created);
    EXPECT_EQ(builds, 1u);
}

TEST_P(NamespaceTableCreationTest, ConcurrentCreationPublishesOneWinner) {
    std::atomic<size_t> builds{0};
    NamespaceTable table([&builds](const TenantId&) {
        builds.fetch_add(1, std::memory_order_relaxed);
        return std::make_unique<Namespace>();
    });
    const TenantId& id = GetParam();

    constexpr int kRacers = 16;
    std::vector<Namespace*> observed(kRacers);
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
            observed[i] = &table.GetOrCreate(id);
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
        EXPECT_EQ(observed[i], observed[0])
            << "racer " << i << " kept a losing build";
    }
    EXPECT_EQ(table.Lookup(id), observed[0]);
    // Racers that missed the lookup may each build, but only one build is
    // published and every racer holds it.
    EXPECT_GE(builds.load(std::memory_order_relaxed), 1u);
    EXPECT_LE(builds.load(std::memory_order_relaxed),
              static_cast<size_t>(kRacers));
}

// The default namespace resolves without the map lock, a named one through
// it; both must behave the same.
INSTANTIATE_TEST_SUITE_P(DefaultAndNamed, NamespaceTableCreationTest,
                         ::testing::Values(TenantId::Default(),
                                           TenantId("tenant-a")));

TEST(NamespaceTableTest, VisitReachesEveryNamespaceAndCarriesABroadcast) {
    NamespaceTable table(MakeNamespace);
    std::vector<Namespace*> created;
    for (const TenantId& id : {TenantId::Default(), TenantId("tenant-a")}) {
        created.push_back(&table.GetOrCreate(id));
    }
    for (Namespace* ns : created) {
        ASSERT_NE(test::PublishObject(ns->objects, "k1", "g1"), 0u);
        ASSERT_NE(test::PublishObject(ns->objects, "k2", "g1"), 0u);
        // A restored namespace starts without membership.
        test::ObjectRouteTestPeer::DropGroupMemberships(ns->objects);
        ASSERT_TRUE(ns->objects.GroupMembers("g1").empty());
    }

    // The broadcast a standby restore needs: one walk reaches every
    // namespace, the default one included.
    std::vector<Namespace*> visited;
    table.Visit([&](const TenantId& id, Namespace& ns) {
        EXPECT_EQ(table.Lookup(id), &ns) << id.value();
        visited.push_back(&ns);
        ns.objects.RebuildGroupState();
    });

    std::sort(visited.begin(), visited.end());
    std::sort(created.begin(), created.end());
    EXPECT_EQ(visited, created);
    for (const Namespace* ns : created) {
        EXPECT_EQ(ns->objects.GroupMembers("g1").size(), 2u);
    }
}

TEST(NamespaceTableTest, VisitWalksTheNamespacesPresentWhenItStarted) {
    NamespaceTable table(MakeNamespace);
    (void)table.GetOrCreate(TenantId("tenant-a"));
    (void)table.GetOrCreate(TenantId("tenant-b"));

    // Creating a namespace from inside the walk is allowed and must not change
    // what that walk sees: it iterates the namespaces present when it started.
    std::vector<std::string> seen;
    table.Visit([&](const TenantId& id, Namespace&) {
        seen.push_back(id.value());
        (void)table.GetOrCreate(TenantId("tenant-c"));
    });
    EXPECT_EQ(seen.size(), 2u);
    EXPECT_EQ(std::find(seen.begin(), seen.end(), "tenant-c"), seen.end());

    // The next walk sees it.
    std::vector<std::string> after;
    table.Visit(
        [&](const TenantId& id, Namespace&) { after.push_back(id.value()); });
    EXPECT_EQ(after.size(), 3u);
    EXPECT_NE(std::find(after.begin(), after.end(), "tenant-c"), after.end());
}

TEST(NamespaceTableTest, ConcurrentLookupCreateAndVisitStayConsistent) {
    // Every namespace this table builds arrives with one object already
    // inserted, so a reader that finds one without it saw the namespace
    // published before it had finished being built.
    NamespaceTable table([](const TenantId&) {
        auto ns = std::make_unique<Namespace>();
        [[maybe_unused]] const route::Generation generation =
            test::PublishObject(ns->objects, "k1");
        assert(generation != 0);
        return ns;
    });
    const std::vector<TenantId> ids = {
        TenantId::Default(), TenantId("tenant-a"), TenantId("tenant-b"),
        TenantId("tenant-c")};
    constexpr uint64_t kRounds = 20000;

    // Once found, a namespace stays where it was: every thread must keep
    // finding the address first published for its id.
    std::array<std::atomic<Namespace*>, 4> published{};
    std::atomic<int> violations{0};
    const auto check = [&](size_t index, Namespace* ns) {
        if (ns == nullptr) {
            return;
        }
        Namespace* expected = nullptr;
        if (!published[index].compare_exchange_strong(expected, ns) &&
            expected != ns) {
            violations.fetch_add(1, std::memory_order_relaxed);
        }
        if (ns->objects.ObjectCount() != 1) {
            violations.fetch_add(1, std::memory_order_relaxed);
        }
    };

    std::vector<std::thread> threads;
    for (int c = 0; c < 2; ++c) {
        threads.emplace_back([&, c] {
            for (uint64_t i = c; i < kRounds; ++i) {
                const size_t index = i % ids.size();
                check(index, &table.GetOrCreate(ids[index]));
            }
        });
    }
    for (int l = 0; l < 4; ++l) {
        threads.emplace_back([&, l] {
            for (uint64_t i = l; i < kRounds; ++i) {
                const size_t index = i % ids.size();
                check(index, table.Lookup(ids[index]));
            }
        });
    }
    // A walker creates namespaces from inside its own callback, which must
    // neither deadlock nor disturb the walk it is part of.
    threads.emplace_back([&] {
        for (uint64_t walk = 0; walk < kRounds / 10; ++walk) {
            std::vector<std::string> seen;
            table.Visit([&](const TenantId& id, Namespace& ns) {
                const size_t index =
                    std::find(ids.begin(), ids.end(), id) - ids.begin();
                if (std::find(seen.begin(), seen.end(), id.value()) !=
                    seen.end()) {
                    violations.fetch_add(1, std::memory_order_relaxed);
                }
                seen.push_back(id.value());
                check(index, &ns);
                (void)table.GetOrCreate(ids[walk % ids.size()]);
            });
        }
    });
    for (auto& thread : threads) {
        thread.join();
    }

    EXPECT_EQ(violations.load(), 0);
    for (size_t index = 0; index < ids.size(); ++index) {
        EXPECT_EQ(table.Lookup(ids[index]), published[index].load())
            << ids[index].value();
    }
}

}  // namespace
}  // namespace metadata
}  // namespace mooncake
