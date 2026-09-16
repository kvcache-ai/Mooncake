#include "tenant/tenant_directory.h"

#include <algorithm>
#include <atomic>
#include <memory>
#include <thread>
#include <vector>

#include <gtest/gtest.h>

namespace mooncake {
namespace metadata {
namespace {

using TestDirectory = TenantDirectory<std::shared_ptr<int>>;

TEST(TenantDirectoryTest, UpsertLookupRemoveRoundTrip) {
    TestDirectory dir;
    const TenantId tenant("tenant-a");

    // Empty container: no tenant.
    EXPECT_EQ(dir.Lookup(tenant), nullptr);

    dir.Upsert(tenant, std::make_shared<int>(42));
    auto handle = dir.Lookup(tenant);
    ASSERT_NE(handle, nullptr);
    EXPECT_EQ(*handle, 42);

    dir.Remove(tenant);
    EXPECT_EQ(dir.Lookup(tenant), nullptr);
}

TEST(TenantDirectoryTest,
     CowRemoveKeepsOutstandingHandleAliveAndDistinguishesRecreate) {
    TestDirectory dir;
    const TenantId tenant("tenant-a");

    dir.Upsert(tenant, std::make_shared<int>(7));
    auto old_handle = dir.Lookup(tenant);  // reader that "escaped" the snapshot

    // Remove the tenant: the directory forgets it, but the outstanding handle
    // still owns the old target (no use-after-free).
    dir.Remove(tenant);
    EXPECT_EQ(dir.Lookup(tenant), nullptr);

    // A handle obtained before the remove is still valid and still points to
    // the *old* target even after the tenant id is recreated.
    ASSERT_NE(old_handle, nullptr);
    EXPECT_EQ(*old_handle, 7);

    dir.Upsert(tenant, std::make_shared<int>(99));
    auto new_handle = dir.Lookup(tenant);
    ASSERT_NE(new_handle, nullptr);
    EXPECT_EQ(*new_handle, 99);
    EXPECT_NE(old_handle.get(), new_handle.get());  // distinct generation
    EXPECT_EQ(*old_handle, 7);                      // old generation untouched
}

TEST(TenantDirectoryTest, GetOrCreateReturnsWinningHandleToAllRacers) {
    TestDirectory dir;
    const TenantId tenant("tenant-get-or-create");

    // Miss keeps the directory empty until the factory result is published.
    auto created =
        dir.GetOrCreate(tenant, [] { return std::make_shared<int>(1); });
    ASSERT_NE(created, nullptr);
    EXPECT_EQ(*created, 1);

    // Hit returns the published handle and never invokes the factory.
    bool factory_ran = false;
    auto hit = dir.GetOrCreate(tenant, [&] {
        factory_ran = true;
        return std::make_shared<int>(2);
    });
    EXPECT_FALSE(factory_ran);
    EXPECT_EQ(hit.get(), created.get());
    EXPECT_EQ(*hit, 1);

    // Concurrent racers for an ABSENT tenant must all observe the ONE winning
    // handle, even though every loser also ran its factory. This is the
    // guarantee a plain Lookup + Upsert pair cannot make.
    constexpr int kRacers = 16;
    dir.Remove(tenant);
    EXPECT_EQ(dir.Lookup(tenant), nullptr);
    std::vector<std::shared_ptr<int>> observed(kRacers);
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
            observed[i] = dir.GetOrCreate(
                tenant, [] { return std::make_shared<int>(-1); });
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
    EXPECT_EQ(dir.Lookup(tenant).get(), observed[0].get())
        << "the winning handle must be the reachable one";
}

TEST(TenantDirectoryTest, ConcurrentReadsDuringWritesAreSafeAndLinearizable) {
    TestDirectory dir;
    const TenantId tenant("tenant-a");
    dir.Upsert(tenant, std::make_shared<int>(0));

    constexpr int kWriterIterations = 200'000;
    std::atomic<bool> readers_stopped{false};
    std::atomic<int> invalid_reads{0};

    const auto reader = [&]() {
        while (!readers_stopped.load(std::memory_order_relaxed)) {
            auto handle = dir.Lookup(tenant);
            if (handle != nullptr) {
                const int v = *handle;
                if (v != 0 && v != 1) {
                    invalid_reads.fetch_add(1, std::memory_order_relaxed);
                }
            }
        }
    };

    std::vector<std::thread> readers;
    for (int i = 0; i < 8; ++i) {
        readers.emplace_back(reader);
    }

    for (int i = 0; i < kWriterIterations; ++i) {
        dir.Upsert(tenant, std::make_shared<int>(i % 2));
    }

    readers_stopped.store(true, std::memory_order_relaxed);
    for (auto& t : readers) {
        t.join();
    }

    EXPECT_EQ(invalid_reads.load(std::memory_order_relaxed), 0);
    auto final_handle = dir.Lookup(tenant);
    ASSERT_NE(final_handle, nullptr);
    EXPECT_TRUE(*final_handle == 0 || *final_handle == 1);
}

TEST(TenantDirectoryTest, VisitEnumeratesEveryTenantSnapshotConsistently) {
    TestDirectory dir;
    dir.Upsert(TenantId("tenant-a"), std::make_shared<int>(1));
    dir.Upsert(TenantId("tenant-b"), std::make_shared<int>(2));
    dir.Upsert(TenantId("tenant-c"), std::make_shared<int>(3));

    // Visit sees all three (order unspecified).
    std::vector<std::string> seen;
    dir.Visit([&](const TenantId& tenant, const std::shared_ptr<int>& handle) {
        seen.push_back(tenant.value());
        EXPECT_NE(handle, nullptr);
    });
    EXPECT_EQ(seen.size(), 3u);
    for (const auto& t : {"tenant-a", "tenant-b", "tenant-c"}) {
        EXPECT_TRUE(std::find(seen.begin(), seen.end(), t) != seen.end());
    }

    // A sibling upsert after the snapshot is taken is not guaranteed to be seen
    // by an in-flight Visit, but a fresh Visit sees it.
    dir.Upsert(TenantId("tenant-d"), std::make_shared<int>(4));
    size_t count_after = 0;
    dir.Visit(
        [&](const TenantId&, const std::shared_ptr<int>&) { ++count_after; });
    EXPECT_EQ(count_after, 4u);
}

}  // namespace
}  // namespace metadata
}  // namespace mooncake
