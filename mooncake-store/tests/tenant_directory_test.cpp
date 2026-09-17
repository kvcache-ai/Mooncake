#include "tenant/tenant_directory.h"

#include <algorithm>
#include <atomic>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include <gtest/gtest.h>

namespace mooncake {
namespace metadata {
namespace {

using TestDirectory = TenantDirectory<std::shared_ptr<int>>;

TEST(TenantDirectoryTest, RemovingATenantKeepsAnOutstandingHandleAlive) {
    TestDirectory directory;
    const TenantId tenant("tenant-a");
    EXPECT_EQ(directory.Lookup(tenant), nullptr);

    directory.Upsert(tenant, std::make_shared<int>(7));
    auto before = directory.Lookup(tenant);

    // Removing the tenant drops it from the directory, and the handle a reader
    // took earlier still owns the old target.
    directory.Remove(tenant);
    EXPECT_EQ(directory.Lookup(tenant), nullptr);
    ASSERT_NE(before, nullptr);
    EXPECT_EQ(*before, 7);

    // A later tenant with the same id is a new target; the old handle keeps
    // pointing at the old one.
    directory.Upsert(tenant, std::make_shared<int>(99));
    auto after = directory.Lookup(tenant);
    ASSERT_NE(after, nullptr);
    EXPECT_EQ(*after, 99);
    EXPECT_NE(before.get(), after.get());
    EXPECT_EQ(*before, 7);
}

TEST(TenantDirectoryTest, GetOrCreateGivesEveryRacerTheWinningHandle) {
    TestDirectory directory;
    const TenantId tenant("tenant-get-or-create");

    auto created =
        directory.GetOrCreate(tenant, [] { return std::make_shared<int>(1); });
    ASSERT_NE(created, nullptr);
    EXPECT_EQ(*created, 1);

    // A second call finds the tenant and leaves the factory alone.
    bool factory_ran = false;
    auto found = directory.GetOrCreate(tenant, [&] {
        factory_ran = true;
        return std::make_shared<int>(2);
    });
    EXPECT_FALSE(factory_ran);
    EXPECT_EQ(found.get(), created.get());
    EXPECT_EQ(*found, 1);

    // Racers for an absent tenant: the winner's factory is the only one that
    // runs, and every racer comes back with the handle it published.
    constexpr int kRacers = 16;
    directory.Remove(tenant);
    EXPECT_EQ(directory.Lookup(tenant), nullptr);
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
            observed[i] = directory.GetOrCreate(
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
    EXPECT_EQ(directory.Lookup(tenant).get(), observed[0].get());
}

TEST(TenantDirectoryTest, LookupsDuringWritesSeeOneWholePublish) {
    TestDirectory directory;
    const TenantId tenant("tenant-a");
    directory.Upsert(tenant, std::make_shared<int>(0));

    constexpr int kWriterIterations = 200'000;
    std::atomic<bool> readers_stopped{false};
    std::atomic<int> torn_reads{0};

    // Every write publishes a new int, so a reader that ever sees a value that
    // was never published saw a half-applied frame.
    const auto reader = [&]() {
        while (!readers_stopped.load(std::memory_order_relaxed)) {
            auto handle = directory.Lookup(tenant);
            if (handle != nullptr) {
                const int value = *handle;
                if (value != 0 && value != 1) {
                    torn_reads.fetch_add(1, std::memory_order_relaxed);
                }
            }
        }
    };

    std::vector<std::thread> readers;
    for (int i = 0; i < 8; ++i) {
        readers.emplace_back(reader);
    }

    for (int i = 0; i < kWriterIterations; ++i) {
        directory.Upsert(tenant, std::make_shared<int>(i % 2));
    }

    readers_stopped.store(true, std::memory_order_relaxed);
    for (auto& thread : readers) {
        thread.join();
    }

    EXPECT_EQ(torn_reads.load(std::memory_order_relaxed), 0);
    auto final_handle = directory.Lookup(tenant);
    ASSERT_NE(final_handle, nullptr);
    EXPECT_TRUE(*final_handle == 0 || *final_handle == 1);
}

TEST(TenantDirectoryTest, VisitWalksTheFrameItLoaded) {
    TestDirectory directory;
    directory.Upsert(TenantId("tenant-a"), std::make_shared<int>(1));
    directory.Upsert(TenantId("tenant-b"), std::make_shared<int>(2));

    // Publishing from inside the walk is allowed and must not change what that
    // walk sees: it iterates the frame it loaded, not the directory's current
    // one.
    std::vector<std::string> seen;
    bool published = false;
    directory.Visit([&](const TenantId& tenant,
                        const std::shared_ptr<int>& handle) {
        seen.push_back(tenant.value());
        EXPECT_NE(handle, nullptr);
        if (!published) {
            published = true;
            directory.Upsert(TenantId("tenant-c"), std::make_shared<int>(3));
            directory.Remove(TenantId("tenant-a"));
        }
    });

    EXPECT_EQ(seen.size(), 2u);
    EXPECT_NE(std::find(seen.begin(), seen.end(), "tenant-a"), seen.end());
    EXPECT_NE(std::find(seen.begin(), seen.end(), "tenant-b"), seen.end());
    EXPECT_EQ(std::find(seen.begin(), seen.end(), "tenant-c"), seen.end());

    // The published frame is what the next walk sees.
    std::vector<std::string> after;
    directory.Visit([&](const TenantId& tenant, const std::shared_ptr<int>&) {
        after.push_back(tenant.value());
    });
    ASSERT_EQ(after.size(), 2u);
    EXPECT_NE(std::find(after.begin(), after.end(), "tenant-b"), after.end());
    EXPECT_NE(std::find(after.begin(), after.end(), "tenant-c"), after.end());
}

}  // namespace
}  // namespace metadata
}  // namespace mooncake
