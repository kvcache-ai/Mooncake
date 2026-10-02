#include "object_index.h"
#include "object_test_helpers.h"

#include <algorithm>
#include <atomic>
#include <cstddef>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include <gtest/gtest.h>

namespace mooncake {
namespace {

// The stripe counts a route can be built with: one stripe makes every key share
// one lock, the rest spread keys over that many.
constexpr size_t kStripeCounts[] = {1, 4, 16, 64};

TEST(ObjectIndexTest, InsertPublishesTheEntryUnderItsKey) {
    ObjectIndex store;
    EXPECT_EQ(store.ObjectCount(), 0u);

    auto e1 = test::MakeObjectEntry("k1");
    EXPECT_TRUE(store.Insert(e1));
    EXPECT_TRUE(store.Contains("k1"));
    EXPECT_EQ(store.ObjectCount(), 1u);

    auto entry = store.Get("k1");
    ASSERT_NE(entry, nullptr);
    EXPECT_EQ(entry.get(), e1.get());  // the route hands back the same entry
    EXPECT_EQ(entry->key(), "k1");
    EXPECT_EQ(store.Get("missing"), nullptr);
}

TEST(ObjectIndexTest, EraseIfHonoursEntryIdentity) {
    ObjectIndex store;
    auto e1 = test::MakeObjectEntry("k1");
    ASSERT_TRUE(store.Insert(e1));

    // The route points at e1, so a stale pointer may not erase it.
    auto replacement = test::MakeObjectEntry("k1");
    EXPECT_FALSE(store.EraseIf("k1", replacement));
    EXPECT_TRUE(store.Contains("k1"));

    EXPECT_TRUE(store.EraseIf("k1", e1));
    EXPECT_FALSE(store.EraseIf("k1", e1));  // slot already gone
    EXPECT_FALSE(store.Contains("k1"));
    EXPECT_EQ(store.ObjectCount(), 0u);
}

TEST(ObjectIndexTest, DuplicateInsertIsRejected) {
    ObjectIndex store;
    auto winner = test::MakeObjectEntry("k1");
    ASSERT_TRUE(store.Insert(winner));

    // The loser must neither clobber the routed entry nor look published: it
    // never reaches the route, so the slot still names the winner and the
    // handle it kept stands for no publication.
    auto loser = test::MakeObjectEntry("k1");
    EXPECT_FALSE(store.Insert(loser));
    EXPECT_FALSE(loser->IsPublished());
    EXPECT_EQ(store.ObjectCount(), 1u);
    EXPECT_EQ(store.Get("k1").get(), winner.get());
}

TEST(ObjectIndexTest, IsPublishedMarksTheOnePublicationOfAnInstance) {
    ObjectIndex store;
    auto entry = test::MakeObjectEntry("k1");
    EXPECT_FALSE(entry->IsPublished());

    ASSERT_TRUE(store.Insert(entry));
    EXPECT_TRUE(entry->IsPublished());

    // The claim is what forbids publishing this instance again, so erasing its
    // slot does not take it back.
    ASSERT_TRUE(store.EraseIf("k1", entry));
    EXPECT_TRUE(entry->IsPublished());
}

TEST(ObjectIndexTest, IsCurrentComparesTheHandleTheRoutePublishes) {
    ObjectIndex store;
    auto e1 = test::MakeObjectEntry("k1");
    EXPECT_FALSE(store.IsCurrent("k1", e1));  // never published
    EXPECT_FALSE(store.IsCurrent("k1", nullptr));
    EXPECT_FALSE(store.IsCurrent("k1", store.Get("k1")));

    ASSERT_TRUE(store.Insert(e1));
    EXPECT_TRUE(store.IsCurrent("k1", e1));
    // The handle a lookup hands back names the publication it found.
    EXPECT_TRUE(store.IsCurrent("k1", store.Get("k1")));

    // Another instance under the same key is a different publication, and a
    // key the route does not hold publishes nothing.
    EXPECT_FALSE(store.IsCurrent("k1", test::MakeObjectEntry("k1")));
    EXPECT_FALSE(store.IsCurrent("k2", e1));

    // Nothing is current once the slot is gone.
    ASSERT_TRUE(store.EraseIf("k1", e1));
    EXPECT_FALSE(store.IsCurrent("k1", e1));

    // A replacement is current under its own handle only.
    auto e2 = test::MakeObjectEntry("k1");
    ASSERT_TRUE(store.Insert(e2));
    EXPECT_FALSE(store.IsCurrent("k1", e1));
    EXPECT_TRUE(store.IsCurrent("k1", e2));
}

TEST(ObjectIndexTest, SnapshotObjectsEnumeratesEveryEntry) {
    ObjectIndex store;
    ASSERT_TRUE(store.Insert(test::MakeObjectEntry("k1")));
    ASSERT_TRUE(store.Insert(test::MakeObjectEntry("k2", "g1")));
    ASSERT_TRUE(store.Insert(test::MakeObjectEntry("k3", "g1")));

    std::vector<std::string> keys;
    for (const auto& entry : store.SnapshotObjects()) {
        keys.push_back(entry->key());
    }
    // Enumeration order is the map's, so compare as a set.
    std::sort(keys.begin(), keys.end());
    EXPECT_EQ(keys, (std::vector<std::string>{"k1", "k2", "k3"}));
}

TEST(ObjectIndexTest, EveryStripeCountRoutesTheSameWay) {
    for (const size_t count : kStripeCounts) {
        ObjectIndex store{count};
        EXPECT_EQ(store.StripeCount(), count) << count;
        EXPECT_TRUE(store.Empty()) << count;
        EXPECT_EQ(store.ObjectCount(), 0u) << count;

        auto entry = test::MakeObjectEntry("k1");
        ASSERT_TRUE(store.Insert(entry)) << count;
        EXPECT_TRUE(store.Contains("k1")) << count;
        EXPECT_EQ(store.ObjectCount(), 1u) << count;
        EXPECT_FALSE(store.Empty()) << count;

        // Whichever stripe the key landed in, the route hands back the same
        // handle.
        EXPECT_EQ(store.Get("k1").get(), entry.get()) << count;
        EXPECT_EQ(store.Get("missing"), nullptr) << count;
        EXPECT_TRUE(store.IsCurrent("k1", entry)) << count;

        // Identity still decides the erase, at any count.
        EXPECT_FALSE(store.EraseIf("k1", test::MakeObjectEntry("k1"))) << count;
        EXPECT_TRUE(store.Contains("k1")) << count;
        EXPECT_TRUE(store.EraseIf("k1", entry)) << count;
        EXPECT_FALSE(store.IsCurrent("k1", entry)) << count;
        EXPECT_TRUE(store.Empty()) << count;
        EXPECT_EQ(store.ObjectCount(), 0u) << count;
    }
}

TEST(ObjectIndexTest, AKeyLandsInOneStripeHoweverOftenItIsReached) {
    for (const size_t count : kStripeCounts) {
        ObjectIndex store{count};
        auto entry = test::MakeObjectEntry("k1");
        ASSERT_TRUE(store.Insert(entry)) << count;

        // Every accessor hashes the key the same way, so repeated lookups all
        // reach the stripe the insert placed the entry in.
        for (int i = 0; i < 64; ++i) {
            EXPECT_TRUE(store.Contains("k1")) << count;
            EXPECT_EQ(store.Get("k1").get(), entry.get()) << count;
            EXPECT_TRUE(store.IsCurrent("k1", entry)) << count;
        }

        // The snapshot walks every stripe, so the key every lookup found is the
        // one handle it hands back.
        const auto snapshot = store.SnapshotObjects();
        ASSERT_EQ(snapshot.size(), 1u) << count;
        EXPECT_EQ(snapshot[0].get(), entry.get()) << count;

        EXPECT_TRUE(store.EraseIf("k1", entry)) << count;
        EXPECT_TRUE(store.Empty()) << count;
    }
}

// The handles a snapshot hands back are strong: nothing but the collection
// itself has to keep the publication alive, so the route slot can go while what
// the snapshot named is still readable.
TEST(ObjectIndexTest, SnapshotObjectsHoldsTheHandlesItCollected) {
    ObjectIndex store;
    auto entry = test::MakeObjectEntry("k1");
    ASSERT_TRUE(store.Insert(entry));

    const auto snapshot = store.SnapshotObjects();
    ASSERT_EQ(snapshot.size(), 1u);
    // The caller drops its own reference: the route slot and the snapshot are
    // what hold the entry now.
    entry.reset();
    EXPECT_EQ(snapshot[0]->key(), "k1");

    ASSERT_TRUE(store.EraseIf("k1", snapshot[0]));
    EXPECT_TRUE(store.Empty());
    EXPECT_EQ(snapshot[0]->key(), "k1");
    EXPECT_FALSE(store.IsCurrent("k1", snapshot[0]));
    EXPECT_FALSE(store.Contains("k1"));
    EXPECT_EQ(store.Get("k1"), nullptr);
}

// A collection taken while the route is mutated. Each stripe is copied under
// its own lock, so a key that lands in one stripe is named at most once and
// every handle handed back was routed when its stripe was copied; which instant
// each stripe contributes is not fixed, because the stripes are walked one
// after another. This pins what the walk guarantees while it overlaps a mutator
// that keeps replacing keys.
TEST(ObjectIndexTest, SnapshotObjectsOverlappingMutationNamesEachKeyOnce) {
    constexpr size_t kKeys = 512;
    constexpr int kCollections = 200;
    ObjectIndex store{16};

    std::vector<std::shared_ptr<ObjectEntry>> live;
    live.reserve(kKeys);
    for (size_t i = 0; i < kKeys; ++i) {
        live.push_back(test::MakeObjectEntry("k" + std::to_string(i)));
        ASSERT_TRUE(store.Insert(live[i]));
    }

    std::atomic<bool> stop{false};
    std::atomic<int> failures{0};
    std::thread mutating([&] {
        size_t at = 0;
        while (!stop.load(std::memory_order_relaxed)) {
            const std::string key = live[at]->key();
            auto fresh = test::MakeObjectEntry(key);
            if (!store.EraseIf(key, live[at]) || !store.Insert(fresh)) {
                failures.fetch_add(1, std::memory_order_relaxed);
            }
            live[at] = std::move(fresh);
            at = (at + 1) % live.size();
        }
    });

    for (int round = 0; round < kCollections; ++round) {
        const auto snapshot = store.SnapshotObjects();
        std::vector<std::string> keys;
        keys.reserve(snapshot.size());
        for (const auto& entry : snapshot) {
            ASSERT_NE(entry, nullptr);
            keys.push_back(entry->key());
        }
        std::sort(keys.begin(), keys.end());
        EXPECT_EQ(std::adjacent_find(keys.begin(), keys.end()), keys.end())
            << "a key landed in two stripes, or one was walked twice";
    }

    stop.store(true, std::memory_order_relaxed);
    mutating.join();
    EXPECT_EQ(failures.load(), 0);
    // The mutator only ever replaces a key, so the route holds every one of
    // them however the collections overlapped it.
    EXPECT_EQ(store.ObjectCount(), kKeys);
}

TEST(ObjectIndexTest, ConcurrentInsertOfDistinctKeysPublishesEveryOne) {
    constexpr size_t kThreads = 4;
    constexpr size_t kKeysPerThread = 500;

    for (const size_t count : kStripeCounts) {
        ObjectIndex store{count};
        // Keys are distinct and each thread owns its own, so no two threads
        // contend for the same route slot and every insert publishes.
        std::vector<std::shared_ptr<ObjectEntry>> entries;
        entries.reserve(kThreads * kKeysPerThread);
        for (size_t t = 0; t < kThreads; ++t) {
            for (size_t i = 0; i < kKeysPerThread; ++i) {
                entries.push_back(test::MakeObjectEntry(
                    "t" + std::to_string(t) + "_k" + std::to_string(i)));
            }
        }

        std::atomic<int> rejected{0};
        std::vector<std::thread> threads;
        threads.reserve(kThreads);
        for (size_t t = 0; t < kThreads; ++t) {
            threads.emplace_back([&, t] {
                for (size_t i = 0; i < kKeysPerThread; ++i) {
                    if (!store.Insert(entries[t * kKeysPerThread + i])) {
                        rejected.fetch_add(1, std::memory_order_relaxed);
                    }
                }
            });
        }
        for (auto& thread : threads) {
            thread.join();
        }

        EXPECT_EQ(rejected.load(), 0) << count;
        EXPECT_EQ(store.ObjectCount(), entries.size()) << count;
        EXPECT_FALSE(store.Empty()) << count;
        for (const auto& entry : entries) {
            EXPECT_EQ(store.Get(entry->key()).get(), entry.get()) << count;
        }
    }
}

}  // namespace
}  // namespace mooncake
