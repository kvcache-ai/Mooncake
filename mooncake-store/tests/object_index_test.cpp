#include "object_index.h"
#include "object_test_helpers.h"

#include <algorithm>
#include <atomic>
#include <future>
#include <string>
#include <thread>
#include <vector>

#include <gtest/gtest.h>

namespace mooncake {
namespace {

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

TEST(ObjectIndexTest, ObjectsVisitsEveryEntryOnce) {
    ObjectIndex store;
    ASSERT_TRUE(store.Insert(test::MakeObjectEntry("k1")));
    ASSERT_TRUE(store.Insert(test::MakeObjectEntry("k2", "g1")));
    ASSERT_TRUE(store.Insert(test::MakeObjectEntry("k3", "g1")));

    std::vector<std::string> keys;
    for (auto object : store.Objects()) {
        EXPECT_EQ(object.key(), object.handle()->key());
        keys.push_back(object.key());
    }
    std::sort(keys.begin(), keys.end());
    EXPECT_EQ(keys, (std::vector<std::string>{"k1", "k2", "k3"}));

    size_t count = 0;
    for (auto object : ObjectIndex().Objects()) {
        (void)object;
        ++count;
    }
    EXPECT_EQ(count, 0u);
}

TEST(ObjectIndexTest, MutableObjectsWritesUnderTheEntryLock) {
    ObjectIndex store;
    auto entry = test::MakeObjectEntry("k1");
    ASSERT_TRUE(store.Insert(entry));

    for (auto object : store.MutableObjects()) {
        object.state().is_processing = true;
    }
    EXPECT_TRUE(entry->WithSharedAccess(
        [](const ObjectMetadata&, const ObjectEntry::State& state) {
            return state.is_processing;
        }));
}

// Leaving the loop early releases the stripe and the entry it stood on, so
// the route and the entry are writable again.
TEST(ObjectIndexTest, BreakingOutOfAWalkReleasesItsLocks) {
    ObjectIndex store;
    std::vector<std::shared_ptr<ObjectEntry>> entries;
    for (int i = 0; i < 8; ++i) {
        entries.push_back(test::MakeObjectEntry("k" + std::to_string(i)));
        ASSERT_TRUE(store.Insert(entries.back()));
    }
    for (auto object : store.Objects()) {
        (void)object;
        break;
    }
    for (const auto& entry : entries) {
        entry->WithExclusiveAccess([](ObjectMetadata&, ObjectEntry::State&) {});
        EXPECT_TRUE(store.EraseIf(entry->key(), entry));
    }
    EXPECT_TRUE(store.Empty());
}

// An entry locked elsewhere is not waited for under the stripe lock: the walk
// visits everything else first, then waits for it.
TEST(ObjectIndexTest, BusyEntryIsVisitedAfterTheOthers) {
    ObjectIndex store;
    auto busy = test::MakeObjectEntry("busy");
    ASSERT_TRUE(store.Insert(busy));
    for (int i = 0; i < 8; ++i) {
        ASSERT_TRUE(
            store.Insert(test::MakeObjectEntry("k" + std::to_string(i))));
    }

    std::promise<void> held;
    std::promise<void> release;
    std::thread holder([&] {
        busy->WithExclusiveAccess([&](ObjectMetadata&, ObjectEntry::State&) {
            held.set_value();
            release.get_future().wait();
        });
    });
    held.get_future().wait();

    std::vector<std::string> keys;
    for (auto object : store.Objects()) {
        keys.push_back(object.key());
        if (keys.size() == 8) {
            release.set_value();
        }
    }
    holder.join();
    ASSERT_EQ(keys.size(), 9u);
    EXPECT_EQ(keys.back(), "busy");
}

// A busy entry is visited only while the route still publishes it: one
// replaced before the walk gets its lock is skipped. Whether the replacement
// is seen depends on whether its stripe was walked yet, so only the replaced
// entry's absence is checked.
TEST(ObjectIndexTest, BusyEntryReplacedMeanwhileIsSkipped) {
    ObjectIndex store;
    auto busy = test::MakeObjectEntry("busy");
    ASSERT_TRUE(store.Insert(busy));
    ASSERT_TRUE(store.Insert(test::MakeObjectEntry("other")));

    std::promise<void> held;
    std::promise<void> replace;
    std::thread holder([&] {
        busy->WithExclusiveAccess([&](ObjectMetadata&, ObjectEntry::State&) {
            held.set_value();
            replace.get_future().wait();
            // Entry, then route: the order every teardown takes.
            EXPECT_TRUE(store.EraseIf("busy", busy));
            EXPECT_TRUE(store.Insert(test::MakeObjectEntry("busy")));
        });
    });
    held.get_future().wait();

    std::vector<std::shared_ptr<ObjectEntry>> visited;
    for (auto object : store.Objects()) {
        visited.push_back(object.handle());
        if (object.key() == "other") {
            replace.set_value();
        }
    }
    holder.join();
    EXPECT_EQ(std::count(visited.begin(), visited.end(), busy), 0);
    EXPECT_EQ(std::count_if(visited.begin(), visited.end(),
                            [](const auto& e) { return e->key() == "other"; }),
              1);
}

#ifndef NDEBUG
TEST(ObjectIndexDeathTest, RouteAccessFromAWalkAsserts) {
    ObjectIndex store;
    ASSERT_TRUE(store.Insert(test::MakeObjectEntry("k1")));
    EXPECT_DEATH(
        {
            for (auto object : store.Objects()) {
                (void)store.Contains(object.key());
            }
        },
        "route access from inside an object walk");
}
#endif

}  // namespace
}  // namespace mooncake
