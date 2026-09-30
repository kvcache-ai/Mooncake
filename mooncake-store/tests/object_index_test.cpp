#include "object_index.h"
#include "object_test_helpers.h"

#include <algorithm>
#include <string>
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

}  // namespace
}  // namespace mooncake
