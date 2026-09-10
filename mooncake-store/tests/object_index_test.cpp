#include "tenant/object_index.h"

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include <gtest/gtest.h>

namespace mooncake {
namespace metadata {
namespace {

// Build an entry with a minimal (128 B, replica-less) envelope; the store
// tests exercise routing/membership, not replica validity.
std::shared_ptr<ObjectEntry> MakeEntry(const std::string& key,
                                       const std::string& group_id) {
    return std::make_shared<ObjectEntry>(std::make_unique<ObjectMetadata>(
        UUID{1, 2}, std::chrono::system_clock::now(), 128,
        std::vector<Replica>{}, std::nullopt, false, ObjectDataType::UNKNOWN,
        group_id, TenantId(), key));
}

// --- Group membership ---

// --- Object route ---

TEST(ObjectIndexTest, InsertGetEraseContainsObjectCount) {
    ObjectIndex store;
    EXPECT_EQ(store.ObjectCount(), 0u);

    auto e1 = MakeEntry("k1", "");
    EXPECT_TRUE(store.Insert("k1", e1));
    EXPECT_TRUE(store.Contains("k1"));
    EXPECT_EQ(store.ObjectCount(), 1u);

    auto entry = store.Get("k1");
    ASSERT_NE(entry, nullptr);
    EXPECT_EQ(entry->key(), "k1");
    EXPECT_EQ(entry.get(), e1.get());  // same underlying entry

    EXPECT_EQ(store.Get("missing"), nullptr);
    // Identity-checked erase: only the pinned entry's slot may go, and the
    // second call is a no-op (slot already gone).
    EXPECT_TRUE(store.EraseIf("k1", e1.get()));
    EXPECT_FALSE(store.EraseIf("k1", e1.get()));
    EXPECT_FALSE(store.Contains("k1"));
    EXPECT_EQ(store.ObjectCount(), 0u);
}

TEST(ObjectIndexTest, InsertAssignsGenerationsAndIsCurrentTracksReplacement) {
    ObjectIndex store;
    auto e1 = MakeEntry("k1", "");
    ASSERT_TRUE(store.Insert("k1", e1));
    EXPECT_GT(e1->generation(), 0u);  // publication assigns a generation
    EXPECT_TRUE(store.IsCurrent("k1", e1.get()));

    EXPECT_TRUE(store.EraseIf("k1", e1.get()));
    EXPECT_FALSE(store.IsCurrent("k1", e1.get()));

    // A replacement of the same key gets a fresh, higher generation; the
    // stale instance is never current again.
    auto e2 = MakeEntry("k1", "");
    EXPECT_EQ(e2->generation(), 0u);  // unpublished
    ASSERT_TRUE(store.Insert("k1", e2));
    EXPECT_GT(e2->generation(), e1->generation());
    EXPECT_TRUE(store.IsCurrent("k1", e2.get()));
    EXPECT_FALSE(store.IsCurrent("k1", e1.get()));
}

TEST(ObjectIndexTest, DuplicateInsertIsRejected) {
    ObjectIndex store;
    store.Insert("k1", MakeEntry("k1", ""));
    // Second insert for the same key must not clobber the original.
    EXPECT_FALSE(store.Insert("k1", MakeEntry("k1", "")));
    EXPECT_EQ(store.ObjectCount(), 1u);
    ASSERT_NE(store.Get("k1"), nullptr);
    EXPECT_EQ(store.Get("k1")->key(), "k1");
}

TEST(ObjectIndexTest, SnapshotObjectsEnumeratesEveryEntry) {
    ObjectIndex store;
    store.Insert("k1", MakeEntry("k1", ""));
    store.Insert("k2", MakeEntry("k2", "g1"));
    store.Insert("k3", MakeEntry("k3", "g1"));

    std::vector<std::string> keys;
    for (const auto& entry : store.SnapshotObjects()) {
        keys.push_back(entry->key());
    }
    EXPECT_EQ(keys.size(), 3u);
    EXPECT_TRUE(std::find(keys.begin(), keys.end(), "k1") != keys.end());
    EXPECT_TRUE(std::find(keys.begin(), keys.end(), "k2") != keys.end());
    EXPECT_TRUE(std::find(keys.begin(), keys.end(), "k3") != keys.end());
}

// --- Accessors ---

TEST(ObjectIndexTest, WithObjectScopeRespectsPresenceAndAbsence) {
    ObjectIndex store;
    auto singleton = MakeEntry("k1", "");
    store.Insert("k1", singleton);
    auto& raw = singleton->metadata();

    // Present key -> WithObject reaches the envelope under the per-object
    // lock. Absent key -> callback is not invoked.
    bool called = false;
    store.WithObject("missing", [&](ObjectMetadata&) { FAIL(); });

    called = false;
    store.WithObject("k1", [&](ObjectMetadata& m) {
        called = true;
        EXPECT_EQ(&m, &raw);
    });
    EXPECT_TRUE(called);
}

}  // namespace
}  // namespace metadata
}  // namespace mooncake
