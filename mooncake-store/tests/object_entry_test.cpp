#include "object_entry.h"
#include "object_test_helpers.h"

#include <memory>
#include <utility>

#include <gtest/gtest.h>

namespace mooncake {
namespace {

TEST(ObjectEntryTest, OwnsMetadataEnvelopeFromConstruction) {
    auto metadata = test::MakeObjectMetadata("k1");
    auto* raw = metadata.get();
    auto entry = std::make_shared<ObjectEntry>(std::move(metadata));

    EXPECT_EQ(&entry->metadata(), raw);
    EXPECT_EQ(entry->metadata().size, 128u);

    // The callback observes the same envelope the accessor exposed.
    bool called = false;
    entry->WithMetadata([&](ObjectMetadata& m) {
        called = true;
        EXPECT_EQ(&m, raw);
        m.object_checksum = 42;
    });
    EXPECT_TRUE(called);
    ASSERT_TRUE(entry->metadata().object_checksum.has_value());
    EXPECT_EQ(*entry->metadata().object_checksum, 42u);
    EXPECT_EQ(&entry->metadata(), raw);  // envelope stays wired
}

TEST(ObjectEntryTest, TryLockUniqueReportsWhetherTheEntryWasFree) {
    auto entry = test::MakeObjectEntry("k1");

    // Free -> an owning lock the caller keeps for as long as it is in scope.
    auto first = entry->TryLockUnique();
    ASSERT_TRUE(first.owns_lock());

    // Held -> an empty lock.
    auto second = entry->TryLockUnique();
    EXPECT_FALSE(second.owns_lock());

    first.unlock();
    auto third = entry->TryLockUnique();
    EXPECT_TRUE(third.owns_lock());
}

}  // namespace
}  // namespace mooncake
