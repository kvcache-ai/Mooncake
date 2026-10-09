#include "object_entry.h"
#include "object_index.h"
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

    // The envelope is wired from construction on, so a callback always sees
    // one and there is nothing to null-check at the call sites.
    entry->WithUnpublished([&](ObjectMetadata& m, ObjectEntry::State&) {
        EXPECT_EQ(&m, raw);
        EXPECT_EQ(m.size, 128u);
        m.object_checksum = 42;
    });

    test::ObjectEntryTestPeer::WithSharedAccess(
        *entry, [&](const ObjectMetadata& m, const ObjectEntry::State& state) {
            ASSERT_TRUE(m.object_checksum.has_value());
            EXPECT_EQ(*m.object_checksum, 42u);
            EXPECT_FALSE(state.is_processing);
        });
}

TEST(ObjectEntryTest, AConstructedEntryIsNotYetPublished) {
    auto entry = test::MakeObjectEntry("k1");

    // Publication is the route's claim on an entry, so an entry that was only
    // constructed stands for no publication yet.
    EXPECT_FALSE(entry->IsPublished());
}

TEST(ObjectEntryTest, AccessorsSeeTheStateWrittenUnderTheLock) {
    auto entry = test::MakeObjectEntry("k1");
    entry->WithUnpublished([](ObjectMetadata&, ObjectEntry::State& state) {
        state.is_processing = true;
    });

    // A shared reader sees the write, and its callback's value comes back.
    const bool processing = test::ObjectEntryTestPeer::WithSharedAccess(
        *entry, [](const ObjectMetadata&, const ObjectEntry::State& state) {
            return state.is_processing;
        });
    EXPECT_TRUE(processing);
}

#ifndef NDEBUG
// Once a route publishes the entry it belongs to the tenant, so the path for
// filling in an entry before publication refuses it.
TEST(ObjectEntryDeathTest, WithUnpublishedRefusesAPublishedEntry) {
    ObjectIndex index;
    auto entry = test::MakeObjectEntry("k1");
    ASSERT_TRUE(index.Insert(entry));
    EXPECT_DEATH(
        entry->WithUnpublished([](ObjectMetadata&, ObjectEntry::State&) {}),
        "IsPublished");
}
#endif

}  // namespace
}  // namespace mooncake
