#include "tenant/object_entry.h"

#include <chrono>
#include <memory>
#include <vector>

#include <gtest/gtest.h>

namespace mooncake {
namespace metadata {
namespace {

// Build a real metadata envelope for the accessor tests. ObjectMetadata is
// non-movable, so it is owned through a unique_ptr and constructed in place.
std::unique_ptr<ObjectMetadata> MakeMetadata(const std::string& user_key) {
    return std::make_unique<ObjectMetadata>(
        UUID{1, 2}, std::chrono::system_clock::now(), 128,
        std::vector<Replica>{}, std::nullopt, false, ObjectDataType::UNKNOWN,
        std::string{}, TenantId(), user_key);
}

TEST(ObjectEntryTest, OwnsMetadataEnvelopeFromConstruction) {
    auto metadata = MakeMetadata("k1");
    auto* raw = metadata.get();
    auto entry = std::make_shared<ObjectEntry>(std::move(metadata));

    // The envelope is wired from construction on: metadata() returns a
    // reference, so there is nothing to null-check at the call sites.
    EXPECT_EQ(&entry->metadata(), raw);
    EXPECT_EQ(entry->metadata().size, 128u);

    // WithMetadata runs the callback while the per-object lock is held, and
    // the callback observes the same envelope the accessor exposed.
    bool called = false;
    entry->WithMetadata([&](ObjectMetadata& m) {
        called = true;
        EXPECT_EQ(&m, raw);
        m.object_checksum = 42;
    });
    EXPECT_TRUE(called);
    EXPECT_TRUE(entry->metadata().object_checksum.has_value());
    EXPECT_EQ(*entry->metadata().object_checksum, 42u);
    EXPECT_EQ(&entry->metadata(), raw);  // envelope stays wired
}

}  // namespace
}  // namespace metadata
}  // namespace mooncake
