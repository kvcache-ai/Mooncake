#include "tenant/object_entry.h"

#include <chrono>
#include <memory>
#include <vector>

#include <gtest/gtest.h>

namespace mooncake {
namespace tenant {
namespace {

// Build a real metadata envelope for the accessor tests. ObjectMetadata is
// non-movable, so it is owned through a unique_ptr and constructed in place.
std::unique_ptr<ObjectMetadata> MakeMetadata(const std::string& user_key) {
    return std::make_unique<ObjectMetadata>(
        UUID{1, 2}, std::chrono::system_clock::now(), 128,
        std::vector<Replica>{}, std::nullopt, false, ObjectDataType::UNKNOWN,
        std::string{}, TenantId(), user_key);
}

TEST(ObjectEntryTest, HoldsAndScopesMetadataEnvelope) {
    auto entry = std::make_shared<ObjectEntry>("k1", "");
    // Not wired yet: metadata is null and callback accessors are no-ops.
    EXPECT_FALSE(entry->has_metadata());
    EXPECT_EQ(entry->metadata(), nullptr);
    EXPECT_FALSE(entry->TakeMetadata());
    bool called = false;
    entry->WithMetadata([&](ObjectMetadata&) { called = true; });
    EXPECT_FALSE(called);

    // Attach ownership of a metadata envelope.
    auto metadata = MakeMetadata("k1");
    auto* raw = metadata.get();
    auto prior = entry->SetMetadata(std::move(metadata));
    EXPECT_EQ(prior, nullptr);  // nothing owned before
    EXPECT_TRUE(entry->has_metadata());
    EXPECT_EQ(entry->metadata(), raw);
    EXPECT_EQ(entry->metadata()->size, 128u);  // readable through the accessor

    // WithMetadata runs the callback while the per-object lock is held, and the
    // callback observes the same envelope the accessor exposed.
    called = false;
    entry->WithMetadata([&](ObjectMetadata& m) {
        called = true;
        EXPECT_EQ(&m, raw);
        m.object_checksum = 42;
    });
    EXPECT_TRUE(called);
    EXPECT_TRUE(entry->metadata()->object_checksum.has_value());
    EXPECT_EQ(*entry->metadata()->object_checksum, 42u);

    // Taking ownership returns the previously-wired envelope and empties the
    // slot.
    auto recovered = entry->TakeMetadata();
    EXPECT_EQ(recovered.get(), raw);
    EXPECT_FALSE(entry->has_metadata());
}

}  // namespace
}  // namespace tenant
}  // namespace mooncake
