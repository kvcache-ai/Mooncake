#include "tenant/object_entry.h"

#include <chrono>
#include <memory>
#include <thread>
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
        std::vector<Replica>{}, std::nullopt, false,
        ObjectDataType::UNKNOWN, std::string{}, TenantId(), user_key);
}

TEST(ObjectEntryTest, HoldsKeyGroupAndGroupStatus) {
    ObjectEntry singleton("k1", "");
    EXPECT_EQ(singleton.key(), "k1");
    EXPECT_EQ(singleton.group_id(), "");
    EXPECT_FALSE(singleton.IsGrouped());

    ObjectEntry member("k2", "g1");
    EXPECT_EQ(member.key(), "k2");
    EXPECT_EQ(member.group_id(), "g1");
    EXPECT_TRUE(member.IsGrouped());
}

TEST(ObjectEntryTest, WiresOwnOrSharedLease) {
    ObjectEntry entry("k1", "");
    EXPECT_EQ(entry.lease(), nullptr);

    auto own = std::make_shared<Lease>();
    entry.set_lease(own);
    EXPECT_EQ(entry.lease().get(), own.get());

    // An entry can be re-pointed at a group's shared lease.
    auto shared = std::make_shared<Lease>();
    entry.set_lease(shared);
    EXPECT_EQ(entry.lease().get(), shared.get());
}

TEST(ObjectEntryTest, ConsolidatesPerKeyRuntimeTaskState) {
    ObjectEntry entry("k1", "g1");
    EXPECT_FALSE(entry.replication_task.has_value());
    EXPECT_FALSE(entry.promotion_task.has_value());
    EXPECT_FALSE(entry.promotion_candidate.has_value());
    EXPECT_FALSE(entry.offloading_task.has_value());
    EXPECT_FALSE(entry.dynamic_replication_pending.has_value());
    EXPECT_FALSE(entry.is_processing);

    entry.is_processing = true;
    entry.replication_task = ReplicationTask{};   // consolidation target
    entry.promotion_task = PromotionTask{};
    entry.promotion_candidate = PromotionCandidate{};
    entry.offloading_task = OffloadingTask{};
    entry.dynamic_replication_pending = DynamicReplicaPending{};

    EXPECT_TRUE(entry.is_processing);
    EXPECT_TRUE(entry.replication_task.has_value());
    EXPECT_TRUE(entry.promotion_task.has_value());
    EXPECT_TRUE(entry.promotion_candidate.has_value());
    EXPECT_TRUE(entry.offloading_task.has_value());
    EXPECT_TRUE(entry.dynamic_replication_pending.has_value());
}

TEST(ObjectEntryTest, PerObjectMutexSerializesMutableAccess) {
    auto entry = std::make_shared<ObjectEntry>("k1", "");
    int shared_counter = 0;
    int writes = 0;

    const auto writer = [&]() {
        for (int i = 0; i < 10'000; ++i) {
            std::unique_lock<std::shared_mutex> lock(entry->mutex);
            ++writes;
        }
    };
    const auto reader = [&]() {
        for (int i = 0; i < 10'000; ++i) {
            std::shared_lock<std::shared_mutex> lock(entry->mutex);
            (void)shared_counter;
        }
    };

    std::vector<std::thread> threads;
    threads.emplace_back(writer);
    threads.emplace_back(reader);
    threads.emplace_back(reader);
    threads.emplace_back(writer);
    for (auto& t : threads) {
        t.join();
    }

    EXPECT_EQ(writes, 20'000);
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
    entry->metadata()->size;  // readable through the accessor

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

    // Taking ownership returns the previously-wired envelope and empties the slot.
    auto recovered = entry->TakeMetadata();
    EXPECT_EQ(recovered.get(), raw);
    EXPECT_FALSE(entry->has_metadata());
}

}  // namespace
}  // namespace tenant
}  // namespace mooncake
