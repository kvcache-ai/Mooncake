#include <gtest/gtest.h>

#include <atomic>
#include <thread>

#include "local_ssd/manager.h"

namespace mooncake {
namespace {

OffloadTaskItem Offload(std::string tenant, std::string key, int64_t size) {
    return OffloadTaskItem{
        .tenant_id = std::move(tenant), .key = std::move(key), .size = size};
}

PromotionTaskItem Promotion(std::string tenant, std::string key, int64_t size) {
    return PromotionTaskItem{
        .tenant_id = std::move(tenant), .key = std::move(key), .size = size};
}

TEST(LocalSsdTaskMailboxTest, OffloadsAreTenantScopedDeduplicatedAndLimited) {
    LocalSsdTaskMailbox mailbox(true);
    EXPECT_TRUE(mailbox.EnqueueOffload(Offload("tenant-a", "key", 1), 2) ==
                ErrorCode::OK);
    EXPECT_TRUE(mailbox.EnqueueOffload(Offload("tenant-b", "key", 2), 2) ==
                ErrorCode::OK);
    EXPECT_TRUE(mailbox.EnqueueOffload(Offload("tenant-a", "key", 3), 3) ==
                ErrorCode::OBJECT_ALREADY_EXISTS);
    EXPECT_TRUE(mailbox.EnqueueOffload(Offload("tenant-c", "key", 3), 2) ==
                ErrorCode::KEYS_ULTRA_LIMIT);

    EXPECT_TRUE(mailbox.RemoveOffload(TenantId("tenant-a"), "key"));
    EXPECT_FALSE(mailbox.RemoveOffload(TenantId("tenant-a"), "key"));
    auto tasks = mailbox.SetOffloadingAndTakePending(true);
    ASSERT_EQ(tasks.size(), 1);
    EXPECT_EQ(tasks.front(), Offload("tenant-b", "key", 2));
    EXPECT_TRUE(mailbox.SetOffloadingAndTakePending(true).empty());
}

TEST(LocalSsdTaskMailboxTest, DisabledOffloadingDrainsPendingTasks) {
    LocalSsdTaskMailbox mailbox(true);
    ASSERT_TRUE(mailbox.EnqueueOffload(Offload("tenant", "key", 1), 10) ==
                ErrorCode::OK);
    auto tasks = mailbox.SetOffloadingAndTakePending(false);
    ASSERT_EQ(tasks.size(), 1);
    EXPECT_TRUE(mailbox.EnqueueOffload(Offload("tenant", "next", 2), 10) ==
                ErrorCode::UNABLE_OFFLOADING);
}

TEST(LocalSsdTaskMailboxTest, PromotionsAreBatchedAndRemoveAllIsConsumed) {
    LocalSsdTaskMailbox mailbox(false);
    EXPECT_TRUE(mailbox.EnqueuePromotion(Promotion("a", "key", 1)) ==
                ErrorCode::OK);
    EXPECT_TRUE(mailbox.EnqueuePromotion(Promotion("b", "key", 2)) ==
                ErrorCode::OK);
    EXPECT_TRUE(mailbox.EnqueuePromotion(Promotion("a", "key", 3)) ==
                ErrorCode::OBJECT_ALREADY_EXISTS);
    EXPECT_EQ(mailbox.TakePromotions(1).size(), 1);
    EXPECT_EQ(mailbox.TakePromotions(10).size(), 1);
    EXPECT_TRUE(mailbox.TakePromotions(10).empty());

    EXPECT_FALSE(mailbox.ConsumeRemoveAll());
    mailbox.RequestRemoveAll();
    EXPECT_TRUE(mailbox.ConsumeRemoveAll());
    EXPECT_FALSE(mailbox.ConsumeRemoveAll());
}

TEST(LocalSsdManagerTest, ManagesRegistrationCapacityAndUsage) {
    LocalSsdManager manager;
    UUID client{1, 2};
    EXPECT_TRUE(manager.RegisterClient(client, true) == ErrorCode::OK);
    EXPECT_TRUE(manager.RegisterClient(client, false) ==
                ErrorCode::SEGMENT_ALREADY_EXISTS);

    auto change = manager.ReportCapacity(client, 1024);
    ASSERT_TRUE(change.has_value());
    EXPECT_EQ(change->previous_bytes, 0);
    EXPECT_EQ(change->current_bytes, 1024);
    EXPECT_TRUE(manager.AdjustUsedBytes(client, 300));
    auto usage = manager.GetUsage(client);
    ASSERT_TRUE(usage.has_value());
    EXPECT_EQ(usage->total_capacity_bytes, 1024);
    EXPECT_EQ(usage->used_bytes, 300);

    auto capacity = manager.UnregisterClient(client);
    ASSERT_TRUE(capacity.has_value());
    EXPECT_EQ(*capacity, 1024);
    EXPECT_FALSE(manager.GetUsage(client).has_value());
    EXPECT_FALSE(manager.AdjustUsedBytes(client, 1));
}

TEST(LocalSsdManagerTest, UnregisterSerializesWithConcurrentOperations) {
    LocalSsdManager manager;
    UUID client{3, 4};
    ASSERT_TRUE(manager.RegisterClient(client, true) == ErrorCode::OK);
    std::atomic<bool> stop{false};
    std::thread worker([&] {
        while (!stop.load(std::memory_order_relaxed)) {
            manager.AdjustUsedBytes(client, 1);
            manager.GetUsage(client);
        }
    });
    auto removed = manager.UnregisterClient(client);
    stop.store(true, std::memory_order_relaxed);
    worker.join();
    EXPECT_TRUE(removed.has_value());
    EXPECT_FALSE(manager.GetUsage(client).has_value());
}

TEST(LocalSsdManagerTest, CancelsOffloadMirrorsOnlyWhenAllArePending) {
    LocalSsdManager manager;
    UUID client_a{10, 11};
    UUID client_b{12, 13};
    ASSERT_TRUE(manager.RegisterClient(client_a, true) == ErrorCode::OK);
    ASSERT_TRUE(manager.RegisterClient(client_b, true) == ErrorCode::OK);

    const auto enqueue_mirrors = [&] {
        ASSERT_TRUE(manager.EnqueueOffload(client_a,
                                           Offload("tenant", "key", 1),
                                           10) == ErrorCode::OK);
        ASSERT_TRUE(manager.EnqueueOffload(client_b,
                                           Offload("tenant", "key", 1),
                                           10) == ErrorCode::OK);
    };

    enqueue_mirrors();
    auto claimed = manager.SetOffloadingAndTakePending(client_b, true);
    ASSERT_TRUE(claimed.has_value());
    ASSERT_EQ(claimed->size(), 1);
    EXPECT_FALSE(manager.CancelOffloadsIfAllPending({client_a, client_b},
                                                    TenantId("tenant"), "key"));
    auto still_pending = manager.SetOffloadingAndTakePending(client_a, true);
    ASSERT_TRUE(still_pending.has_value());
    ASSERT_EQ(still_pending->size(), 1);

    enqueue_mirrors();
    EXPECT_TRUE(manager.CancelOffloadsIfAllPending({client_b, client_a},
                                                   TenantId("tenant"), "key"));
    EXPECT_TRUE(manager.SetOffloadingAndTakePending(client_a, true)->empty());
    EXPECT_TRUE(manager.SetOffloadingAndTakePending(client_b, true)->empty());
}

TEST(LocalSsdManagerTest, RestoreResetsRuntimeOnlyState) {
    LocalSsdManager manager;
    UUID client{5, 6};
    ASSERT_TRUE(manager.RegisterClient(client, true) == ErrorCode::OK);
    ASSERT_TRUE(manager.ReportCapacity(client, 2048).has_value());
    ASSERT_TRUE(manager.AdjustUsedBytes(client, 512));
    ASSERT_TRUE(manager.EnqueueOffload(client, Offload("tenant", "key", 10),
                                       10) == ErrorCode::OK);
    ASSERT_TRUE(manager.EnqueuePromotion(
                    client, Promotion("tenant", "key", 10)) == ErrorCode::OK);
    manager.RequestRemoveAll();

    auto state = manager.ExportPersistedState();
    manager.RestorePersistedState(std::move(state));
    auto usage = manager.GetUsage(client);
    ASSERT_TRUE(usage.has_value());
    EXPECT_EQ(usage->total_capacity_bytes, 2048);
    EXPECT_EQ(usage->used_bytes, 0);
    EXPECT_TRUE(manager.TakePromotions(client, 10)->empty());
    EXPECT_FALSE(*manager.ConsumeRemoveAll(client));
    auto offloads = manager.SetOffloadingAndTakePending(client, true);
    ASSERT_TRUE(offloads.has_value());
    ASSERT_EQ(offloads->size(), 1);
    EXPECT_EQ(offloads->front(), Offload("tenant", "key", 10));
}

}  // namespace
}  // namespace mooncake
