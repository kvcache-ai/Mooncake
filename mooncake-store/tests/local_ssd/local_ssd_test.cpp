#include <gtest/gtest.h>

#include <atomic>
#include <limits>
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

TEST(LocalSsdTaskMailboxTest, CountModeIgnoresConfiguredByteLimits) {
    LocalSsdTaskMailbox mailbox(true, {1, false});
    ASSERT_EQ(mailbox.EnqueueOffload(Offload("t", "a", 10), 2), ErrorCode::OK);
    ASSERT_EQ(mailbox.EnqueueOffload(Offload("t", "b", 10), 2), ErrorCode::OK);
    EXPECT_EQ(mailbox.EnqueueOffload(Offload("t", "c", 1), 2),
              ErrorCode::KEYS_ULTRA_LIMIT);
    EXPECT_EQ(mailbox.SetOffloadingAndTakePending(true).size(), 2);
}

TEST(LocalSsdTaskMailboxTest, ByteLimitReplacesCountLimitAndReclaimsCapacity) {
    LocalSsdTaskMailbox mailbox(true, {10, true});
    EXPECT_EQ(mailbox.EnqueueOffload(Offload("t", "zero", 0), 0),
              ErrorCode::INVALID_PARAMS);
    EXPECT_EQ(mailbox.EnqueueOffload(Offload("t", "negative", -1), 0),
              ErrorCode::INVALID_PARAMS);
    ASSERT_EQ(mailbox.EnqueueOffload(Offload("t", "a", 4), 0), ErrorCode::OK);
    EXPECT_EQ(mailbox.EnqueueOffload(Offload("t", "a", 4), 0),
              ErrorCode::OBJECT_ALREADY_EXISTS);
    ASSERT_EQ(mailbox.EnqueueOffload(Offload("t", "b", 6), 0), ErrorCode::OK);
    EXPECT_EQ(mailbox.EnqueueOffload(Offload("t", "full", 1), 100),
              ErrorCode::KEYS_ULTRA_LIMIT);
    ASSERT_TRUE(mailbox.RemoveOffload(TenantId("t"), "a"));
    EXPECT_EQ(mailbox.EnqueueOffload(Offload("t", "c", 4), 0), ErrorCode::OK);
    EXPECT_EQ(mailbox.SetOffloadingAndTakePending(true).size(), 2);
    EXPECT_EQ(mailbox.EnqueueOffload(Offload("t", "all", 10), 0),
              ErrorCode::OK);
}

TEST(LocalSsdTaskMailboxTest, CapacityModeDrainsWholeQueueAndReclaimsCapacity) {
    LocalSsdTaskMailbox mailbox(true, {12, true});
    for (int i = 0; i < 4; ++i)
        ASSERT_EQ(mailbox.EnqueueOffload(Offload("t", std::to_string(i), 3), 1),
                  ErrorCode::OK);
    EXPECT_EQ(mailbox.SetOffloadingAndTakePending(true).size(), 4);
    ASSERT_EQ(mailbox.EnqueueOffload(Offload("t", "new", 12), 1),
              ErrorCode::OK);
    EXPECT_EQ(mailbox.SetOffloadingAndTakePending(false).size(), 1);
    EXPECT_TRUE(mailbox.SetOffloadingAndTakePending(true).empty());
    EXPECT_EQ(mailbox.EnqueueOffload(Offload("t", "again", 12), 1),
              ErrorCode::OK);
}

TEST(LocalSsdTaskMailboxTest, PayloadCounterDoesNotOverflow) {
    LocalSsdTaskMailbox mailbox(true);
    const auto size = std::numeric_limits<int64_t>::max();
    ASSERT_EQ(mailbox.EnqueueOffload(Offload("t", "a", size), 4),
              ErrorCode::OK);
    ASSERT_EQ(mailbox.EnqueueOffload(Offload("t", "b", size), 4),
              ErrorCode::OK);
    EXPECT_EQ(mailbox.EnqueueOffload(Offload("t", "c", size), 4),
              ErrorCode::KEYS_ULTRA_LIMIT);
    EXPECT_EQ(mailbox.SetOffloadingAndTakePending(true).size(), 2);
    EXPECT_EQ(mailbox.EnqueueOffload(Offload("t", "d", size), 4),
              ErrorCode::OK);
}

TEST(LocalSsdManagerTest, ByteAccountingSurvivesMirroredCancel) {
    LocalSsdManager manager({10, true});
    UUID a{101, 102}, b{103, 104};
    ASSERT_EQ(manager.RegisterClient(a, true), ErrorCode::OK);
    ASSERT_EQ(manager.RegisterClient(b, true), ErrorCode::OK);
    for (const auto& client : {a, b})
        ASSERT_EQ(manager.EnqueueOffload(client, Offload("t", "key", 10), 0),
                  ErrorCode::OK);
    EXPECT_TRUE(
        manager.CancelOffloadsIfAllPending({a, b}, TenantId("t"), "key"));
    for (const auto& client : {a, b})
        ASSERT_EQ(manager.EnqueueOffload(client, Offload("t", "next", 10), 0),
                  ErrorCode::OK);
    EXPECT_EQ(manager.EnqueueOffload(a, Offload("t", "full", 1), 0),
              ErrorCode::KEYS_ULTRA_LIMIT);
    EXPECT_EQ(manager.RemoveOffloadFromAll(TenantId("t"), "next"), 2);
    EXPECT_EQ(manager.EnqueueOffload(a, Offload("t", "again", 10), 0),
              ErrorCode::OK);
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

TEST(LocalSsdTaskMailboxTest, PromotionsAreDeliveredByRecency) {
    LocalSsdTaskMailbox mailbox(false);
    ASSERT_TRUE(mailbox.EnqueuePromotion(Promotion("t", "a", 1)) ==
                ErrorCode::OK);
    ASSERT_TRUE(mailbox.EnqueuePromotion(Promotion("t", "b", 2)) ==
                ErrorCode::OK);
    ASSERT_TRUE(mailbox.EnqueuePromotion(Promotion("t", "c", 3)) ==
                ErrorCode::OK);

    // Without any touch, the most recent enqueue is delivered first.
    auto first = mailbox.TakePromotions(1);
    ASSERT_EQ(first.size(), 1);
    EXPECT_EQ(first.front(), Promotion("t", "c", 3));

    // A touch re-marks an older entry ahead of newer ones.
    ASSERT_TRUE(mailbox.TouchPromotion(TenantId("t"), "a"));
    auto second = mailbox.TakePromotions(1);
    ASSERT_EQ(second.size(), 1);
    EXPECT_EQ(second.front(), Promotion("t", "a", 1));

    // A duplicate enqueue keeps the dedup contract but refreshes recency.
    ASSERT_TRUE(mailbox.EnqueuePromotion(Promotion("t", "b", 2)) ==
                ErrorCode::OBJECT_ALREADY_EXISTS);
    auto third = mailbox.TakePromotions(1);
    ASSERT_EQ(third.size(), 1);
    EXPECT_EQ(third.front(), Promotion("t", "b", 2));

    EXPECT_TRUE(mailbox.TakePromotions(10).empty());
    EXPECT_FALSE(mailbox.TouchPromotion(TenantId("t"), "missing"));
}

TEST(LocalSsdTaskMailboxTest, TakePromotionsKeepsGlobalRecencyAcrossTicks) {
    LocalSsdTaskMailbox mailbox(false);
    for (const char* key : {"k1", "k2", "k3", "k4", "k5"}) {
        ASSERT_TRUE(mailbox.EnqueuePromotion(Promotion("t", key, 1)) ==
                    ErrorCode::OK);
    }
    ASSERT_TRUE(mailbox.TouchPromotion(TenantId("t"), "k2"));

    // k2 (touched) first, then the newest enqueue. The batch limit must not
    // degrade the remainder to hash order on the next tick.
    auto batch = mailbox.TakePromotions(2);
    ASSERT_EQ(batch.size(), 2);
    EXPECT_EQ(batch[0], Promotion("t", "k2", 1));
    EXPECT_EQ(batch[1], Promotion("t", "k5", 1));

    auto rest = mailbox.TakePromotions(10);
    ASSERT_EQ(rest.size(), 3);
    EXPECT_EQ(rest[0], Promotion("t", "k4", 1));
    EXPECT_EQ(rest[1], Promotion("t", "k3", 1));
    EXPECT_EQ(rest[2], Promotion("t", "k1", 1));
}

TEST(LocalSsdManagerTest, TouchPromotionIsClientScoped) {
    LocalSsdManager manager;
    UUID client{7, 8};
    UUID other{9, 10};
    ASSERT_TRUE(manager.RegisterClient(client, false) == ErrorCode::OK);
    ASSERT_TRUE(manager.RegisterClient(other, false) == ErrorCode::OK);
    ASSERT_TRUE(manager.EnqueuePromotion(client, Promotion("t", "a", 1)) ==
                ErrorCode::OK);
    ASSERT_TRUE(manager.EnqueuePromotion(client, Promotion("t", "b", 2)) ==
                ErrorCode::OK);

    // Unknown clients and foreign clients cannot touch the entry.
    EXPECT_FALSE(manager.TouchPromotion(UUID{11, 12}, TenantId("t"), "a"));
    EXPECT_FALSE(manager.TouchPromotion(other, TenantId("t"), "a"));

    ASSERT_TRUE(manager.TouchPromotion(client, TenantId("t"), "a"));
    auto taken = manager.TakePromotions(client, 1);
    ASSERT_EQ(taken->size(), 1);
    EXPECT_EQ(taken->front(), Promotion("t", "a", 1));
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

}  // namespace
}  // namespace mooncake
