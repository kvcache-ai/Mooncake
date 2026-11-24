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
    EXPECT_FALSE(manager.HasClient(client));
    EXPECT_TRUE(manager.RegisterClient(client, true) == ErrorCode::OK);
    EXPECT_TRUE(manager.HasClient(client));
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
    EXPECT_FALSE(manager.HasClient(client));
    EXPECT_FALSE(manager.GetUsage(client).has_value());
    EXPECT_FALSE(manager.AdjustUsedBytes(client, 1));
    EXPECT_FALSE(manager.UnregisterClient(client).has_value());
}

TEST(LocalSsdManagerTest, ReportsCapacityDeltasAndConsumesRemoveAll) {
    LocalSsdManager manager;
    UUID client_a{20, 21};
    UUID client_b{22, 23};
    UUID unknown{24, 25};

    auto bad_unknown_cap = manager.ReportCapacity(unknown, 1024);
    ASSERT_FALSE(bad_unknown_cap.has_value());
    EXPECT_TRUE(bad_unknown_cap.error() == ErrorCode::SEGMENT_NOT_FOUND);

    auto bad_unknown_poll = manager.ConsumeRemoveAll(unknown);
    ASSERT_FALSE(bad_unknown_poll.has_value());
    EXPECT_TRUE(bad_unknown_poll.error() == ErrorCode::SEGMENT_NOT_FOUND);

    ASSERT_TRUE(manager.RegisterClient(client_a, true) == ErrorCode::OK);
    ASSERT_TRUE(manager.RegisterClient(client_b, true) == ErrorCode::OK);

    auto negative_cap = manager.ReportCapacity(client_a, -1);
    ASSERT_FALSE(negative_cap.has_value());
    EXPECT_TRUE(negative_cap.error() == ErrorCode::INVALID_PARAMS);

    auto c1 = manager.ReportCapacity(client_a, 4096);
    ASSERT_TRUE(c1.has_value());
    EXPECT_EQ(c1->previous_bytes, 0);
    EXPECT_EQ(c1->current_bytes, 4096);

    auto c2 = manager.ReportCapacity(client_a, 4096);
    ASSERT_TRUE(c2.has_value());
    EXPECT_EQ(c2->previous_bytes, 4096);
    EXPECT_EQ(c2->current_bytes, 4096);

    auto c3 = manager.ReportCapacity(client_a, 8192);
    ASSERT_TRUE(c3.has_value());
    EXPECT_EQ(c3->previous_bytes, 4096);
    EXPECT_EQ(c3->current_bytes, 8192);

    auto c4 = manager.ReportCapacity(client_a, 0);
    ASSERT_TRUE(c4.has_value());
    EXPECT_EQ(c4->previous_bytes, 8192);
    EXPECT_EQ(c4->current_bytes, 0);

    auto poll1 = manager.ConsumeRemoveAll(client_a);
    ASSERT_TRUE(poll1.has_value());
    EXPECT_FALSE(*poll1);

    manager.RequestRemoveAll({client_a});
    auto poll2 = manager.ConsumeRemoveAll(client_a);
    ASSERT_TRUE(poll2.has_value());
    EXPECT_TRUE(*poll2);
    auto poll3 = manager.ConsumeRemoveAll(client_a);
    ASSERT_TRUE(poll3.has_value());
    EXPECT_FALSE(*poll3);

    auto poll_b = manager.ConsumeRemoveAll(client_b);
    ASSERT_TRUE(poll_b.has_value());
    EXPECT_FALSE(*poll_b);
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
