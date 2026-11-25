#include <gtest/gtest.h>

#include <algorithm>
#include <atomic>
#include <string>
#include <thread>
#include <vector>

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

std::vector<std::string> Keys(const std::vector<OffloadTaskItem>& tasks) {
    std::vector<std::string> keys;
    keys.reserve(tasks.size());
    for (const auto& task : tasks) keys.push_back(task.key);
    return keys;
}

// offload_ordered_buckets: offload tasks come out in enqueue order, so the
// keys of one put stay adjacent and the store packs them into one bucket.
TEST(LocalSsdTaskMailboxTest, OrderedOffloadsAreDeliveredInEnqueueOrder) {
    LocalSsdTaskMailbox mailbox(true, /*ordered_offloads=*/true);
    // Neither name order nor insertion into a hash map matches this order.
    std::vector<std::string> enqueued;
    for (int i = 0; i < 50; ++i) {
        enqueued.push_back("k" + std::to_string((i * 37) % 50));
        ASSERT_TRUE(mailbox.EnqueueOffload(Offload("t", enqueued.back(), i),
                                           1000) == ErrorCode::OK);
    }
    // A removed key is skipped, a key removed and enqueued again goes last,
    // and a duplicate enqueue is still rejected.
    ASSERT_TRUE(mailbox.RemoveOffload(TenantId("t"), enqueued[3]));
    ASSERT_TRUE(mailbox.RemoveOffload(TenantId("t"), enqueued[10]));
    ASSERT_TRUE(mailbox.EnqueueOffload(Offload("t", enqueued[10], 10), 1000) ==
                ErrorCode::OK);
    ASSERT_TRUE(mailbox.EnqueueOffload(Offload("t", enqueued[5], 5), 1000) ==
                ErrorCode::OBJECT_ALREADY_EXISTS);

    std::vector<OffloadTaskItem> expected;
    for (int i = 0; i < 50; ++i) {
        if (i == 3 || i == 10) continue;
        expected.push_back(Offload("t", enqueued[i], i));
    }
    expected.push_back(Offload("t", enqueued[10], 10));
    EXPECT_EQ(mailbox.SetOffloadingAndTakePending(true), expected);
    EXPECT_TRUE(mailbox.SetOffloadingAndTakePending(true).empty());

    // The next take starts a fresh order, across tenants too.
    ASSERT_TRUE(mailbox.EnqueueOffload(Offload("t2", "z", 1), 1000) ==
                ErrorCode::OK);
    ASSERT_TRUE(mailbox.EnqueueOffload(Offload("t", "a", 2), 1000) ==
                ErrorCode::OK);
    ASSERT_TRUE(mailbox.EnqueueOffload(Offload("t2", "a", 3), 1000) ==
                ErrorCode::OK);
    EXPECT_EQ(mailbox.SetOffloadingAndTakePending(false),
              (std::vector<OffloadTaskItem>{Offload("t2", "z", 1),
                                            Offload("t", "a", 2),
                                            Offload("t2", "a", 3)}));
}

// Enqueue/remove churn between two takes compacts the order list without
// losing, duplicating or reordering queued tasks.
TEST(LocalSsdTaskMailboxTest, OrderedOffloadsSurviveChurnCompaction) {
    constexpr size_t kLimit = 8;
    LocalSsdTaskMailbox mailbox(true, /*ordered_offloads=*/true);
    ASSERT_TRUE(mailbox.EnqueueOffload(Offload("t", "first", 0), kLimit) ==
                ErrorCode::OK);
    ASSERT_TRUE(mailbox.EnqueueOffload(Offload("t", "moved", 0), kLimit) ==
                ErrorCode::OK);
    for (int round = 0; round < 200; ++round) {
        const std::string key = "churn" + std::to_string(round);
        ASSERT_TRUE(mailbox.EnqueueOffload(Offload("t", key, 1), kLimit) ==
                    ErrorCode::OK);
        ASSERT_TRUE(mailbox.RemoveOffload(TenantId("t"), key));
        // "moved" is removed and enqueued again in every round, leaving one
        // stale entry each time.
        ASSERT_TRUE(mailbox.RemoveOffload(TenantId("t"), "moved"));
        ASSERT_TRUE(mailbox.EnqueueOffload(Offload("t", "moved", round),
                                           kLimit) == ErrorCode::OK);
    }
    for (int i = 0; i < 5; ++i) {
        ASSERT_TRUE(mailbox.EnqueueOffload(
                        Offload("t", "live" + std::to_string(i), i), kLimit) ==
                    ErrorCode::OK);
    }
    EXPECT_EQ(mailbox.SetOffloadingAndTakePending(true),
              (std::vector<OffloadTaskItem>{
                  Offload("t", "first", 0), Offload("t", "moved", 199),
                  Offload("t", "live0", 0), Offload("t", "live1", 1),
                  Offload("t", "live2", 2), Offload("t", "live3", 3),
                  Offload("t", "live4", 4)}));
    EXPECT_TRUE(mailbox.SetOffloadingAndTakePending(true).empty());
}

// Knob off (the default): the same tasks as before, in unspecified order.
TEST(LocalSsdTaskMailboxTest, UnorderedOffloadsDeliverTheSameSet) {
    LocalSsdTaskMailbox mailbox(true);
    std::vector<std::string> enqueued;
    for (int i = 0; i < 20; ++i) {
        enqueued.push_back("k" + std::to_string(i));
        ASSERT_TRUE(mailbox.EnqueueOffload(Offload("t", enqueued.back(), i),
                                           1000) == ErrorCode::OK);
    }
    ASSERT_TRUE(mailbox.RemoveOffload(TenantId("t"), "k7"));
    enqueued.erase(enqueued.begin() + 7);
    auto keys = Keys(mailbox.SetOffloadingAndTakePending(true));
    std::sort(keys.begin(), keys.end());
    std::sort(enqueued.begin(), enqueued.end());
    EXPECT_EQ(keys, enqueued);
    EXPECT_TRUE(mailbox.SetOffloadingAndTakePending(true).empty());
}

// The manager hands the setting to every client's mailbox, and a cancelled
// mirror leaves no gap in the order.
TEST(LocalSsdManagerTest, OrderedOffloadsApplyToEveryClient) {
    LocalSsdManager manager(/*ordered_offloads=*/true);
    EXPECT_TRUE(manager.ordered_offloads());
    EXPECT_FALSE(LocalSsdManager().ordered_offloads());
    const UUID client_a{1, 2};
    const UUID client_b{3, 4};
    ASSERT_TRUE(manager.RegisterClient(client_a, true) == ErrorCode::OK);
    ASSERT_TRUE(manager.RegisterClient(client_b, true) == ErrorCode::OK);
    const std::vector<std::string> order{"m", "c", "x", "a", "q", "b"};
    for (const auto& key : order) {
        ASSERT_TRUE(manager.EnqueueOffload(client_a, Offload("t", key, 1),
                                           100) == ErrorCode::OK);
        ASSERT_TRUE(manager.EnqueueOffload(client_b, Offload("t", key, 2),
                                           100) == ErrorCode::OK);
    }
    ASSERT_TRUE(manager.CancelOffloadsIfAllPending({client_a, client_b},
                                                   TenantId("t"), "x"));
    const std::vector<std::string> expected{"m", "c", "a", "q", "b"};
    for (const auto& client : {client_a, client_b}) {
        auto tasks = manager.SetOffloadingAndTakePending(client, true);
        ASSERT_TRUE(tasks.has_value());
        EXPECT_EQ(Keys(*tasks), expected);
    }
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
