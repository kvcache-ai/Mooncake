#include "fixture.h"

namespace mooncake::test {
TEST_F(PromotionOnHitTest,
       BatchGetReplicaListForAdminDoesNotUpdateCacheHitMetrics) {
    MasterServiceConfig config;
    config.enable_offload = true;
    config.default_kv_lease_ttl = 2000;
    auto service = std::make_unique<MasterService>(config);

    constexpr size_t seg_size = 1024 * 1024 * 16;
    auto ctx = PrepareSegment(*service, "metrics_segment", kDefaultSegmentBase,
                              seg_size);

    const std::string key = "k_admin_no_metric";
    PutObject(*service, ctx.client_id, key, 1024);

    using CacheHitStat = MasterMetricManager::CacheHitStat;
    auto mem_hits = []() {
        auto stats = MasterMetricManager::instance().calculate_cache_stats();
        return stats[CacheHitStat::MEMORY_HITS];
    };

    const double before_admin = mem_hits();

    // Read-only admin query must leave the memory-cache-hit counter untouched.
    auto admin_result = service->BatchGetReplicaListForAdmin(
        std::vector<std::string>{key}, "default");
    ASSERT_EQ(admin_result.size(), 1u);
    ASSERT_TRUE(admin_result[0].has_value());
    EXPECT_EQ(mem_hits(), before_admin);

    // The client-facing path does bump it, proving the assertion above is
    // meaningful rather than a counter that never moves.
    (void)service->BatchGetReplicaList(std::vector<std::string>{key},
                                       "default");
    EXPECT_GT(mem_hits(), before_admin);

    service->RemoveAll();
}

// PromotionObjectHeartbeat returns an empty task list when called against a
// client that has no LocalDiskSegment registered.
TEST_F(PromotionOnHitTest, RemoveDuringPromotion) {
    MasterServiceConfig config;
    config.enable_offload = true;
    config.promotion_on_hit = true;
    config.promotion_admission_threshold = 1;
    config.default_kv_lease_ttl = 100;  // short lease: Remove won't block
    config.put_start_discard_timeout_sec = 0;
    config.put_start_release_timeout_sec = 1;
    auto service = std::make_unique<MasterService>(config);

    constexpr size_t seg_size = 1024 * 1024 * 16;
    auto ctx =
        PrepareSegment(*service, "test_segment", kDefaultSegmentBase, seg_size);

    ASSERT_TRUE(InjectLocalDiskReplica(*service, ctx.client_id, "k_cold", 1024,
                                       ctx.segment_name));

    auto& mm = MasterMetricManager::instance();
    const int64_t cancelled_pre = mm.get_promotion_cancelled();

    // Queue a promotion task.
    {
        auto r = service->GetReplicaList("k_cold", "default");
        ASSERT_TRUE(r.has_value());
    }

    // Lease must expire before we can call non-force Remove (or use force).
    auto rm = service->Remove("k_cold", "default", /*force=*/true);
    // Remove returns REPLICA_IS_NOT_READY if any replica is non-COMPLETE.
    // The injected LOCAL_DISK replica is COMPLETE, so this should succeed.
    ASSERT_TRUE(rm.has_value())
        << "Remove on a LOCAL_DISK-only key with a queued promotion should "
        << "succeed (all replicas COMPLETE); error=" << rm.error();
    EXPECT_EQ(mm.get_promotion_cancelled() - cancelled_pre, 1)
        << "Remove of a key with an in-flight promotion task must bump "
        << "promotion_cancelled";

    // NotifyPromotionSuccess on the now-removed key must surface the missing
    // metadata cleanly, not crash.
    auto notify =
        service->NotifyPromotionSuccess(ctx.client_id, "k_cold", "default");
    ASSERT_FALSE(notify.has_value());
    EXPECT_EQ(notify.error(), ErrorCode::OBJECT_NOT_FOUND);

    // Wait for the reaper; it must tolerate the missing metadata entry
    // (the source replica it would dec_refcnt is already gone). 3s for
    // CI-safe margin over the 1s release timeout.
    std::this_thread::sleep_for(std::chrono::seconds(3));

    // Re-injecting the key and re-triggering must work end-to-end, proving
    // the per-shard PromotionTask was reaped (not stuck).
    ASSERT_TRUE(InjectLocalDiskReplica(*service, ctx.client_id, "k_cold", 1024,
                                       ctx.segment_name));
    {
        auto r = service->GetReplicaList("k_cold", "default");
        ASSERT_TRUE(r.has_value());
        auto pending = service->PromotionObjectHeartbeat(ctx.client_id);
        ASSERT_TRUE(pending.has_value());
        EXPECT_EQ(pending->size(), 1u)
            << "After Remove + reap, the same key must re-enqueue cleanly";
    }

    service->RemoveAll();
}

// Cross-host promotion: the LOCAL_DISK source's holder client has no DRAM
// segment of its own. The reader's DRAM segment (segment_b) is the only
// allocator candidate, so PromotionAllocStart must place the new MEMORY
// replica there — proving the master's allocation strategy crosses the
// host boundary cleanly.
TEST_F(PromotionOnHitTest, RemoveErasesPromotionTask) {
    MasterServiceConfig config;
    config.enable_offload = true;
    config.promotion_on_hit = true;
    config.promotion_admission_threshold = 1;
    config.promotion_queue_limit = 1;  // makes the slot observable
    config.default_kv_lease_ttl = 5000;
    // Long task TTL so any cap-slot reclaim must come from the
    // metadata-erase path, not the reaper.
    config.put_start_release_timeout_sec = 300;
    auto service = std::make_unique<MasterService>(config);

    constexpr size_t seg_size = 1024 * 1024 * 16;
    auto holder =
        PrepareSegment(*service, "seg_a", kDefaultSegmentBase, seg_size);
    ASSERT_TRUE(InjectLocalDiskReplica(*service, holder.client_id, "k_first",
                                       1024, holder.segment_name));
    ASSERT_TRUE(InjectLocalDiskReplica(*service, holder.client_id, "k_second",
                                       1024, holder.segment_name));

    // Admit task 1.
    {
        auto r = service->GetReplicaList("k_first", "default");
        ASSERT_TRUE(r.has_value());
    }
    {
        auto pending = service->PromotionObjectHeartbeat(holder.client_id);
        ASSERT_TRUE(pending.has_value());
        EXPECT_EQ(CountPromotionTask(*pending, "k_first"), 1u);
    }

    // Remove k_first with force=true. With the fix, this also wipes
    // k_first's promotion_tasks entry and decrements
    // promotion_in_flight_ back to 0.
    auto rm = service->Remove("k_first", "default", /*force=*/true);
    ASSERT_TRUE(rm.has_value())
        << "Remove should succeed; error=" << rm.error();

    // Now admit a different key. With queue_limit=1, this only succeeds
    // if the slot was freed by Remove. Without the in-flight cleanup,
    // it would stay pinned for the full 300s TTL.
    {
        auto r = service->GetReplicaList("k_second", "default");
        ASSERT_TRUE(r.has_value());
    }
    auto pending_post = service->PromotionObjectHeartbeat(holder.client_id);
    ASSERT_TRUE(pending_post.has_value());
    EXPECT_EQ(CountPromotionTask(*pending_post, "k_second"), 1u)
        << "k_second must be admittable after Remove of k_first — Remove "
        << "must erase the in-flight promotion_tasks entry and decrement "
        << "promotion_in_flight_, otherwise queue_limit=1 stays saturated.";

    service->RemoveAll();
}

// RemoveByRegex on a key with an in-flight PromotionTask must drop the
// task entry, same as Remove. Mirror of RemoveErasesPromotionTask, but
// exercises the regex path which iterates shards directly without an
// accessor object.
TEST_F(PromotionOnHitTest, RemoveByRegexErasesPromotionTask) {
    MasterServiceConfig config;
    config.enable_offload = true;
    config.promotion_on_hit = true;
    config.promotion_admission_threshold = 1;
    config.promotion_queue_limit = 1;
    config.default_kv_lease_ttl = 5000;
    config.put_start_release_timeout_sec = 300;
    auto service = std::make_unique<MasterService>(config);

    constexpr size_t seg_size = 1024 * 1024 * 16;
    auto holder =
        PrepareSegment(*service, "seg_a", kDefaultSegmentBase, seg_size);
    ASSERT_TRUE(InjectLocalDiskReplica(*service, holder.client_id, "regex_k1",
                                       1024, holder.segment_name));
    ASSERT_TRUE(InjectLocalDiskReplica(*service, holder.client_id, "other_k2",
                                       1024, holder.segment_name));

    // Admit task on regex_k1.
    {
        auto r = service->GetReplicaList("regex_k1", "default");
        ASSERT_TRUE(r.has_value());
    }

    // RemoveByRegex matches regex_k1 only.
    auto removed = service->RemoveByRegex("^regex_", "default", /*force=*/true);
    ASSERT_TRUE(removed.has_value())
        << "RemoveByRegex should succeed; error=" << removed.error();
    EXPECT_EQ(removed.value(), 1) << "exactly one key (regex_k1) should match";

    // Slot must be free — admit on other_k2 (different shard or same,
    // doesn't matter because counter is global).
    {
        auto r = service->GetReplicaList("other_k2", "default");
        ASSERT_TRUE(r.has_value());
    }
    auto pending_post = service->PromotionObjectHeartbeat(holder.client_id);
    ASSERT_TRUE(pending_post.has_value());
    EXPECT_EQ(CountPromotionTask(*pending_post, "other_k2"), 1u)
        << "other_k2 must be admittable after RemoveByRegex of regex_k1 "
        << "— RemoveByRegex must erase the in-flight promotion_tasks "
        << "entry. Otherwise queue_limit=1 stays saturated.";

    service->RemoveAll();
}

// RemoveAll on a key with an in-flight PromotionTask must drop the
// task entry alongside the metadata. Same shape as
// RemoveErasesPromotionTask but exercises the bulk-erase loop in
// MasterService::RemoveAll, which iterates every shard and erases
// metadata entries directly.
TEST_F(PromotionOnHitTest, RemoveAllErasesPromotionTask) {
    MasterServiceConfig config;
    config.enable_offload = true;
    config.promotion_on_hit = true;
    config.promotion_admission_threshold = 1;
    config.promotion_queue_limit = 1;
    config.default_kv_lease_ttl = 5000;
    config.put_start_release_timeout_sec = 300;
    auto service = std::make_unique<MasterService>(config);

    constexpr size_t seg_size = 1024 * 1024 * 16;
    auto holder =
        PrepareSegment(*service, "seg_a", kDefaultSegmentBase, seg_size);
    ASSERT_TRUE(InjectLocalDiskReplica(*service, holder.client_id, "k_first",
                                       1024, holder.segment_name));

    auto& mm = MasterMetricManager::instance();
    const int64_t cancelled_pre = mm.get_promotion_cancelled();

    {
        auto r = service->GetReplicaList("k_first", "default");
        ASSERT_TRUE(r.has_value());
    }
    {
        auto pending = service->PromotionObjectHeartbeat(holder.client_id);
        ASSERT_TRUE(pending.has_value());
        EXPECT_EQ(CountPromotionTask(*pending, "k_first"), 1u);
    }

    auto removed = service->RemoveAll(/*force=*/true);
    EXPECT_GE(removed, 1) << "RemoveAll should erase k_first";

    EXPECT_EQ(mm.get_promotion_cancelled() - cancelled_pre, 1)
        << "RemoveAll on a key with an in-flight promotion must route "
        << "through EraseMetadataEntry and bump promotion_cancelled_total.";

    ASSERT_TRUE(InjectLocalDiskReplica(*service, holder.client_id, "k_second",
                                       1024, holder.segment_name));
    {
        auto r = service->GetReplicaList("k_second", "default");
        ASSERT_TRUE(r.has_value());
    }
    auto pending_post = service->PromotionObjectHeartbeat(holder.client_id);
    ASSERT_TRUE(pending_post.has_value());
    EXPECT_EQ(CountPromotionTask(*pending_post, "k_second"), 1u)
        << "k_second must be admittable after RemoveAll of k_first — "
        << "otherwise queue_limit=1 stays saturated until reaper TTL.";

    service->RemoveAll(/*force=*/true);
}

// BatchRemove normal-completion path on a key with an in-flight
// PromotionTask must drop the task entry. ReMountSegment registers the
// holder in ok_client_ so CleanupStaleHandles returns false and
// BatchRemove takes the non-stale branch.
TEST_F(PromotionOnHitTest, BatchRemoveErasesPromotionTask) {
    MasterServiceConfig config;
    config.enable_offload = true;
    config.promotion_on_hit = true;
    config.promotion_admission_threshold = 1;
    config.promotion_queue_limit = 1;
    config.default_kv_lease_ttl = 5000;
    config.put_start_release_timeout_sec = 300;
    auto service = std::make_unique<MasterService>(config);

    constexpr size_t seg_size = 1024 * 1024 * 16;
    auto holder =
        PrepareSegment(*service, "seg_a", kDefaultSegmentBase, seg_size);
    {
        Segment seg_a = MakeSegment("seg_a", kDefaultSegmentBase, seg_size);
        seg_a.id = holder.segment_id;
        std::vector<Segment> segs{seg_a};
        auto remount = service->ReMountSegment(segs, holder.client_id);
        ASSERT_TRUE(remount.has_value()) << "ReMount failed";
    }
    ASSERT_TRUE(InjectLocalDiskReplica(*service, holder.client_id, "k_first",
                                       1024, holder.segment_name));
    ASSERT_TRUE(InjectLocalDiskReplica(*service, holder.client_id, "k_second",
                                       1024, holder.segment_name));

    auto& mm = MasterMetricManager::instance();
    const int64_t cancelled_pre = mm.get_promotion_cancelled();

    {
        auto r = service->GetReplicaList("k_first", "default");
        ASSERT_TRUE(r.has_value());
    }
    {
        auto pending = service->PromotionObjectHeartbeat(holder.client_id);
        ASSERT_TRUE(pending.has_value());
        EXPECT_EQ(CountPromotionTask(*pending, "k_first"), 1u);
    }

    auto results = service->BatchRemove({"k_first"}, "default", /*force=*/true);
    ASSERT_EQ(results.size(), 1u);
    EXPECT_TRUE(results[0].has_value())
        << "BatchRemove should succeed; error=" << results[0].error();

    EXPECT_EQ(mm.get_promotion_cancelled() - cancelled_pre, 1)
        << "BatchRemove normal path on a key with an in-flight promotion "
        << "must bump promotion_cancelled_total.";

    {
        auto r = service->GetReplicaList("k_second", "default");
        ASSERT_TRUE(r.has_value());
    }
    auto pending_post = service->PromotionObjectHeartbeat(holder.client_id);
    ASSERT_TRUE(pending_post.has_value());
    EXPECT_EQ(CountPromotionTask(*pending_post, "k_second"), 1u)
        << "k_second must be admittable after BatchRemove of k_first — "
        << "otherwise queue_limit=1 stays saturated until reaper TTL.";

    service->RemoveAll(/*force=*/true);
}

// BatchRemove stale-handle path on a key with an in-flight
// PromotionTask must drop the task entry. The holder is mounted via
// PrepareSegment only (no ReMount), so its client is absent from
// ok_client_; BatchRemove's CleanupStaleHandles then erases the
// LOCAL_DISK replica and the stale-handle branch fires.
TEST_F(PromotionOnHitTest, BatchRemoveStaleHandleErasesPromotionTask) {
    MasterServiceConfig config;
    config.enable_offload = true;
    config.promotion_on_hit = true;
    config.promotion_admission_threshold = 1;
    config.promotion_queue_limit = 1;
    config.default_kv_lease_ttl = 5000;
    config.put_start_release_timeout_sec = 300;
    auto service = std::make_unique<MasterService>(config);

    constexpr size_t seg_size = 1024 * 1024 * 16;
    auto holder =
        PrepareSegment(*service, "seg_a", kDefaultSegmentBase, seg_size);
    ASSERT_TRUE(InjectLocalDiskReplica(*service, holder.client_id, "k_first",
                                       1024, holder.segment_name));

    auto& mm = MasterMetricManager::instance();
    const int64_t cancelled_pre = mm.get_promotion_cancelled();

    {
        auto r = service->GetReplicaList("k_first", "default");
        ASSERT_TRUE(r.has_value());
    }
    {
        auto pending = service->PromotionObjectHeartbeat(holder.client_id);
        ASSERT_TRUE(pending.has_value());
        EXPECT_EQ(CountPromotionTask(*pending, "k_first"), 1u);
    }

    auto results = service->BatchRemove({"k_first"}, "default", /*force=*/true);
    ASSERT_EQ(results.size(), 1u);
    EXPECT_FALSE(results[0].has_value())
        << "stale-handle path should report OBJECT_NOT_FOUND once the "
        << "LOCAL_DISK replica is wiped.";

    EXPECT_EQ(mm.get_promotion_cancelled() - cancelled_pre, 1)
        << "BatchRemove stale-handle path must also bump "
        << "promotion_cancelled_total.";

    auto second_holder = PrepareSegment(
        *service, "seg_b", kDefaultSegmentBase + seg_size, seg_size);
    {
        Segment seg_b =
            MakeSegment("seg_b", kDefaultSegmentBase + seg_size, seg_size);
        seg_b.id = second_holder.segment_id;
        std::vector<Segment> segs{seg_b};
        auto remount = service->ReMountSegment(segs, second_holder.client_id);
        ASSERT_TRUE(remount.has_value()) << "ReMount failed";
    }
    ASSERT_TRUE(InjectLocalDiskReplica(*service, second_holder.client_id,
                                       "k_second", 1024,
                                       second_holder.segment_name));
    {
        auto r = service->GetReplicaList("k_second", "default");
        ASSERT_TRUE(r.has_value());
    }
    auto pending_post =
        service->PromotionObjectHeartbeat(second_holder.client_id);
    ASSERT_TRUE(pending_post.has_value());
    EXPECT_EQ(CountPromotionTask(*pending_post, "k_second"), 1u)
        << "k_second must be admittable after the stale-handle "
        << "BatchRemove — otherwise queue_limit=1 stays saturated until "
        << "reaper TTL.";

    service->RemoveAll(/*force=*/true);
}

// The funnel metrics (admitted, completed, completed_bytes, in_flight)
// track a successful promotion lifecycle end-to-end: a single
// admission bumps admitted+1 and in_flight+1; NotifyPromotionSuccess
// bumps completed+1 and completed_bytes+object_size and brings
// in_flight back to 0.
TEST_F(PromotionOnHitTest, MetricsFunnelTracksSuccessfulPromotion) {
    MasterServiceConfig config;
    config.enable_offload = true;
    config.promotion_on_hit = true;
    config.promotion_admission_threshold = 1;
    config.default_kv_lease_ttl = 2000;
    auto service = std::make_unique<MasterService>(config);

    constexpr size_t seg_size = 1024 * 1024 * 16;
    constexpr int64_t kObjBytes = 4096;
    auto seg = PrepareSegment(*service, "seg_a", kDefaultSegmentBase, seg_size);
    ASSERT_TRUE(InjectLocalDiskReplica(*service, seg.client_id, "k_hot",
                                       kObjBytes, seg.segment_name));

    auto& mm = MasterMetricManager::instance();
    const int64_t admitted_pre = mm.get_promotion_admitted();
    const int64_t completed_pre = mm.get_promotion_completed();
    const int64_t bytes_pre = mm.get_promotion_completed_bytes();
    const int64_t in_flight_pre = mm.get_promotion_in_flight();

    // Admit.
    {
        auto r = service->GetReplicaList("k_hot", "default");
        ASSERT_TRUE(r.has_value());
    }
    EXPECT_EQ(mm.get_promotion_admitted() - admitted_pre, 1);
    EXPECT_EQ(mm.get_promotion_in_flight() - in_flight_pre, 1);

    // Drive AllocStart + NotifyPromotionSuccess.
    auto alloc = service->PromotionAllocStart(seg.client_id, "k_hot", "default",
                                              kObjBytes, {});
    ASSERT_TRUE(alloc.has_value());
    auto notify =
        service->NotifyPromotionSuccess(seg.client_id, "k_hot", "default");
    ASSERT_TRUE(notify.has_value());

    EXPECT_EQ(mm.get_promotion_completed() - completed_pre, 1);
    EXPECT_EQ(mm.get_promotion_completed_bytes() - bytes_pre, kObjBytes);
    EXPECT_EQ(mm.get_promotion_in_flight() - in_flight_pre, 0);

    service->RemoveAll();
}

// Each rejection gate (frequency / watermark / cap) increments its
// own counter when its branch fires.
TEST_F(PromotionOnHitTest, MetricsRejectionCountersIncrementOnGateMiss) {
    MasterServiceConfig config;
    config.enable_offload = true;
    config.promotion_on_hit = true;
    // Threshold > 1 so the first Get is rejected on frequency.
    config.promotion_admission_threshold = 2;
    config.promotion_queue_limit = 1;
    config.default_kv_lease_ttl = 2000;
    auto service = std::make_unique<MasterService>(config);

    constexpr size_t seg_size = 1024 * 1024 * 16;
    auto seg = PrepareSegment(*service, "seg_a", kDefaultSegmentBase, seg_size);
    ASSERT_TRUE(InjectLocalDiskReplica(*service, seg.client_id, "k_a", 1024,
                                       seg.segment_name));
    ASSERT_TRUE(InjectLocalDiskReplica(*service, seg.client_id, "k_b", 1024,
                                       seg.segment_name));

    auto& mm = MasterMetricManager::instance();
    const int64_t freq_pre = mm.get_promotion_rejected_frequency();
    const int64_t cap_pre = mm.get_promotion_rejected_cap();

    // 1st Get on k_a: freq=1, threshold=2 → rejected on frequency.
    {
        auto r = service->GetReplicaList("k_a", "default");
        ASSERT_TRUE(r.has_value());
    }
    EXPECT_EQ(mm.get_promotion_rejected_frequency() - freq_pre, 1);

    // 2nd Get on k_a: freq=2, admits. Now in-flight = 1 == limit.
    {
        auto r = service->GetReplicaList("k_a", "default");
        ASSERT_TRUE(r.has_value());
    }
    // Get on k_b: freq=1, threshold=2 → rejected on frequency.
    {
        auto r = service->GetReplicaList("k_b", "default");
        ASSERT_TRUE(r.has_value());
    }
    // Get on k_b again: freq=2, gets past frequency, but cap=1
    // saturated → rejected on cap.
    {
        auto r = service->GetReplicaList("k_b", "default");
        ASSERT_TRUE(r.has_value());
    }
    EXPECT_EQ(mm.get_promotion_rejected_cap() - cap_pre, 1);

    service->RemoveAll();

    // Watermark gate uses a fresh service configured with
    // eviction_high_watermark_ratio = 0.0 so any non-negative DRAM
    // usage trips it. threshold = 1 makes the first Get clear the
    // frequency gate and reach the watermark check.
    MasterServiceConfig wm_config;
    wm_config.enable_offload = true;
    wm_config.promotion_on_hit = true;
    wm_config.promotion_admission_threshold = 1;
    wm_config.promotion_queue_limit = 50;
    wm_config.eviction_high_watermark_ratio = 0.0;
    wm_config.default_kv_lease_ttl = 2000;
    auto wm_service = std::make_unique<MasterService>(wm_config);

    auto wm_seg =
        PrepareSegment(*wm_service, "wm_seg", kDefaultSegmentBase, seg_size);
    ASSERT_TRUE(InjectLocalDiskReplica(*wm_service, wm_seg.client_id, "k_w",
                                       1024, wm_seg.segment_name));

    const int64_t wm_pre = mm.get_promotion_rejected_watermark();
    {
        auto r = wm_service->GetReplicaList("k_w", "default");
        ASSERT_TRUE(r.has_value());
    }
    EXPECT_EQ(mm.get_promotion_rejected_watermark() - wm_pre, 1);

    wm_service->RemoveAll();
}

TEST_F(PromotionOnHitTest, MetricsRemoveMidPromotionCountsAsCancelled) {
    MasterServiceConfig config;
    config.enable_offload = true;
    config.promotion_on_hit = true;
    config.promotion_admission_threshold = 1;
    config.default_kv_lease_ttl = 2000;
    auto service = std::make_unique<MasterService>(config);

    constexpr size_t seg_size = 1024 * 1024 * 16;
    auto seg = PrepareSegment(*service, "seg_a", kDefaultSegmentBase, seg_size);
    ASSERT_TRUE(InjectLocalDiskReplica(*service, seg.client_id, "k_drop", 1024,
                                       seg.segment_name));

    auto& mm = MasterMetricManager::instance();
    const int64_t admitted_pre = mm.get_promotion_admitted();
    const int64_t cancelled_pre = mm.get_promotion_cancelled();
    const int64_t in_flight_pre = mm.get_promotion_in_flight();

    // Admit a promotion. in_flight goes from 0 to 1.
    {
        auto r = service->GetReplicaList("k_drop", "default");
        ASSERT_TRUE(r.has_value());
    }
    ASSERT_EQ(mm.get_promotion_admitted() - admitted_pre, 1);
    ASSERT_EQ(mm.get_promotion_in_flight() - in_flight_pre, 1);

    auto rm = service->Remove("k_drop", "default", /*force=*/true);
    ASSERT_TRUE(rm.has_value()) << "error=" << rm.error();

    EXPECT_EQ(mm.get_promotion_in_flight() - in_flight_pre, 0);
    EXPECT_EQ(mm.get_promotion_cancelled() - cancelled_pre, 1);

    service->RemoveAll();
}

}  // namespace mooncake::test
