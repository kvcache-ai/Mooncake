#include "fixture.h"

namespace mooncake::test {
TEST_F(PromotionOnHitTest, DefaultOffNoPromotion) {
    MasterServiceConfig config;
    config.enable_offload = true;
    config.promotion_on_hit = false;  // explicitly off
    config.default_kv_lease_ttl = 2000;
    auto service = std::make_unique<MasterService>(config);

    constexpr size_t seg_size = 1024 * 1024 * 16;
    auto ctx =
        PrepareSegment(*service, "test_segment", kDefaultSegmentBase, seg_size);

    PutObject(*service, ctx.client_id, "k1");
    // GetReplicaList many times. With promotion_on_hit=false, nothing should
    // appear in promotion_objects regardless of access count.
    for (int i = 0; i < 5; ++i) {
        auto resp = service->GetReplicaList("k1", "default");
        ASSERT_TRUE(resp.has_value());
    }

    auto pending = service->PromotionObjectHeartbeat(ctx.client_id);
    ASSERT_TRUE(pending.has_value());
    EXPECT_EQ(pending->size(), 0u);

    service->RemoveAll();
}

// With promotion enabled but no LOCAL_DISK replica present, no promotion
// should fire (the trigger gate any_local_disk is false).
TEST_F(PromotionOnHitTest, NoLocalDiskNoPromotion) {
    MasterServiceConfig config;
    config.enable_offload = true;
    config.promotion_on_hit = true;
    config.promotion_admission_threshold = 1;  // promote on first touch
    config.default_kv_lease_ttl = 2000;
    auto service = std::make_unique<MasterService>(config);

    constexpr size_t seg_size = 1024 * 1024 * 16;
    auto ctx =
        PrepareSegment(*service, "test_segment", kDefaultSegmentBase, seg_size);

    PutObject(*service, ctx.client_id, "k_mem_only");
    for (int i = 0; i < 5; ++i) {
        auto resp = service->GetReplicaList("k_mem_only", "default");
        ASSERT_TRUE(resp.has_value());
    }

    auto pending = service->PromotionObjectHeartbeat(ctx.client_id);
    ASSERT_TRUE(pending.has_value());
    EXPECT_EQ(pending->size(), 0u)
        << "Memory-only key should not trigger promotion";

    service->RemoveAll();
}

// Single-shot Get on a key with both MEMORY and LOCAL_DISK should not
// promote (any_memory=true → trigger gate fails).
TEST_F(PromotionOnHitTest, MemoryReplicaPresentNoPromotion) {
    MasterServiceConfig config;
    config.enable_offload = true;
    config.promotion_on_hit = true;
    config.promotion_admission_threshold = 1;  // first-touch
    config.default_kv_lease_ttl = 2000;
    auto service = std::make_unique<MasterService>(config);

    constexpr size_t seg_size = 1024 * 1024 * 16;
    auto ctx =
        PrepareSegment(*service, "test_segment", kDefaultSegmentBase, seg_size);

    PutObject(*service, ctx.client_id, "k_dual", 1024);
    // Add a LOCAL_DISK replica next to the MEMORY one.
    ASSERT_TRUE(InjectLocalDiskReplica(*service, ctx.client_id, "k_dual", 1024,
                                       ctx.segment_name));

    for (int i = 0; i < 5; ++i) {
        auto resp = service->GetReplicaList("k_dual", "default");
        ASSERT_TRUE(resp.has_value());
    }

    auto pending = service->PromotionObjectHeartbeat(ctx.client_id);
    ASSERT_TRUE(pending.has_value());
    EXPECT_EQ(pending->size(), 0u)
        << "MEMORY replica still present: should not promote";

    service->RemoveAll();
}

TEST_F(PromotionOnHitTest, BatchGetReplicaListPromotesLocalDiskOnlyObject) {
    MasterServiceConfig config;
    config.enable_offload = true;
    config.promotion_on_hit = true;
    config.promotion_admission_threshold = 1;
    config.promotion_max_per_heartbeat = 2;
    config.default_kv_lease_ttl = 2000;
    auto service = std::make_unique<MasterService>(config);

    constexpr size_t seg_size = 1024 * 1024 * 16;
    auto ctx =
        PrepareSegment(*service, "test_segment", kDefaultSegmentBase, seg_size);

    const std::string single_key = "k_single_get_promote";
    const std::string batch_key = "k_batch_get_promote";
    ASSERT_TRUE(InjectLocalDiskReplica(*service, ctx.client_id, single_key,
                                       1024, ctx.segment_name));
    ASSERT_TRUE(InjectLocalDiskReplica(*service, ctx.client_id, batch_key, 1024,
                                       ctx.segment_name));

    auto& mm = MasterMetricManager::instance();
    const int64_t admitted_pre = mm.get_promotion_admitted();
    const int64_t in_flight_pre = mm.get_promotion_in_flight();

    auto single_result = service->GetReplicaList(single_key, "default");
    ASSERT_TRUE(single_result.has_value());
    ASSERT_EQ(single_result->replicas.size(), 1u);
    EXPECT_TRUE(single_result->replicas[0].is_local_disk_replica());

    auto batch_result = service->BatchGetReplicaList(
        std::vector<std::string>{batch_key}, "default");
    ASSERT_EQ(batch_result.size(), 1u);
    ASSERT_TRUE(batch_result[0].has_value());
    ASSERT_EQ(batch_result[0]->replicas.size(), 1u);
    EXPECT_TRUE(batch_result[0]->replicas[0].is_local_disk_replica());

    EXPECT_EQ(mm.get_promotion_admitted() - admitted_pre, 2);
    EXPECT_EQ(mm.get_promotion_in_flight() - in_flight_pre, 2);

    auto pending = service->PromotionObjectHeartbeat(ctx.client_id);
    ASSERT_TRUE(pending.has_value());
    EXPECT_EQ(pending->size(), 2u);
    EXPECT_EQ(CountPromotionTask(*pending, single_key), 1u);
    EXPECT_EQ(CountPromotionTask(*pending, batch_key), 1u);

    service->RemoveAll();
}

// The read-only admin batch query must NOT trigger promotion-on-hit, even for a
// LOCAL_DISK-only key. Contrast with BatchGetReplicaListPromotesLocalDiskOnly
// Object above, where the client-facing BatchGetReplicaList admits the key for
// promotion.
TEST_F(PromotionOnHitTest,
       BatchGetReplicaListForAdminDoesNotPromoteLocalDiskOnlyObject) {
    MasterServiceConfig config;
    config.enable_offload = true;
    config.promotion_on_hit = true;
    config.promotion_admission_threshold = 1;
    config.promotion_max_per_heartbeat = 2;
    config.default_kv_lease_ttl = 2000;
    auto service = std::make_unique<MasterService>(config);

    constexpr size_t seg_size = 1024 * 1024 * 16;
    auto ctx = PrepareSegment(*service, "test_segment_admin",
                              kDefaultSegmentBase, seg_size);

    const std::string key = "k_batch_admin_no_promote";
    ASSERT_TRUE(InjectLocalDiskReplica(*service, ctx.client_id, key, 1024,
                                       ctx.segment_name));

    auto& mm = MasterMetricManager::instance();
    const int64_t admitted_pre = mm.get_promotion_admitted();
    const int64_t in_flight_pre = mm.get_promotion_in_flight();

    auto result = service->BatchGetReplicaListForAdmin(
        std::vector<std::string>{key}, "default");
    ASSERT_EQ(result.size(), 1u);
    ASSERT_TRUE(result[0].has_value());
    ASSERT_EQ(result[0]->replicas.size(), 1u);
    EXPECT_TRUE(result[0]->replicas[0].is_local_disk_replica());

    // No promotion admitted or enqueued by the read-only admin path.
    EXPECT_EQ(mm.get_promotion_admitted() - admitted_pre, 0);
    EXPECT_EQ(mm.get_promotion_in_flight() - in_flight_pre, 0);
    auto pending = service->PromotionObjectHeartbeat(ctx.client_id);
    ASSERT_TRUE(pending.has_value());
    EXPECT_EQ(pending->size(), 0u);
    EXPECT_EQ(CountPromotionTask(*pending, key), 0u);

    service->RemoveAll();
}

// The read-only admin batch query must NOT update the store-observed cache-hit
// counters. Contrast with the client-facing BatchGetReplicaList, which bumps
// the memory-cache-hit counter for a MEMORY replica.
TEST_F(PromotionOnHitTest, RacingReadersDedup) {
    MasterServiceConfig config;
    config.enable_offload = true;
    config.promotion_on_hit = true;
    config.promotion_admission_threshold = 1;  // first-touch fires the gate
    config.default_kv_lease_ttl = 2000;
    auto service = std::make_unique<MasterService>(config);

    constexpr size_t seg_size = 1024 * 1024 * 16;
    auto ctx =
        PrepareSegment(*service, "test_segment", kDefaultSegmentBase, seg_size);

    // LOCAL_DISK-only key: NotifyOffloadSuccess on a never-PUT key creates
    // metadata with only the LOCAL_DISK replica (AddReplica's Create-on-
    // missing path).
    ASSERT_TRUE(InjectLocalDiskReplica(*service, ctx.client_id, "k_cold", 1024,
                                       ctx.segment_name));

    constexpr int kThreads = 32;
    constexpr int kReadsPerThread = 10;
    std::vector<std::thread> threads;
    threads.reserve(kThreads);
    for (int t = 0; t < kThreads; ++t) {
        threads.emplace_back([&service]() {
            for (int j = 0; j < kReadsPerThread; ++j) {
                auto r = service->GetReplicaList("k_cold", "default");
                EXPECT_TRUE(r.has_value());
            }
        });
    }
    for (auto& t : threads) t.join();

    auto pending = service->PromotionObjectHeartbeat(ctx.client_id);
    ASSERT_TRUE(pending.has_value());
    EXPECT_EQ(pending->size(), 1u)
        << "Concurrent readers (" << kThreads << " x " << kReadsPerThread
        << ") must dedupe to a single promotion task";
    ASSERT_TRUE(CountPromotionTask(*pending, "k_cold"));

    service->RemoveAll();
}

// A PromotionTask that never receives NotifyPromotionSuccess must be
// reaped after `put_start_release_timeout_sec` so that:
//   (a) the source LOCAL_DISK replica's refcnt is decremented (no
//       permanent pin → eviction can still free it later);
//   (b) the dedup gate is unblocked, so a subsequent read can re-enqueue
//       the same key for retry.
TEST_F(PromotionOnHitTest, QueueLimitRejectsBeyondCap) {
    MasterServiceConfig config;
    config.enable_offload = true;
    config.promotion_on_hit = true;
    config.promotion_admission_threshold = 1;
    config.promotion_queue_limit = 1;  // any 1 task saturates a shard
    config.default_kv_lease_ttl = 2000;
    auto service = std::make_unique<MasterService>(config);

    constexpr size_t seg_size = 1024 * 1024 * 16;
    auto seg = PrepareSegment(*service, "seg_a", kDefaultSegmentBase, seg_size);

    // Find two keys that hash to the same shard. MasterService::
    // getShardIndex is private but the formula is deterministic
    // (std::hash<std::string>{}(key) % kNumShards), so we can mirror
    // it here. kNumShards=1024 (master_service.h:889).
    constexpr size_t kNumShardsLocal = 1024;
    auto shard_of = [](const std::string& k) {
        return std::hash<std::string>{}(k) % kNumShardsLocal;
    };
    const std::string k1 = "qlim_first";
    std::string k2;
    for (int i = 0; i < 100000 && k2.empty(); ++i) {
        std::string candidate = "qlim_collide_" + std::to_string(i);
        if (shard_of(candidate) == shard_of(k1)) {
            k2 = candidate;
        }
    }
    ASSERT_FALSE(k2.empty())
        << "could not find a same-shard collision for " << k1;

    ASSERT_TRUE(InjectLocalDiskReplica(*service, seg.client_id, k1, 1024,
                                       seg.segment_name));
    ASSERT_TRUE(InjectLocalDiskReplica(*service, seg.client_id, k2, 1024,
                                       seg.segment_name));

    auto& mm = MasterMetricManager::instance();
    const int64_t cap_rej_pre = mm.get_promotion_rejected_cap();

    // First read on k1 enqueues a task in shard S.
    auto r1 = service->GetReplicaList(k1, "default");
    ASSERT_TRUE(r1.has_value());

    // Second read on k2 (same shard S, different key, so no dedup) must
    // be dropped by the cap gate: the cluster-wide in-flight counter is
    // already 1, which meets promotion_queue_limit_ = 1.
    auto r2 = service->GetReplicaList(k2, "default");
    ASSERT_TRUE(r2.has_value()) << "read itself must still succeed; "
                                << "queue gate is silent";

    // Drain the holder's queue: only k1 should appear.
    auto heartbeat = service->PromotionObjectHeartbeat(seg.client_id);
    ASSERT_TRUE(heartbeat.has_value());
    EXPECT_EQ(heartbeat->size(), 1u)
        << "promotion_queue_limit=1 should admit only the first task "
        << "globally; k2's enqueue must be dropped";
    EXPECT_EQ(CountPromotionTask(*heartbeat, k1), 1u)
        << "k1 was read first and should be the surviving task";
    EXPECT_EQ(CountPromotionTask(*heartbeat, k2), 0u)
        << "k2 was rejected by the cap gate; should not appear";
    EXPECT_EQ(mm.get_promotion_rejected_cap() - cap_rej_pre, 1)
        << "k2's rejection must increment promotion_rejected_cap";

    service->RemoveAll();
}

// PromotionObjectHeartbeat caps the per-call response at kMaxPerHeartbeat
// (1) and leaves the remainder queued for subsequent heartbeats. This is
// the master-side bound that keeps the client's heartbeat thread inside
// the liveness window when many keys are queued, while guaranteeing no
// task is dropped — a regression from the prior client-side cap that
// silently discarded leftovers.
TEST_F(PromotionOnHitTest, QueueLimitRejectsCrossShard) {
    MasterServiceConfig config;
    config.enable_offload = true;
    config.promotion_on_hit = true;
    config.promotion_admission_threshold = 1;
    config.promotion_queue_limit = 1;  // 1 in-flight task globally
    config.default_kv_lease_ttl = 2000;
    auto service = std::make_unique<MasterService>(config);

    constexpr size_t seg_size = 1024 * 1024 * 16;
    auto seg = PrepareSegment(*service, "seg_a", kDefaultSegmentBase, seg_size);

    // Find two keys hashing to *different* shards. With the old per-shard
    // heuristic this would let both through (each shard's count is 0
    // independently). With the global counter, only the first goes in.
    constexpr size_t kNumShardsLocal = 1024;
    auto shard_of = [](const std::string& k) {
        return std::hash<std::string>{}(k) % kNumShardsLocal;
    };
    const std::string k1 = "xshard_first";
    std::string k2;
    for (int i = 0; i < 100000 && k2.empty(); ++i) {
        std::string candidate = "xshard_other_" + std::to_string(i);
        if (shard_of(candidate) != shard_of(k1)) {
            k2 = candidate;
        }
    }
    ASSERT_FALSE(k2.empty()) << "couldn't find a different-shard key";
    ASSERT_NE(shard_of(k1), shard_of(k2));

    ASSERT_TRUE(InjectLocalDiskReplica(*service, seg.client_id, k1, 1024,
                                       seg.segment_name));
    ASSERT_TRUE(InjectLocalDiskReplica(*service, seg.client_id, k2, 1024,
                                       seg.segment_name));

    auto r1 = service->GetReplicaList(k1, "default");
    ASSERT_TRUE(r1.has_value());

    // k2 lives in a different shard, but the global cap is already met
    // by k1's task — k2 must be rejected.
    auto r2 = service->GetReplicaList(k2, "default");
    ASSERT_TRUE(r2.has_value()) << "read itself still succeeds";

    auto heartbeat = service->PromotionObjectHeartbeat(seg.client_id);
    ASSERT_TRUE(heartbeat.has_value());
    EXPECT_EQ(heartbeat->size(), 1u)
        << "with global cap=1 and one task already in shard " << shard_of(k1)
        << ", a key hashing to shard " << shard_of(k2)
        << " must be rejected by the global gate. A per-shard heuristic "
        << "would admit it here since the destination shard's local "
        << "count is 0.";
    EXPECT_EQ(CountPromotionTask(*heartbeat, k1), 1u);
    EXPECT_EQ(CountPromotionTask(*heartbeat, k2), 0u);

    service->RemoveAll();
}

// PromotionAllocStart must reset PromotionTask.start_time so the reaper
// TTL covers the active-transfer phase on its own and is not consumed by
// the queue-wait phase. Without the reset, a task that waited in the
// holder's promotion_objects queue for most of the original TTL could
// enter active transfer with little budget left; if the SSD read + RDMA
// write of a large object then ran past expiry, the reaper would
// EraseReplicaByID on the staged MEMORY replica mid-flight and the
// allocator could hand the freed buffer to a concurrent Put while the
// client's RDMA write is still landing into it (use-after-free in the
// allocator + silent data corruption for the concurrent Put).
//
// We can't directly observe start_time from a black-box test, so we
// arrange the timing so the reset is the only thing that distinguishes
// "task still alive" from "task reaped" at the assertion points:
//
//   T=0    : admit task   (original start_time = T=0)
//   T=Wq   : AllocStart   (resets start_time = T=Wq)
//   T=Wq+Wa: assertion 1  -- alive iff the reset happened
//                            (Wq + Wa > TTL,  so without reset the
//                             original start_time has aged past TTL)
//                            (Wa < TTL,        so with the reset the
//                             new start_time has NOT aged past TTL)
//   T=Wq+Wb: assertion 2  -- reaped
//                            (Wb > TTL, so even with the reset the
//                             active-transfer phase has now exceeded its
//                             own full window)
//
// Concretely with TTL = 2s, Wq = 1.5s, Wa = 1.5s (-> 3.0s elapsed,
// 1.5s since AllocStart), Wb = 3.0s (-> 4.5s elapsed, 3.0s since
// AllocStart). The "alive" assertion at Wq+Wa is the one that proves
// the reset is wired up correctly.
//
// QuerySegments(seg).first (used bytes) is the observable: AllocStart
// bumps it, the reaper's EraseReplicaByID returns it to baseline. We
// can't use GetReplicaList because the master filters out PROCESSING
// replicas from the response.
TEST_F(PromotionOnHitTest, AdmissionThresholdZeroClampsToOne) {
    MasterServiceConfig config;
    config.enable_offload = true;
    config.promotion_on_hit = true;
    config.promotion_admission_threshold = 0;  // would bypass the gate
    auto service = std::make_unique<MasterService>(config);
    EXPECT_EQ(GetPromotionAdmissionThresholdForTesting(service.get()), 1u)
        << "threshold=0 must be clamped to 1; otherwise every Get-on-"
        << "LOCAL_DISK admits a promotion and the frequency gate is "
        << "silently disabled.";
}

// promotion_admission_threshold above the CountMinSketch saturating
// max (255) would make the gate unreachable (freq saturates at 255,
// `255 < 256` is true forever → no admissions). Verify the constructor
// clamps high too.
TEST_F(PromotionOnHitTest, AdmissionThresholdAboveMaxClampsToMax) {
    MasterServiceConfig config;
    config.enable_offload = true;
    config.promotion_on_hit = true;
    config.promotion_admission_threshold = 1000;  // > 255
    auto service = std::make_unique<MasterService>(config);
    EXPECT_EQ(GetPromotionAdmissionThresholdForTesting(service.get()), 255u)
        << "threshold above the CountMinSketch counter max (255) must "
        << "be clamped to 255; otherwise the gate is unreachable.";
}

// Remove(force=true) on a key with an in-flight PromotionTask must
// drop the task entry alongside the metadata, so promotion_in_flight_
// is decremented immediately rather than pinned for ~10 min until the
// reaper sweeps. With queue_limit=1, the test admits one task, removes
// the key, then admits a second task on a different key — only
// succeeds if the slot was freed.
TEST_F(PromotionOnHitTest, AdmissionFrequencyIsTenantScoped) {
    MasterServiceConfig config;
    config.enable_offload = true;
    config.enable_multi_tenants = true;
    config.tenant_quota_connector_type = "file";
    config.promotion_on_hit = true;
    config.promotion_admission_threshold = 2;
    config.default_kv_lease_ttl = 2000;
    const std::string key = "shared_hot_key";
    const std::string tenant_a = "tenant_promotion_a";
    const std::string tenant_b = "tenant_promotion_b";
    config.tenant_quota_connector_uri = WriteTenantQuotaPolicyFile(
        {{tenant_a, 64 * 1024 * 1024}, {tenant_b, 64 * 1024 * 1024}});
    auto service = std::make_unique<MasterService>(config);

    constexpr size_t seg_size = 1024 * 1024 * 16;
    auto seg =
        PrepareSegment(*service, "seg_tenant", kDefaultSegmentBase, seg_size);
    ASSERT_TRUE(InjectLocalDiskReplica(*service, seg.client_id, key, 1024,
                                       seg.segment_name, tenant_a));
    ASSERT_TRUE(InjectLocalDiskReplica(*service, seg.client_id, key, 1024,
                                       seg.segment_name, tenant_b));

    {
        auto r = service->GetReplicaList(key, tenant_a);
        ASSERT_TRUE(r.has_value());
    }
    {
        auto r = service->GetReplicaList(key, tenant_b);
        ASSERT_TRUE(r.has_value());
    }
    auto pending_after_one_each =
        service->PromotionObjectHeartbeat(seg.client_id);
    ASSERT_TRUE(pending_after_one_each.has_value());
    EXPECT_TRUE(pending_after_one_each->empty())
        << "same user key in two tenants must not share promotion heat";

    {
        auto r = service->GetReplicaList(key, tenant_a);
        ASSERT_TRUE(r.has_value());
    }
    auto pending_after_tenant_a_second =
        service->PromotionObjectHeartbeat(seg.client_id);
    ASSERT_TRUE(pending_after_tenant_a_second.has_value());
    ASSERT_EQ(pending_after_tenant_a_second->size(), 1u);
    EXPECT_EQ((*pending_after_tenant_a_second)[0].tenant_id, tenant_a);
    EXPECT_EQ((*pending_after_tenant_a_second)[0].key, key);

    service->RemoveAll();
}

// promotion_max_per_heartbeat controls how many tasks
// PromotionObjectHeartbeat returns per call. Set the knob to 3 and
// verify the master returns up to 3 tasks per heartbeat.
}  // namespace mooncake::test
