// Unit tests for the L2->L1 promotion-on-hit master-side path. Exercises
// the master-service entry points directly without going through the RPC
// layer.

#include "master_service.h"

#include <glog/logging.h>
#include <gtest/gtest.h>

#include <chrono>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include "types.h"

namespace mooncake::test {

class PromotionOnHitTest : public ::testing::Test {
   protected:
    void SetUp() override {
        google::InitGoogleLogging("PromotionOnHitTest");
        FLAGS_logtostderr = true;
    }

    void TearDown() override { google::ShutdownGoogleLogging(); }

    static constexpr size_t kDefaultSegmentBase = 0x300000000;

    Segment MakeSegment(std::string name, size_t base, size_t size) const {
        Segment segment;
        segment.id = generate_uuid();
        segment.name = std::move(name);
        segment.base = base;
        segment.size = size;
        segment.te_endpoint = segment.name;
        return segment;
    }

    struct MountedSegmentContext {
        UUID segment_id;
        UUID client_id;
        std::string segment_name;
    };

    MountedSegmentContext PrepareSegment(MasterService& service,
                                         std::string name, size_t base,
                                         size_t size) const {
        Segment segment = MakeSegment(std::move(name), base, size);
        UUID client_id = generate_uuid();
        auto mount_result = service.MountSegment(segment, client_id);
        EXPECT_TRUE(mount_result.has_value());
        auto mount_ld = service.MountLocalDiskSegment(client_id, true);
        EXPECT_TRUE(mount_ld.has_value());
        return {.segment_id = segment.id,
                .client_id = client_id,
                .segment_name = segment.name};
    }

    // Put an object and complete it (creates a MEMORY replica).
    void PutObject(MasterService& service, const UUID& client_id,
                   const std::string& key, size_t size = 1024) {
        ReplicateConfig config;
        config.replica_num = 1;
        auto put_start = service.PutStart(client_id, key, size, config);
        ASSERT_TRUE(put_start.has_value()) << "PutStart failed for key=" << key;
        auto put_end = service.PutEnd(client_id, key, ReplicaType::MEMORY);
        ASSERT_TRUE(put_end.has_value()) << "PutEnd failed for key=" << key;
    }

    // Force a key into the LOCAL_DISK-only state by:
    //   1. Putting a MEMORY replica, then evicting it via RemoveByKey.
    //   2. Re-creating the metadata via NotifyOffloadSuccess with a synthetic
    //      LOCAL_DISK descriptor.
    // Cleaner approach used here: do a full PutStart+PutEnd cycle with
    // offload_on_evict=true, which leaves an offload task pending. We then
    // simulate the heartbeat by calling NotifyOffloadSuccess directly to
    // attach a LOCAL_DISK replica, and manually drop the MEMORY replica via
    // a forced eviction. For a v1 unit test we cheat by using the simpler
    // setup: NotifyOffloadSuccess on a key whose MEMORY replica we can let
    // eviction naturally remove.
    //
    // Simplest path that doesn't require a real FileStorage: put a key, then
    // call NotifyOffloadSuccess to add a LOCAL_DISK replica alongside, then
    // evict the MEMORY (via lease expiry + watermark). For a unit test, we
    // skip the eviction and instead test the trigger logic on a key with
    // BOTH MEMORY and LOCAL_DISK replicas: the trigger should NOT fire
    // because the gate requires no MEMORY replica. So we need a key with
    // ONLY LOCAL_DISK.
    //
    // To get a clean LOCAL_DISK-only state we use this sequence:
    //   1. PutStart + write + PutEnd → key has 1 MEMORY replica.
    //   2. Master configured with offload_on_evict=false so PutEnd attempts
    //      immediate offload via PushOffloadingQueue.
    //   3. NotifyOffloadSuccess → adds LOCAL_DISK replica (now 2 replicas).
    //   4. RemoveByKey is too coarse (drops everything). Instead we call
    //      a helper EvictMemReplicas which we add in this test fixture by
    //      poking the MasterService's eviction thread under high pressure.
    //
    // For v1 simplicity, we just test the trigger on a key with BOTH MEMORY
    // and LOCAL_DISK replicas. We assert the inverted behavior: any_memory
    // is true, so no promotion task is queued. That's still a meaningful
    // gate test. For LOCAL_DISK-only behavior we'd need eviction to land,
    // which the existing test infrastructure already exercises in
    // offload_on_evict_test under near-full segment pressure; we mirror
    // that pattern in the second test below.
    bool InjectLocalDiskReplica(MasterService& service, const UUID& client_id,
                                const std::string& key, int64_t size,
                                const std::string& transport_endpoint) {
        std::vector<std::string> keys{key};
        StorageObjectMetadata sm;
        sm.bucket_id = 0;
        sm.offset = 0;
        sm.key_size = static_cast<int64_t>(key.size());
        sm.data_size = size;
        sm.transport_endpoint = transport_endpoint;
        std::vector<StorageObjectMetadata> metas{sm};
        auto res = service.NotifyOffloadSuccess(client_id, keys, metas);
        return res.has_value();
    }

    // Register a client as a LOCAL_DISK holder only (no DRAM segment).
    // This simulates the cross-host case where the LOCAL_DISK source lives
    // on a different node than the DRAM target chosen for promotion.
    UUID PrepareLocalDiskOnlyClient(MasterService& service) const {
        UUID client_id = generate_uuid();
        auto mount_ld = service.MountLocalDiskSegment(client_id, true);
        EXPECT_TRUE(mount_ld.has_value());
        return client_id;
    }
};

// Sanity: with promotion disabled, no path mutates promotion_objects.
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
        auto resp = service->GetReplicaList("k1");
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
        auto resp = service->GetReplicaList("k_mem_only");
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
        auto resp = service->GetReplicaList("k_dual");
        ASSERT_TRUE(resp.has_value());
    }

    auto pending = service->PromotionObjectHeartbeat(ctx.client_id);
    ASSERT_TRUE(pending.has_value());
    EXPECT_EQ(pending->size(), 0u)
        << "MEMORY replica still present: should not promote";

    service->RemoveAll();
}

// PromotionObjectHeartbeat returns an empty map when called against a
// client that has no LocalDiskSegment registered.
TEST_F(PromotionOnHitTest, HeartbeatReturnsErrorForUnknownClient) {
    MasterServiceConfig config;
    config.enable_offload = true;
    config.promotion_on_hit = true;
    auto service = std::make_unique<MasterService>(config);

    UUID unknown_client = generate_uuid();
    auto pending = service->PromotionObjectHeartbeat(unknown_client);
    ASSERT_FALSE(pending.has_value());
    EXPECT_EQ(pending.error(), ErrorCode::SEGMENT_NOT_FOUND);
}

// PromotionAllocStart on a non-existent key returns OBJECT_NOT_FOUND.
TEST_F(PromotionOnHitTest, AllocStartUnknownKey) {
    MasterServiceConfig config;
    config.enable_offload = true;
    config.promotion_on_hit = true;
    auto service = std::make_unique<MasterService>(config);

    auto resp = service->PromotionAllocStart("nonexistent", 1024, {});
    ASSERT_FALSE(resp.has_value());
    EXPECT_EQ(resp.error(), ErrorCode::OBJECT_NOT_FOUND);
}

// NotifyPromotionSuccess on a non-existent key returns OBJECT_NOT_FOUND.
TEST_F(PromotionOnHitTest, NotifyUnknownKey) {
    MasterServiceConfig config;
    config.enable_offload = true;
    config.promotion_on_hit = true;
    auto service = std::make_unique<MasterService>(config);

    UUID client_id = generate_uuid();
    auto resp = service->NotifyPromotionSuccess(client_id, "nonexistent");
    ASSERT_FALSE(resp.has_value());
    EXPECT_EQ(resp.error(), ErrorCode::OBJECT_NOT_FOUND);
}

// Concurrent readers racing into TryPushPromotionQueue must dedupe to a
// single PromotionTask. Without dedup, the source LOCAL_DISK replica's
// refcnt would be incremented N times and the per-segment promotion_objects
// map would either error on the second insert (current behavior) or
// double-queue.
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
                auto r = service->GetReplicaList("k_cold");
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
    ASSERT_TRUE(pending->count("k_cold"));

    service->RemoveAll();
}

// A PromotionTask that never receives NotifyPromotionSuccess must be
// reaped after `put_start_release_timeout_sec` so that:
//   (a) the source LOCAL_DISK replica's refcnt is decremented (no
//       permanent pin → eviction can still free it later);
//   (b) the dedup gate is unblocked, so a subsequent read can re-enqueue
//       the same key for retry.
TEST_F(PromotionOnHitTest, StalePromotionReaper) {
    MasterServiceConfig config;
    config.enable_offload = true;
    config.promotion_on_hit = true;
    config.promotion_admission_threshold = 1;
    config.default_kv_lease_ttl = 2000;
    // Short staleness window: the reaper uses put_start_release_timeout_sec
    // for promotion tasks. The master enforces release > discard, so set
    // discard to a still-smaller value.
    config.put_start_discard_timeout_sec = 0;
    config.put_start_release_timeout_sec = 1;
    auto service = std::make_unique<MasterService>(config);

    constexpr size_t seg_size = 1024 * 1024 * 16;
    auto ctx =
        PrepareSegment(*service, "test_segment", kDefaultSegmentBase, seg_size);

    ASSERT_TRUE(InjectLocalDiskReplica(*service, ctx.client_id, "k_cold", 1024,
                                       ctx.segment_name));

    // Trigger #1: enqueue, then drain the per-segment queue. Drain leaves
    // the per-shard PromotionTask intact (the heartbeat is best-effort GC,
    // not the authoritative state).
    {
        auto r = service->GetReplicaList("k_cold");
        ASSERT_TRUE(r.has_value());
        auto pending = service->PromotionObjectHeartbeat(ctx.client_id);
        ASSERT_TRUE(pending.has_value());
        EXPECT_EQ(pending->size(), 1u);
    }

    // Without reap, dedup blocks re-enqueue. Confirm: GetReplicaList again,
    // heartbeat must be empty because PromotionTask still pins the slot.
    {
        auto r = service->GetReplicaList("k_cold");
        ASSERT_TRUE(r.has_value());
        auto pending = service->PromotionObjectHeartbeat(ctx.client_id);
        ASSERT_TRUE(pending.has_value());
        EXPECT_EQ(pending->size(), 0u)
            << "Dedup gate should block re-enqueue while task is in flight";
    }

    // Wait past the staleness window; the eviction thread reaps the task.
    // Eviction loop sleeps for kEvictionThreadSleepMs (10 ms), so 2s wall
    // clock gives ~200 attempts — plenty.
    std::this_thread::sleep_for(std::chrono::seconds(2));

    // Trigger #3: with the task reaped, dedup is unblocked and a fresh
    // GetReplicaList must enqueue again.
    {
        auto r = service->GetReplicaList("k_cold");
        ASSERT_TRUE(r.has_value());
        auto pending = service->PromotionObjectHeartbeat(ctx.client_id);
        ASSERT_TRUE(pending.has_value());
        EXPECT_EQ(pending->size(), 1u)
            << "After reap, a fresh read must re-enqueue the same key";
    }

    service->RemoveAll();
}

// Force-Remove on a key with a queued PromotionTask must not corrupt
// state. The PromotionTask references a Replica*; once the metadata is
// gone, NotifyPromotionSuccess and the reaper must both tolerate the
// missing entry.
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

    // Queue a promotion task.
    {
        auto r = service->GetReplicaList("k_cold");
        ASSERT_TRUE(r.has_value());
    }

    // Lease must expire before we can call non-force Remove (or use force).
    auto rm = service->Remove("k_cold", /*force=*/true);
    // Remove returns REPLICA_IS_NOT_READY if any replica is non-COMPLETE.
    // The injected LOCAL_DISK replica is COMPLETE, so this should succeed.
    ASSERT_TRUE(rm.has_value())
        << "Remove on a LOCAL_DISK-only key with a queued promotion should "
        << "succeed (all replicas COMPLETE); error=" << rm.error();

    // NotifyPromotionSuccess on the now-removed key must surface the missing
    // metadata cleanly, not crash.
    auto notify = service->NotifyPromotionSuccess(ctx.client_id, "k_cold");
    ASSERT_FALSE(notify.has_value());
    EXPECT_EQ(notify.error(), ErrorCode::OBJECT_NOT_FOUND);

    // Wait for the reaper; it must tolerate the missing metadata entry
    // (the source replica it would dec_refcnt is already gone).
    std::this_thread::sleep_for(std::chrono::seconds(2));

    // Re-injecting the key and re-triggering must work end-to-end, proving
    // the per-shard PromotionTask was reaped (not stuck).
    ASSERT_TRUE(InjectLocalDiskReplica(*service, ctx.client_id, "k_cold", 1024,
                                       ctx.segment_name));
    {
        auto r = service->GetReplicaList("k_cold");
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
TEST_F(PromotionOnHitTest, MultiSegmentAllocPicksAvailableSegment) {
    MasterServiceConfig config;
    config.enable_offload = true;
    config.promotion_on_hit = true;
    config.promotion_admission_threshold = 1;
    config.default_kv_lease_ttl = 2000;
    auto service = std::make_unique<MasterService>(config);

    // Holder: LOCAL_DISK only, no DRAM. Stands in for "host A" that has the
    // SSD copy but no free RAM.
    UUID holder_client_id = PrepareLocalDiskOnlyClient(*service);

    // Reader-side DRAM target: "host B" with a fresh DRAM segment.
    constexpr size_t seg_size = 1024 * 1024 * 16;
    auto reader_ctx =
        PrepareSegment(*service, "segment_b", kDefaultSegmentBase, seg_size);

    // The cold key: only replica is LOCAL_DISK on the holder client.
    ASSERT_TRUE(InjectLocalDiskReplica(*service, holder_client_id, "k_cold",
                                       1024, "segment_a_endpoint"));

    // Direct PromotionAllocStart — bypass the gate to test the allocation
    // path in isolation.
    auto resp = service->PromotionAllocStart("k_cold", 1024, {});
    ASSERT_TRUE(resp.has_value())
        << "PromotionAllocStart should succeed when any DRAM segment has "
        << "capacity; error=" << resp.error();

    const auto& mem_desc = resp.value().memory_descriptor;
    ASSERT_TRUE(mem_desc.is_memory_replica());
    EXPECT_EQ(
        mem_desc.get_memory_descriptor().buffer_descriptor.transport_endpoint_,
        reader_ctx.segment_name)
        << "New MEMORY replica must be allocated on the only DRAM segment "
        << "(segment_b), not on the LOCAL_DISK holder which has no DRAM";

    // Sanity: the staged MEMORY replica is PROCESSING (visible only after
    // NotifyPromotionSuccess flips it COMPLETE).
    EXPECT_EQ(mem_desc.status, ReplicaStatus::PROCESSING);

    service->RemoveAll();
}

// preferred_segments is honored: promotion can be steered to a specific
// DRAM segment (e.g., the reader's local one). v1 doesn't pass preferred_
// segments from TryPushPromotionQueue, but PromotionAllocStart respects it
// for clients that want to bias toward locality.
TEST_F(PromotionOnHitTest, MultiSegmentAllocRespectsPreferred) {
    MasterServiceConfig config;
    config.enable_offload = true;
    config.promotion_on_hit = true;
    config.promotion_admission_threshold = 1;
    auto service = std::make_unique<MasterService>(config);

    constexpr size_t seg_size = 1024 * 1024 * 16;
    auto seg_a =
        PrepareSegment(*service, "segment_a", kDefaultSegmentBase, seg_size);
    auto seg_b = PrepareSegment(*service, "segment_b",
                                kDefaultSegmentBase + seg_size, seg_size);

    ASSERT_TRUE(InjectLocalDiskReplica(*service, seg_a.client_id, "k_cold",
                                       1024, seg_a.segment_name));

    auto resp =
        service->PromotionAllocStart("k_cold", 1024, {seg_b.segment_name});
    ASSERT_TRUE(resp.has_value());
    const auto& mem_desc = resp.value().memory_descriptor;
    EXPECT_EQ(
        mem_desc.get_memory_descriptor().buffer_descriptor.transport_endpoint_,
        seg_b.segment_name)
        << "preferred_segments={segment_b} should pin the new MEMORY "
        << "replica to segment_b";

    service->RemoveAll();
}

// promotion_queue_limit caps how many in-flight tasks can sit in a
// single shard (gate: shard_size * kNumShards >= limit). With limit=1
// the very first queued task in a shard saturates that shard, so a
// second LOCAL_DISK-only read on a key hashing to the same shard must
// be silently dropped by the cap gate (reads still succeed; just no
// new task is enqueued).
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

    // First read on k1 enqueues a task in shard S.
    auto r1 = service->GetReplicaList(k1);
    ASSERT_TRUE(r1.has_value());

    // Second read on k2 (same shard S, different key, so no dedup) must
    // be dropped by the cap gate: the cluster-wide in-flight counter is
    // already 1, which meets promotion_queue_limit_ = 1.
    auto r2 = service->GetReplicaList(k2);
    ASSERT_TRUE(r2.has_value()) << "read itself must still succeed; "
                                << "queue gate is silent";

    // Drain the holder's queue: only k1 should appear.
    auto heartbeat = service->PromotionObjectHeartbeat(seg.client_id);
    ASSERT_TRUE(heartbeat.has_value());
    EXPECT_EQ(heartbeat->size(), 1u)
        << "promotion_queue_limit=1 should admit only the first task "
        << "globally; k2's enqueue must be dropped";
    EXPECT_EQ(heartbeat->count(k1), 1u)
        << "k1 was read first and should be the surviving task";
    EXPECT_EQ(heartbeat->count(k2), 0u)
        << "k2 was rejected by the cap gate; should not appear";

    service->RemoveAll();
}

// PromotionObjectHeartbeat caps the per-call response at kMaxPerHeartbeat
// (1) and leaves the remainder queued for subsequent heartbeats. This is
// the master-side bound that keeps the client's heartbeat thread inside
// the liveness window when many keys are queued, while guaranteeing no
// task is dropped — a regression from the prior client-side cap that
// silently discarded leftovers.
TEST_F(PromotionOnHitTest, HeartbeatBoundedBatchPreservesLeftovers) {
    MasterServiceConfig config;
    config.enable_offload = true;
    config.promotion_on_hit = true;
    config.promotion_admission_threshold = 1;
    config.default_kv_lease_ttl = 2000;
    auto service = std::make_unique<MasterService>(config);

    constexpr size_t seg_size = 1024 * 1024 * 16;
    auto seg = PrepareSegment(*service, "seg_a", kDefaultSegmentBase, seg_size);

    // Push three keys into the holder's promotion_objects via the trigger
    // path. Each is a LOCAL_DISK-only key (no MEMORY replica), so the
    // first GetReplicaList per key crosses the admission threshold (=1)
    // and TryPushPromotionQueue enqueues a task.
    const std::vector<std::string> keys{"hb_k1", "hb_k2", "hb_k3"};
    for (const auto& k : keys) {
        ASSERT_TRUE(InjectLocalDiskReplica(*service, seg.client_id, k, 1024,
                                           seg.segment_name));
        auto r = service->GetReplicaList(k);
        ASSERT_TRUE(r.has_value());
    }

    // First heartbeat returns at most 1 key.
    auto tick1 = service->PromotionObjectHeartbeat(seg.client_id);
    ASSERT_TRUE(tick1.has_value());
    EXPECT_EQ(tick1->size(), 1u)
        << "heartbeat should return at most kMaxPerHeartbeat=1 entry";
    std::string first_key = tick1->begin()->first;
    EXPECT_TRUE(std::find(keys.begin(), keys.end(), first_key) != keys.end());

    // Second heartbeat returns another key (a different one).
    auto tick2 = service->PromotionObjectHeartbeat(seg.client_id);
    ASSERT_TRUE(tick2.has_value());
    EXPECT_EQ(tick2->size(), 1u);
    std::string second_key = tick2->begin()->first;
    EXPECT_NE(second_key, first_key)
        << "second heartbeat must drain a different leftover key, not "
        << "re-return the one already extracted";

    // Third heartbeat returns the third key.
    auto tick3 = service->PromotionObjectHeartbeat(seg.client_id);
    ASSERT_TRUE(tick3.has_value());
    EXPECT_EQ(tick3->size(), 1u);
    std::string third_key = tick3->begin()->first;
    EXPECT_NE(third_key, first_key);
    EXPECT_NE(third_key, second_key);

    // Fourth heartbeat: queue is now empty.
    auto tick4 = service->PromotionObjectHeartbeat(seg.client_id);
    ASSERT_TRUE(tick4.has_value());
    EXPECT_TRUE(tick4->empty())
        << "after draining all queued keys, heartbeat must return empty";

    // Sanity: master-side promotion_tasks records are intact for all keys
    // (they're cleared by NotifyPromotionSuccess, not by Heartbeat), so the
    // source refcnts remain pinned until processed.
    for (const auto& k : keys) {
        auto rl = service->GetReplicaList(k);
        ASSERT_TRUE(rl.has_value()) << "key " << k << " should still exist";
    }

    service->RemoveAll();
}

// Issue 1 regression: the promotion task reaper must pop the staged
// PROCESSING MEMORY replica added by PromotionAllocStart. Without it,
// the staged replica was orphaned forever: it's not in
// shard->processing_keys (so DiscardExpiredProcessingReplicas can't see
// it) and the previous reaper code only touched the source LOCAL_DISK
// refcnt and the task entry. The orphan held its allocator buffer
// indefinitely.
//
// We can't observe the staged replica via GetReplicaList because the
// master filters out PROCESSING entries (clients can only read COMPLETE
// replicas), so we use QuerySegments to watch the DRAM allocator's used
// bytes: AllocStart bumps it, and the reaper must return it to baseline.
// NotifyPromotionSuccess on a reaped task must also fail cleanly.
TEST_F(PromotionOnHitTest, ReaperPopsStagedMemoryReplicaOnExpiry) {
    MasterServiceConfig config;
    config.enable_offload = true;
    config.promotion_on_hit = true;
    config.promotion_admission_threshold = 1;
    config.default_kv_lease_ttl = 2000;
    config.put_start_discard_timeout_sec = 0;
    config.put_start_release_timeout_sec = 1;
    auto service = std::make_unique<MasterService>(config);

    constexpr size_t seg_size = 1024 * 1024 * 16;
    auto ctx = PrepareSegment(*service, "seg_a", kDefaultSegmentBase, seg_size);
    ASSERT_TRUE(InjectLocalDiskReplica(*service, ctx.client_id, "k_cold", 1024,
                                       ctx.segment_name));

    // Baseline allocator usage on the DRAM segment.
    auto seg_baseline = service->QuerySegments(ctx.segment_name);
    ASSERT_TRUE(seg_baseline.has_value());
    const size_t used_baseline = seg_baseline->first;

    // Trigger the gate to enqueue a PromotionTask.
    {
        auto r = service->GetReplicaList("k_cold");
        ASSERT_TRUE(r.has_value());
    }
    // Drive the AllocStart side so alloc_id != 0 — this is the exact
    // setup that left an orphaned PROCESSING MEMORY replica pre-fix.
    auto alloc = service->PromotionAllocStart("k_cold", 1024, {});
    ASSERT_TRUE(alloc.has_value());

    // After AllocStart, the DRAM allocator must have committed bytes for
    // the staged PROCESSING MEMORY replica.
    auto seg_after_alloc = service->QuerySegments(ctx.segment_name);
    ASSERT_TRUE(seg_after_alloc.has_value());
    EXPECT_GT(seg_after_alloc->first, used_baseline)
        << "PromotionAllocStart should bump segment used bytes "
        << "(allocator-tracked PROCESSING MEMORY replica)";

    // Sleep past the staleness window; the eviction thread reaps the
    // task and (with the fix) pops the staged replica via
    // EraseReplicaByID, which releases the buffer back to the allocator.
    std::this_thread::sleep_for(std::chrono::seconds(2));

    auto seg_after_reap = service->QuerySegments(ctx.segment_name);
    ASSERT_TRUE(seg_after_reap.has_value());
    EXPECT_EQ(seg_after_reap->first, used_baseline)
        << "after reap: staged PROCESSING MEMORY replica's buffer must "
        << "be freed back to the DRAM allocator. Pre-fix the buffer "
        << "leaked and used bytes stayed elevated until the object "
        << "itself was removed or evicted.";

    // NotifyPromotionSuccess for a reaped task must not commit anything
    // and must return REPLICA_IS_NOT_READY (the task entry is gone, so
    // the alloc_id lookup at the top of NotifyPromotionSuccess fails
    // fast).
    auto notify = service->NotifyPromotionSuccess(ctx.client_id, "k_cold");
    ASSERT_FALSE(notify.has_value());
    EXPECT_EQ(notify.error(), ErrorCode::REPLICA_IS_NOT_READY);

    service->RemoveAll();
}

// Issue 2 regression: the cap gate must be cluster-wide. The old
// implementation used `shard->size() * kNumShards >= promotion_queue_limit_`,
// which made the cap fire ~1024x too eagerly on skewed workloads (hot
// keys cluster in few shards). With a global atomic counter, a task in
// shard A counts toward the cap that gates a task in shard B.
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

    auto r1 = service->GetReplicaList(k1);
    ASSERT_TRUE(r1.has_value());

    // k2 lives in a different shard, but the global cap is already met
    // by k1's task — k2 must be rejected.
    auto r2 = service->GetReplicaList(k2);
    ASSERT_TRUE(r2.has_value()) << "read itself still succeeds";

    auto heartbeat = service->PromotionObjectHeartbeat(seg.client_id);
    ASSERT_TRUE(heartbeat.has_value());
    EXPECT_EQ(heartbeat->size(), 1u)
        << "with global cap=1 and one task already in shard " << shard_of(k1)
        << ", a key hashing to shard " << shard_of(k2)
        << " must be rejected by the global gate (pre-fix it would have "
        << "been admitted since its shard's local count was 0)";
    EXPECT_EQ(heartbeat->count(k1), 1u);
    EXPECT_EQ(heartbeat->count(k2), 0u);

    service->RemoveAll();
}

}  // namespace mooncake::test

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
