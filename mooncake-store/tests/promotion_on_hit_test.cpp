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

    // Friend access to MasterService::promotion_admission_threshold_, which
    // is otherwise private. PromotionOnHitTest is friended; TEST_F-generated
    // subclasses are not, hence this static funnel.
    static uint32_t GetPromotionAdmissionThresholdForTesting(
        MasterService* service) {
        return service->promotion_admission_threshold_;
    }

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

    // Inject a synthetic LOCAL_DISK replica for `key` on `client_id`'s
    // segment via NotifyOffloadSuccess. Lets tests put a key into
    // LOCAL_DISK-only state without running the full offload pipeline.
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

    auto resp =
        service->PromotionAllocStart(generate_uuid(), "nonexistent", 1024, {});
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

    // Wait until the reaper actually sweeps the stale task. On slower
    // coverage runners a fixed 2s sleep is not always enough to guarantee
    // the background eviction thread has run after the 1s TTL expires.
    // Keep issuing a fresh read because re-enqueue only happens on demand
    // after the old PromotionTask has been removed.
    bool requeued = false;
    const auto deadline =
        std::chrono::steady_clock::now() + std::chrono::seconds(5);
    while (std::chrono::steady_clock::now() < deadline) {
        auto r = service->GetReplicaList("k_cold");
        ASSERT_TRUE(r.has_value());
        auto pending = service->PromotionObjectHeartbeat(ctx.client_id);
        ASSERT_TRUE(pending.has_value());
        if (pending->size() == 1u) {
            requeued = true;
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }
    EXPECT_TRUE(requeued)
        << "After reap, a fresh read must re-enqueue the same key";

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

    // Seed the PromotionTask through the gate — PromotionAllocStart now
    // requires an in-flight task to exist (rejects orphaned-stage path).
    {
        auto r = service->GetReplicaList("k_cold");
        ASSERT_TRUE(r.has_value());
    }

    // PromotionAllocStart — test the segment-selection logic on top of
    // the gate-seeded task.
    auto resp =
        service->PromotionAllocStart(holder_client_id, "k_cold", 1024, {});
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
// DRAM segment (e.g., the reader's local one). TryPushPromotionQueue
// currently doesn't pass preferred_segments through, but
// PromotionAllocStart respects them for clients biasing toward locality.
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

    // Seed the PromotionTask through the gate so AllocStart's
    // task-existence check passes.
    {
        auto r = service->GetReplicaList("k_cold");
        ASSERT_TRUE(r.has_value());
    }

    auto resp = service->PromotionAllocStart(seg_a.client_id, "k_cold", 1024,
                                             {seg_b.segment_name});
    ASSERT_TRUE(resp.has_value());
    const auto& mem_desc = resp.value().memory_descriptor;
    EXPECT_EQ(
        mem_desc.get_memory_descriptor().buffer_descriptor.transport_endpoint_,
        seg_b.segment_name)
        << "preferred_segments={segment_b} should pin the new MEMORY "
        << "replica to segment_b";

    service->RemoveAll();
}

// promotion_queue_limit caps total in-flight tasks cluster-wide
// (gate: promotion_in_flight_ >= limit). With limit=1 the very first
// queued task saturates the cap, so a second LOCAL_DISK-only read —
// even on a key in the same shard — must be silently dropped by the
// cap gate (reads still succeed; just no new task is enqueued).
// QueueLimitRejectsCrossShard covers the same-cap-across-different-
// shards case.
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

// The promotion task reaper must pop the staged PROCESSING MEMORY
// replica added by PromotionAllocStart. The staged replica is not in
// shard->processing_keys, so DiscardExpiredProcessingReplicas's main
// sweep can't see it, and the reaper for promotion tasks is the only
// place that knows the replica exists. Without this path the orphan
// holds its allocator buffer indefinitely (until the object is removed
// or evicted).
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
    // setup that produces an orphaned PROCESSING MEMORY replica if the
    // reaper does not pop it.
    auto alloc =
        service->PromotionAllocStart(ctx.client_id, "k_cold", 1024, {});
    ASSERT_TRUE(alloc.has_value());

    // After AllocStart, the DRAM allocator must have committed bytes for
    // the staged PROCESSING MEMORY replica.
    auto seg_after_alloc = service->QuerySegments(ctx.segment_name);
    ASSERT_TRUE(seg_after_alloc.has_value());
    EXPECT_GT(seg_after_alloc->first, used_baseline)
        << "PromotionAllocStart should bump segment used bytes "
        << "(allocator-tracked PROCESSING MEMORY replica)";

    // Wait past the staleness window; the eviction thread reaps the
    // task and (with the fix) pops the staged replica via
    // EraseReplicaByID, which releases the buffer back to the allocator.
    // Poll instead of a fixed sleep — the eviction thread cadence and
    // the test runner's scheduling jitter (especially under suite load)
    // both vary, so a hard 2s sleep is flaky here.
    constexpr auto kPollDeadline = std::chrono::seconds(5);
    constexpr auto kPollInterval = std::chrono::milliseconds(50);
    const auto poll_start = std::chrono::steady_clock::now();
    size_t used_after_reap = 0;
    while (std::chrono::steady_clock::now() - poll_start < kPollDeadline) {
        auto q = service->QuerySegments(ctx.segment_name);
        ASSERT_TRUE(q.has_value());
        used_after_reap = q->first;
        if (used_after_reap == used_baseline) break;
        std::this_thread::sleep_for(kPollInterval);
    }
    EXPECT_EQ(used_after_reap, used_baseline)
        << "after reap: staged PROCESSING MEMORY replica's buffer must "
        << "be freed back to the DRAM allocator. If this fires, the "
        << "reaper is not popping the staged replica and the buffer "
        << "leaks until the object itself is removed or evicted.";

    // NotifyPromotionSuccess for a reaped task must not commit anything
    // and must return REPLICA_IS_NOT_READY (the task entry is gone, so
    // the alloc_id lookup at the top of NotifyPromotionSuccess fails
    // fast).
    auto notify = service->NotifyPromotionSuccess(ctx.client_id, "k_cold");
    ASSERT_FALSE(notify.has_value());
    EXPECT_EQ(notify.error(), ErrorCode::REPLICA_IS_NOT_READY);

    service->RemoveAll();
}

// The cap gate must be cluster-wide, not per-shard. Promotion targets
// skewed hot keys, which by definition cluster into a small number of
// shards; a `shard->size() * kNumShards >= limit` heuristic fires
// roughly kNumShards-times too eagerly on that workload. With a global
// atomic counter, a task in shard A counts toward the cap that gates a
// task in shard B.
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
        << " must be rejected by the global gate. A per-shard heuristic "
        << "would admit it here since the destination shard's local "
        << "count is 0.";
    EXPECT_EQ(heartbeat->count(k1), 1u);
    EXPECT_EQ(heartbeat->count(k2), 0u);

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
TEST_F(PromotionOnHitTest, AllocStartResetsTaskDeadline) {
    MasterServiceConfig config;
    config.enable_offload = true;
    config.promotion_on_hit = true;
    config.promotion_admission_threshold = 1;
    config.default_kv_lease_ttl = 8000;
    config.put_start_discard_timeout_sec = 0;
    config.put_start_release_timeout_sec = 2;
    auto service = std::make_unique<MasterService>(config);

    constexpr size_t seg_size = 1024 * 1024 * 16;
    auto ctx = PrepareSegment(*service, "seg_a", kDefaultSegmentBase, seg_size);
    ASSERT_TRUE(InjectLocalDiskReplica(*service, ctx.client_id, "k_late", 1024,
                                       ctx.segment_name));

    auto seg_baseline = service->QuerySegments(ctx.segment_name);
    ASSERT_TRUE(seg_baseline.has_value());
    const size_t used_baseline = seg_baseline->first;

    // T=0 : admit. start_time = T=0.
    {
        auto r = service->GetReplicaList("k_late");
        ASSERT_TRUE(r.has_value());
    }

    // T=Wq : simulate the holder's queue having been backlogged. With
    // TTL = 2s and Wq = 1.5s the task is still alive at AllocStart
    // (the queue-wait phase's own window hasn't expired yet — 1.5 < 2).
    std::this_thread::sleep_for(std::chrono::milliseconds(1500));

    auto alloc =
        service->PromotionAllocStart(ctx.client_id, "k_late", 1024, {});
    ASSERT_TRUE(alloc.has_value())
        << "AllocStart must succeed before the queue-wait phase's TTL "
        << "expires (1.5s elapsed, TTL is 2s). If this fires the test "
        << "timing is wrong, not the feature.";

    // Buffer has been staged.
    auto seg_after_alloc = service->QuerySegments(ctx.segment_name);
    ASSERT_TRUE(seg_after_alloc.has_value());
    EXPECT_GT(seg_after_alloc->first, used_baseline)
        << "PromotionAllocStart should bump segment used bytes "
        << "(allocator-tracked PROCESSING MEMORY replica)";

    // T=Wq+Wa : total elapsed since admission is 3.0s > TTL=2s. If the
    // reset is missing, the reaper sees the original start_time = 0
    // and expires the task here, calling EraseReplicaByID which would
    // free the staged buffer mid-RDMA-write in production. With the
    // reset, start_time was bumped at AllocStart so age-since-reset is
    // only 1.5s < TTL=2s and the task stays alive. This is the
    // assertion that proves the reset is in place.
    std::this_thread::sleep_for(std::chrono::milliseconds(1500));
    auto seg_after_wait = service->QuerySegments(ctx.segment_name);
    ASSERT_TRUE(seg_after_wait.has_value());
    EXPECT_GT(seg_after_wait->first, used_baseline)
        << "post-AllocStart staged MEMORY buffer must still be alive: "
        << "queue-wait + active-transfer wall time (3.0s) has exceeded "
        << "the bare TTL (2s), but the start_time reset at AllocStart "
        << "gives the active-transfer phase its own fresh TTL window. "
        << "If this fires, the reset in PromotionAllocStart is missing "
        << "or broken — without it the reaper frees the staged buffer "
        << "here while a client's RDMA write could still be in flight.";

    // T=Wq+Wb : sleep another 3s for a total of 4.5s since AllocStart.
    // The active-transfer phase's own TTL has now expired regardless
    // of the reset; the reaper must fire and free the staged buffer.
    std::this_thread::sleep_for(std::chrono::seconds(3));
    auto seg_after_reap = service->QuerySegments(ctx.segment_name);
    ASSERT_TRUE(seg_after_reap.has_value());
    EXPECT_EQ(seg_after_reap->first, used_baseline)
        << "after the active-transfer phase's own TTL expires "
        << "(4.5s > 2s since AllocStart), the reaper must fire and "
        << "free the staged buffer via EraseReplicaByID";

    service->RemoveAll();
}

// NotifyPromotionSuccess must decrement the cluster-wide in-flight
// counter so future admissions can use the slot; otherwise queue_limit
// remains saturated forever after the first successful promotion. Also
// exercises the only success-path end-to-end coverage of
// NotifyPromotionSuccess in the suite.
TEST_F(PromotionOnHitTest, NotifySuccessDecrementsCounter) {
    MasterServiceConfig config;
    config.enable_offload = true;
    config.promotion_on_hit = true;
    config.promotion_admission_threshold = 1;
    config.promotion_queue_limit = 1;  // 1 in-flight task globally
    config.default_kv_lease_ttl = 2000;
    auto service = std::make_unique<MasterService>(config);

    constexpr size_t seg_size = 1024 * 1024 * 16;
    auto seg = PrepareSegment(*service, "seg_a", kDefaultSegmentBase, seg_size);

    ASSERT_TRUE(InjectLocalDiskReplica(*service, seg.client_id, "k_first", 1024,
                                       seg.segment_name));
    ASSERT_TRUE(InjectLocalDiskReplica(*service, seg.client_id, "k_second",
                                       1024, seg.segment_name));

    // Admit task 1. promotion_in_flight_ goes from 0 -> 1.
    {
        auto r = service->GetReplicaList("k_first");
        ASSERT_TRUE(r.has_value());
    }

    // Drive the full success path: AllocStart stages the PROCESSING MEMORY
    // replica and records alloc_id; NotifyPromotionSuccess flips it
    // COMPLETE, drops the source LOCAL_DISK refcnt, erases the task, and
    // decrements the counter.
    auto alloc =
        service->PromotionAllocStart(seg.client_id, "k_first", 1024, {});
    ASSERT_TRUE(alloc.has_value())
        << "AllocStart should succeed; error=" << alloc.error();
    auto notify = service->NotifyPromotionSuccess(seg.client_id, "k_first");
    ASSERT_TRUE(notify.has_value())
        << "NotifyPromotionSuccess happy path should succeed; if this "
        << "fires, AllocStart did not record alloc_id, or the staged "
        << "replica is missing/non-PROCESSING; error=" << notify.error();

    // Counter must now be 0. A second admission on a *different* key must
    // be allowed. If fetch_sub is missing on the success path, the cap is
    // still saturated at 1 and TryPushPromotionQueue silently drops this
    // attempt.
    {
        auto r = service->GetReplicaList("k_second");
        ASSERT_TRUE(r.has_value());
    }
    auto pending = service->PromotionObjectHeartbeat(seg.client_id);
    ASSERT_TRUE(pending.has_value());
    EXPECT_EQ(pending->size(), 1u)
        << "k_second must be admitted after k_first succeeds — the global "
        << "in-flight counter must decrement on the NotifyPromotionSuccess "
        << "success path. Without fetch_sub, the cap stays saturated and "
        << "k_second is silently dropped.";
    EXPECT_EQ(pending->count("k_second"), 1u);

    service->RemoveAll();
}

// PromotionAllocStart must reject when the in-flight task has been
// reaped between the holder's heartbeat and the AllocStart RPC arriving
// (e.g. client stall past put_start_release_timeout_sec_). Without the
// check, AllocStart would allocate + AddReplicas a PROCESSING MEMORY
// replica with nothing tracking it: the generic PROCESSING reaper only
// iterates shard->processing_keys (never populated by promotion) and
// the promotion-task reaper has nothing left to iterate, so the buffer
// leaks until the object is removed or evicted.
TEST_F(PromotionOnHitTest, AllocStartRejectsReapedTask) {
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

    // Baseline allocator usage.
    auto seg_baseline = service->QuerySegments(ctx.segment_name);
    ASSERT_TRUE(seg_baseline.has_value());
    const size_t used_baseline = seg_baseline->first;

    // Admit the task.
    {
        auto r = service->GetReplicaList("k_cold");
        ASSERT_TRUE(r.has_value());
    }

    // Wait past the TTL so the reaper sweeps the task. AllocStart has
    // not yet been called, so alloc_id is 0; the reaper's
    // EraseReplicaByID branch is a no-op and only the task entry is
    // removed. We can't easily poll for reap externally (the only
    // user-facing observable would be re-admitting through the gate,
    // which would create a fresh task and defeat the test), so use a
    // fixed sleep with margin. Matches the StalePromotionReaper pattern
    // (TTL=1s, sleep=2s).
    std::this_thread::sleep_for(std::chrono::seconds(2));

    // AllocStart on a reaped task must reject without allocating.
    // Allocating would leave an orphaned PROCESSING MEMORY replica
    // attached to the object.
    auto alloc =
        service->PromotionAllocStart(ctx.client_id, "k_cold", 1024, {});
    ASSERT_FALSE(alloc.has_value())
        << "AllocStart must reject when the task has been reaped — "
        << "otherwise the staged PROCESSING MEMORY replica is orphaned";
    EXPECT_EQ(alloc.error(), ErrorCode::REPLICA_IS_NOT_READY);

    // The DRAM allocator must be at baseline: no buffer was staged.
    auto seg_after = service->QuerySegments(ctx.segment_name);
    ASSERT_TRUE(seg_after.has_value());
    EXPECT_EQ(seg_after->first, used_baseline)
        << "AllocStart on a reaped task must not allocate. If used bytes "
        << "grew here, AllocStart got to AddReplicas before the task-"
        << "existence check and the staged buffer is now orphaned (no "
        << "reaper iterates it).";

    service->RemoveAll();
}

// NotifyPromotionSuccess must reject calls from a client that is not
// the holder of the source LOCAL_DISK replica. Without the check, any
// client knowing the key could flip the staged PROCESSING MEMORY replica
// to COMPLETE before the holder's RDMA write has landed, exposing torn
// data to readers.
TEST_F(PromotionOnHitTest, NotifyRejectsNonHolder) {
    MasterServiceConfig config;
    config.enable_offload = true;
    config.promotion_on_hit = true;
    config.promotion_admission_threshold = 1;
    config.default_kv_lease_ttl = 2000;
    auto service = std::make_unique<MasterService>(config);

    constexpr size_t seg_size = 1024 * 1024 * 16;
    auto holder =
        PrepareSegment(*service, "seg_a", kDefaultSegmentBase, seg_size);
    ASSERT_TRUE(InjectLocalDiskReplica(*service, holder.client_id, "k_cold",
                                       1024, holder.segment_name));

    // Admit + stage.
    {
        auto r = service->GetReplicaList("k_cold");
        ASSERT_TRUE(r.has_value());
    }
    auto alloc =
        service->PromotionAllocStart(holder.client_id, "k_cold", 1024, {});
    ASSERT_TRUE(alloc.has_value());

    // An unrelated client tries to Notify. Must be rejected as
    // INVALID_PARAMS so the staged replica stays PROCESSING.
    UUID intruder_id = generate_uuid();
    ASSERT_NE(intruder_id, holder.client_id);
    auto bad_notify = service->NotifyPromotionSuccess(intruder_id, "k_cold");
    ASSERT_FALSE(bad_notify.has_value())
        << "Notify from a non-holder client must be rejected — otherwise "
        << "any client knowing the key can commit someone else's "
        << "still-being-written replica";
    EXPECT_EQ(bad_notify.error(), ErrorCode::INVALID_PARAMS);

    // The staged replica must still be PROCESSING (not committed by the
    // rejected call). Readers must not see it via GetReplicaList yet —
    // GetReplicaList filters PROCESSING replicas, and pre-AllocStart
    // there are no COMPLETE replicas to read for a LOCAL_DISK-only key
    // (only the LOCAL_DISK descriptor itself).
    auto r_check = service->GetReplicaList("k_cold");
    ASSERT_TRUE(r_check.has_value());
    bool saw_complete_memory = false;
    for (const auto& d : r_check.value().replicas) {
        if (d.is_memory_replica() && d.status == ReplicaStatus::COMPLETE) {
            saw_complete_memory = true;
            break;
        }
    }
    EXPECT_FALSE(saw_complete_memory)
        << "Rejected Notify must not have committed the staged replica";

    // The legitimate holder must still be able to Notify successfully.
    auto good_notify =
        service->NotifyPromotionSuccess(holder.client_id, "k_cold");
    ASSERT_TRUE(good_notify.has_value())
        << "Holder Notify on the same task must succeed after a rejected "
        << "intruder Notify — the task entry should be untouched by the "
        << "rejection path; error=" << good_notify.error();

    service->RemoveAll();
}

// NotifyPromotionFailure must immediately release the task slot and the
// staged buffer so that transient client-side errors (SSD throttling,
// RDMA flakes) do not pin promotion_queue_limit_ for the full reaper
// TTL. With queue_limit=1 and a holder client that fails after a
// successful AllocStart, a second admission on a different key would
// be silently dropped until reaper TTL if Notify-Failure didn't
// fast-release. The TTL is set high enough here that any test pass
// can be attributed to the explicit release, not to reaper expiry.
TEST_F(PromotionOnHitTest, NotifyFailureReleasesStateImmediately) {
    MasterServiceConfig config;
    config.enable_offload = true;
    config.promotion_on_hit = true;
    config.promotion_admission_threshold = 1;
    config.promotion_queue_limit = 1;  // cap=1 makes the slot observable
    config.default_kv_lease_ttl = 5000;
    // Long TTL so the test's pass-fail signal cannot be attributed to
    // reaper sweep; only NotifyPromotionFailure could plausibly release.
    config.put_start_release_timeout_sec = 300;
    auto service = std::make_unique<MasterService>(config);

    constexpr size_t seg_size = 1024 * 1024 * 16;
    auto seg = PrepareSegment(*service, "seg_a", kDefaultSegmentBase, seg_size);

    ASSERT_TRUE(InjectLocalDiskReplica(*service, seg.client_id, "k_a", 1024,
                                       seg.segment_name));
    ASSERT_TRUE(InjectLocalDiskReplica(*service, seg.client_id, "k_b", 1024,
                                       seg.segment_name));

    auto seg_baseline = service->QuerySegments(seg.segment_name);
    ASSERT_TRUE(seg_baseline.has_value());
    const size_t used_baseline = seg_baseline->first;

    // Admit + stage k_a. promotion_in_flight_ goes 0 -> 1.
    {
        auto r = service->GetReplicaList("k_a");
        ASSERT_TRUE(r.has_value());
    }
    auto alloc = service->PromotionAllocStart(seg.client_id, "k_a", 1024, {});
    ASSERT_TRUE(alloc.has_value());

    // The staged PROCESSING MEMORY buffer is allocated.
    auto seg_after_alloc = service->QuerySegments(seg.segment_name);
    ASSERT_TRUE(seg_after_alloc.has_value());
    EXPECT_GT(seg_after_alloc->first, used_baseline)
        << "AllocStart should commit a buffer in the DRAM allocator";

    // Holder reports failure (simulating SSD read error after AllocStart
    // succeeded). Master must immediately reap the staged replica and
    // decrement the slot counter.
    auto failure = service->NotifyPromotionFailure(seg.client_id, "k_a");
    ASSERT_TRUE(failure.has_value())
        << "NotifyPromotionFailure on a valid in-flight task from the "
        << "legitimate holder must succeed; error=" << failure.error();

    // The staged buffer must be freed back to the DRAM allocator. If
    // this fires, NotifyPromotionFailure did not pop the staged replica
    // via EraseReplicaByID — same orphan-replica shape that originally
    // motivated the reaper fix.
    auto seg_after_release = service->QuerySegments(seg.segment_name);
    ASSERT_TRUE(seg_after_release.has_value());
    EXPECT_EQ(seg_after_release->first, used_baseline)
        << "NotifyPromotionFailure must release the staged buffer back "
        << "to the allocator; otherwise it leaks until the object is "
        << "removed or evicted.";

    // The slot must be freed: a second admission on a different key must
    // succeed even though queue_limit=1. Without the failure-side
    // decrement the cap would stay saturated until reaper TTL.
    {
        auto r = service->GetReplicaList("k_b");
        ASSERT_TRUE(r.has_value());
    }
    auto heartbeat = service->PromotionObjectHeartbeat(seg.client_id);
    ASSERT_TRUE(heartbeat.has_value());
    EXPECT_EQ(heartbeat->count("k_b"), 1u)
        << "k_b admission must succeed after k_a's failure released the "
        << "slot. If this fires, NotifyPromotionFailure did not decrement "
        << "promotion_in_flight_, and transient client-side errors "
        << "would saturate the queue limit for the full reaper TTL.";

    // Idempotency: repeated failure notification on the same key must be
    // safe (return OK without underflowing the counter).
    auto failure_again = service->NotifyPromotionFailure(seg.client_id, "k_a");
    EXPECT_TRUE(failure_again.has_value())
        << "Repeated NotifyPromotionFailure should be idempotent (return "
        << "OK on already-released task), not error.";

    service->RemoveAll();
}

// NotifyPromotionFailure must reject calls from a client that is not the
// holder. Without this gate, any client knowing the key could prematurely
// release a legitimate in-flight promotion's master state and free the
// staged buffer mid-RDMA-write. Mirror of NotifyRejectsNonHolder for the
// Notify-Success path.
TEST_F(PromotionOnHitTest, NotifyFailureRejectsNonHolder) {
    MasterServiceConfig config;
    config.enable_offload = true;
    config.promotion_on_hit = true;
    config.promotion_admission_threshold = 1;
    config.default_kv_lease_ttl = 5000;
    config.put_start_release_timeout_sec = 300;
    auto service = std::make_unique<MasterService>(config);

    constexpr size_t seg_size = 1024 * 1024 * 16;
    auto holder =
        PrepareSegment(*service, "seg_a", kDefaultSegmentBase, seg_size);
    ASSERT_TRUE(InjectLocalDiskReplica(*service, holder.client_id, "k_cold",
                                       1024, holder.segment_name));

    {
        auto r = service->GetReplicaList("k_cold");
        ASSERT_TRUE(r.has_value());
    }
    auto alloc =
        service->PromotionAllocStart(holder.client_id, "k_cold", 1024, {});
    ASSERT_TRUE(alloc.has_value());

    // Intruder calls Failure with the wrong client_id.
    UUID intruder_id = generate_uuid();
    ASSERT_NE(intruder_id, holder.client_id);
    auto bad_failure = service->NotifyPromotionFailure(intruder_id, "k_cold");
    ASSERT_FALSE(bad_failure.has_value())
        << "Failure from a non-holder client must be rejected.";
    EXPECT_EQ(bad_failure.error(), ErrorCode::INVALID_PARAMS);

    // The legitimate holder must still be able to either commit via
    // Notify-Success or release via Notify-Failure on the same task. Use
    // Notify-Failure here to exercise the surviving-task path.
    auto good_failure =
        service->NotifyPromotionFailure(holder.client_id, "k_cold");
    ASSERT_TRUE(good_failure.has_value())
        << "Holder Failure must succeed after a rejected intruder "
        << "Failure — the task entry should be untouched by the "
        << "rejection path; error=" << good_failure.error();

    service->RemoveAll();
}

// PromotionAllocStart must reject callers that aren't the holder. Mirror
// of the Notify gate. Without this gate a client that drained another's
// promotion_objects queue via PromotionObjectHeartbeat could call
// AllocStart to stage arbitrary DRAM allocations on the destination
// segment, with no path to commit (Notify rejects on holder_id mismatch)
// — they'd just sit pinned until reaper TTL.
TEST_F(PromotionOnHitTest, AllocStartRejectsNonHolder) {
    MasterServiceConfig config;
    config.enable_offload = true;
    config.promotion_on_hit = true;
    config.promotion_admission_threshold = 1;
    config.default_kv_lease_ttl = 5000;
    auto service = std::make_unique<MasterService>(config);

    constexpr size_t seg_size = 1024 * 1024 * 16;
    auto holder =
        PrepareSegment(*service, "seg_a", kDefaultSegmentBase, seg_size);
    ASSERT_TRUE(InjectLocalDiskReplica(*service, holder.client_id, "k_cold",
                                       1024, holder.segment_name));

    auto seg_baseline = service->QuerySegments(holder.segment_name);
    ASSERT_TRUE(seg_baseline.has_value());
    const size_t used_baseline = seg_baseline->first;

    {
        auto r = service->GetReplicaList("k_cold");
        ASSERT_TRUE(r.has_value());
    }

    UUID intruder_id = generate_uuid();
    ASSERT_NE(intruder_id, holder.client_id);
    auto bad_alloc =
        service->PromotionAllocStart(intruder_id, "k_cold", 1024, {});
    ASSERT_FALSE(bad_alloc.has_value())
        << "AllocStart from a non-holder client must be rejected — "
        << "otherwise an attacker that drained another's queue could "
        << "stage arbitrary DRAM allocations.";
    EXPECT_EQ(bad_alloc.error(), ErrorCode::INVALID_PARAMS);

    // No buffer was allocated.
    auto seg_after_bad = service->QuerySegments(holder.segment_name);
    ASSERT_TRUE(seg_after_bad.has_value());
    EXPECT_EQ(seg_after_bad->first, used_baseline)
        << "Rejected AllocStart must not have allocated any DRAM.";

    // The legitimate holder must still be able to AllocStart (task
    // untouched by the rejection).
    auto good_alloc =
        service->PromotionAllocStart(holder.client_id, "k_cold", 1024, {});
    ASSERT_TRUE(good_alloc.has_value())
        << "Holder AllocStart on the same task must succeed after a "
        << "rejected intruder AllocStart; error=" << good_alloc.error();

    service->RemoveAll();
}

// PromotionAllocStart must reject size that doesn't match the task's
// recorded object_size. The size is captured from the source LOCAL_DISK
// descriptor at task admission; mismatch indicates a buggy caller or
// malicious request and would let the caller request an arbitrary-size
// DRAM buffer (smaller → eventual RDMA-write overflow risk; larger →
// wasted DRAM pinned until reaper TTL).
TEST_F(PromotionOnHitTest, AllocStartRejectsSizeMismatch) {
    MasterServiceConfig config;
    config.enable_offload = true;
    config.promotion_on_hit = true;
    config.promotion_admission_threshold = 1;
    config.default_kv_lease_ttl = 5000;
    auto service = std::make_unique<MasterService>(config);

    constexpr size_t seg_size = 1024 * 1024 * 16;
    auto holder =
        PrepareSegment(*service, "seg_a", kDefaultSegmentBase, seg_size);
    constexpr int64_t kRealSize = 1024;
    ASSERT_TRUE(InjectLocalDiskReplica(*service, holder.client_id, "k_cold",
                                       kRealSize, holder.segment_name));

    auto seg_baseline = service->QuerySegments(holder.segment_name);
    ASSERT_TRUE(seg_baseline.has_value());
    const size_t used_baseline = seg_baseline->first;

    {
        auto r = service->GetReplicaList("k_cold");
        ASSERT_TRUE(r.has_value());
    }

    // Mismatched-size requests must be rejected, regardless of direction.
    for (uint64_t bad_size : {static_cast<uint64_t>(kRealSize) / 2,
                              static_cast<uint64_t>(kRealSize) * 4}) {
        auto bad_alloc = service->PromotionAllocStart(holder.client_id,
                                                      "k_cold", bad_size, {});
        ASSERT_FALSE(bad_alloc.has_value())
            << "AllocStart with size=" << bad_size
            << " (task.object_size=" << kRealSize << ") must be rejected.";
        EXPECT_EQ(bad_alloc.error(), ErrorCode::INVALID_PARAMS);

        // No buffer must have been staged.
        auto seg_after_bad = service->QuerySegments(holder.segment_name);
        ASSERT_TRUE(seg_after_bad.has_value());
        EXPECT_EQ(seg_after_bad->first, used_baseline)
            << "Rejected AllocStart (size=" << bad_size
            << ") must not have allocated any DRAM.";
    }

    // Correct size must still work — the task was not consumed by the
    // rejections.
    auto good_alloc = service->PromotionAllocStart(
        holder.client_id, "k_cold", static_cast<uint64_t>(kRealSize), {});
    ASSERT_TRUE(good_alloc.has_value())
        << "AllocStart with the correct size must succeed after rejected "
        << "size-mismatch attempts; error=" << good_alloc.error();

    service->RemoveAll();
}

// When a holder client expires, ClientMonitorFunc must clean up its
// dangling promotion_tasks entries and decrement the global in-flight
// counter. Without this cleanup, the entries stay pinned until reaper
// TTL — and on a rolling restart of many holders the cluster-wide cap
// promotion_queue_limit_ saturates and blocks all new admissions for
// the full TTL.
//
// Test mechanism: short client_live_ttl_sec, admit a promotion, stop
// pinging, wait for ClientMonitorFunc to expire the client and call
// ClearInvalidHandles. Then assert promotion_in_flight_ is back to 0
// by attempting a second admission with queue_limit=1 on a fresh
// client.
TEST_F(PromotionOnHitTest, ClientExpiryClearsPromotionTask) {
    MasterServiceConfig config;
    config.enable_offload = true;
    config.promotion_on_hit = true;
    config.promotion_admission_threshold = 1;
    config.promotion_queue_limit = 1;  // cap=1 makes the slot observable
    config.default_kv_lease_ttl = 5000;
    // Long task TTL so that any clearing we see must come from
    // ClearInvalidHandles, not from the promotion-task reaper.
    config.put_start_release_timeout_sec = 300;
    // Short client TTL so expiration is fast.
    config.client_live_ttl_sec = 1;
    auto service = std::make_unique<MasterService>(config);

    constexpr size_t seg_size = 1024 * 1024 * 16;
    auto holder =
        PrepareSegment(*service, "seg_a", kDefaultSegmentBase, seg_size);
    ASSERT_TRUE(InjectLocalDiskReplica(*service, holder.client_id, "k_cold",
                                       1024, holder.segment_name));

    // Admit the promotion. promotion_in_flight_ goes 0 -> 1.
    {
        auto r = service->GetReplicaList("k_cold");
        ASSERT_TRUE(r.has_value());
    }

    // Sanity: with queue_limit=1 the cap is saturated. A second
    // different-shard admission must be rejected right now.
    auto second_holder = PrepareSegment(
        *service, "seg_b", kDefaultSegmentBase + seg_size, seg_size);
    // Promote second_holder into ok_client_ via ReMountSegment so its
    // LOCAL_DISK replicas survive any ClearInvalidHandles run triggered
    // by the first holder's expiry. MountSegment alone does not register
    // the client as alive (only ReMountSegment does), and
    // CleanupStaleHandles uses ok_client_ to decide which LOCAL_DISK
    // replicas to erase — without this, second_holder's k_other replica
    // would be wiped alongside the first holder's k_cold replica when
    // ClearInvalidHandles runs.
    {
        Segment seg_b =
            MakeSegment("seg_b", kDefaultSegmentBase + seg_size, seg_size);
        seg_b.id = second_holder.segment_id;
        std::vector<Segment> segs{seg_b};
        auto remount = service->ReMountSegment(segs, second_holder.client_id);
        ASSERT_TRUE(remount.has_value()) << "ReMount failed";
    }
    ASSERT_TRUE(InjectLocalDiskReplica(*service, second_holder.client_id,
                                       "k_other", 1024,
                                       second_holder.segment_name));
    {
        auto r = service->GetReplicaList("k_other");
        ASSERT_TRUE(r.has_value());
    }
    auto pending_pre =
        service->PromotionObjectHeartbeat(second_holder.client_id);
    ASSERT_TRUE(pending_pre.has_value());
    EXPECT_EQ(pending_pre->count("k_other"), 0u)
        << "Sanity: queue_limit=1 should block the second admission "
        << "while the first task is in flight.";

    // Wait for the first holder to expire while keeping the second
    // holder alive via periodic Pings. ClientMonitorFunc runs every
    // kClientMonitorSleepMs (1s) and expires clients whose last ping is
    // older than client_live_ttl_sec (1s); we ping second_holder every
    // 200ms so it stays alive across the 4s wait. Without these pings
    // both holders would expire together and the second holder's
    // LOCAL_DISK segment would be unmounted — making "k_other" un-
    // admittable for reasons unrelated to the promotion-task slot.
    {
        const auto deadline =
            std::chrono::steady_clock::now() + std::chrono::seconds(4);
        while (std::chrono::steady_clock::now() < deadline) {
            (void)service->Ping(second_holder.client_id);
            std::this_thread::sleep_for(std::chrono::milliseconds(200));
        }
    }

    // ClearInvalidHandles should have erased the holder's LOCAL_DISK
    // source replica AND (with the fix) the promotion_tasks entry,
    // decrementing the global in-flight counter. Re-admit a promotion
    // on the second holder; with queue_limit=1 this can only succeed if
    // the slot was freed.
    {
        auto r = service->GetReplicaList("k_other");
        ASSERT_TRUE(r.has_value())
            << "GetReplicaList(k_other) failed with error=" << r.error();
    }
    auto pending_post =
        service->PromotionObjectHeartbeat(second_holder.client_id);
    ASSERT_TRUE(pending_post.has_value());
    EXPECT_EQ(pending_post->count("k_other"), 1u)
        << "After the holder expired, ClearInvalidHandles must have "
        << "erased its promotion_tasks entry and decremented "
        << "promotion_in_flight_. Otherwise the global cap remains "
        << "saturated by the dead holder's task for "
        << "put_start_release_timeout_sec_ seconds, and this admission "
        << "is dropped.";

    service->RemoveAll();
}

// promotion_admission_threshold=0 would silently bypass the frequency
// gate (`freq < 0` is never true for uint8_t freq), letting every Get
// on a LOCAL_DISK-only key admit a promotion. master.cpp clamps the
// flag at parse time, and MasterService's constructor adds a
// defense-in-depth clamp for direct-construction paths (tests,
// embedded users). Verify the clamp lifts 0 → 1.
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
        auto r = service->GetReplicaList("k_first");
        ASSERT_TRUE(r.has_value());
    }
    {
        auto pending = service->PromotionObjectHeartbeat(holder.client_id);
        ASSERT_TRUE(pending.has_value());
        EXPECT_EQ(pending->count("k_first"), 1u);
    }

    // Remove k_first with force=true. With the fix, this also wipes
    // k_first's promotion_tasks entry and decrements
    // promotion_in_flight_ back to 0.
    auto rm = service->Remove("k_first", /*force=*/true);
    ASSERT_TRUE(rm.has_value())
        << "Remove should succeed; error=" << rm.error();

    // Now admit a different key. With queue_limit=1, this only succeeds
    // if the slot was freed by Remove. Without the in-flight cleanup,
    // it would stay pinned for the full 300s TTL.
    {
        auto r = service->GetReplicaList("k_second");
        ASSERT_TRUE(r.has_value());
    }
    auto pending_post = service->PromotionObjectHeartbeat(holder.client_id);
    ASSERT_TRUE(pending_post.has_value());
    EXPECT_EQ(pending_post->count("k_second"), 1u)
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
        auto r = service->GetReplicaList("regex_k1");
        ASSERT_TRUE(r.has_value());
    }

    // RemoveByRegex matches regex_k1 only.
    auto removed = service->RemoveByRegex("^regex_", /*force=*/true);
    ASSERT_TRUE(removed.has_value())
        << "RemoveByRegex should succeed; error=" << removed.error();
    EXPECT_EQ(removed.value(), 1) << "exactly one key (regex_k1) should match";

    // Slot must be free — admit on other_k2 (different shard or same,
    // doesn't matter because counter is global).
    {
        auto r = service->GetReplicaList("other_k2");
        ASSERT_TRUE(r.has_value());
    }
    auto pending_post = service->PromotionObjectHeartbeat(holder.client_id);
    ASSERT_TRUE(pending_post.has_value());
    EXPECT_EQ(pending_post->count("other_k2"), 1u)
        << "other_k2 must be admittable after RemoveByRegex of regex_k1 "
        << "— RemoveByRegex must erase the in-flight promotion_tasks "
        << "entry. Otherwise queue_limit=1 stays saturated.";

    service->RemoveAll();
}

}  // namespace mooncake::test

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
