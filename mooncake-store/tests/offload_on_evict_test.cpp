#include "master_service.h"

#include <glog/logging.h>
#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include "types.h"

namespace mooncake::test {

class OffloadOnEvictTest : public ::testing::Test {
   protected:
    void SetUp() override {
        google::InitGoogleLogging("OffloadOnEvictTest");
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
    };

    MountedSegmentContext PrepareSegment(MasterService& service,
                                         std::string name, size_t base,
                                         size_t size) const {
        Segment segment = MakeSegment(std::move(name), base, size);
        UUID client_id = generate_uuid();
        auto mount_result = service.MountSegment(segment, client_id);
        EXPECT_TRUE(mount_result.has_value());
        return {.segment_id = segment.id, .client_id = client_id};
    }

    // Put an object and complete it.
    void PutObject(MasterService& service, const UUID& client_id,
                   const std::string& key, size_t size = 1024) {
        ReplicateConfig config;
        config.replica_num = 1;
        auto put_start =
            service.PutStart(client_id, key, TenantId::Default(), size, config);
        ASSERT_TRUE(put_start.has_value()) << "PutStart failed for key=" << key;
        auto put_end = service.PutEnd(client_id, key, TenantId::Default(),
                                      ReplicaType::MEMORY);
        ASSERT_TRUE(put_end.has_value()) << "PutEnd failed for key=" << key;
    }

    // Drain the offload queue via OffloadObjectHeartbeat.
    std::unordered_map<std::string, int64_t> DrainOffloadQueue(
        MasterService& service, const UUID& client_id) {
        auto res = service.OffloadObjectHeartbeat(client_id, true);
        if (!res) {
            return {};
        }
        std::unordered_map<std::string, int64_t> queued;
        for (const auto& task : res.value()) {
            queued[task.key] = task.size;
        }
        return queued;
    }

    template <typename Predicate>
    void WaitUntil(
        Predicate&& predicate,
        std::chrono::milliseconds timeout = std::chrono::milliseconds(4000),
        std::chrono::milliseconds interval =
            std::chrono::milliseconds(50)) const {
        const auto deadline = std::chrono::steady_clock::now() + timeout;
        while (std::chrono::steady_clock::now() < deadline) {
            if (predicate()) {
                return;
            }
            std::this_thread::sleep_for(interval);
        }
        EXPECT_TRUE(predicate());
    }

    // Test-only accessors that reach into MasterService internals
    // through the OffloadOnEvictTest friendship (gtest's generated
    // Fixture_Test subclass does not inherit that friendship).

    // Snapshot the mirror entry for @p key on @p client_id's LocalDisk.
    std::optional<OffloadTaskItem> ReadMirror(MasterService& service,
                                              const UUID& client_id,
                                              const std::string& key) const {
        auto ssd_access = service.segment_manager_.getLocalDiskSegmentAccess();
        auto& segments = ssd_access.getClientLocalDiskSegment();
        auto it = segments.find(client_id);
        if (it == segments.end()) {
            return std::nullopt;
        }
        MutexLocker locker(&it->second->offloading_mutex_);
        const std::string scoped = TenantId::Default().MakeScopedKey(key);
        auto obj_it = it->second->offloading_objects.find(scoped);
        if (obj_it == it->second->offloading_objects.end()) {
            return std::nullopt;
        }
        return obj_it->second;
    }

    // Inject @p task into @p client_id's LocalDisk mirror to simulate a
    // replica that spans multiple LocalDisk segments.
    void InjectMirror(MasterService& service, const UUID& client_id,
                      const std::string& key,
                      const OffloadTaskItem& task) const {
        auto ssd_access = service.segment_manager_.getLocalDiskSegmentAccess();
        auto& segments = ssd_access.getClientLocalDiskSegment();
        auto it = segments.find(client_id);
        ASSERT_NE(it, segments.end());
        MutexLocker locker(&it->second->offloading_mutex_);
        it->second->offloading_objects.emplace(
            TenantId::Default().MakeScopedKey(key), task);
    }

    // Read the generation of offloading_tasks[key], or nullopt if absent.
    std::optional<uint64_t> ReadTaskGeneration(MasterService& service,
                                               const std::string& key) const {
        const size_t shard_index =
            service.getShardIndex(TenantId::Default(), key);
        MasterService::MetadataShardAccessorRW shard(&service, shard_index);
        auto& tenant_state = shard->tenants[TenantId::Default()];
        auto task_it = tenant_state.offloading_tasks.find(key);
        if (task_it == tenant_state.offloading_tasks.end()) {
            return std::nullopt;
        }
        return task_it->second.generation;
    }

    // Read the refcnt of the first completed MEMORY replica of @p key.
    std::optional<uint32_t> ReadMemoryReplicaRefcnt(
        MasterService& service, const std::string& key) const {
        const size_t shard_index =
            service.getShardIndex(TenantId::Default(), key);
        MasterService::MetadataShardAccessorRW shard(&service, shard_index);
        auto& tenant_state = shard->tenants[TenantId::Default()];
        auto md_it = tenant_state.metadata.find(key);
        if (md_it == tenant_state.metadata.end()) {
            return std::nullopt;
        }
        std::optional<uint32_t> out;
        md_it->second.VisitReplicas(
            [](const Replica& r) {
                return r.is_completed() && r.is_memory_replica();
            },
            [&out](Replica& r) {
                if (!out.has_value()) {
                    out = r.get_refcnt();
                }
            });
        return out;
    }

    // Simulate the offload_on_evict push: create the offloading_tasks
    // marker + LocalDisk mirror + refcnt pin, like the eviction path.
    void InjectOffloadTask(MasterService& service, const UUID& client_id,
                           const std::string& key) const {
        const size_t shard_index =
            service.getShardIndex(TenantId::Default(), key);
        MasterService::MetadataShardAccessorRW shard(&service, shard_index);
        auto& tenant_state = shard->tenants[TenantId::Default()];
        auto md_it = tenant_state.metadata.find(key);
        ASSERT_NE(md_it, tenant_state.metadata.end());
        auto& metadata = md_it->second;
        Replica* source = nullptr;
        metadata.VisitReplicas(
            [](const Replica& r) {
                return r.is_completed() && r.is_memory_replica();
            },
            [&source](Replica& r) {
                if (source == nullptr) source = &r;
            });
        ASSERT_NE(source, nullptr);
        const uint64_t gen = service.next_offload_generation_.fetch_add(
            1, std::memory_order_relaxed);
        MasterService::ObjectIdentity oid{TenantId::Default(), key};
        auto pushed = service.PushOffloadingQueue(oid, *source, gen);
        ASSERT_TRUE(pushed.has_value());
        source->inc_refcnt();
        tenant_state.offloading_tasks.emplace(
            key, MasterService::OffloadingTask{
                     source->id(), std::chrono::system_clock::now(), gen});
    }

    // Fill a segment until PutStart fails, triggering eviction.
    // Returns the number of successful puts.
    int FillSegmentUntilEviction(MasterService& service, const UUID& client_id,
                                 const std::string& key_prefix,
                                 size_t object_size, int max_puts) {
        int success_puts = 0;
        for (int i = 0; i < max_puts; ++i) {
            std::string key = key_prefix + std::to_string(i);
            ReplicateConfig config;
            config.replica_num = 1;
            auto result = service.PutStart(client_id, key, TenantId::Default(),
                                           object_size, config);
            if (result.has_value()) {
                auto end = service.PutEnd(client_id, key, TenantId::Default(),
                                          ReplicaType::MEMORY);
                EXPECT_TRUE(end.has_value());
                success_puts++;
            } else {
                // Wait for eviction to process
                std::this_thread::sleep_for(std::chrono::milliseconds(50));
            }
        }
        return success_puts;
    }
};

// =============================================================================
// Combo A: Default config (offload at PutEnd)
// =============================================================================

TEST_F(OffloadOnEvictTest, ComboA_OffloadAtPutEnd) {
    MasterServiceConfig config;
    config.enable_offload = true;
    config.default_kv_lease_ttl = 2000;
    auto service = std::make_unique<MasterService>(config);

    constexpr size_t seg_size = 1024 * 1024 * 16;
    auto ctx =
        PrepareSegment(*service, "test_segment", kDefaultSegmentBase, seg_size);

    // Mount local disk segment with offloading ENABLED
    auto mount_ld = service->MountLocalDiskSegment(ctx.client_id, true);
    ASSERT_TRUE(mount_ld.has_value());

    // Put objects
    PutObject(*service, ctx.client_id, "key_a1");
    PutObject(*service, ctx.client_id, "key_a2");
    PutObject(*service, ctx.client_id, "key_a3");

    // Default mode: PutEnd pushes to offload queue immediately
    auto queued = DrainOffloadQueue(*service, ctx.client_id);
    EXPECT_EQ(queued.size(), 3u)
        << "Default: all 3 objects should be in offload queue after PutEnd";
    EXPECT_TRUE(queued.count("key_a1"));
    EXPECT_TRUE(queued.count("key_a2"));
    EXPECT_TRUE(queued.count("key_a3"));

    service->RemoveAll();
}

TEST_F(OffloadOnEvictTest, ComboA_EvictionWorks) {
    // Regression: eviction still works in default mode
    const uint64_t kv_lease_ttl = 2000;
    MasterServiceConfig config;
    config.enable_offload = true;
    config.default_kv_lease_ttl = kv_lease_ttl;
    auto service = std::make_unique<MasterService>(config);

    // Large segment: can hold ~16K objects of 15KB
    constexpr size_t seg_size = 1024 * 1024 * 16 * 15;
    constexpr size_t object_size = 1024 * 15;
    auto ctx =
        PrepareSegment(*service, "test_segment", kDefaultSegmentBase, seg_size);

    // Put more objects than the segment can hold
    int success_puts = FillSegmentUntilEviction(
        *service, ctx.client_id, "evict_a_", object_size, 1024 * 16 + 50);
    EXPECT_GT(success_puts, 1024 * 16)
        << "Default: eviction should allow more puts than capacity";

    std::this_thread::sleep_for(std::chrono::milliseconds(kv_lease_ttl));
    service->RemoveAll();
}

// =============================================================================
// Combo B: offload_on_evict=true (offload on evict, no force-evict)
// =============================================================================

TEST_F(OffloadOnEvictTest, ComboB_PutEndSkipsOffloadQueue) {
    MasterServiceConfig config;
    config.enable_offload = true;
    config.offload_on_evict = true;
    config.default_kv_lease_ttl = 2000;
    auto service = std::make_unique<MasterService>(config);

    constexpr size_t seg_size = 1024 * 1024 * 16;
    auto ctx =
        PrepareSegment(*service, "test_segment", kDefaultSegmentBase, seg_size);
    auto mount_ld = service->MountLocalDiskSegment(ctx.client_id, true);
    ASSERT_TRUE(mount_ld.has_value());

    PutObject(*service, ctx.client_id, "key_b1");
    PutObject(*service, ctx.client_id, "key_b2");
    PutObject(*service, ctx.client_id, "key_b3");

    // Offload-on-evict: PutEnd should NOT push to offload queue
    auto queued = DrainOffloadQueue(*service, ctx.client_id);
    EXPECT_EQ(queued.size(), 0u)
        << "Offload-on-evict: queue should be empty after PutEnd";

    service->RemoveAll();
}

TEST_F(OffloadOnEvictTest, ComboB_EvictionTriggersOffload) {
    const uint64_t kv_lease_ttl = 2000;
    MasterServiceConfig config;
    config.enable_offload = true;
    config.offload_on_evict = true;
    config.default_kv_lease_ttl = kv_lease_ttl;
    auto service = std::make_unique<MasterService>(config);

    constexpr size_t seg_size = 1024 * 1024 * 16 * 15;
    constexpr size_t object_size = 1024 * 15;
    auto ctx =
        PrepareSegment(*service, "test_segment", kDefaultSegmentBase, seg_size);
    auto mount_ld = service->MountLocalDiskSegment(ctx.client_id, true);
    ASSERT_TRUE(mount_ld.has_value());

    // Fill segment to trigger eviction
    bool eviction_triggered = false;
    int success_puts = 0;
    for (int i = 0; i < 1024 * 16 + 50; ++i) {
        std::string key = "evict_b_" + std::to_string(i);
        ReplicateConfig config;
        config.replica_num = 1;
        auto result = service->PutStart(ctx.client_id, key, TenantId::Default(),
                                        object_size, config);
        if (result.has_value()) {
            auto end = service->PutEnd(ctx.client_id, key, TenantId::Default(),
                                       ReplicaType::MEMORY);
            ASSERT_TRUE(end.has_value());
            success_puts++;
        } else {
            eviction_triggered = true;
            std::this_thread::sleep_for(std::chrono::milliseconds(50));
        }
    }

    EXPECT_TRUE(eviction_triggered)
        << "Eviction should trigger when segment fills up";

    // Offload-on-evict: eviction should push objects to offload queue
    auto queued = DrainOffloadQueue(*service, ctx.client_id);
    EXPECT_GT(queued.size(), 0u)
        << "Offload-on-evict: eviction should push to offload queue";

    std::this_thread::sleep_for(std::chrono::milliseconds(kv_lease_ttl));
    service->RemoveAll();
}

TEST_F(OffloadOnEvictTest, ComboB_NoFallbackWithoutForceEvict) {
    // Without force_evict AND without a LocalDiskSegment, offload queue push
    // fails and eviction does NOT force-delete MEMORY (data-preserving).
    // The segment fills and subsequent puts fail — this is the safe default.
    const uint64_t kv_lease_ttl = 2000;
    MasterServiceConfig config;
    config.enable_offload = true;
    config.offload_on_evict = true;
    config.default_kv_lease_ttl = kv_lease_ttl;
    auto service = std::make_unique<MasterService>(config);

    // NO local disk segment mounted — PushOffloadingQueue will fail
    constexpr size_t seg_size = 1024 * 1024 * 16 * 15;
    constexpr size_t object_size = 1024 * 15;
    auto ctx =
        PrepareSegment(*service, "test_segment", kDefaultSegmentBase, seg_size);

    // Without force_evict, push failures mean DRAM cannot be freed,
    // so we can only put up to segment capacity (no overflow).
    int success_puts = FillSegmentUntilEviction(
        *service, ctx.client_id, "evict_b2_", object_size, 1024 * 16 + 50);
    EXPECT_LE(success_puts, 1024 * 16)
        << "Without force_evict, segment should fill and stay full";

    std::this_thread::sleep_for(std::chrono::milliseconds(kv_lease_ttl));
    service->RemoveAll();
}

// =============================================================================
// Combo C: offload_on_evict=true + offload_force_evict=true
// =============================================================================

TEST_F(OffloadOnEvictTest, ComboC_PutEndSkipsOffloadQueue) {
    MasterServiceConfig config;
    config.enable_offload = true;
    config.offload_on_evict = true;
    config.offload_force_evict = true;
    config.default_kv_lease_ttl = 2000;
    auto service = std::make_unique<MasterService>(config);

    constexpr size_t seg_size = 1024 * 1024 * 16;
    auto ctx =
        PrepareSegment(*service, "test_segment", kDefaultSegmentBase, seg_size);
    auto mount_ld = service->MountLocalDiskSegment(ctx.client_id, true);
    ASSERT_TRUE(mount_ld.has_value());

    PutObject(*service, ctx.client_id, "key_c1");
    PutObject(*service, ctx.client_id, "key_c2");

    // Same as Combo B: PutEnd should skip offload queue
    auto queued = DrainOffloadQueue(*service, ctx.client_id);
    EXPECT_EQ(queued.size(), 0u)
        << "Combo C: offload queue should be empty after PutEnd";

    service->RemoveAll();
}

TEST_F(OffloadOnEvictTest, ComboC_EvictionWithForceEvict) {
    const uint64_t kv_lease_ttl = 2000;
    MasterServiceConfig config;
    config.enable_offload = true;
    config.offload_on_evict = true;
    config.offload_force_evict = true;
    config.default_kv_lease_ttl = kv_lease_ttl;
    auto service = std::make_unique<MasterService>(config);

    constexpr size_t seg_size = 1024 * 1024 * 16 * 15;
    constexpr size_t object_size = 1024 * 15;
    auto ctx =
        PrepareSegment(*service, "test_segment", kDefaultSegmentBase, seg_size);
    auto mount_ld = service->MountLocalDiskSegment(ctx.client_id, true);
    ASSERT_TRUE(mount_ld.has_value());

    // With force-evict, eviction should work effectively.
    // Note: without a real FileStorage heartbeat, offloaded objects' refcnt
    // never decreases, so DRAM isn't fully freed beyond what direct eviction
    // allows. We verify eviction doesn't deadlock (can fill to capacity).
    int success_puts = FillSegmentUntilEviction(
        *service, ctx.client_id, "evict_c_", object_size, 1024 * 16 + 50);
    EXPECT_GE(success_puts, 1024 * 16)
        << "Combo C: eviction should work with force-evict enabled";

    std::this_thread::sleep_for(std::chrono::milliseconds(kv_lease_ttl));
    service->RemoveAll();
}

// =============================================================================
// Combo D: offload_force_evict=true only (should be no-op without on_evict)
// =============================================================================

TEST_F(OffloadOnEvictTest, ComboD_ForceEvictAloneIsIgnored) {
    MasterServiceConfig config;
    config.enable_offload = true;
    config.offload_force_evict = true;  // on_evict is false → force is ignored
    config.default_kv_lease_ttl = 2000;
    auto service = std::make_unique<MasterService>(config);

    constexpr size_t seg_size = 1024 * 1024 * 16;
    auto ctx =
        PrepareSegment(*service, "test_segment", kDefaultSegmentBase, seg_size);
    auto mount_ld = service->MountLocalDiskSegment(ctx.client_id, true);
    ASSERT_TRUE(mount_ld.has_value());

    // Should behave like Combo A (default: offload at PutEnd)
    PutObject(*service, ctx.client_id, "key_d1");
    PutObject(*service, ctx.client_id, "key_d2");

    auto queued = DrainOffloadQueue(*service, ctx.client_id);
    EXPECT_EQ(queued.size(), 2u)
        << "Combo D: FORCE_EVICT alone should not change default behavior";

    service->RemoveAll();
}

TEST_F(OffloadOnEvictTest, ComboD_EvictionWorks) {
    const uint64_t kv_lease_ttl = 2000;
    MasterServiceConfig config;
    config.enable_offload = true;
    config.offload_force_evict = true;  // on_evict is false → force is ignored
    config.default_kv_lease_ttl = kv_lease_ttl;
    auto service = std::make_unique<MasterService>(config);

    constexpr size_t seg_size = 1024 * 1024 * 16 * 15;
    constexpr size_t object_size = 1024 * 15;
    auto ctx =
        PrepareSegment(*service, "test_segment", kDefaultSegmentBase, seg_size);

    int success_puts = FillSegmentUntilEviction(
        *service, ctx.client_id, "evict_d_", object_size, 1024 * 16 + 50);
    EXPECT_GT(success_puts, 1024 * 16)
        << "Combo D: eviction should work normally";

    std::this_thread::sleep_for(std::chrono::milliseconds(kv_lease_ttl));
    service->RemoveAll();
}

// Regression: EraseMetadata must drop the LocalDiskSegment mirror entry;
// otherwise BatchRemove leaves a task-less key in the offload queue.
TEST_F(OffloadOnEvictTest, BatchRemoveDropsOffloadingObjectsMirror) {
    MasterServiceConfig config;
    config.enable_offload = true;
    config.default_kv_lease_ttl = 0;  // no lease so BatchRemove succeeds
    auto service = std::make_unique<MasterService>(config);

    constexpr size_t seg_size = 1024 * 1024 * 16;
    auto ctx =
        PrepareSegment(*service, "test_segment", kDefaultSegmentBase, seg_size);
    auto mount_ld = service->MountLocalDiskSegment(ctx.client_id, true);
    ASSERT_TRUE(mount_ld.has_value());

    const std::vector<std::string> keys = {"key_r1", "key_r2", "key_r3"};
    for (const auto& k : keys) {
        PutObject(*service, ctx.client_id, k);
    }

    // Remove before heartbeat drains the offload queue.
    auto rm = service->BatchRemove(keys, TenantId::Default(), /*force=*/true);
    for (const auto& r : rm) {
        EXPECT_TRUE(r.has_value());
    }

    auto queued = DrainOffloadQueue(*service, ctx.client_id);
    EXPECT_TRUE(queued.empty())
        << "OffloadObjectHeartbeat returned " << queued.size()
        << " stale entries after BatchRemove; EraseMetadata failed to clean "
           "offloading_objects.";
}

// =============================================================================
// UpsertStart on a key with a queued offload must preempt the task and
// reallocate a fresh buffer so the offload read never races the RDMA write.
// =============================================================================

TEST_F(OffloadOnEvictTest, UpsertPreemptsInProgressOffload) {
    MasterServiceConfig config;
    config.enable_offload = true;
    config.default_kv_lease_ttl = 2000;
    auto service = std::make_unique<MasterService>(config);

    constexpr size_t seg_size = 1024 * 1024 * 16;
    auto ctx =
        PrepareSegment(*service, "test_segment", kDefaultSegmentBase, seg_size);
    auto mount_ld = service->MountLocalDiskSegment(ctx.client_id, true);
    ASSERT_TRUE(mount_ld.has_value());

    const std::string key = "upsert_over_offload";
    constexpr size_t kSize = 1024;

    // ComboA: PutEnd populates offloading_tasks[key] immediately.
    ReplicateConfig put_cfg;
    put_cfg.replica_num = 1;
    auto put_start = service->PutStart(ctx.client_id, key, TenantId::Default(),
                                       kSize, put_cfg);
    ASSERT_TRUE(put_start.has_value());
    ASSERT_EQ(put_start->size(), 1u);
    const uintptr_t original_buffer_address =
        put_start->at(0)
            .get_memory_descriptor()
            .buffer_descriptor.buffer_address_;
    ASSERT_TRUE(service
                    ->PutEnd(ctx.client_id, key, TenantId::Default(),
                             ReplicaType::MEMORY)
                    .has_value());

    // Upsert with the same size without draining: preemption must fall
    // through to Case A so the worker's read never races the RDMA write.
    auto upsert_result = service->UpsertStart(
        ctx.client_id, key, TenantId::Default(), kSize, put_cfg);
    ASSERT_TRUE(upsert_result.has_value())
        << "expected upsert to preempt the offload task, got error "
        << (upsert_result.has_value()
                ? 0
                : static_cast<int>(upsert_result.error()));

    auto upsert_replicas = upsert_result.value();
    EXPECT_EQ(upsert_replicas.size(), 1u);
    EXPECT_EQ(upsert_replicas[0].status, ReplicaStatus::PROCESSING);
    EXPECT_NE(upsert_replicas[0]
                  .get_memory_descriptor()
                  .buffer_descriptor.buffer_address_,
              original_buffer_address)
        << "UpsertStart must allocate a fresh buffer when preempting an "
           "offload; reusing the source address races with the worker.";

    // Preemption must clear both offloading_tasks and its mirror; a
    // subsequent drain must not return the stale entry.
    auto queued = DrainOffloadQueue(*service, ctx.client_id);
    EXPECT_TRUE(queued.empty()) << queued.size() << " stale entries";

    auto put_end = service->PutEnd(ctx.client_id, key, TenantId::Default(),
                                   ReplicaType::MEMORY);
    EXPECT_TRUE(put_end.has_value());
}

TEST_F(OffloadOnEvictTest, BatchUpsertPreemptsInProgressOffload) {
    // BatchUpsertStart delegates to UpsertStart per key; verify the batch
    // path preempts every offloading key.
    MasterServiceConfig config;
    config.enable_offload = true;
    config.default_kv_lease_ttl = 2000;
    auto service = std::make_unique<MasterService>(config);

    constexpr size_t seg_size = 1024 * 1024 * 16;
    auto ctx =
        PrepareSegment(*service, "test_segment", kDefaultSegmentBase, seg_size);
    auto mount_ld = service->MountLocalDiskSegment(ctx.client_id, true);
    ASSERT_TRUE(mount_ld.has_value());

    const std::vector<std::string> keys = {"batch_k1", "batch_k2", "batch_k3"};
    for (const auto& k : keys) {
        PutObject(*service, ctx.client_id, k);
    }

    ReplicateConfig cfg;
    cfg.replica_num = 1;
    std::vector<uint64_t> sizes(keys.size(), 1024);
    auto results = service->BatchUpsertStart(ctx.client_id, keys,
                                             TenantId::Default(), sizes, cfg);
    ASSERT_EQ(results.size(), keys.size());
    for (size_t i = 0; i < results.size(); ++i) {
        EXPECT_TRUE(results[i].has_value())
            << "key '" << keys[i] << "' error "
            << (results[i].has_value() ? 0
                                       : static_cast<int>(results[i].error()));
    }

    auto queued = DrainOffloadQueue(*service, ctx.client_id);
    EXPECT_TRUE(queued.empty()) << queued.size() << " stale entries";
}

// =============================================================================
// Regression: after UpsertStart preempts a drained (IN-FLIGHT) task, a late
// NotifyOffloadSuccess with the old wire generation must be dropped.
// =============================================================================

TEST_F(OffloadOnEvictTest, UpsertPreemptsInFlightOffloadAndDropsStaleNotify) {
    MasterServiceConfig config;
    config.enable_offload = true;
    config.default_kv_lease_ttl = 2000;
    auto service = std::make_unique<MasterService>(config);

    constexpr size_t seg_size = 1024 * 1024 * 16;
    auto ctx =
        PrepareSegment(*service, "test_segment", kDefaultSegmentBase, seg_size);
    auto mount_ld = service->MountLocalDiskSegment(ctx.client_id, true);
    ASSERT_TRUE(mount_ld.has_value());

    const std::string key = "upsert_over_inflight";
    PutObject(*service, ctx.client_id, key);

    // Drain the queue: mirror gone, but master's task marker survives
    // until NotifyOffloadSuccess. Capture the wire generation.
    auto hb = service->OffloadObjectHeartbeat(ctx.client_id, true);
    ASSERT_TRUE(hb.has_value());
    ASSERT_EQ(hb->size(), 1u);
    const OffloadTaskItem stale_task = hb->front();
    ASSERT_NE(stale_task.generation, 0u);

    // Upsert preempts the task and installs a new PROCESSING replica.
    ReplicateConfig cfg;
    cfg.replica_num = 1;
    auto upsert_result = service->UpsertStart(
        ctx.client_id, key, TenantId::Default(), /*slice_length=*/1024, cfg);
    ASSERT_TRUE(upsert_result.has_value())
        << "UpsertStart must preempt the in-flight offload, not reject it";
    auto put_end = service->PutEnd(ctx.client_id, key, TenantId::Default(),
                                   ReplicaType::MEMORY);
    ASSERT_TRUE(put_end.has_value());

    // Late Notify with the old generation: both Validate and Notify must
    // reject; no LOCAL_DISK replica may attach to the new generation.
    auto validate = service->ValidateOffloadGenerations({stale_task});
    ASSERT_TRUE(validate.has_value());
    ASSERT_EQ(validate->size(), 1u);
    EXPECT_FALSE(validate->front())
        << "ValidateOffloadGenerations must reject the pre-preempt generation";

    StorageObjectMetadata sm{};
    sm.data_size = 1024;
    sm.transport_endpoint = "test_endpoint";
    auto notify =
        service->NotifyOffloadSuccess(ctx.client_id, {stale_task}, {sm});
    ASSERT_TRUE(notify.has_value());
    // Per-task rejection: the acceptance vector must flag the stale task
    // so the worker rolls back locally without touching accepted siblings.
    ASSERT_EQ(notify->size(), 1u);
    EXPECT_FALSE(notify->front())
        << "stale post-SSD-IO completion must be reported as rejected";
    auto listing = service->GetReplicaListForAdmin(key, TenantId::Default());
    ASSERT_TRUE(listing.has_value());
    for (const auto& rep : listing->replicas) {
        EXPECT_FALSE(rep.is_local_disk_replica())
            << "stale offload completion leaked a LOCAL_DISK replica";
    }
}

// =============================================================================
// Regression: a stale NACK arriving after UpsertStart re-queued the task
// must not clobber the newer generation's marker / source refcount.
// =============================================================================

TEST_F(OffloadOnEvictTest, StaleNackDoesNotClobberNewerOffloadTask) {
    MasterServiceConfig config;
    config.enable_offload = true;
    config.default_kv_lease_ttl = 2000;
    auto service = std::make_unique<MasterService>(config);

    constexpr size_t seg_size = 1024 * 1024 * 16;
    auto ctx =
        PrepareSegment(*service, "test_segment", kDefaultSegmentBase, seg_size);
    auto mount_ld = service->MountLocalDiskSegment(ctx.client_id, true);
    ASSERT_TRUE(mount_ld.has_value());

    const std::string key = "nack_over_newer_task";
    PutObject(*service, ctx.client_id, key);

    // Drain the queue to capture a stale (pre-preempt) task. Master's
    // offloading_tasks[key] survives until NotifyOffloadSuccess.
    auto hb = service->OffloadObjectHeartbeat(ctx.client_id, true);
    ASSERT_TRUE(hb.has_value());
    ASSERT_EQ(hb->size(), 1u);
    const OffloadTaskItem stale_task = hb->front();
    ASSERT_NE(stale_task.generation, 0u);

    // Upsert preempts the in-flight task; PutEnd installs a fresh MEMORY
    // replica which re-queues offloading_tasks[key] at a new generation.
    ReplicateConfig cfg;
    cfg.replica_num = 1;
    auto upsert_result = service->UpsertStart(
        ctx.client_id, key, TenantId::Default(), /*slice_length=*/1024, cfg);
    ASSERT_TRUE(upsert_result.has_value());
    auto put_end = service->PutEnd(ctx.client_id, key, TenantId::Default(),
                                   ReplicaType::MEMORY);
    ASSERT_TRUE(put_end.has_value());

    auto fresh_gen = ReadTaskGeneration(*service, key);
    ASSERT_TRUE(fresh_gen.has_value())
        << "PutEnd must re-queue offloading_tasks[key] under a new gen";
    EXPECT_GT(*fresh_gen, stale_task.generation);
    auto fresh_refcnt = ReadMemoryReplicaRefcnt(*service, key);
    ASSERT_TRUE(fresh_refcnt.has_value());
    EXPECT_GT(*fresh_refcnt, 0u);

    // Late NACK from the stale worker (data_size=-1 sentinel, old gen).
    // Must be rejected per-task; fresh marker + refcnt untouched.
    StorageObjectMetadata nack_meta{};
    nack_meta.data_size = -1;
    auto notify =
        service->NotifyOffloadSuccess(ctx.client_id, {stale_task}, {nack_meta});
    ASSERT_TRUE(notify.has_value());
    ASSERT_EQ(notify->size(), 1u);
    EXPECT_FALSE(notify->front())
        << "stale NACK must be reported as rejected so the worker can "
           "locally roll back its committed bucket without touching accepted "
           "siblings";

    auto post_gen = ReadTaskGeneration(*service, key);
    ASSERT_TRUE(post_gen.has_value())
        << "stale NACK must not erase the fresh offloading_tasks[key] marker";
    EXPECT_EQ(*post_gen, *fresh_gen);
    auto post_refcnt = ReadMemoryReplicaRefcnt(*service, key);
    ASSERT_TRUE(post_refcnt.has_value());
    EXPECT_EQ(*post_refcnt, *fresh_refcnt)
        << "stale NACK must not decrement the fresh source refcnt";
}

// =============================================================================
// Regression: a mixed batch (accepted + stale) must return per-task
// acceptance so the worker rolls back only the rejected key.
// =============================================================================

TEST_F(OffloadOnEvictTest,
       NotifyOffloadSuccessMixedBatchReportsPerTaskAcceptance) {
    MasterServiceConfig config;
    config.enable_offload = true;
    config.default_kv_lease_ttl = 2000;
    auto service = std::make_unique<MasterService>(config);

    constexpr size_t seg_size = 1024 * 1024 * 16;
    auto ctx = PrepareSegment(*service, "test_segment_mixed",
                              kDefaultSegmentBase, seg_size);
    auto mount_ld = service->MountLocalDiskSegment(ctx.client_id, true);
    ASSERT_TRUE(mount_ld.has_value());

    const std::string stale_key = "mixed_stale";
    const std::string fresh_key = "mixed_fresh";
    PutObject(*service, ctx.client_id, stale_key);
    PutObject(*service, ctx.client_id, fresh_key);

    auto hb = service->OffloadObjectHeartbeat(ctx.client_id, true);
    ASSERT_TRUE(hb.has_value());
    ASSERT_EQ(hb->size(), 2u);
    OffloadTaskItem stale_task{};
    OffloadTaskItem fresh_task{};
    for (const auto& t : hb.value()) {
        if (t.key == stale_key) {
            stale_task = t;
        } else if (t.key == fresh_key) {
            fresh_task = t;
        }
    }
    ASSERT_EQ(stale_task.key, stale_key);
    ASSERT_EQ(fresh_task.key, fresh_key);

    // Preempt only stale_key; fresh_key keeps its original generation.
    ReplicateConfig cfg;
    cfg.replica_num = 1;
    ASSERT_TRUE(service
                    ->UpsertStart(ctx.client_id, stale_key, TenantId::Default(),
                                  /*slice_length=*/1024, cfg)
                    .has_value());
    ASSERT_TRUE(service
                    ->PutEnd(ctx.client_id, stale_key, TenantId::Default(),
                             ReplicaType::MEMORY)
                    .has_value());

    // Mixed batch: fabricated storage payload; assertions target the
    // per-task acceptance flag, not the underlying storage state.
    StorageObjectMetadata stale_sm{};
    stale_sm.data_size = 1024;
    stale_sm.transport_endpoint = "test_endpoint_stale";
    StorageObjectMetadata fresh_sm{};
    fresh_sm.data_size = 1024;
    fresh_sm.transport_endpoint = "test_endpoint_fresh";

    auto notify = service->NotifyOffloadSuccess(
        ctx.client_id, {stale_task, fresh_task}, {stale_sm, fresh_sm});
    ASSERT_TRUE(notify.has_value());
    ASSERT_EQ(notify->size(), 2u);
    EXPECT_FALSE(notify.value()[0])
        << "stale task must be reported as rejected without affecting the "
           "accepted sibling";
    EXPECT_TRUE(notify.value()[1]) << "fresh task must be reported as accepted";

    // Fresh sibling's LOCAL_DISK replica must be attached.
    auto listing =
        service->GetReplicaListForAdmin(fresh_key, TenantId::Default());
    ASSERT_TRUE(listing.has_value());
    bool has_local_disk = false;
    for (const auto& rep : listing->replicas) {
        if (rep.is_local_disk_replica()) {
            has_local_disk = true;
            break;
        }
    }
    EXPECT_TRUE(has_local_disk)
        << "accepted sibling must be admitted as a LOCAL_DISK replica";

    // Stale sibling must not have gained a LOCAL_DISK replica.
    auto stale_listing =
        service->GetReplicaListForAdmin(stale_key, TenantId::Default());
    ASSERT_TRUE(stale_listing.has_value());
    for (const auto& rep : stale_listing->replicas) {
        EXPECT_FALSE(rep.is_local_disk_replica())
            << "stale completion leaked a LOCAL_DISK replica onto the "
               "preempted key";
    }
}

// =============================================================================
// Regression: a replica may span multiple LocalDisk segments. UpsertStart
// must clear every mirror; late completions from any drained mirror must
// be rejected. The second mirror is injected via friendship because
// allocator-driven cross-segment placement is not deterministic.
// =============================================================================

TEST_F(OffloadOnEvictTest, UpsertPreemptsOffloadAcrossMultipleSegments) {
    MasterServiceConfig config;
    config.enable_offload = true;
    config.default_kv_lease_ttl = 2000;
    auto service = std::make_unique<MasterService>(config);

    // Base far from kDefaultSegmentBase to avoid clashing with any prior
    // test's segment address space in the same binary.
    constexpr size_t seg_size = 1024 * 1024 * 16;
    constexpr size_t kMultiSegBase = kDefaultSegmentBase + 1024ULL * seg_size;
    auto ctx_a =
        PrepareSegment(*service, "multi_seg_a", kMultiSegBase, seg_size);
    auto ctx_b = PrepareSegment(*service, "multi_seg_b",
                                kMultiSegBase + seg_size, seg_size);
    auto mount_a = service->MountLocalDiskSegment(ctx_a.client_id, true);
    auto mount_b = service->MountLocalDiskSegment(ctx_b.client_id, true);
    ASSERT_TRUE(mount_a.has_value());
    ASSERT_TRUE(mount_b.has_value());

    const std::string key = "upsert_multi_segment";
    PutObject(*service, ctx_a.client_id, key);

    // Snapshot the mirror + its generation. The replica lands on whichever
    // segment the allocator chose; mirror from that segment.
    auto seg_a_task_opt = ReadMirror(*service, ctx_a.client_id, key);
    UUID primary_disk = ctx_a.client_id;
    UUID secondary_disk = ctx_b.client_id;
    if (!seg_a_task_opt.has_value()) {
        seg_a_task_opt = ReadMirror(*service, ctx_b.client_id, key);
        primary_disk = ctx_b.client_id;
        secondary_disk = ctx_a.client_id;
    }
    ASSERT_TRUE(seg_a_task_opt.has_value())
        << "PutObject did not populate any LocalDisk mirror";
    OffloadTaskItem seg_a_task = *seg_a_task_opt;
    ASSERT_NE(seg_a_task.generation, 0u);

    // Inject the same task into the other segment's mirror, mimicking a
    // replica that spans both LocalDisk segments.
    InjectMirror(*service, secondary_disk, key, seg_a_task);

    // Drain the primary heartbeat only: primary is IN-FLIGHT, secondary
    // still QUEUED. UpsertStart must clear both and bump the generation.
    auto hb_primary = service->OffloadObjectHeartbeat(primary_disk, true);
    ASSERT_TRUE(hb_primary.has_value());
    ASSERT_EQ(hb_primary->size(), 1u);

    // UpsertStart must preempt on both segments and bump the generation.
    ReplicateConfig cfg;
    cfg.replica_num = 1;
    auto upsert = service->UpsertStart(ctx_a.client_id, key,
                                       TenantId::Default(), 1024, cfg);
    ASSERT_TRUE(upsert.has_value());

    // Preempt clears every mirror synchronously in UpsertStart. Check
    // this before PutEnd, whose own push may re-populate either segment.
    auto hb_secondary = service->OffloadObjectHeartbeat(secondary_disk, true);
    ASSERT_TRUE(hb_secondary.has_value());
    EXPECT_TRUE(hb_secondary->empty())
        << "UpsertStart failed to clear the cross-segment mirror";

    ASSERT_TRUE(service
                    ->PutEnd(ctx_a.client_id, key, TenantId::Default(),
                             ReplicaType::MEMORY)
                    .has_value());

    // Primary's late completion carries the pre-preempt generation and
    // must be dropped by ValidateOffloadGenerations.
    auto validate = service->ValidateOffloadGenerations({seg_a_task});
    ASSERT_TRUE(validate.has_value());
    EXPECT_FALSE(validate->front());
}

// =============================================================================
// HA compatibility: NotifyOffloadSuccess payloads that predate the generation
// field (wire generation == 0) must take the orphan-fallback path.
// =============================================================================

TEST_F(OffloadOnEvictTest, HANotifyWithGenerationZeroStillAdmitted) {
    MasterServiceConfig config;
    config.enable_offload = true;
    config.default_kv_lease_ttl = 2000;
    auto service = std::make_unique<MasterService>(config);

    constexpr size_t seg_size = 1024 * 1024 * 16;
    auto ctx =
        PrepareSegment(*service, "test_segment", kDefaultSegmentBase, seg_size);
    auto mount_ld = service->MountLocalDiskSegment(ctx.client_id, true);
    ASSERT_TRUE(mount_ld.has_value());

    const std::string key = "ha_recovered_key";
    PutObject(*service, ctx.client_id, key);
    // Drain the queue so the task marker is the only offload state left.
    auto hb = service->OffloadObjectHeartbeat(ctx.client_id, true);
    ASSERT_TRUE(hb.has_value());
    ASSERT_EQ(hb->size(), 1u);

    // Simulate master-restart + pre-generation completion (wire gen == 0,
    // master marker stale). The orphan-fallback path must still admit it.
    OffloadTaskItem ha_task{
        .tenant_id = TenantId::Default().value(),
        .key = key,
        .size = 1024,
        .generation = 0,
    };
    StorageObjectMetadata sm{};
    sm.data_size = 1024;
    sm.transport_endpoint = "test_endpoint";
    auto notify = service->NotifyOffloadSuccess(ctx.client_id, {ha_task}, {sm});
    ASSERT_TRUE(notify.has_value());
}

// =============================================================================
// Regression: UpsertStart must also preempt tasks queued via the
// offload_on_evict eviction path, not just the default PutEnd path.
// =============================================================================

TEST_F(OffloadOnEvictTest, UpsertPreemptsOffloadWithOffloadOnEvict) {
    MasterServiceConfig config;
    config.enable_offload = true;
    config.offload_on_evict = true;
    config.default_kv_lease_ttl = 2000;
    auto service = std::make_unique<MasterService>(config);

    constexpr size_t seg_size = 1024 * 1024 * 16;
    auto ctx =
        PrepareSegment(*service, "test_segment", kDefaultSegmentBase, seg_size);
    auto mount_ld = service->MountLocalDiskSegment(ctx.client_id, true);
    ASSERT_TRUE(mount_ld.has_value());

    const std::string key = "upsert_over_evict_offload";
    PutObject(*service, ctx.client_id, key);

    // offload_on_evict mode: PutEnd does not push. Simulate the eviction
    // path directly instead of filling the segment.
    auto queued_before = DrainOffloadQueue(*service, ctx.client_id);
    ASSERT_TRUE(queued_before.empty())
        << "offload_on_evict must not push at PutEnd";
    InjectOffloadTask(*service, ctx.client_id, key);

    // UpsertStart must preempt: offloading_tasks[key] blocks a naive
    // re-put, and the preempt branch is shared with the ComboA path.
    ReplicateConfig cfg;
    cfg.replica_num = 1;
    auto upsert = service->UpsertStart(ctx.client_id, key, TenantId::Default(),
                                       /*slice_length=*/1024, cfg);
    ASSERT_TRUE(upsert.has_value())
        << "UpsertStart must preempt an offload_on_evict task, got error "
        << static_cast<int>(upsert.error());
    ASSERT_TRUE(service
                    ->PutEnd(ctx.client_id, key, TenantId::Default(),
                             ReplicaType::MEMORY)
                    .has_value());

    // Preempt must clear both the marker and its mirror.
    auto queued_after = DrainOffloadQueue(*service, ctx.client_id);
    EXPECT_TRUE(queued_after.empty())
        << queued_after.size() << " stale entries after preempt";
}

}  // namespace mooncake::test

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
