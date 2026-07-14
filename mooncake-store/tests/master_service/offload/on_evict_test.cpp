#include "fixture.h"

namespace mooncake::test {
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
        auto result = service->PutStart(ctx.client_id, key, "default",
                                        object_size, config);
        if (result.has_value()) {
            auto end = service->PutEnd(ctx.client_id, key, "default",
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

}  // namespace mooncake::test
