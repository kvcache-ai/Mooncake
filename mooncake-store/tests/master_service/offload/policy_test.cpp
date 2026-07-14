#include "fixture.h"

namespace mooncake::test {
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

}  // namespace mooncake::test
