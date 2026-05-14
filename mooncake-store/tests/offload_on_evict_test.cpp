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
        auto put_start = service.PutStart(client_id, key, size, config);
        ASSERT_TRUE(put_start.has_value()) << "PutStart failed for key=" << key;
        auto put_end = service.PutEnd(client_id, key, ReplicaType::MEMORY);
        ASSERT_TRUE(put_end.has_value()) << "PutEnd failed for key=" << key;
    }

    // Drain the offload queue via OffloadObjectHeartbeat.
    std::unordered_map<std::string, int64_t> DrainOffloadQueue(
        MasterService& service, const UUID& client_id) {
        auto res = service.OffloadObjectHeartbeat(client_id, true);
        if (!res) {
            return {};
        }
        return std::move(res.value());
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
            auto result = service.PutStart(client_id, key, object_size, config);
            if (result.has_value()) {
                auto end = service.PutEnd(client_id, key, ReplicaType::MEMORY);
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
        auto result =
            service->PutStart(ctx.client_id, key, object_size, config);
        if (result.has_value()) {
            auto end = service->PutEnd(ctx.client_id, key, ReplicaType::MEMORY);
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

}  // namespace mooncake::test

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
