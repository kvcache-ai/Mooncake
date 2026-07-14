#include "fixture.h"

namespace mooncake::test {
TEST_F(MasterServiceSSDTest, RemoveDecrementsCacheTotalMetrics) {
    auto service_ = CreateMasterServiceWithSSDFeat("/mnt/ssd");
    auto& metrics = MasterMetricManager::instance();
    using CacheHitStat = MasterMetricManager::CacheHitStat;
    const auto base_stats = metrics.calculate_cache_stats();
    const double base_memory_total = base_stats.at(CacheHitStat::MEMORY_TOTAL);
    const double base_ssd_total = base_stats.at(CacheHitStat::SSD_TOTAL);

    constexpr size_t buffer = 0x320000000;
    constexpr size_t size = 1024 * 1024 * 64;
    Segment segment;
    segment.id = generate_uuid();
    segment.name = "test_segment_remove_metrics";
    segment.base = buffer;
    segment.size = size;
    segment.te_endpoint = segment.name;
    UUID client_id = generate_uuid();

    ASSERT_TRUE(service_->MountSegment(segment, client_id).has_value());

    std::string key = "remove_cache_total_metric_key";
    ASSERT_TRUE(
        service_->PutStart(client_id, key, "default", 1024, {.replica_num = 1})
            .has_value());
    EXPECT_TRUE(service_->PutEnd(client_id, key, "default", ReplicaType::MEMORY)
                    .has_value());
    EXPECT_TRUE(service_->PutEnd(client_id, key, "default", ReplicaType::DISK)
                    .has_value());

    auto stats = metrics.calculate_cache_stats();
    EXPECT_EQ(stats[CacheHitStat::MEMORY_TOTAL], base_memory_total + 1);
    EXPECT_EQ(stats[CacheHitStat::SSD_TOTAL], base_ssd_total + 1);

    ASSERT_TRUE(service_->Remove(key, "default", /*force=*/true).has_value());

    stats = metrics.calculate_cache_stats();
    EXPECT_EQ(stats[CacheHitStat::MEMORY_TOTAL], base_memory_total);
    EXPECT_EQ(stats[CacheHitStat::SSD_TOTAL], base_ssd_total);
}

TEST_F(MasterServiceSSDTest, RemoveReleasesLocalDiskUsageTracking) {
    auto service = CreateSsdAwareOffloadService();
    UUID client1 = generate_uuid();
    UUID client2 = generate_uuid();
    const std::string segment1 = "ssd_remove_segment_1";
    const std::string segment2 = "ssd_remove_segment_2";
    MountMemoryAndLocalDisk(*service, client1, segment1, 0x400000000);
    MountMemoryAndLocalDisk(*service, client2, segment2, 0x500000000);

    PutAndOffload(*service, client1, "ssd_remove_released", 800, segment1);
    PutAndOffload(*service, client2, "ssd_remove_baseline", 100, segment2);

    ASSERT_TRUE(service->Remove("ssd_remove_released", "default").has_value());

    ExpectNextAllocationOnSegment(*service, client1, "ssd_remove_probe",
                                  segment1);
}

TEST_F(MasterServiceSSDTest,
       BatchReplicaClearAllSegmentsReleasesLocalDiskUsageTracking) {
    auto service = CreateSsdAwareOffloadService();
    UUID client1 = generate_uuid();
    UUID client2 = generate_uuid();
    const std::string segment1 = "ssd_clear_segment_1";
    const std::string segment2 = "ssd_clear_segment_2";
    MountMemoryAndLocalDisk(*service, client1, segment1, 0x600000000);
    MountMemoryAndLocalDisk(*service, client2, segment2, 0x700000000);

    PutAndOffload(*service, client1, "ssd_clear_released", 800, segment1);
    PutAndOffload(*service, client2, "ssd_clear_baseline", 100, segment2);

    auto clear_result =
        service->BatchReplicaClear({"ssd_clear_released"}, client1, "");
    ASSERT_TRUE(clear_result.has_value());
    ASSERT_EQ(clear_result->size(), 1u);
    EXPECT_EQ((*clear_result)[0], "ssd_clear_released");

    ExpectNextAllocationOnSegment(*service, client1, "ssd_clear_probe",
                                  segment1);
}

// Test that after offloading more data to segment1, the next allocation prefers
// segment2 which has more SSD free space.
TEST_F(MasterServiceSSDTest, SsdFreeRatioFirstPrefersFresherSsdAfterOffload) {
    auto service = CreateSsdAwareOffloadService();
    UUID client1 = generate_uuid();
    UUID client2 = generate_uuid();
    const std::string segment1 = "ssd_fresher_seg_1";
    const std::string segment2 = "ssd_fresher_seg_2";
    // Each segment reports total SSD capacity = 1000 bytes
    MountMemoryAndLocalDisk(*service, client1, segment1, 0x800000000);
    MountMemoryAndLocalDisk(*service, client2, segment2, 0x900000000);

    // Offload 800 bytes to segment1 → ssd_used[seg1]=800, free=20%
    PutAndOffload(*service, client1, "ssd_fresher_heavy", 800, segment1);
    // Offload 100 bytes to segment2 → ssd_used[seg2]=100, free=90%
    PutAndOffload(*service, client2, "ssd_fresher_light", 100, segment2);

    // segment2 has higher SSD free ratio → allocation should prefer segment2
    ExpectNextAllocationOnSegment(*service, client2, "ssd_fresher_probe",
                                  segment2);
}

// Test that EvictDiskReplica decrements ssd_used_bytes so that the evicted
// segment becomes preferred again for the next allocation.
TEST_F(MasterServiceSSDTest, EvictDiskReplicaDecrementsLocalDiskUsageTracking) {
    auto service = CreateSsdAwareOffloadService();
    UUID client1 = generate_uuid();
    UUID client2 = generate_uuid();
    const std::string segment1 = "ssd_evict_dec_seg_1";
    const std::string segment2 = "ssd_evict_dec_seg_2";
    MountMemoryAndLocalDisk(*service, client1, segment1, 0xa00000000);
    MountMemoryAndLocalDisk(*service, client2, segment2, 0xb00000000);

    // Offload 800 bytes to segment1 → ssd_used[seg1]=800 (20% free)
    PutAndOffload(*service, client1, "ssd_evict_dec_heavy", 800, segment1);
    // Offload 100 bytes to segment2 → ssd_used[seg2]=100 (90% free)
    PutAndOffload(*service, client2, "ssd_evict_dec_light", 100, segment2);

    // segment2 has more SSD free space → should be preferred
    ExpectNextAllocationOnSegment(*service, client2, "ssd_evict_dec_probe1",
                                  segment2);

    // Evict the LOCAL_DISK replica of the heavy object from segment1.
    // NotifyOffloadSuccess creates a LOCAL_DISK replica (not DISK).
    // This decrements ssd_used[seg1] by 800 → ssd_used[seg1]=0 (100% free)
    auto evict_result = service->EvictDiskReplica(
        client1, "ssd_evict_dec_heavy", "default", ReplicaType::LOCAL_DISK);
    ASSERT_TRUE(evict_result.has_value());

    // After eviction: segment1 has 100% free, segment2 has 90% free
    // → segment1 should now be preferred
    ExpectNextAllocationOnSegment(*service, client1, "ssd_evict_dec_probe2",
                                  segment1);
}

// Evicting a LOCAL_DISK replica via EvictDiskReplica must decrement
// file_cache_nums_ even when the object still has a MEMORY replica (so
// accessor.Erase() does not run). Without SyncCacheTotalAccounting in the
// LOCAL_DISK eviction branch, the gauge would stay over-counted.
TEST_F(MasterServiceSSDTest, EvictDiskReplicaDecrementsFileCacheNums) {
    auto& metrics = MasterMetricManager::instance();
    auto service = CreateSsdAwareOffloadService();
    UUID client_id = generate_uuid();
    const std::string segment = "ssd_evict_cache_total_segment";
    MountMemoryAndLocalDisk(*service, client_id, segment, 0xc00000000);

    const int64_t baseline = metrics.get_file_cache_nums();
    const int64_t baseline_mem = metrics.get_mem_cache_nums();

    PutAndOffload(*service, client_id, "ssd_evict_cache_total_key", 128,
                  segment);

    // After offload: file_cache_nums_ increments by 1 (LOCAL_DISK replica),
    // mem_cache_nums_ also increments by 1 (MEMORY replica from PutEnd).
    EXPECT_EQ(metrics.get_file_cache_nums(), baseline + 1);
    EXPECT_EQ(metrics.get_mem_cache_nums(), baseline_mem + 1);

    auto evict_result =
        service->EvictDiskReplica(client_id, "ssd_evict_cache_total_key",
                                  "default", ReplicaType::LOCAL_DISK);
    ASSERT_TRUE(evict_result.has_value());

    // After evicting LOCAL_DISK: file_cache_nums_ returns to baseline,
    // mem_cache_nums_ unchanged (MEMORY replica still present).
    EXPECT_EQ(metrics.get_file_cache_nums(), baseline);
    EXPECT_EQ(metrics.get_mem_cache_nums(), baseline_mem + 1);
}

// Real-path performance comparison: MasterService PutStart throughput for
// three configurations:
//   (A) RANDOM, no offload        — baseline, original behavior
//   (B) RANDOM, with offload      — isolates disk-replica creation overhead
//   (C) SSD_FREE_RATIO_FIRST, with offload — adds SSD metrics lock + sorting
//
// Comparing A→B separates the cost of mounting LocalDisk segments.
// Comparing B→C isolates the pure SSD-ranking strategy overhead.
//
// Each round: PutStart → PutEnd(MEMORY) (timed) → Remove (not timed).
TEST_F(MasterServiceSSDTest,
       SsdFreeRatioFirstVsRandomMasterServicePerformance) {
    constexpr int kNumNodes = 32;
    constexpr size_t kSegmentSize = 8 * 1024 * 1024;  // 8 MiB each
    constexpr size_t kSliceSize = 512;  // 512 B – focus on strategy cost
    constexpr int kWarmupRounds = 50;
    constexpr int kBenchmarkRounds = 300;

    // Build a MasterService with kNumNodes segments. with_ssd=true also
    // mounts LocalDisk and reports varied SSD capacity per node.
    auto buildAndMount =
        [&](AllocationStrategyType strategy, bool with_ssd, size_t base_start,
            const std::string& tag) -> std::unique_ptr<MasterService> {
        MasterServiceConfig config;
        config.enable_offload = with_ssd;
        config.default_kv_lease_ttl = 10000;
        config.allocation_strategy_type = strategy;
        auto svc = std::make_unique<MasterService>(config);

        for (int i = 0; i < kNumNodes; i++) {
            UUID cid = generate_uuid();
            Segment seg;
            seg.id = generate_uuid();
            seg.name = "ms_perf_" + std::to_string(i) + "_" + tag;
            seg.base = base_start + static_cast<size_t>(i) * kSegmentSize;
            seg.size = kSegmentSize;
            seg.te_endpoint = seg.name;
            (void)svc->MountSegment(seg, cid);
            if (with_ssd) {
                (void)svc->MountLocalDiskSegment(cid, true);
                // Vary total SSD capacity so nodes have distinct free ratios
                (void)svc->ReportSsdCapacity(
                    cid, static_cast<int64_t>(1024 * 1024) * (i + 1));
            }
        }
        return svc;
    };

    // Measure kRounds of PutStart + PutEnd(MEMORY). Remove is called after
    // timing to free allocator space without inflating the measurement.
    auto runBenchmark = [&](MasterService& svc, const std::string& key_pfx,
                            int rounds) -> std::chrono::microseconds {
        const UUID writer = generate_uuid();
        ReplicateConfig cfg;
        cfg.replica_num = 1;
        std::chrono::microseconds total{0};

        for (int i = 0; i < rounds; i++) {
            const std::string key = key_pfx + std::to_string(i);
            auto t0 = std::chrono::steady_clock::now();
            (void)svc.PutStart(writer, key, "default", kSliceSize, cfg);
            (void)svc.PutEnd(writer, key, "default", ReplicaType::MEMORY);
            total += std::chrono::duration_cast<std::chrono::microseconds>(
                std::chrono::steady_clock::now() - t0);
            (void)svc.Remove(key, "default", /*force=*/true);
        }
        return total;
    };

    // (A) RANDOM, no offload – baseline
    auto svc_a = buildAndMount(AllocationStrategyType::RANDOM, false,
                               0xc00000000ULL, "A");
    (void)runBenchmark(*svc_a, "ms_A_wu_", kWarmupRounds);
    auto elapsed_a = runBenchmark(*svc_a, "ms_A_bm_", kBenchmarkRounds);

    // (B) RANDOM, with offload – quantify disk-replica overhead alone
    auto svc_b = buildAndMount(AllocationStrategyType::RANDOM, true,
                               0xd00000000ULL, "B");
    (void)runBenchmark(*svc_b, "ms_B_wu_", kWarmupRounds);
    auto elapsed_b = runBenchmark(*svc_b, "ms_B_bm_", kBenchmarkRounds);

    // (C) SSD_FREE_RATIO_FIRST, with offload – full new feature
    auto svc_c = buildAndMount(AllocationStrategyType::SSD_FREE_RATIO_FIRST,
                               true, 0xe00000000ULL, "C");
    (void)runBenchmark(*svc_c, "ms_C_wu_", kWarmupRounds);
    auto elapsed_c = runBenchmark(*svc_c, "ms_C_bm_", kBenchmarkRounds);

    auto us_per_op = [&](std::chrono::microseconds us) {
        return static_cast<double>(us.count()) / kBenchmarkRounds;
    };
    double ratio_b_a =
        static_cast<double>(elapsed_b.count()) / elapsed_a.count();
    double ratio_c_b =
        static_cast<double>(elapsed_c.count()) / elapsed_b.count();
    double ratio_c_a =
        static_cast<double>(elapsed_c.count()) / elapsed_a.count();

    std::cout
        << "\n=== MasterService Real-Path Performance (PutStart+PutEnd) ===\n"
        << "Nodes: " << kNumNodes << " | Slice: " << kSliceSize
        << " B | Rounds: " << kBenchmarkRounds << "\n\n"
        << "  (A) RANDOM, offload=OFF (baseline):         " << elapsed_a.count()
        << " us  |  " << std::fixed << std::setprecision(3)
        << us_per_op(elapsed_a) << " us/op\n"
        << "  (B) RANDOM, offload=ON  (disk replica cost):" << elapsed_b.count()
        << " us  |  " << us_per_op(elapsed_b) << " us/op  ["
        << std::setprecision(2) << ratio_b_a << "x vs A]\n"
        << "  (C) SSD_FREE_RATIO_FIRST, offload=ON:       " << elapsed_c.count()
        << " us  |  " << us_per_op(elapsed_c) << " us/op  [" << ratio_c_b
        << "x vs B]\n\n"
        << "  A→B  disk-replica overhead:   " << std::setprecision(1)
        << (ratio_b_a - 1.0) * 100.0 << "%\n"
        << "  B→C  SSD-ranking overhead:    " << (ratio_c_b - 1.0) * 100.0
        << "%\n"
        << "  A→C  total overhead vs origin:" << (ratio_c_a - 1.0) * 100.0
        << "%\n\n";
}

}  // namespace mooncake::test
