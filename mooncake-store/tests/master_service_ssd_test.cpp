#include "master_service.h"

#include <glog/logging.h>
#include <gtest/gtest.h>

#include <memory>
#include <mutex>
#include <shared_mutex>
#include <thread>
#include <vector>

#include "master_metric_manager.h"
#include "types.h"

namespace mooncake::test {

std::unique_ptr<MasterService> CreateMasterServiceWithSSDFeat(
    const std::string& root_fs_dir) {
    return std::make_unique<MasterService>(
        MasterServiceConfig::builder().set_root_fs_dir(root_fs_dir).build());
}

class MasterServiceSSDTest : public ::testing::Test {
   protected:
    void SetUp() override {
        google::InitGoogleLogging("MasterServiceTest");
        FLAGS_logtostderr = true;
    }

    void TearDown() override { google::ShutdownGoogleLogging(); }

    // PushOffloadingQueue AND its ObjectIdentity parameter type are both
    // private to MasterService; this fixture is a friend, but friendship is not
    // inherited by the per-test subclass TEST_F generates. So both naming
    // ObjectIdentity and calling PushOffloadingQueue must happen inside a
    // member of this class, not in the TEST_F body. Take plain tenant/key args
    // and build the private identity here. See issue #2997.
    static tl::expected<void, ErrorCode> CallPushOffloadingQueue(
        MasterService& service, const TenantId& tenant, const std::string& key,
        Replica& replica) {
        const MasterService::ObjectIdentity id{tenant, key};
        return service.PushOffloadingQueue(id, replica);
    }
};

std::unique_ptr<MasterService> CreateSsdAwareOffloadService() {
    MasterServiceConfig config;
    config.enable_offload = true;
    config.default_kv_lease_ttl = 0;
    config.allocation_strategy_type =
        AllocationStrategyType::SSD_FREE_RATIO_FIRST;
    return std::make_unique<MasterService>(config);
}

void MountMemoryAndLocalDisk(MasterService& service, const UUID& client_id,
                             const std::string& segment_name,
                             size_t base_addr) {
    Segment segment;
    segment.id = generate_uuid();
    segment.name = segment_name;
    segment.base = base_addr;
    segment.size = 64 * 1024 * 1024;
    segment.te_endpoint = segment.name;

    ASSERT_TRUE(service.MountSegment(segment, client_id).has_value());
    ASSERT_TRUE(service.MountLocalDiskSegment(client_id, true).has_value());
    ASSERT_TRUE(service.ReportSsdCapacity(client_id, 1000).has_value());
}

void PutAndOffload(MasterService& service, const UUID& client_id,
                   const std::string& key, int64_t object_size,
                   const std::string& local_disk_endpoint) {
    ReplicateConfig config;
    config.replica_num = 1;

    ASSERT_TRUE(
        service
            .PutStart(client_id, key, TenantId::Default(), object_size, config)
            .has_value());
    ASSERT_TRUE(
        service.PutEnd(client_id, key, TenantId::Default(), ReplicaType::MEMORY)
            .has_value());

    StorageObjectMetadata metadata;
    metadata.data_size = object_size;
    metadata.transport_endpoint = local_disk_endpoint;
    OffloadTaskItem task{.tenant_id = TenantId::Default().value(),
                         .key = key,
                         .size = object_size};
    ASSERT_TRUE(service.NotifyOffloadSuccess(client_id, {task}, {metadata})
                    .has_value());
}

TEST_F(MasterServiceSSDTest, PutRevokeProcessingDiskKeepsSsdTotal) {
    auto service_ = CreateMasterServiceWithSSDFeat("/mnt/ssd");
    auto& metrics = MasterMetricManager::instance();
    using CacheHitStat = MasterMetricManager::CacheHitStat;
    const auto base_stats = metrics.calculate_cache_stats();
    const double base_memory_total = base_stats.at(CacheHitStat::MEMORY_TOTAL);
    const double base_ssd_total = base_stats.at(CacheHitStat::SSD_TOTAL);

    constexpr size_t buffer = 0x310000000;
    constexpr size_t size = 1024 * 1024 * 64;
    Segment segment;
    segment.id = generate_uuid();
    segment.name = "test_segment_revoke_processing_disk";
    segment.base = buffer;
    segment.size = size;
    segment.te_endpoint = segment.name;
    UUID client_id = generate_uuid();

    ASSERT_TRUE(service_->MountSegment(segment, client_id).has_value());

    std::string key = "revoke_processing_disk_metric_key";
    ASSERT_TRUE(service_
                    ->PutStart(client_id, key, TenantId::Default(), 1024,
                               {.replica_num = 1})
                    .has_value());
    EXPECT_TRUE(
        service_
            ->PutEnd(client_id, key, TenantId::Default(), ReplicaType::MEMORY)
            .has_value());

    auto stats = metrics.calculate_cache_stats();
    EXPECT_EQ(stats[CacheHitStat::MEMORY_TOTAL], base_memory_total + 1);
    EXPECT_EQ(stats[CacheHitStat::SSD_TOTAL], base_ssd_total);

    EXPECT_TRUE(
        service_
            ->PutRevoke(client_id, key, TenantId::Default(), ReplicaType::DISK)
            .has_value());

    stats = metrics.calculate_cache_stats();
    EXPECT_EQ(stats[CacheHitStat::MEMORY_TOTAL], base_memory_total + 1);
    EXPECT_EQ(stats[CacheHitStat::SSD_TOTAL], base_ssd_total);

    ASSERT_TRUE(
        service_->Remove(key, TenantId::Default(), /*force=*/true).has_value());
    stats = metrics.calculate_cache_stats();
    EXPECT_EQ(stats[CacheHitStat::MEMORY_TOTAL], base_memory_total);
    EXPECT_EQ(stats[CacheHitStat::SSD_TOTAL], base_ssd_total);
}

TEST_F(MasterServiceSSDTest, EvictObject) {
    auto service_ = CreateMasterServiceWithSSDFeat("/mnt/ssd");
    // Mount a segment that can hold about 1024 * 16 objects.
    // As the eviction is processed separately for each shard,
    // we need to fill each shard with enough objects to thoroughly
    // test the eviction process.
    constexpr size_t buffer = 0x300000000;
    constexpr size_t size = 1024 * 1024 * 16 * 15;
    constexpr size_t object_size = 1024 * 15;
    std::string segment_name = "test_segment";
    Segment segment;
    segment.id = generate_uuid();
    segment.name = segment_name;
    segment.base = buffer;
    segment.size = size;
    segment.te_endpoint = segment.name;
    UUID client_id = generate_uuid();
    auto mount_result = service_->MountSegment(segment, client_id);
    ASSERT_TRUE(mount_result.has_value());

    // Verify if we can put objects more than the segment can hold
    int success_puts = 0;
    for (int i = 0; i < 1024 * 16 + 50; ++i) {
        std::string key = "test_key" + std::to_string(i);
        uint64_t slice_length = object_size;
        ReplicateConfig config;
        config.replica_num = 1;
        auto put_start_result = service_->PutStart(
            client_id, key, TenantId::Default(), slice_length, config);
        if (put_start_result.has_value()) {
            auto put_end_mem_result = service_->PutEnd(
                client_id, key, TenantId::Default(), ReplicaType::MEMORY);
            auto put_end_disk_result = service_->PutEnd(
                client_id, key, TenantId::Default(), ReplicaType::DISK);
            ASSERT_TRUE(put_end_mem_result.has_value());
            ASSERT_TRUE(put_end_disk_result.has_value());
            success_puts++;
        } else {
            // wait for eviction to work
            std::this_thread::sleep_for(std::chrono::milliseconds(50));
        }
    }
    ASSERT_GT(success_puts, 1024 * 16);

    // Verify if we can get objects more than the segment can hold
    int success_gets = 0;
    for (int i = 0; i < 1024 * 16 + 50; ++i) {
        std::string key = "test_key" + std::to_string(i);
        auto get_result = service_->GetReplicaList(key, TenantId::Default());
        if (get_result.has_value()) {
            success_gets++;
        }
    }
    ASSERT_GT(success_gets, 1024 * 16);

    std::this_thread::sleep_for(
        std::chrono::milliseconds(DEFAULT_DEFAULT_KV_LEASE_TTL));
    service_->RemoveAll();
}

TEST_F(MasterServiceSSDTest, PutStartExpires) {
    // Reset storage space metrics.
    MasterMetricManager::instance().reset_allocated_mem_size();
    MasterMetricManager::instance().reset_total_mem_capacity();

    MasterServiceConfig master_config;
    master_config.root_fs_dir = "/mnt/ssd";
    master_config.put_start_discard_timeout_sec = 3;
    master_config.put_start_release_timeout_sec = 5;
    std::unique_ptr<MasterService> service_(new MasterService(master_config));

    constexpr size_t kReplicaCnt = 2;  // 1 memory replica + 1 disk replica
    constexpr size_t kBaseAddr = 0x300000000;
    constexpr size_t kSegmentSize = 1024 * 1024 * 16;  // 16MB

    // Mount a segment.
    std::string segment_name = "test_segment";
    Segment segment;
    segment.id = generate_uuid();
    segment.name = segment_name;
    segment.base = kBaseAddr;
    segment.size = kSegmentSize;
    segment.te_endpoint = segment.name;
    auto client_id = generate_uuid();
    auto mount_result = service_->MountSegment(segment, client_id);
    ASSERT_TRUE(mount_result.has_value());

    std::string key = "test_key";
    uint64_t value_length = 16 * 1024 * 1024;  // 16MB
    uint64_t slice_length = value_length;
    ReplicateConfig config;

    auto test_discard_replica = [&](ReplicaType discard_type) {
        const auto reserve_type = discard_type == ReplicaType::MEMORY
                                      ? ReplicaType::DISK
                                      : ReplicaType::MEMORY;

        // Put key, should success.
        auto put_start_result = service_->PutStart(
            client_id, key, TenantId::Default(), slice_length, config);
        EXPECT_TRUE(put_start_result.has_value());
        auto replica_list = put_start_result.value();
        EXPECT_EQ(replica_list.size(), kReplicaCnt);
        for (size_t i = 0; i < kReplicaCnt; i++) {
            EXPECT_EQ(ReplicaStatus::PROCESSING, replica_list[i].status);
        }

        // Complete the reserved replica.
        auto put_end_result =
            service_->PutEnd(client_id, key, TenantId::Default(), reserve_type);
        EXPECT_TRUE(put_end_result.has_value());

        // Wait for a while until the put-start expired.
        for (size_t i = 0; i <= master_config.put_start_discard_timeout_sec;
             i++) {
            // Keep mounted segments alive.
            auto result = service_->Ping(client_id);
            EXPECT_TRUE(result.has_value());
            // Protect the key from eviction.
            auto get_result =
                service_->GetReplicaList(key, TenantId::Default());
            EXPECT_TRUE(get_result.has_value());
            std::this_thread::sleep_for(std::chrono::seconds(1));
        }

        // Put key again, should fail because the object has had an completed
        // replica.
        put_start_result = service_->PutStart(
            client_id, key, TenantId::Default(), slice_length, config);
        EXPECT_FALSE(put_start_result.has_value());
        EXPECT_EQ(put_start_result.error(), ErrorCode::OBJECT_ALREADY_EXISTS);

        // Wait for a while until the discarded replicas are released.
        for (size_t i = 0; i <= master_config.put_start_release_timeout_sec;
             i++) {
            // Keep mounted segments alive.
            auto result = service_->Ping(client_id);
            EXPECT_TRUE(result.has_value());
            // Protect the key from eviction.
            auto get_result =
                service_->GetReplicaList(key, TenantId::Default());
            EXPECT_TRUE(get_result.has_value());
            std::this_thread::sleep_for(std::chrono::seconds(1));
        }

        // PutEnd must reject a replica discarded after the write expired.
        put_end_result =
            service_->PutEnd(client_id, key, TenantId::Default(), discard_type);
        ASSERT_FALSE(put_end_result.has_value());
        EXPECT_EQ(put_end_result.error(), ErrorCode::INVALID_WRITE);

        // Check that the key has only one replica.
        auto get_result = service_->GetReplicaList(key, TenantId::Default());
        EXPECT_TRUE(get_result.has_value());
        EXPECT_EQ(get_result.value().replicas.size(), 1);
        if (reserve_type == ReplicaType::MEMORY) {
            EXPECT_TRUE(get_result.value().replicas[0].is_memory_replica());
        } else {
            EXPECT_TRUE(get_result.value().replicas[0].is_disk_replica());
        }

        // Wait for the key to expire.
        for (size_t i = 0; i <= DEFAULT_DEFAULT_KV_LEASE_TTL / 1000; i++) {
            auto result = service_->Ping(client_id);
            EXPECT_TRUE(result.has_value());
            std::this_thread::sleep_for(std::chrono::seconds(1));
        }
        service_->RemoveAll();
    };

    test_discard_replica(ReplicaType::DISK);
    test_discard_replica(ReplicaType::MEMORY);
}

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
    ASSERT_TRUE(service_
                    ->PutStart(client_id, key, TenantId::Default(), 1024,
                               {.replica_num = 1})
                    .has_value());
    EXPECT_TRUE(
        service_
            ->PutEnd(client_id, key, TenantId::Default(), ReplicaType::MEMORY)
            .has_value());
    EXPECT_TRUE(
        service_->PutEnd(client_id, key, TenantId::Default(), ReplicaType::DISK)
            .has_value());

    auto stats = metrics.calculate_cache_stats();
    EXPECT_EQ(stats[CacheHitStat::MEMORY_TOTAL], base_memory_total + 1);
    EXPECT_EQ(stats[CacheHitStat::SSD_TOTAL], base_ssd_total + 1);

    ASSERT_TRUE(
        service_->Remove(key, TenantId::Default(), /*force=*/true).has_value());

    stats = metrics.calculate_cache_stats();
    EXPECT_EQ(stats[CacheHitStat::MEMORY_TOTAL], base_memory_total);
    EXPECT_EQ(stats[CacheHitStat::SSD_TOTAL], base_ssd_total);
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
                                  TenantId::Default(), ReplicaType::LOCAL_DISK);
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
            (void)svc.PutStart(writer, key, TenantId::Default(), kSliceSize,
                               cfg);
            (void)svc.PutEnd(writer, key, TenantId::Default(),
                             ReplicaType::MEMORY);
            total += std::chrono::duration_cast<std::chrono::microseconds>(
                std::chrono::steady_clock::now() - t0);
            (void)svc.Remove(key, TenantId::Default(), /*force=*/true);
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

// Friended by MasterService: runs the two halves of UnmountLocalDiskSegment
// (deregistration, replica sweep) as separate steps, so a competing mount +
// register can be serialized between them -- the interleaving is pinned by
// construction instead of hoping a scheduler produces it. The helpers are
// members of this class because friendship does not extend to the
// TEST_F-generated subclasses.
class LocalDiskUnmountInterleavingTest : public MasterServiceSSDTest {
   protected:
    static void DeregisterHalf(MasterService& service, const UUID& client_id) {
        std::unique_lock<std::shared_mutex> snapshot_lock(
            service.snapshot_mutex_);
        service.local_ssd_manager_.UnregisterClient(client_id);
    }

    static void SweepHalf(MasterService& service, const UUID& client_id) {
        service.ClearLocalDiskHandlesOwnedBy(client_id);
    }
};

TEST_F(LocalDiskUnmountInterleavingTest,
       MountAndRegisterBetweenRemovalAndSweepSurvives) {
    auto service = CreateSsdAwareOffloadService();
    UUID leaving = generate_uuid();
    UUID late = generate_uuid();
    const std::string leaving_segment = "ssd_interleave_leaving_segment";
    const std::string late_segment = "ssd_interleave_late_segment";
    MountMemoryAndLocalDisk(*service, leaving, leaving_segment, 0x1300000000);
    PutAndOffload(*service, leaving, "ssd_interleave_leaving_key", 1024,
                  leaving_segment);

    // First half of UnmountLocalDiskSegment(leaving): the client is
    // deregistered; the sweep has not run.
    DeregisterHalf(*service, leaving);

    // The interleaving under test: another store mounts and registers a
    // replica before the sweep reaches its shard. Whether the client monitor
    // has admitted `late` to the alive set yet does not matter to an
    // owner-targeted sweep -- while a liveness-complement sweep taken before
    // this mount would classify the replica stale and erase it, and with it
    // the key, since this disk replica is the key's only one.
    MountMemoryAndLocalDisk(*service, late, late_segment, 0x1400000000);
    StorageObjectMetadata late_metadata;
    late_metadata.data_size = 1024;
    late_metadata.transport_endpoint = late_segment;
    OffloadTaskItem late_task{.tenant_id = TenantId::Default().value(),
                              .key = "ssd_interleave_late_key",
                              .size = 1024};
    ASSERT_TRUE(
        service->NotifyOffloadSuccess(late, {late_task}, {late_metadata})
            .has_value());

    // Second half: the sweep.
    SweepHalf(*service, leaving);

    // The leaving owner's disk replica is gone (the memory replica stays)...
    auto leaving_replicas = service->GetReplicaList(
        "ssd_interleave_leaving_key", TenantId::Default());
    ASSERT_TRUE(leaving_replicas.has_value());
    ASSERT_EQ(1u, leaving_replicas.value().replicas.size());
    EXPECT_TRUE(leaving_replicas.value().replicas[0].is_memory_replica());

    // ...while the late mounter's registration survived the sweep.
    auto late_replicas =
        service->GetReplicaList("ssd_interleave_late_key", TenantId::Default());
    ASSERT_TRUE(late_replicas.has_value());
    ASSERT_EQ(1u, late_replicas.value().replicas.size());
    EXPECT_TRUE(late_replicas.value().replicas[0].is_local_disk_replica());
}

// Regression test for issue #2997.
//
// PushOffloadingQueue has two no-op paths that used to return a silent success
// ({}) without enqueuing anything:
//
//   1. get_segment_names() is empty   — the replica carries no source segment
//      metadata (e.g. a DISK/LOCAL_DISK/DFS replica).
//   2. every segment name is nullopt  — a MEMORY/NOF replica whose backing
//      buffer is absent or has an invalid allocator, so get_segment_names()
//      yields a single nullopt entry and the loop body is skipped for it.
//
// In both cases the caller's `if (result)` branch fired and executed
// inc_refcnt() + offloading_tasks.emplace() for work that was never submitted,
// leaking the source replica's refcount until the 600s TTL reaper cleared the
// phantom task. The fix returns UNABLE_OFFLOADING from both paths.
//
// These two states cannot arise from the public PutStart/PutEnd path — that
// path always produces a MEMORY replica with a valid buffer, whose
// segment_names is a single real name, so PushOffloadingQueue reaches
// EnqueueOffload and (pre-fix as well as post-fix) already returned
// UNABLE_OFFLOADING via SEGMENT_NOT_FOUND. To actually guard the two lines this
// PR changed, the test constructs the degenerate replicas directly and calls
// PushOffloadingQueue through the test-friend seam, asserting the no-op is
// reported as a failure rather than a silent success.
TEST_F(MasterServiceSSDTest, PushOffloadingQueueReportsNoopAsFailure) {
    auto service = CreateSsdAwareOffloadService();
    // ObjectIdentity is private to MasterService, so it is constructed inside
    // the friend helper from these plain args rather than named here (see
    // helper).
    const TenantId tenant = TenantId::Default();
    const std::string key = "noop_offload_key";

    // Path 2: MEMORY replica with a null buffer -> get_segment_names() is
    // [nullopt] -> the loop enqueues nothing -> the !any_enqueued guard fires.
    Replica all_nullopt_replica(/*buffer=*/nullptr, ReplicaStatus::COMPLETE);
    ASSERT_FALSE(all_nullopt_replica.get_segment_names().empty());
    auto r2 =
        CallPushOffloadingQueue(*service, tenant, key, all_nullopt_replica);
    ASSERT_FALSE(r2.has_value())
        << "all-nullopt segment names must not report a silent success "
           "(issue #2997)";
    EXPECT_EQ(ErrorCode::UNABLE_OFFLOADING, r2.error());

    // Path 1: a non-MEMORY/non-NOF replica -> get_segment_names() is empty ->
    // the empty-source guard fires before the loop.
    Replica empty_names_replica(/*file_path=*/"/tmp/nonexistent_offload_src",
                                /*object_size=*/1024, ReplicaStatus::COMPLETE);
    ASSERT_TRUE(empty_names_replica.get_segment_names().empty());
    auto r1 =
        CallPushOffloadingQueue(*service, tenant, key, empty_names_replica);
    ASSERT_FALSE(r1.has_value())
        << "empty segment names must not report a silent success (issue #2997)";
    EXPECT_EQ(ErrorCode::UNABLE_OFFLOADING, r1.error());
}

}  // namespace mooncake::test

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
