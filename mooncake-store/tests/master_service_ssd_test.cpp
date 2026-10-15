#include "master_service.h"
#include "master_service/master_service_test_peer.h"

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

    // Reach PushOffloadingQueue and its private identity type through the
    // test peer to exercise replica states unavailable via PutStart (#2997).
    static tl::expected<void, ErrorCode> CallPushOffloadingQueue(
        MasterService& service, const TenantId& tenant, const std::string& key,
        Replica& replica) {
        const MasterServiceTestPeer::ObjectIdentity id{tenant, key};
        return MasterServiceTestPeer(service).PushOffloadingQueue(id, replica);
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
                   const std::string& local_disk_endpoint,
                   const std::string& memory_segment = "") {
    ReplicateConfig config;
    config.replica_num = 1;
    // The store that completes the offload must hold the memory replica.
    config.preferred_segment = memory_segment;

    ASSERT_TRUE(
        service
            .PutStart(client_id, key, TenantId::Default(), object_size, config)
            .has_value());
    ASSERT_TRUE(
        service.PutEnd(client_id, key, TenantId::Default(), ReplicaType::MEMORY)
            .has_value());

    // A store copies only what its heartbeat drained.
    auto drained = service.OffloadObjectHeartbeat(client_id, true);
    ASSERT_TRUE(drained.has_value());
    ASSERT_EQ(drained->size(), 1u);
    ASSERT_EQ(drained->front().key, key);

    StorageObjectMetadata metadata;
    metadata.data_size = object_size;
    metadata.transport_endpoint = local_disk_endpoint;
    ASSERT_TRUE(service.NotifyOffloadSuccess(client_id, *drained, {metadata})
                    .has_value());
}

void PutMemoryObject(MasterService& service, const UUID& client_id,
                     const std::string& key, int64_t object_size) {
    ReplicateConfig config;
    config.replica_num = 1;
    ASSERT_TRUE(
        service
            .PutStart(client_id, key, TenantId::Default(), object_size, config)
            .has_value());
    ASSERT_TRUE(
        service.PutEnd(client_id, key, TenantId::Default(), ReplicaType::MEMORY)
            .has_value());
}

OffloadTaskItem Task(const std::string& key, int64_t size) {
    return OffloadTaskItem{
        .tenant_id = TenantId::Default().value(), .key = key, .size = size};
}

// The id of the key's current offload task, or nullopt without one.
std::optional<L4TaskId> OffloadTaskId(MasterService& service,
                                      const std::string& key) {
    MasterServiceTestPeer::MetadataAccessorRW accessor(
        &service, MasterServiceTestPeer::ObjectIdentity{
                      .tenant_id = TenantId::Default(), .user_key = key});
    if (!accessor.Exists() || !accessor.GetState().offloading_task) {
        return std::nullopt;
    }
    return accessor.GetState().offloading_task->task_id;
}

// Queues one more offload item for `key` onto the mailbox of the store that
// holds its first memory replica, with a fresh id that no task has. It
// replaces the item queued there, like an item left over from an earlier
// task of the key.
void QueueForeignItem(MasterService& service, const std::string& key) {
    MasterServiceTestPeer peer(service);
    const MasterServiceTestPeer::ObjectIdentity id{
        .tenant_id = TenantId::Default(), .user_key = key};
    MasterServiceTestPeer::MetadataAccessorRW accessor(&service, id);
    ASSERT_TRUE(accessor.Exists());
    bool pushed = false;
    accessor.Get().VisitReplicas(
        [](const Replica& replica) { return replica.is_memory_replica(); },
        [&](Replica& replica) {
            if (!pushed) {
                pushed = peer.PushOffloadingQueue(id, replica).has_value();
            }
        });
    ASSERT_TRUE(pushed);
}

void ExpectMemoryOnly(MasterService& service, const std::string& key) {
    auto replicas = service.GetReplicaList(key, TenantId::Default());
    ASSERT_TRUE(replicas.has_value());
    for (const auto& replica : replicas->replicas) {
        EXPECT_TRUE(replica.is_memory_replica()) << key;
    }
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

// Uses the test peer to run the two halves of UnmountLocalDiskSegment
// (deregistration, replica sweep) as separate steps, so a competing mount +
// register can be serialized between them -- the interleaving is pinned by
// construction instead of hoping a scheduler produces it.
class LocalDiskUnmountInterleavingTest : public MasterServiceSSDTest {
   protected:
    static void DeregisterHalf(MasterService& service, const UUID& client_id) {
        std::unique_lock<std::shared_mutex> snapshot_lock(
            MasterServiceTestPeer::MasterStateMutex(service));
        MasterServiceTestPeer::LocalSsdManager(service).UnregisterClient(
            client_id);
    }

    static void SweepHalf(MasterService& service, const UUID& client_id) {
        MasterServiceTestPeer(service).ClearLocalDiskHandlesOwnedBy(client_id);
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
                  leaving_segment, leaving_segment);

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
    PutAndOffload(*service, late, "ssd_interleave_late_key", 1024, late_segment,
                  late_segment);

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
    ASSERT_EQ(2u, late_replicas.value().replicas.size());
    EXPECT_TRUE(late_replicas.value().replicas[1].is_local_disk_replica());
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
// inc_refcnt() and recorded an offloading task for work that was never
// submitted, leaking the source replica's refcount until the 600s TTL reaper
// cleared the phantom task. The fix returns UNABLE_OFFLOADING from both paths.
//
// These two states cannot arise from the public PutStart/PutEnd path — that
// path always produces a MEMORY replica with a valid buffer, whose
// segment_names is a single real name, so PushOffloadingQueue reaches
// EnqueueOffload and (pre-fix as well as post-fix) already returned
// UNABLE_OFFLOADING via SEGMENT_NOT_FOUND. To actually guard the two lines this
// PR changed, the test constructs the degenerate replicas directly and calls
// PushOffloadingQueue through the test peer, asserting the no-op is
// reported as a failure rather than a silent success.
TEST_F(MasterServiceSSDTest, PushOffloadingQueueReportsNoopAsFailure) {
    auto service = CreateSsdAwareOffloadService();
    // The helper constructs the peer-exposed identity from tenant/key args.
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

// A late completion of an offload whose object was removed while the store
// was copying must not bring the key back. The store's file holds the
// removed object's bytes, and a later put of the key must not fail as if the
// key still existed.
TEST_F(MasterServiceSSDTest, LateCompletionDoesNotRecreateARemovedKey) {
    auto service = CreateSsdAwareOffloadService();
    const UUID client_id = generate_uuid();
    MountMemoryAndLocalDisk(*service, client_id, "removed_segment",
                            0x430000000);
    PutMemoryObject(*service, client_id, "removed_key", 4096);
    // The store takes the task and starts copying; meanwhile the key is
    // removed.
    auto drained = service->OffloadObjectHeartbeat(client_id, true);
    ASSERT_TRUE(drained.has_value());
    ASSERT_EQ(drained->size(), 1u);
    ASSERT_TRUE(
        service->Remove("removed_key", TenantId::Default(), true).has_value());

    StorageObjectMetadata file;
    file.data_size = 4096;
    file.transport_endpoint = "disk_endpoint";
    ASSERT_TRUE(
        service->NotifyOffloadSuccess(client_id, *drained, {file}).has_value());
    auto exists = service->ExistKey("removed_key", TenantId::Default());
    ASSERT_TRUE(exists.has_value());
    EXPECT_FALSE(exists.value());
    ReplicateConfig config;
    config.replica_num = 1;
    EXPECT_TRUE(service
                    ->PutStart(client_id, "removed_key", TenantId::Default(),
                               4096, config)
                    .has_value());
}

// A re-registration (a completion without an offload task taken by the
// client, as sent by the store's rescan) never registers a disk replica:
// neither onto an existing object nor by creating a missing one. The store's
// file can predate the current object under that key, because Remove does
// not delete it, and a removed key must stay removed.
TEST_F(MasterServiceSSDTest, ReRegistrationNeverRegistersADiskReplica) {
    auto service = CreateSsdAwareOffloadService();
    const UUID client_id = generate_uuid();
    MountMemoryAndLocalDisk(*service, client_id, "rereg_segment", 0x320000000);
    PutAndOffload(*service, client_id, "rereg_key", 4096, "disk_endpoint");
    PutAndOffload(*service, client_id, "rereg_removed", 4096, "disk_endpoint");
    ASSERT_TRUE(
        service->Remove("rereg_key", TenantId::Default(), true).has_value());
    ASSERT_TRUE(service->Remove("rereg_removed", TenantId::Default(), true)
                    .has_value());
    // Re-put with new bytes of the same size. The store skips offloading it
    // (it still holds the old file) and NACKs the task.
    PutMemoryObject(*service, client_id, "rereg_key", 4096);
    auto drained = service->OffloadObjectHeartbeat(client_id, true);
    ASSERT_TRUE(drained.has_value());
    ASSERT_EQ(drained->size(), 1u);
    StorageObjectMetadata nack;
    nack.data_size = -1;
    ASSERT_TRUE(
        service->NotifyOffloadSuccess(client_id, *drained, {nack}).has_value());

    // Rescan: the old files of both keys. The batch succeeds, so the store
    // finishes its rescan and does not retry it.
    StorageObjectMetadata file;
    file.data_size = 4096;
    file.transport_endpoint = "disk_endpoint";
    ASSERT_TRUE(service
                    ->NotifyOffloadSuccess(
                        client_id,
                        {Task("rereg_key", 4096), Task("rereg_removed", 4096)},
                        {file, file})
                    .has_value());
    auto existing = service->GetReplicaList("rereg_key", TenantId::Default());
    ASSERT_TRUE(existing.has_value());
    ASSERT_EQ(existing->replicas.size(), 1u);
    EXPECT_TRUE(existing->replicas[0].is_memory_replica());
    auto removed =
        service->GetReplicaList("rereg_removed", TenantId::Default());
    ASSERT_FALSE(removed.has_value());
    EXPECT_EQ(removed.error(), ErrorCode::OBJECT_NOT_FOUND);
}

// A file the store already had is reported in the same shape as a
// completion. While the key's current task is still queued for the store
// (not taken), such a report is a re-registration and must not complete it.
TEST_F(MasterServiceSSDTest, ReRegistrationDoesNotCompleteAQueuedTask) {
    auto service = CreateSsdAwareOffloadService();
    const UUID client_id = generate_uuid();
    MountMemoryAndLocalDisk(*service, client_id, "queued_segment", 0x340000000);
    PutMemoryObject(*service, client_id, "queued_key", 4096);
    StorageObjectMetadata file;
    file.data_size = 4096;
    file.transport_endpoint = "disk_endpoint";
    ASSERT_TRUE(service
                    ->NotifyOffloadSuccess(client_id,
                                           {Task("queued_key", 4096)}, {file})
                    .has_value());
    ExpectMemoryOnly(*service, "queued_key");

    // The real offload still completes after the store drained it.
    auto drained = service->OffloadObjectHeartbeat(client_id, true);
    ASSERT_TRUE(drained.has_value());
    ASSERT_EQ(drained->size(), 1u);
    ASSERT_TRUE(
        service->NotifyOffloadSuccess(client_id, *drained, {file}).has_value());
    auto replicas = service->GetReplicaList("queued_key", TenantId::Default());
    ASSERT_TRUE(replicas.has_value());
    EXPECT_EQ(replicas->replicas.size(), 2u);
}

// The completion of an offload whose object was removed while the store was
// copying (its source buffer freed and possibly reused) must not complete the
// task of an object re-put under the same key, even when the same store has
// taken that task too: it names its own task.
TEST_F(MasterServiceSSDTest, LateCompletionOfARemovedObjectIsRefused) {
    auto service = CreateSsdAwareOffloadService();
    const UUID client_id = generate_uuid();
    MountMemoryAndLocalDisk(*service, client_id, "aba_segment", 0x350000000);
    PutMemoryObject(*service, client_id, "aba_key", 4096);
    // The store takes the task and starts copying.
    auto first = service->OffloadObjectHeartbeat(client_id, true);
    ASSERT_TRUE(first.has_value());
    ASSERT_EQ(first->size(), 1u);
    // Meanwhile the key is removed and re-put with bytes of the same size,
    // and the store takes the new task right away.
    ASSERT_TRUE(
        service->Remove("aba_key", TenantId::Default(), true).has_value());
    PutMemoryObject(*service, client_id, "aba_key", 4096);
    auto second = service->OffloadObjectHeartbeat(client_id, true);
    ASSERT_TRUE(second.has_value());
    ASSERT_EQ(second->size(), 1u);
    ASSERT_NE(*second->front().task_id, *first->front().task_id);

    // The completion of the first copy arrives: refused.
    StorageObjectMetadata file;
    file.data_size = 4096;
    file.transport_endpoint = "disk_endpoint";
    ASSERT_TRUE(
        service->NotifyOffloadSuccess(client_id, *first, {file}).has_value());
    ExpectMemoryOnly(*service, "aba_key");
    ASSERT_EQ(OffloadTaskId(*service, "aba_key"), *second->front().task_id);

    // The second copy's completion is registered.
    ASSERT_TRUE(
        service->NotifyOffloadSuccess(client_id, *second, {file}).has_value());
    auto replicas = service->GetReplicaList("aba_key", TenantId::Default());
    ASSERT_TRUE(replicas.has_value());
    EXPECT_EQ(replicas->replicas.size(), 2u);
}

// A NACK can overtake the completion of the same copy (a completion RPC that
// timed out at the store is rolled back and NACKed, but may still be
// processed by the master afterwards). That late completion must not
// complete a newer task of the key either.
TEST_F(MasterServiceSSDTest, CompletionOvertakenByItsNackIsRefused) {
    auto service = CreateSsdAwareOffloadService();
    const UUID client_id = generate_uuid();
    MountMemoryAndLocalDisk(*service, client_id, "nack_segment", 0x360000000);
    PutMemoryObject(*service, client_id, "nack_key", 4096);
    auto first = service->OffloadObjectHeartbeat(client_id, true);
    ASSERT_TRUE(first.has_value());
    ASSERT_EQ(first->size(), 1u);
    StorageObjectMetadata nack;
    nack.data_size = -1;
    ASSERT_TRUE(
        service->NotifyOffloadSuccess(client_id, *first, {nack}).has_value());
    ASSERT_FALSE(OffloadTaskId(*service, "nack_key").has_value());
    ASSERT_TRUE(
        service->Remove("nack_key", TenantId::Default(), true).has_value());
    PutMemoryObject(*service, client_id, "nack_key", 4096);
    auto second = service->OffloadObjectHeartbeat(client_id, true);
    ASSERT_TRUE(second.has_value());
    ASSERT_EQ(second->size(), 1u);

    StorageObjectMetadata file;
    file.data_size = 4096;
    file.transport_endpoint = "disk_endpoint";
    ASSERT_TRUE(
        service->NotifyOffloadSuccess(client_id, *first, {file}).has_value());
    ExpectMemoryOnly(*service, "nack_key");
    ASSERT_TRUE(
        service->NotifyOffloadSuccess(client_id, *second, {file}).has_value());
    auto replicas = service->GetReplicaList("nack_key", TenantId::Default());
    ASSERT_TRUE(replicas.has_value());
    EXPECT_EQ(replicas->replicas.size(), 2u);
}

// The scenario that returns another object's bytes when a report is matched
// by key alone:
// 1. The store copies O1 and sends the completion; the RPC times out at the
//    store, which rolls the bucket back and NACKs. The completion request is
//    still on its way to the master.
// 2. The NACK is processed first and ends T1. O1 is removed, O2 is put, and
//    the store offloads O2 normally (its file for the key holds O2's bytes).
// 3. O2 is removed. The master drops O2's disk replica; the store keeps the
//    file (an orphan).
// 4. O3 (same size) is put and the store takes T3. It already has a file for
//    the key, so it does not copy and will NACK at the end of its cycle.
// 5. In that window the late completion of step 1 is processed. By key it
//    looks like the report of T3 by its taker; registered, it would send
//    reads of O3 to the store's file, which holds O2's bytes.
// The late completion names T1 and is refused.
TEST_F(MasterServiceSSDTest, LateCompletionAfterOrphanSkipIsRefused) {
    auto service = CreateSsdAwareOffloadService();
    const UUID client_id = generate_uuid();
    MountMemoryAndLocalDisk(*service, client_id, "orphan_segment", 0x3f0000000);
    StorageObjectMetadata file;
    file.data_size = 4096;
    file.transport_endpoint = "disk_endpoint";
    StorageObjectMetadata nack;
    nack.data_size = -1;
    const std::string key = "orphan_key";

    // 1.
    PutMemoryObject(*service, client_id, key, 4096);
    auto item1 = service->OffloadObjectHeartbeat(client_id, true);
    ASSERT_TRUE(item1.has_value());
    ASSERT_EQ(item1->size(), 1u);
    const auto held_completion = *item1;  // still in flight
    // 2.
    ASSERT_TRUE(
        service->NotifyOffloadSuccess(client_id, *item1, {nack}).has_value());
    ASSERT_TRUE(service->Remove(key, TenantId::Default(), true).has_value());
    PutMemoryObject(*service, client_id, key, 4096);
    auto item2 = service->OffloadObjectHeartbeat(client_id, true);
    ASSERT_TRUE(item2.has_value());
    ASSERT_EQ(item2->size(), 1u);
    ASSERT_TRUE(
        service->NotifyOffloadSuccess(client_id, *item2, {file}).has_value());
    auto with_disk = service->GetReplicaList(key, TenantId::Default());
    ASSERT_TRUE(with_disk.has_value());
    ASSERT_EQ(with_disk->replicas.size(), 2u);
    // 3.
    ASSERT_TRUE(service->Remove(key, TenantId::Default(), true).has_value());
    // 4.
    PutMemoryObject(*service, client_id, key, 4096);
    auto item3 = service->OffloadObjectHeartbeat(client_id, true);
    ASSERT_TRUE(item3.has_value());
    ASSERT_EQ(item3->size(), 1u);
    // 5.
    ASSERT_TRUE(
        service->NotifyOffloadSuccess(client_id, held_completion, {file})
            .has_value());
    ExpectMemoryOnly(*service, key);
    // T3 is untouched; the store's NACK ends it.
    ASSERT_EQ(OffloadTaskId(*service, key), *item3->front().task_id);
    ASSERT_TRUE(
        service->NotifyOffloadSuccess(client_id, *item3, {nack}).has_value());
    EXPECT_FALSE(OffloadTaskId(*service, key).has_value());
    ExpectMemoryOnly(*service, key);
}

// A late NACK of an earlier task of the key does not end a newer task, whose
// own completion is then registered.
TEST_F(MasterServiceSSDTest, StaleNackDoesNotReleaseANewerTask) {
    auto service = CreateSsdAwareOffloadService();
    const UUID client_id = generate_uuid();
    MountMemoryAndLocalDisk(*service, client_id, "stale_nack_segment",
                            0x400000000);
    PutMemoryObject(*service, client_id, "stale_nack_key", 4096);
    auto item1 = service->OffloadObjectHeartbeat(client_id, true);
    ASSERT_TRUE(item1.has_value());
    ASSERT_EQ(item1->size(), 1u);
    ASSERT_TRUE(service->Remove("stale_nack_key", TenantId::Default(), true)
                    .has_value());
    PutMemoryObject(*service, client_id, "stale_nack_key", 4096);
    auto item2 = service->OffloadObjectHeartbeat(client_id, true);
    ASSERT_TRUE(item2.has_value());
    ASSERT_EQ(item2->size(), 1u);

    StorageObjectMetadata nack;
    nack.data_size = -1;
    ASSERT_TRUE(
        service->NotifyOffloadSuccess(client_id, *item1, {nack}).has_value());
    ASSERT_EQ(OffloadTaskId(*service, "stale_nack_key"),
              *item2->front().task_id);
    StorageObjectMetadata file;
    file.data_size = 4096;
    file.transport_endpoint = "disk_endpoint";
    ASSERT_TRUE(
        service->NotifyOffloadSuccess(client_id, *item2, {file}).has_value());
    auto replicas =
        service->GetReplicaList("stale_nack_key", TenantId::Default());
    ASSERT_TRUE(replicas.has_value());
    EXPECT_EQ(replicas->replicas.size(), 2u);
}

// Two master instances draw different epochs, so a report of a task of one
// (an earlier master: before a roll, a restart or a fail-over) never matches
// a task of the other, even for the same key, client and sequence number.
TEST_F(MasterServiceSSDTest, TaskIdsDifferAcrossMasterInstances) {
    auto first = CreateSsdAwareOffloadService();
    auto second = CreateSsdAwareOffloadService();
    EXPECT_NE(first->L4TaskEpoch(), second->L4TaskEpoch());
    EXPECT_NE(first->L4TaskEpoch(), 0u);
    EXPECT_NE(second->L4TaskEpoch(), 0u);

    const UUID client_id = generate_uuid();
    MountMemoryAndLocalDisk(*first, client_id, "epoch_segment", 0x410000000);
    MountMemoryAndLocalDisk(*second, client_id, "epoch_segment", 0x410000000);
    PutMemoryObject(*first, client_id, "epoch_key", 4096);
    PutMemoryObject(*second, client_id, "epoch_key", 4096);
    auto from_first = first->OffloadObjectHeartbeat(client_id, true);
    auto from_second = second->OffloadObjectHeartbeat(client_id, true);
    ASSERT_TRUE(from_first.has_value());
    ASSERT_TRUE(from_second.has_value());
    ASSERT_EQ(from_first->size(), 1u);
    ASSERT_EQ(from_second->size(), 1u);
    EXPECT_EQ(from_first->front().task_id->seq,
              from_second->front().task_id->seq);

    StorageObjectMetadata file;
    file.data_size = 4096;
    file.transport_endpoint = "disk_endpoint";
    ASSERT_TRUE(second->NotifyOffloadSuccess(client_id, *from_first, {file})
                    .has_value());
    ExpectMemoryOnly(*second, "epoch_key");
    ASSERT_TRUE(second->NotifyOffloadSuccess(client_id, *from_second, {file})
                    .has_value());
    auto replicas = second->GetReplicaList("epoch_key", TenantId::Default());
    ASSERT_TRUE(replicas.has_value());
    EXPECT_EQ(replicas->replicas.size(), 2u);
}

// A task re-created while the item of the key's previous task is still
// queued (that task expired before any drain) is handed out: its item
// replaces the old one in the store's queue.
TEST_F(MasterServiceSSDTest, ReCreatedTaskReplacesThePendingItem) {
    MasterServiceConfig config;
    config.enable_offload = true;
    config.offload_on_evict = true;
    config.default_kv_lease_ttl = 0;
    auto service = std::make_unique<MasterService>(config);
    const UUID client_id = generate_uuid();
    MountMemoryAndLocalDisk(*service, client_id, "requeue_segment",
                            0x420000000);
    PutMemoryObject(*service, client_id, "requeue_key", 4096);
    MasterServiceTestPeer peer(*service);
    // Eviction queues the first task (offload_on_evict).
    peer.RunBatchEvictForTesting(1.0, 1.0);
    const auto first = OffloadTaskId(*service, "requeue_key");
    ASSERT_TRUE(first.has_value());
    // It expires without a drain (TTL reaper); its item stays queued.
    {
        MasterServiceTestPeer::MetadataShardAccessorRW shard(
            service.get(),
            peer.getShardIndex(TenantId::Default(), "requeue_key"));
        peer.DiscardExpiredProcessingReplicas(
            shard, std::chrono::system_clock::now() + std::chrono::hours(1));
    }
    ASSERT_FALSE(OffloadTaskId(*service, "requeue_key").has_value());
    // The next eviction queues the next task of the key.
    peer.RunBatchEvictForTesting(1.0, 1.0);
    const auto second = OffloadTaskId(*service, "requeue_key");
    ASSERT_TRUE(second.has_value());
    ASSERT_NE(*second, *first);

    auto drained = service->OffloadObjectHeartbeat(client_id, true);
    ASSERT_TRUE(drained.has_value());
    ASSERT_EQ(drained->size(), 1u);
    EXPECT_EQ(*drained->front().task_id, *second);
    StorageObjectMetadata file;
    file.data_size = 4096;
    file.transport_endpoint = "disk_endpoint";
    ASSERT_TRUE(
        service->NotifyOffloadSuccess(client_id, *drained, {file}).has_value());
    auto replicas = service->GetReplicaList("requeue_key", TenantId::Default());
    ASSERT_TRUE(replicas.has_value());
    bool has_disk = false;
    for (const auto& replica : replicas->replicas) {
        has_disk = has_disk || replica.is_local_disk_replica();
    }
    EXPECT_TRUE(has_disk);
}

// After a restart the master knows nothing of the offloads its stores took
// from the previous master. Their reports name tasks of that master (or no
// task at all) and are refused; the store's next drain is handed out and
// registered as usual.
TEST_F(MasterServiceSSDTest, ReportsOfAnEarlierMasterAreRefused) {
    auto service = CreateSsdAwareOffloadService();
    const UUID client_id = generate_uuid();
    MountMemoryAndLocalDisk(*service, client_id, "cold_segment", 0x390000000);
    PutMemoryObject(*service, client_id, "cold_key", 4096);
    StorageObjectMetadata file;
    file.data_size = 4096;
    file.transport_endpoint = "disk_endpoint";
    // A report of a take from the previous master (no id, as from a master
    // that predates ids, or another epoch) is refused.
    auto previous = Task("cold_key", 4096);
    ASSERT_TRUE(service->NotifyOffloadSuccess(client_id, {previous}, {file})
                    .has_value());
    previous.task_id = L4TaskId{service->L4TaskEpoch() ^ 1, 1};
    ASSERT_TRUE(service->NotifyOffloadSuccess(client_id, {previous}, {file})
                    .has_value());
    ExpectMemoryOnly(*service, "cold_key");

    auto taken = service->OffloadObjectHeartbeat(client_id, true);
    ASSERT_TRUE(taken.has_value());
    ASSERT_EQ(taken->size(), 1u);
    EXPECT_EQ(taken->front().key, "cold_key");
    ASSERT_TRUE(
        service->NotifyOffloadSuccess(client_id, *taken, {file}).has_value());
    auto replicas = service->GetReplicaList("cold_key", TenantId::Default());
    ASSERT_TRUE(replicas.has_value());
    EXPECT_EQ(replicas->replicas.size(), 2u);
}

// Every offload task gets a fresh id, and each item queued for it carries
// that id.
TEST_F(MasterServiceSSDTest, OffloadItemsCarryTheirTaskId) {
    auto service = CreateSsdAwareOffloadService();
    const UUID client_id = generate_uuid();
    MountMemoryAndLocalDisk(*service, client_id, "id_segment", 0x3a0000000);
    PutMemoryObject(*service, client_id, "id_key_a", 4096);
    PutMemoryObject(*service, client_id, "id_key_b", 4096);
    const auto id_a = OffloadTaskId(*service, "id_key_a");
    const auto id_b = OffloadTaskId(*service, "id_key_b");
    ASSERT_TRUE(id_a.has_value());
    ASSERT_TRUE(id_b.has_value());
    EXPECT_EQ(id_a->epoch, service->L4TaskEpoch());
    EXPECT_NE(id_a->epoch, 0u);
    EXPECT_EQ(id_b->epoch, id_a->epoch);
    EXPECT_NE(id_b->seq, id_a->seq);
    auto drained = service->OffloadObjectHeartbeat(client_id, true);
    ASSERT_TRUE(drained.has_value());
    ASSERT_EQ(drained->size(), 2u);
    for (const auto& item : *drained) {
        ASSERT_TRUE(item.task_id.has_value()) << item.key;
        EXPECT_EQ(*item.task_id, item.key == "id_key_a" ? *id_a : *id_b);
    }
}

// A completion without a task id (a store that predates task ids, or a
// rescan entry) is never registered. If its sender took the task, the task
// ends: it would only pin its source until the TTL reaper.
TEST_F(MasterServiceSSDTest,
       IdLessCompletionIsRefusedButReleasesTheTaskOfItsTaker) {
    auto service = CreateSsdAwareOffloadService();
    const UUID client_id = generate_uuid();
    MountMemoryAndLocalDisk(*service, client_id, "noid_segment", 0x3b0000000);
    PutMemoryObject(*service, client_id, "noid_key", 4096);
    StorageObjectMetadata file;
    file.data_size = 4096;
    file.transport_endpoint = "disk_endpoint";
    // Not taken yet: refused, and the queued task stays.
    ASSERT_TRUE(
        service
            ->NotifyOffloadSuccess(client_id, {Task("noid_key", 4096)}, {file})
            .has_value());
    ExpectMemoryOnly(*service, "noid_key");
    ASSERT_TRUE(OffloadTaskId(*service, "noid_key").has_value());

    auto drained = service->OffloadObjectHeartbeat(client_id, true);
    ASSERT_TRUE(drained.has_value());
    ASSERT_EQ(drained->size(), 1u);
    auto without_id = drained->front();
    without_id.task_id.reset();
    ASSERT_TRUE(service->NotifyOffloadSuccess(client_id, {without_id}, {file})
                    .has_value());
    ExpectMemoryOnly(*service, "noid_key");
    EXPECT_FALSE(OffloadTaskId(*service, "noid_key").has_value());
}

// A completion that names another task of the key is refused and leaves the
// key's current task alone; that task's own report is still registered.
TEST_F(MasterServiceSSDTest, CompletionWithAnotherTaskIdIsRefused) {
    auto service = CreateSsdAwareOffloadService();
    const UUID client_id = generate_uuid();
    MountMemoryAndLocalDisk(*service, client_id, "other_id_segment",
                            0x3c0000000);
    PutMemoryObject(*service, client_id, "other_id_key", 4096);
    auto drained = service->OffloadObjectHeartbeat(client_id, true);
    ASSERT_TRUE(drained.has_value());
    ASSERT_EQ(drained->size(), 1u);
    const L4TaskId current = *drained->front().task_id;
    StorageObjectMetadata file;
    file.data_size = 4096;
    file.transport_endpoint = "disk_endpoint";
    for (const L4TaskId other : {L4TaskId{current.epoch, current.seq + 1},
                                 L4TaskId{current.epoch ^ 1, current.seq}}) {
        auto item = drained->front();
        item.task_id = other;
        ASSERT_TRUE(service->NotifyOffloadSuccess(client_id, {item}, {file})
                        .has_value());
        ExpectMemoryOnly(*service, "other_id_key");
        ASSERT_EQ(OffloadTaskId(*service, "other_id_key"), current);
    }
    ASSERT_TRUE(
        service->NotifyOffloadSuccess(client_id, *drained, {file}).has_value());
    auto replicas =
        service->GetReplicaList("other_id_key", TenantId::Default());
    ASSERT_TRUE(replicas.has_value());
    EXPECT_EQ(replicas->replicas.size(), 2u);
}

// A NACK ends only the task it names.
TEST_F(MasterServiceSSDTest, NackWithAnotherTaskIdKeepsTheTask) {
    auto service = CreateSsdAwareOffloadService();
    const UUID client_id = generate_uuid();
    MountMemoryAndLocalDisk(*service, client_id, "nack_id_segment",
                            0x3d0000000);
    PutMemoryObject(*service, client_id, "nack_id_key", 4096);
    auto drained = service->OffloadObjectHeartbeat(client_id, true);
    ASSERT_TRUE(drained.has_value());
    ASSERT_EQ(drained->size(), 1u);
    const L4TaskId current = *drained->front().task_id;
    auto item = drained->front();
    item.task_id = L4TaskId{current.epoch, current.seq + 1};
    StorageObjectMetadata nack;
    nack.data_size = -1;
    ASSERT_TRUE(
        service->NotifyOffloadSuccess(client_id, {item}, {nack}).has_value());
    ASSERT_EQ(OffloadTaskId(*service, "nack_id_key"), current);

    StorageObjectMetadata file;
    file.data_size = 4096;
    file.transport_endpoint = "disk_endpoint";
    ASSERT_TRUE(
        service->NotifyOffloadSuccess(client_id, *drained, {file}).has_value());
    auto replicas = service->GetReplicaList("nack_id_key", TenantId::Default());
    ASSERT_TRUE(replicas.has_value());
    EXPECT_EQ(replicas->replicas.size(), 2u);
}

// A queued item that names no current task is neither handed out nor able
// to end the key's current task when a store drains it with offloading
// disabled.
TEST_F(MasterServiceSSDTest, DrainedItemsOfAnotherTaskAreDropped) {
    auto service = CreateSsdAwareOffloadService();
    const UUID client_id = generate_uuid();
    MountMemoryAndLocalDisk(*service, client_id, "foreign_segment",
                            0x3e0000000);
    PutMemoryObject(*service, client_id, "foreign_key", 4096);
    const auto current = OffloadTaskId(*service, "foreign_key");
    ASSERT_TRUE(current.has_value());

    QueueForeignItem(*service, "foreign_key");
    auto taken = service->OffloadObjectHeartbeat(client_id, true);
    ASSERT_TRUE(taken.has_value());
    EXPECT_TRUE(taken->empty());
    EXPECT_EQ(OffloadTaskId(*service, "foreign_key"), current);

    QueueForeignItem(*service, "foreign_key");
    auto disabled = service->OffloadObjectHeartbeat(client_id, false);
    ASSERT_TRUE(disabled.has_value());
    EXPECT_TRUE(disabled->empty());
    EXPECT_EQ(OffloadTaskId(*service, "foreign_key"), current);
}

// A mirror that did not get the pinned source copies from a buffer nothing
// pins, so only the source owner's completion is registered.
TEST_F(MasterServiceSSDTest, OnlyTheSourceOwnersCompletionIsRegistered) {
    auto service = CreateSsdAwareOffloadService();
    const UUID client_a = generate_uuid();
    const UUID client_b = generate_uuid();
    MountMemoryAndLocalDisk(*service, client_a, "mirror_a", 0x370000000);
    MountMemoryAndLocalDisk(*service, client_b, "mirror_b", 0x380000000);
    ReplicateConfig config;
    config.replica_num = 2;
    ASSERT_TRUE(service
                    ->PutStart(client_a, "mirror_key", TenantId::Default(),
                               4096, config)
                    .has_value());
    ASSERT_TRUE(service
                    ->PutEnd(client_a, "mirror_key", TenantId::Default(),
                             ReplicaType::MEMORY)
                    .has_value());
    auto from_a = service->OffloadObjectHeartbeat(client_a, true);
    auto from_b = service->OffloadObjectHeartbeat(client_b, true);
    ASSERT_TRUE(from_a.has_value());
    ASSERT_TRUE(from_b.has_value());
    ASSERT_EQ(from_a->size(), 1u);
    ASSERT_EQ(from_b->size(), 1u);

    // Find which store owns the pinned source: the first memory replica.
    auto replicas = service->GetReplicaList("mirror_key", TenantId::Default());
    ASSERT_TRUE(replicas.has_value());
    ASSERT_EQ(replicas->replicas.size(), 2u);
    const auto& first_name = replicas->replicas[0]
                                 .get_memory_descriptor()
                                 .buffer_descriptor.transport_endpoint_;
    const bool a_is_source = first_name == "mirror_a";
    const UUID source = a_is_source ? client_a : client_b;
    const UUID other = a_is_source ? client_b : client_a;
    const auto& source_items = a_is_source ? *from_a : *from_b;
    const auto& other_items = a_is_source ? *from_b : *from_a;

    StorageObjectMetadata file;
    file.data_size = 4096;
    file.transport_endpoint = "disk_endpoint";
    // The other mirror reports first: refused, and it ends the task.
    ASSERT_TRUE(
        service->NotifyOffloadSuccess(other, other_items, {file}).has_value());
    auto after_other =
        service->GetReplicaList("mirror_key", TenantId::Default());
    ASSERT_TRUE(after_other.has_value());
    EXPECT_EQ(after_other->replicas.size(), 2u);
    // The source owner's report now finds no task it took: refused as well.
    ASSERT_TRUE(service->NotifyOffloadSuccess(source, source_items, {file})
                    .has_value());
    auto after_source =
        service->GetReplicaList("mirror_key", TenantId::Default());
    ASSERT_TRUE(after_source.has_value());
    EXPECT_EQ(after_source->replicas.size(), 2u);

    // A second object: the source owner reports first and is registered.
    ASSERT_TRUE(service
                    ->PutStart(client_a, "mirror_key2", TenantId::Default(),
                               4096, config)
                    .has_value());
    ASSERT_TRUE(service
                    ->PutEnd(client_a, "mirror_key2", TenantId::Default(),
                             ReplicaType::MEMORY)
                    .has_value());
    auto from_a2 = service->OffloadObjectHeartbeat(client_a, true);
    auto from_b2 = service->OffloadObjectHeartbeat(client_b, true);
    ASSERT_TRUE(from_a2.has_value());
    ASSERT_TRUE(from_b2.has_value());
    auto replicas2 =
        service->GetReplicaList("mirror_key2", TenantId::Default());
    ASSERT_TRUE(replicas2.has_value());
    const bool a_is_source2 =
        replicas2->replicas[0]
            .get_memory_descriptor()
            .buffer_descriptor.transport_endpoint_ == "mirror_a";
    ASSERT_TRUE(service
                    ->NotifyOffloadSuccess(a_is_source2 ? client_a : client_b,
                                           a_is_source2 ? *from_a2 : *from_b2,
                                           {file})
                    .has_value());
    auto registered =
        service->GetReplicaList("mirror_key2", TenantId::Default());
    ASSERT_TRUE(registered.has_value());
    EXPECT_EQ(registered->replicas.size(), 3u);
}

// An offload completion whose size differs from the object is refused.
TEST_F(MasterServiceSSDTest, OffloadCompletionWithAnotherSizeIsRefused) {
    auto service = CreateSsdAwareOffloadService();
    const UUID client_id = generate_uuid();
    MountMemoryAndLocalDisk(*service, client_id, "size_segment", 0x330000000);
    ReplicateConfig config;
    config.replica_num = 1;
    ASSERT_TRUE(
        service
            ->PutStart(client_id, "size_key", TenantId::Default(), 4096, config)
            .has_value());
    ASSERT_TRUE(service
                    ->PutEnd(client_id, "size_key", TenantId::Default(),
                             ReplicaType::MEMORY)
                    .has_value());
    auto tasks = service->OffloadObjectHeartbeat(client_id, true);
    ASSERT_TRUE(tasks.has_value());
    ASSERT_EQ(tasks->size(), 1u);
    StorageObjectMetadata file;
    file.data_size = 2048;
    file.transport_endpoint = "disk_endpoint";
    ASSERT_TRUE(
        service->NotifyOffloadSuccess(client_id, *tasks, {file}).has_value());
    auto replicas = service->GetReplicaList("size_key", TenantId::Default());
    ASSERT_TRUE(replicas.has_value());
    ASSERT_EQ(replicas->replicas.size(), 1u);
    EXPECT_TRUE(replicas->replicas[0].is_memory_replica());
}

}  // namespace mooncake::test

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
