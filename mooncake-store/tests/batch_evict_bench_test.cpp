// Benchmark-only test for MasterService::BatchEvict candidate-selection cost.
//
// This does NOT change production behavior. It drives the real BatchEvict path
// at controlled metadata scale and reports:
//   1. RealBatchEvictScales: end-to-end BatchEvict wall-clock time vs object
//      count (always on; small scales by default, 1M with an env var).
//   2. SingleWaiterSnapshotMutexProbe: how long a single unique_lock waiter on
//      snapshot_mutex_ is blocked while a BatchEvict cycle is in progress
//      (opt-in via env var, since it is slow at 1M scale).
//
// See issue #2560 for context and measurements.

#include "master_service.h"

#include <glog/logging.h>
#include <gtest/gtest.h>

#include <algorithm>
#include <chrono>
#include <cmath>
#include <cstdlib>
#include <iostream>
#include <mutex>
#include <shared_mutex>
#include <string>
#include <thread>
#include <vector>

#include "types.h"

namespace mooncake::test {

class BatchEvictBenchTest : public ::testing::Test {
   protected:
    void SetUp() override {
        static bool glog_initialized = false;
        if (!glog_initialized) {
            google::InitGoogleLogging("BatchEvictBenchTest");
            glog_initialized = true;
        }
        FLAGS_logtostderr = true;
    }

    static constexpr const char* kTenantId = "default";
    static constexpr const char* kSegmentName = "batch_evict_bench_segment";
    static constexpr size_t kSegmentBase = 0x300000000;
    static constexpr uint64_t kObjectSize = 1024;
    static constexpr double kEvictRatioTarget = 0.50;
    static constexpr double kEvictRatioLowerbound = 0.25;

    struct MetadataStats {
        size_t object_count{0};
        size_t completed_memory_replicas{0};
        size_t busy_memory_replicas{0};
        size_t non_memory_replicas{0};
        size_t incomplete_replicas{0};
        size_t unexpired_leases{0};
    };

    static MasterServiceConfig MakeConfig() {
        return MasterServiceConfig::builder()
            .set_memory_allocator(BufferAllocatorType::OFFSET)
            .set_eviction_ratio(0.0)
            .set_eviction_high_watermark_ratio(1.0)
            .set_client_live_ttl_sec(3600)
            .build();
    }

    static size_t SegmentSizeFor(size_t num_objects) {
        constexpr size_t kMinSegmentSize = 16 * 1024 * 1024;
        const size_t needed = num_objects * kObjectSize;
        const size_t headroom = needed / 8 + 1024 * kObjectSize;
        return std::max(kMinSegmentSize, needed + headroom);
    }

    static Segment MakeSegment(size_t num_objects) {
        Segment segment;
        segment.id = generate_uuid();
        segment.name = kSegmentName;
        segment.base = kSegmentBase;
        segment.size = SegmentSizeFor(num_objects);
        segment.te_endpoint = segment.name;
        return segment;
    }

    static std::string MakeKey(size_t index) {
        return "batch_evict_bench_key_" + std::to_string(index);
    }

    static size_t ReadEnvSize(const char* name, size_t default_value) {
        const char* value = std::getenv(name);
        if (value == nullptr || value[0] == '\0') {
            return default_value;
        }
        char* end = nullptr;
        const unsigned long long parsed = std::strtoull(value, &end, 10);
        if (end == value) {
            return default_value;
        }
        return static_cast<size_t>(parsed);
    }

    static uint64_t PercentileValue(std::vector<uint64_t>& values,
                                    double percentile) {
        if (values.empty()) {
            return 0;
        }
        std::sort(values.begin(), values.end());
        const size_t rank = std::max<size_t>(
            1, static_cast<size_t>(std::ceil(percentile * values.size())));
        return values[std::min(rank - 1, values.size() - 1)];
    }

    // Force every object's lease to be expired so BatchEvict treats them all as
    // evictable, and sanity-check the synthetic state.
    static MetadataStats ExpireLeasesAndCollectStats(MasterService& service) {
        MetadataStats stats;
        auto now = std::chrono::system_clock::now();
        const auto base_expiration = now - std::chrono::hours(1);
        size_t ordinal = 0;

        for (size_t shard_idx = 0; shard_idx < MasterService::kNumShards;
             ++shard_idx) {
            MasterService::MetadataShardAccessorRW shard(&service, shard_idx);
            for (auto& [tenant_id, tenant_state] : shard->tenants) {
                if (tenant_id != kTenantId) {
                    continue;
                }
                for (auto& [key, metadata] : tenant_state.metadata) {
                    {
                        SpinLocker locker(&metadata.lock);
                        metadata.lease_timeout =
                            base_expiration +
                            std::chrono::nanoseconds(ordinal++);
                    }

                    ++stats.object_count;
                    if (!metadata.IsLeaseExpired(now)) {
                        ++stats.unexpired_leases;
                    }
                    for (const auto& replica : metadata.GetAllReplicas()) {
                        if (replica.is_memory_replica()) {
                            if (replica.is_completed()) {
                                ++stats.completed_memory_replicas;
                            } else {
                                ++stats.incomplete_replicas;
                            }
                            if (replica.get_refcnt() != 0) {
                                ++stats.busy_memory_replicas;
                            }
                        } else {
                            ++stats.non_memory_replicas;
                        }
                    }
                }
            }
        }

        return stats;
    }

    static void MountBenchSegment(MasterService& service, const UUID& client_id,
                                  size_t num_objects) {
        auto segment = MakeSegment(num_objects);
        auto mount_result = service.MountSegment(segment, client_id);
        ASSERT_TRUE(mount_result.has_value())
            << "MountSegment failed: " << toString(mount_result.error());
    }

    static void CreateCompletedMemoryObjects(MasterService& service,
                                             const UUID& client_id,
                                             size_t num_objects) {
        ReplicateConfig config;
        config.replica_num = 1;
        config.preferred_segment = kSegmentName;

        for (size_t i = 0; i < num_objects; ++i) {
            const std::string key = MakeKey(i);
            auto put_start = service.PutStart(client_id, key, kTenantId,
                                              kObjectSize, config);
            ASSERT_TRUE(put_start.has_value())
                << "PutStart failed for i=" << i
                << ", error=" << toString(put_start.error());
            ASSERT_EQ(put_start->size(), 1u);

            auto put_end =
                service.PutEnd(client_id, key, kTenantId, ReplicaType::MEMORY);
            ASSERT_TRUE(put_end.has_value())
                << "PutEnd failed for i=" << i
                << ", error=" << toString(put_end.error());
        }
    }

    static uint64_t UsedBytes(MasterService& service) {
        auto segment_usage = service.QuerySegments(kSegmentName);
        EXPECT_TRUE(segment_usage.has_value())
            << "QuerySegments failed: " << toString(segment_usage.error());
        if (!segment_usage.has_value()) {
            return 0;
        }
        return segment_usage->first;
    }

    // Build a populated service with all objects expired and evictable.
    static void SetUpEvictableService(MasterService& service,
                                      size_t num_objects) {
        const UUID client_id = generate_uuid();
        ASSERT_NO_FATAL_FAILURE(
            MountBenchSegment(service, client_id, num_objects));
        ASSERT_NO_FATAL_FAILURE(
            CreateCompletedMemoryObjects(service, client_id, num_objects));
        ASSERT_EQ(num_objects, service.GetKeyCount());

        const MetadataStats stats = ExpireLeasesAndCollectStats(service);
        ASSERT_EQ(num_objects, stats.object_count);
        ASSERT_EQ(num_objects, stats.completed_memory_replicas);
        ASSERT_EQ(0u, stats.busy_memory_replicas);
        ASSERT_EQ(0u, stats.non_memory_replicas);
        ASSERT_EQ(0u, stats.incomplete_replicas);
        ASSERT_EQ(0u, stats.unexpired_leases);
    }

    // Measure end-to-end BatchEvict wall-clock time at one scale.
    static void RunOneScale(size_t num_objects) {
        MasterService service(MakeConfig());
        ASSERT_NO_FATAL_FAILURE(SetUpEvictableService(service, num_objects));

        const size_t objects_before = service.GetKeyCount();
        const uint64_t used_before = UsedBytes(service);

        const auto start = std::chrono::steady_clock::now();
        service.BatchEvict(kEvictRatioTarget, kEvictRatioLowerbound);
        const auto end = std::chrono::steady_clock::now();
        const uint64_t total_us =
            std::chrono::duration_cast<std::chrono::microseconds>(end - start)
                .count();

        const size_t objects_after = service.GetKeyCount();
        const uint64_t used_after = UsedBytes(service);
        const size_t evicted_count = objects_before - objects_after;
        const uint64_t freed_bytes =
            used_before >= used_after ? used_before - used_after : 0;

        const size_t lowerbound = static_cast<size_t>(std::ceil(
            static_cast<double>(objects_before) * kEvictRatioLowerbound));
        EXPECT_GE(evicted_count, lowerbound);
        EXPECT_EQ(evicted_count * kObjectSize, freed_bytes);

        std::cout << num_objects << "," << total_us << "," << objects_before
                  << "," << objects_after << "," << evicted_count << ","
                  << freed_bytes << std::endl;
    }

    // One trial: a single unique_lock waiter on snapshot_mutex_ arrives shortly
    // after a BatchEvict cycle starts; measure how long it waits to acquire.
    static void RunLockProbeTrial(size_t num_objects,
                                  std::chrono::microseconds waiter_delay,
                                  uint64_t& total_us_out,
                                  uint64_t& wait_us_out) {
        MasterService service(MakeConfig());
        ASSERT_NO_FATAL_FAILURE(SetUpEvictableService(service, num_objects));

        const size_t objects_before = service.GetKeyCount();

        uint64_t wait_us = 0;
        std::thread waiter([&]() {
            std::this_thread::sleep_for(waiter_delay);
            const auto wait_start = std::chrono::steady_clock::now();
            {
                std::unique_lock<std::shared_mutex> lock(
                    service.snapshot_mutex_);
                const auto wait_end = std::chrono::steady_clock::now();
                wait_us = std::chrono::duration_cast<std::chrono::microseconds>(
                              wait_end - wait_start)
                              .count();
            }
        });

        const auto start = std::chrono::steady_clock::now();
        service.BatchEvict(kEvictRatioTarget, kEvictRatioLowerbound);
        const auto end = std::chrono::steady_clock::now();
        waiter.join();

        const size_t objects_after = service.GetKeyCount();
        const size_t evicted_count = objects_before - objects_after;
        const size_t lowerbound = static_cast<size_t>(std::ceil(
            static_cast<double>(objects_before) * kEvictRatioLowerbound));
        EXPECT_GE(evicted_count, lowerbound);

        total_us_out =
            std::chrono::duration_cast<std::chrono::microseconds>(end - start)
                .count();
        wait_us_out = wait_us;
    }
};

// Always-on: end-to-end BatchEvict time vs object count.
// Default scales are small so this stays cheap in CI. Set
// MOONCAKE_EVICT_BENCH_LARGE=1 to also run the 1M case.
TEST_F(BatchEvictBenchTest, RealBatchEvictScales) {
    std::vector<size_t> scales = {10000, 100000};
    const char* large_mode = std::getenv("MOONCAKE_EVICT_BENCH_LARGE");
    if (large_mode != nullptr && std::string(large_mode) == "1") {
        scales.push_back(1000000);
    }

    std::cout << "num_objects,total_us,objects_before,objects_after,"
                 "evicted_count,freed_bytes"
              << std::endl;
    for (size_t scale : scales) {
        ASSERT_NO_FATAL_FAILURE(RunOneScale(scale));
    }
}

// Opt-in: measures unique_lock(snapshot_mutex_) wait time while BatchEvict
// runs. Slow at 1M (rebuilds the service per trial), so gated behind an env
// var. Configure via:
//   MOONCAKE_EVICT_BENCH_LOCK_PROBE=1     (required to run)
//   MOONCAKE_EVICT_BENCH_LOCK_OBJECTS=N   (default 1000000)
//   MOONCAKE_EVICT_BENCH_LOCK_TRIALS=N    (default 30)
//   MOONCAKE_EVICT_BENCH_LOCK_DELAY_US=N  (default 1000)
TEST_F(BatchEvictBenchTest, SingleWaiterSnapshotMutexProbe) {
    if (std::getenv("MOONCAKE_EVICT_BENCH_LOCK_PROBE") == nullptr) {
        GTEST_SKIP() << "Set MOONCAKE_EVICT_BENCH_LOCK_PROBE=1 to run";
    }

    const size_t num_objects =
        ReadEnvSize("MOONCAKE_EVICT_BENCH_LOCK_OBJECTS", 1000000);
    const size_t trials = std::max<size_t>(
        1, ReadEnvSize("MOONCAKE_EVICT_BENCH_LOCK_TRIALS", 30));
    const auto waiter_delay =
        std::chrono::microseconds(static_cast<std::chrono::microseconds::rep>(
            ReadEnvSize("MOONCAKE_EVICT_BENCH_LOCK_DELAY_US", 1000)));

    std::vector<uint64_t> total_us;
    std::vector<uint64_t> wait_us;
    total_us.reserve(trials);
    wait_us.reserve(trials);

    for (size_t trial = 0; trial < trials; ++trial) {
        uint64_t trial_total = 0;
        uint64_t trial_wait = 0;
        ASSERT_NO_FATAL_FAILURE(RunLockProbeTrial(num_objects, waiter_delay,
                                                  trial_total, trial_wait));
        total_us.push_back(trial_total);
        wait_us.push_back(trial_wait);
    }

    std::cout << "num_objects,trials,batch_evict_total_p50_us,"
                 "unique_lock_wait_p50_us,unique_lock_wait_p95_us,"
                 "unique_lock_wait_max_us"
              << std::endl;
    std::cout << num_objects << "," << trials << ","
              << PercentileValue(total_us, 0.50) << ","
              << PercentileValue(wait_us, 0.50) << ","
              << PercentileValue(wait_us, 0.95) << ","
              << PercentileValue(wait_us, 1.00) << std::endl;
}

}  // namespace mooncake::test
