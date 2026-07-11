#include "master_service.h"

#include <algorithm>
#include <cerrno>
#include <charconv>
#include <chrono>
#include <cmath>
#include <cstdlib>
#include <cstdint>
#include <iostream>
#include <mutex>
#include <shared_mutex>
#include <string>
#include <string_view>
#include <system_error>
#include <thread>
#include <vector>

#include "gflags/gflags.h"
#include "glog/logging.h"
#include "types.h"

namespace mooncake::benchmarks {

class BatchEvictBench {
   public:
    static bool ConfigureFromEnv() {
        if (!ReadEvictionRatios(evict_ratio_target_, evict_ratio_lowerbound_)) {
            return false;
        }

        if (std::getenv("MOONCAKE_EVICT_BENCH_OBJECTS") != nullptr ||
            std::getenv("MOONCAKE_EVICT_BENCH_TARGET_RATIO") != nullptr ||
            std::getenv("MOONCAKE_EVICT_BENCH_LOWERBOUND_RATIO") != nullptr) {
            LOG(INFO) << "BatchEvict benchmark config: target_ratio="
                      << evict_ratio_target_
                      << ", lowerbound_ratio=" << evict_ratio_lowerbound_;
        }
        return true;
    }

    static bool RunRealBatchEvictScales() {
        std::vector<size_t> scales = {10000, 100000};
        const char* objects = std::getenv("MOONCAKE_EVICT_BENCH_OBJECTS");
        if (objects != nullptr) {
            size_t num_objects = 0;
            if (!ReadStrictEnvSize("MOONCAKE_EVICT_BENCH_OBJECTS",
                                   num_objects)) {
                return false;
            }
            if (num_objects == 0) {
                LOG(ERROR) << "MOONCAKE_EVICT_BENCH_OBJECTS must be greater "
                              "than zero";
                return false;
            }
            scales = {num_objects};
        }
        const char* large_mode = std::getenv("MOONCAKE_EVICT_BENCH_LARGE");
        if (objects == nullptr && large_mode != nullptr &&
            std::string(large_mode) == "1") {
            scales.push_back(1000000);
        }

        std::cout << "num_objects,total_us,objects_before,objects_after,"
                     "evicted_count,freed_bytes"
                  << std::endl;
        for (size_t scale : scales) {
            if (!RunOneScale(scale)) {
                return false;
            }
        }
        return true;
    }

    static bool RunSingleWaiterSnapshotMutexProbe() {
        if (std::getenv("MOONCAKE_EVICT_BENCH_LOCK_PROBE") == nullptr) {
            return true;
        }

        const size_t num_objects =
            ReadEnvSize("MOONCAKE_EVICT_BENCH_LOCK_OBJECTS", 1000000);
        const size_t trials = std::max<size_t>(
            1, ReadEnvSize("MOONCAKE_EVICT_BENCH_LOCK_TRIALS", 30));
        const auto waiter_delay = std::chrono::microseconds(
            static_cast<std::chrono::microseconds::rep>(
                ReadEnvSize("MOONCAKE_EVICT_BENCH_LOCK_DELAY_US", 1000)));

        std::vector<uint64_t> batch_evict_total_us;
        std::vector<uint64_t> unique_lock_wait_us;

        batch_evict_total_us.reserve(trials);
        unique_lock_wait_us.reserve(trials);

        for (size_t trial = 0; trial < trials; ++trial) {
            LockProbeTrialResult result;
            if (!RunLockProbeTrial(num_objects, waiter_delay, result)) {
                return false;
            }

            batch_evict_total_us.push_back(result.batch_evict_total_us);
            unique_lock_wait_us.push_back(result.unique_lock_wait_us);
        }

        std::cout << "num_objects,trials,batch_evict_total_p50_us,"
                     "unique_lock_wait_p50_us,unique_lock_wait_p95_us,"
                     "unique_lock_wait_max_us"
                  << std::endl;

        std::cout << num_objects << "," << trials << ","
                  << PercentileValue(batch_evict_total_us, 0.50) << ","
                  << PercentileValue(unique_lock_wait_us, 0.50) << ","
                  << PercentileValue(unique_lock_wait_us, 0.95) << ","
                  << PercentileValue(unique_lock_wait_us, 1.00) << std::endl;
        return true;
    }

   private:
    static constexpr const char* kTenantId = "default";
    static constexpr const char* kSegmentName = "batch_evict_bench_segment";
    static constexpr size_t kSegmentBase = 0x300000000;
    static constexpr uint64_t kObjectSize = 1024;
    static constexpr double kEvictRatioTarget = 0.50;
    static constexpr double kEvictRatioLowerbound = 0.25;

    inline static double evict_ratio_target_ = kEvictRatioTarget;
    inline static double evict_ratio_lowerbound_ = kEvictRatioLowerbound;

    struct MetadataStats {
        size_t object_count{0};
        size_t completed_memory_replicas{0};
        size_t busy_memory_replicas{0};
        size_t non_memory_replicas{0};
        size_t incomplete_replicas{0};
        size_t unexpired_leases{0};
    };

    struct LockProbeTrialResult {
        uint64_t batch_evict_total_us{0};
        uint64_t unique_lock_wait_us{0};
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

    static bool ReadStrictEnvSize(const char* name, size_t& result) {
        const char* value = std::getenv(name);
        if (value == nullptr) {
            LOG(ERROR) << "Environment variable " << name << " is not set";
            return false;
        }
        const std::string_view input(value);
        size_t parsed = 0;
        const auto [end, error] = 
            std::from_chars(input.begin(), input.end(), parsed);
        if (input.empty() || error != std::errc() || end != input.end()) {
            LOG(ERROR) << "Invalid " << name << " value '" << value
                       << "': expected a base-10 size_t";
            return false;
        }

        result = parsed;
        return true;
    }

    static bool ReadEnvDouble(const char* name, double default_value,
                              double& result) {
        const char* value = std::getenv(name);
        if (value == nullptr) {
            result = default_value;
            return true;
        }

        errno = 0;
        char* end = nullptr;
        const double parsed = std::strtod(value, &end);
        if (value[0] == '\0' || end == value || end[0] != '\0' ||
            errno == ERANGE || !std::isfinite(parsed)) {
            LOG(ERROR) << "Invalid " << name << " value '" << value
                       << "': expected a finite floating-point number";
            return false;
        }

        result = parsed;
        return true;
    }

    static bool ReadEvictionRatios(double& target_ratio,
                                   double& lowerbound_ratio) {
        if (!ReadEnvDouble("MOONCAKE_EVICT_BENCH_TARGET_RATIO",
                           kEvictRatioTarget, target_ratio) ||
            !ReadEnvDouble("MOONCAKE_EVICT_BENCH_LOWERBOUND_RATIO",
                           kEvictRatioLowerbound, lowerbound_ratio)) {
            return false;
        }
        if (!(lowerbound_ratio > 0.0 && lowerbound_ratio <= target_ratio &&
              target_ratio <= 1.0)) {
            LOG(ERROR) << "Invalid eviction ratios: require 0 < lowerbound <= "
                          "target <= 1, got target="
                       << target_ratio << ", lowerbound=" << lowerbound_ratio;
            return false;
        }
        return true;
    }

    static uint64_t PercentileValue(std::vector<uint64_t> values,
                                    double percentile) {
        if (values.empty()) {
            return 0;
        }
        std::sort(values.begin(), values.end());
        const size_t rank = std::max<size_t>(
            1, static_cast<size_t>(std::ceil(percentile * values.size())));
        return values[std::min(rank - 1, values.size() - 1)];
    }

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

    static bool MountBenchSegment(MasterService& service, const UUID& client_id,
                                  size_t num_objects) {
        auto segment = MakeSegment(num_objects);
        auto mount_result = service.MountSegment(segment, client_id);
        if (!mount_result.has_value()) {
            LOG(ERROR) << "MountSegment failed: "
                       << toString(mount_result.error());
            return false;
        }
        return true;
    }

    static bool CreateCompletedMemoryObjects(MasterService& service,
                                             const UUID& client_id,
                                             size_t num_objects) {
        ReplicateConfig config;
        config.replica_num = 1;
        config.preferred_segment = kSegmentName;

        for (size_t i = 0; i < num_objects; ++i) {
            const std::string key = MakeKey(i);
            auto put_start = service.PutStart(client_id, key, kTenantId,
                                              kObjectSize, config);
            if (!put_start.has_value()) {
                LOG(ERROR) << "PutStart failed for i=" << i
                           << ", error=" << toString(put_start.error());
                return false;
            }
            if (put_start->size() != 1u) {
                LOG(ERROR) << "PutStart returned " << put_start->size()
                           << " replicas for i=" << i;
                return false;
            }

            auto put_end =
                service.PutEnd(client_id, key, kTenantId, ReplicaType::MEMORY);
            if (!put_end.has_value()) {
                LOG(ERROR) << "PutEnd failed for i=" << i
                           << ", error=" << toString(put_end.error());
                return false;
            }
        }
        return true;
    }

    static bool UsedBytes(MasterService& service, uint64_t& used_bytes) {
        auto segment_usage = service.QuerySegments(kSegmentName);
        if (!segment_usage.has_value()) {
            LOG(ERROR) << "QuerySegments failed: "
                       << toString(segment_usage.error());
            return false;
        }
        used_bytes = segment_usage->first;
        return true;
    }

    static uint64_t WaitForSnapshotUniqueLock(
        MasterService& service, std::chrono::microseconds waiter_delay) {
        std::this_thread::sleep_for(waiter_delay);
        const auto wait_start = std::chrono::steady_clock::now();
        uint64_t wait_us = 0;
        {
            std::unique_lock<std::shared_mutex> lock(service.snapshot_mutex_);
            const auto wait_end = std::chrono::steady_clock::now();
            wait_us = std::chrono::duration_cast<std::chrono::microseconds>(
                          wait_end - wait_start)
                          .count();
        }
        return wait_us;
    }

    static bool ValidatePopulatedObjects(const MasterService& service,
                                         size_t num_objects,
                                         const MetadataStats& stats) {
        if (service.GetKeyCount() != num_objects) {
            LOG(ERROR) << "GetKeyCount mismatch: expected=" << num_objects
                       << ", actual=" << service.GetKeyCount();
            return false;
        }
        if (stats.object_count != num_objects ||
            stats.completed_memory_replicas != num_objects ||
            stats.busy_memory_replicas != 0 || stats.non_memory_replicas != 0 ||
            stats.incomplete_replicas != 0 || stats.unexpired_leases != 0) {
            LOG(ERROR) << "metadata validation failed: objects="
                       << stats.object_count << ", completed_memory_replicas="
                       << stats.completed_memory_replicas
                       << ", busy_memory_replicas="
                       << stats.busy_memory_replicas
                       << ", non_memory_replicas=" << stats.non_memory_replicas
                       << ", incomplete_replicas=" << stats.incomplete_replicas
                       << ", unexpired_leases=" << stats.unexpired_leases;
            return false;
        }
        return true;
    }

    static bool ValidateEvictionResult(size_t objects_before,
                                       size_t evicted_count,
                                       uint64_t freed_bytes) {
        const size_t lowerbound = static_cast<size_t>(
            std::ceil(objects_before * evict_ratio_lowerbound_));
        if (evicted_count < lowerbound) {
            LOG(ERROR) << "evicted_count below lowerbound: evicted="
                       << evicted_count << ", lowerbound=" << lowerbound;
            return false;
        }
        if (evicted_count * kObjectSize != freed_bytes) {
            LOG(ERROR) << "freed_bytes mismatch: evicted_count="
                       << evicted_count << ", object_size=" << kObjectSize
                       << ", freed_bytes=" << freed_bytes;
            return false;
        }
        return true;
    }

    static bool RunOneScale(size_t num_objects) {
        MasterService service(MakeConfig());
        const UUID client_id = generate_uuid();

        if (!MountBenchSegment(service, client_id, num_objects) ||
            !CreateCompletedMemoryObjects(service, client_id, num_objects)) {
            return false;
        }

        const MetadataStats stats = ExpireLeasesAndCollectStats(service);
        if (!ValidatePopulatedObjects(service, num_objects, stats)) {
            return false;
        }

        const size_t objects_before = service.GetKeyCount();
        uint64_t used_before = 0;
        if (!UsedBytes(service, used_before)) {
            return false;
        }

        const auto evict_start = std::chrono::steady_clock::now();
        service.BatchEvict(evict_ratio_target_, evict_ratio_lowerbound_);
        const auto total_us =
            std::chrono::duration_cast<std::chrono::microseconds>(
                std::chrono::steady_clock::now() - evict_start)
                .count();

        const size_t objects_after = service.GetKeyCount();
        uint64_t used_after = 0;
        if (!UsedBytes(service, used_after)) {
            return false;
        }
        const size_t evicted_count = objects_before - objects_after;
        const uint64_t freed_bytes =
            used_before >= used_after ? used_before - used_after : 0;

        if (!ValidateEvictionResult(objects_before, evicted_count,
                                    freed_bytes)) {
            return false;
        }

        std::cout << num_objects << "," << total_us << "," << objects_before
                  << "," << objects_after << "," << evicted_count << ","
                  << freed_bytes << std::endl;
        return true;
    }

    static bool RunLockProbeTrial(size_t num_objects,
                                  std::chrono::microseconds waiter_delay,
                                  LockProbeTrialResult& result) {
        MasterService service(MakeConfig());
        const UUID client_id = generate_uuid();

        if (!MountBenchSegment(service, client_id, num_objects) ||
            !CreateCompletedMemoryObjects(service, client_id, num_objects)) {
            return false;
        }

        const MetadataStats stats = ExpireLeasesAndCollectStats(service);
        if (!ValidatePopulatedObjects(service, num_objects, stats)) {
            return false;
        }

        const size_t objects_before = service.GetKeyCount();
        uint64_t used_before = 0;
        if (!UsedBytes(service, used_before)) {
            return false;
        }

        uint64_t unique_lock_wait_us = 0;
        std::thread waiter([&]() {
            unique_lock_wait_us =
                WaitForSnapshotUniqueLock(service, waiter_delay);
        });

        const auto evict_start = std::chrono::steady_clock::now();
        service.BatchEvict(evict_ratio_target_, evict_ratio_lowerbound_);
        const auto batch_evict_total_us =
            std::chrono::duration_cast<std::chrono::microseconds>(
                std::chrono::steady_clock::now() - evict_start)
                .count();
        waiter.join();

        const size_t objects_after = service.GetKeyCount();
        uint64_t used_after = 0;
        if (!UsedBytes(service, used_after)) {
            return false;
        }
        const size_t evicted_count = objects_before - objects_after;
        const uint64_t freed_bytes =
            used_before >= used_after ? used_before - used_after : 0;

        if (!ValidateEvictionResult(objects_before, evicted_count,
                                    freed_bytes)) {
            return false;
        }

        result.batch_evict_total_us =
            static_cast<uint64_t>(batch_evict_total_us);
        result.unique_lock_wait_us = unique_lock_wait_us;
        return true;
    }
};

}  // namespace mooncake::benchmarks

int main(int argc, char** argv) {
    google::InitGoogleLogging("BatchEvictBench");
    FLAGS_logtostderr = true;
    gflags::ParseCommandLineFlags(&argc, &argv, true);

    using mooncake::benchmarks::BatchEvictBench;
    const bool ok = BatchEvictBench::ConfigureFromEnv() &&
                    BatchEvictBench::RunRealBatchEvictScales() &&
                    BatchEvictBench::RunSingleWaiterSnapshotMutexProbe();

    google::ShutdownGoogleLogging();
    return ok ? 0 : 1;
}
