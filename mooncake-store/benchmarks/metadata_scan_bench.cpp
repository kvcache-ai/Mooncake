#include "master_service.h"

#include <gflags/gflags.h>
#include <glog/logging.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cmath>
#include <cstdint>
#include <cstdlib>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <mutex>
#include <queue>
#include <thread>
#include <unordered_map>
#include <vector>

DEFINE_uint64(num_objects, 0,
              "Run a single custom scale with this object count (0 = default "
              "scales)");
DEFINE_uint64(scan_repeat, 10, "Measurement repetitions per scan pattern");
DEFINE_uint64(contention_puts, 2000,
              "Client puts issued while the background scan loop runs");
DEFINE_bool(run_contention, true,
            "Measure client put latency with and without a concurrent scan");

namespace mooncake::benchmarks {

// Faithful local replica of MasterService::SoftPinDeadlineIndex (min-heap +
// registration map + mutex, lazy deletion with periodic compaction), used as
// the deadline-index alternative to the full-metadata scans.
class LocalDeadlineIndex {
   public:
    using TimePoint = std::chrono::system_clock::time_point;

    struct Entry {
        TimePoint deadline;
        std::string scoped_key;
    };

    void Upsert(std::string scoped_key, const TimePoint& deadline) {
        std::lock_guard<std::mutex> lock(mutex_);
        heap_.push({deadline, scoped_key});
        registrations_[scoped_key].deadline = deadline;
    }

    std::vector<Entry> PopExpired(const TimePoint& now) {
        std::vector<Entry> expired;
        std::lock_guard<std::mutex> lock(mutex_);
        while (!heap_.empty() && heap_.top().deadline <= now) {
            const auto& top = heap_.top();
            const auto it = registrations_.find(top.scoped_key);
            if (it != registrations_.end() &&
                it->second.deadline == top.deadline) {
                expired.push_back(top);
                registrations_.erase(it);
            }
            heap_.pop();
        }
        if (registrations_.size() * kCompactionRatio < heap_.size() &&
            registrations_.size() > kMinCompactionThreshold) {
            std::priority_queue<Entry, std::vector<Entry>, EarlierDeadline>
                compacted;
            for (const auto& [scoped_key, registration] : registrations_) {
                compacted.push({registration.deadline, scoped_key});
            }
            heap_ = std::move(compacted);
        }
        return expired;
    }

   private:
    struct Registration {
        TimePoint deadline;
    };

    struct EarlierDeadline {
        bool operator()(const Entry& lhs, const Entry& rhs) const {
            return lhs.deadline > rhs.deadline;
        }
    };

    static constexpr size_t kMinCompactionThreshold = 4096;
    static constexpr size_t kCompactionRatio = 2;

    std::mutex mutex_;
    std::priority_queue<Entry, std::vector<Entry>, EarlierDeadline> heap_;
    std::unordered_map<std::string, Registration> registrations_;
};

class MetadataScanBench {
   public:
    static bool RunScales() {
        std::vector<size_t> scales = {10000, 100000};
        const char* large_mode = std::getenv("MOONCAKE_SCAN_BENCH_LARGE");
        if (large_mode != nullptr && std::string(large_mode) == "1") {
            scales.push_back(1000000);
        }
        if (FLAGS_num_objects > 0) {
            scales = {static_cast<size_t>(FLAGS_num_objects)};
        }

        std::cout << "num_objects,rss_after_create_mb,object_entry_bytes,"
                     "snapshot_copy_us,entry_lock_scan_us,"
                     "index_pop_expired_none_us,index_pop_expired_all_us,"
                     "put_p50_us_baseline,put_p99_us_baseline,"
                     "put_p50_us_under_scan,put_p99_us_under_scan"
                  << std::endl;
        for (size_t scale : scales) {
            if (!RunOneScale(scale)) {
                return false;
            }
        }
        return true;
    }

   private:
    static constexpr const char* kSegmentName = "metadata_scan_bench_segment";
    static constexpr size_t kSegmentBase = 0x500000000;
    static constexpr uint64_t kObjectSize = 1024;

    static MasterServiceConfig MakeConfig() {
        return MasterServiceConfig::builder()
            .set_memory_allocator(BufferAllocatorType::OFFSET)
            .set_eviction_ratio(0.0)
            .set_eviction_high_watermark_ratio(1.0)
            .set_client_live_ttl_sec(3600)
            .build();
    }

    static Segment MakeSegment(size_t num_objects) {
        Segment segment;
        segment.id = generate_uuid();
        segment.name = kSegmentName;
        segment.base = kSegmentBase;
        segment.size = std::max<size_t>(
            16 * 1024 * 1024, num_objects * kObjectSize + num_objects * 128);
        segment.te_endpoint = segment.name;
        return segment;
    }

    static size_t CurrentRssMb() {
        std::ifstream statm("/proc/self/statm");
        long total_pages = 0;
        long resident_pages = 0;
        statm >> total_pages >> resident_pages;
        return static_cast<size_t>(resident_pages * getpagesize() >> 20);
    }

    static uint64_t PercentileUs(std::vector<uint64_t>& samples,
                                 double percentile) {
        if (samples.empty()) {
            return 0;
        }
        std::sort(samples.begin(), samples.end());
        const size_t rank = std::max<size_t>(
            1, static_cast<size_t>(std::ceil(percentile * samples.size())));
        return samples[std::min(rank - 1, samples.size() - 1)];
    }

    static bool CreateObjects(MasterService& service, const UUID& client_id,
                              size_t num_objects, const std::string& prefix) {
        ReplicateConfig config;
        config.replica_num = 1;
        config.preferred_segment = kSegmentName;
        for (size_t i = 0; i < num_objects; ++i) {
            const std::string key = prefix + std::to_string(i);
            if (!service
                     .PutStart(client_id, key, TenantId::Default(), kObjectSize,
                               config)
                     .has_value() ||
                !service
                     .PutEnd(client_id, key, TenantId::Default(),
                             ReplicaType::MEMORY)
                     .has_value()) {
                LOG(ERROR) << "object creation failed at i=" << i;
                return false;
            }
        }
        return true;
    }

    // The promotion-retry and dynamic-replication expiry scans both walk
    // every entry and take its exclusive lock to inspect one flag.
    static double MeasureEntryLockScan(
        const std::vector<std::shared_ptr<mooncake::metadata::ObjectEntry>>&
            entries,
        uint64_t repeat) {
        const auto begin = std::chrono::steady_clock::now();
        uint64_t sink = 0;
        for (uint64_t r = 0; r < repeat; ++r) {
            for (const auto& entry : entries) {
                auto lk = entry->LockUnique();
                sink += entry->is_processing ? 1 : 0;
            }
        }
        const auto end = std::chrono::steady_clock::now();
        if (sink == entries.size() + 1) {
            LOG(INFO) << "unreachable";
        }
        return std::chrono::duration_cast<std::chrono::microseconds>(end -
                                                                     begin)
                   .count() /
               static_cast<double>(repeat);
    }

    // What the promotion-retry tick costs after the sparse candidate index:
    // copy the key set (size = active candidates), resolve and exclusively
    // lock each candidate. Mirrors RunPromotionCandidateRetry's enumeration.
    static double MeasureCandidateEnumeration(
        mooncake::metadata::TenantCatalog& tenant_state, size_t candidates,
        uint64_t repeat) {
        for (size_t i = 0; i < candidates; ++i) {
            tenant_state.IndexPromotionCandidate("scan_bench_" +
                                                 std::to_string(i));
        }
        uint64_t sink = 0;
        const auto begin = std::chrono::steady_clock::now();
        for (uint64_t r = 0; r < repeat; ++r) {
            for (const auto& key : tenant_state.PromotionCandidateKeys()) {
                auto entry = tenant_state.Get(key);
                if (!entry) {
                    continue;
                }
                auto lk = entry->LockUnique();
                sink += entry->promotion_candidate.has_value() ? 1 : 0;
            }
        }
        const auto end = std::chrono::steady_clock::now();
        LOG(INFO) << "candidate enumerate sink=" << sink;
        return std::chrono::duration_cast<
                   std::chrono::duration<double, std::micro>>(end - begin)
                   .count() /
               repeat;
    }

    static double MeasureSnapshotCopy(
        const mooncake::metadata::TenantCatalog& tenant_state,
        uint64_t repeat) {
        const auto begin = std::chrono::steady_clock::now();
        uint64_t sink = 0;
        for (uint64_t r = 0; r < repeat; ++r) {
            sink += tenant_state.SnapshotObjects().size();
        }
        const auto end = std::chrono::steady_clock::now();
        if (sink == 0) {
            LOG(INFO) << "unreachable";
        }
        return std::chrono::duration_cast<std::chrono::microseconds>(end -
                                                                     begin)
                   .count() /
               static_cast<double>(repeat);
    }

    static bool RunOneScale(size_t num_objects) {
        auto config = MakeConfig();
        auto service = std::make_unique<MasterService>(config);
        const UUID client_id = generate_uuid();
        auto mount_result =
            service->MountSegment(MakeSegment(num_objects), client_id);
        if (!mount_result.has_value()) {
            LOG(ERROR) << "MountSegment failed";
            return false;
        }
        if (!CreateObjects(*service, client_id, num_objects, "scan_bench_")) {
            return false;
        }
        const size_t rss_mb = CurrentRssMb();

        auto tenant_handle = service->catalog_.Lookup(TenantId::Default());
        if (!tenant_handle) {
            LOG(ERROR) << "default tenant missing";
            return false;
        }
        auto entries = tenant_handle->SnapshotObjects();

        const double snapshot_copy_us =
            MeasureSnapshotCopy(*tenant_handle, FLAGS_scan_repeat);
        const double entry_lock_scan_us =
            MeasureEntryLockScan(entries, FLAGS_scan_repeat);
        const double candidate_enum_us = MeasureCandidateEnumeration(
            *tenant_handle, /*candidates=*/8, FLAGS_scan_repeat);

        // Deadline-index alternative: amortized upsert cost and PopExpired
        // cost in the two steady states (nothing due / everything due).
        LocalDeadlineIndex index;
        const auto base_deadline = std::chrono::system_clock::now();
        const auto index_begin = std::chrono::steady_clock::now();
        for (size_t i = 0; i < entries.size(); ++i) {
            index.Upsert("scan_bench_" + std::to_string(i),
                         base_deadline + std::chrono::hours(1));
        }
        const double index_upsert_us =
            std::chrono::duration_cast<std::chrono::microseconds>(
                std::chrono::steady_clock::now() - index_begin)
                .count();
        const auto pop_none_begin = std::chrono::steady_clock::now();
        const size_t none_popped = index.PopExpired(base_deadline).size();
        const auto pop_all_begin = std::chrono::steady_clock::now();
        const size_t all_popped =
            index.PopExpired(base_deadline + std::chrono::hours(2)).size();
        const auto pop_end = std::chrono::steady_clock::now();
        const double pop_none_us =
            std::chrono::duration_cast<
                std::chrono::duration<double, std::micro>>(pop_all_begin -
                                                           pop_none_begin)
                .count();
        const double pop_all_us =
            std::chrono::duration_cast<
                std::chrono::duration<double, std::micro>>(pop_end -
                                                           pop_all_begin)
                .count();
        RunGroupIndexContention();

        LOG(INFO) << "index: upsert_total_us=" << index_upsert_us
                  << ", pop_none=" << none_popped << " in " << pop_none_us
                  << "us, pop_all=" << all_popped << " in " << pop_all_us
                  << "us, candidate_enum_8=" << candidate_enum_us << "us";

        double put_p50_baseline = 0;
        double put_p99_baseline = 0;
        double put_p50_scan = 0;
        double put_p99_scan = 0;
        if (FLAGS_run_contention) {
            std::atomic<bool> scanning{true};
            std::thread scan_thread([&]() {
                while (scanning.load(std::memory_order_relaxed)) {
                    auto snapshot = tenant_handle->SnapshotObjects();
                    for (const auto& entry : snapshot) {
                        auto lk = entry->LockUnique();
                        sink_work_.fetch_add(entry->is_processing ? 1 : 0,
                                             std::memory_order_relaxed);
                    }
                }
            });

            std::vector<uint64_t> latencies;
            latencies.reserve(FLAGS_contention_puts);
            ReplicateConfig put_config;
            put_config.replica_num = 1;
            put_config.preferred_segment = kSegmentName;
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
            const auto put_begin = std::chrono::steady_clock::now();
            for (size_t i = 0; i < FLAGS_contention_puts; ++i) {
                const std::string key =
                    "scan_bench_" + std::to_string(i % num_objects);
                const auto op_begin = std::chrono::steady_clock::now();
                service->PutStart(client_id, key, TenantId::Default(),
                                  kObjectSize, put_config);
                service->PutEnd(client_id, key, TenantId::Default(),
                                ReplicaType::MEMORY);
                latencies.push_back(
                    std::chrono::duration_cast<std::chrono::microseconds>(
                        std::chrono::steady_clock::now() - op_begin)
                        .count());
            }
            const double put_seconds =
                std::chrono::duration_cast<std::chrono::duration<double>>(
                    std::chrono::steady_clock::now() - put_begin)
                    .count();
            put_p50_scan = PercentileUs(latencies, 0.50);
            put_p99_scan = PercentileUs(latencies, 0.99);
            scanning.store(false, std::memory_order_relaxed);
            scan_thread.join();
            LOG(INFO) << "under scan: put throughput=" << std::fixed
                      << std::setprecision(0)
                      << FLAGS_contention_puts / put_seconds << "/s";

            latencies.clear();
            const auto base_begin = std::chrono::steady_clock::now();
            for (size_t i = 0; i < FLAGS_contention_puts; ++i) {
                const std::string key =
                    "scan_bench_" + std::to_string(i % num_objects);
                const auto op_begin = std::chrono::steady_clock::now();
                service->PutStart(client_id, key, TenantId::Default(),
                                  kObjectSize, put_config);
                service->PutEnd(client_id, key, TenantId::Default(),
                                ReplicaType::MEMORY);
                latencies.push_back(
                    std::chrono::duration_cast<std::chrono::microseconds>(
                        std::chrono::steady_clock::now() - op_begin)
                        .count());
            }
            const double base_seconds =
                std::chrono::duration_cast<std::chrono::duration<double>>(
                    std::chrono::steady_clock::now() - base_begin)
                    .count();
            put_p50_baseline = PercentileUs(latencies, 0.50);
            put_p99_baseline = PercentileUs(latencies, 0.99);
            LOG(INFO) << "baseline: put throughput=" << std::fixed
                      << std::setprecision(0)
                      << FLAGS_contention_puts / base_seconds << "/s";
        }

        std::cout << num_objects << "," << rss_mb << ","
                  << sizeof(mooncake::metadata::ObjectEntry) << ","
                  << std::fixed << std::setprecision(1) << snapshot_copy_us
                  << "," << entry_lock_scan_us << "," << pop_none_us << ","
                  << pop_all_us << "," << put_p50_baseline << ","
                  << put_p99_baseline << "," << put_p50_scan << ","
                  << put_p99_scan << std::endl;
        return true;
    }

    static inline std::atomic<uint64_t> sink_work_{0};

    // GroupIndex contention: N threads performing AddMember on distinct
    // groups (the per-put publication path), on a standalone instance — the
    // contention properties belong to GroupIndex itself. Reports aggregate
    // ops/s per thread count.
    static void RunGroupIndexContention() {
        mooncake::metadata::GroupIndex group_index;
        std::cout << "group_index_threads,member_add_ops_per_s" << std::endl;
        for (int threads : {1, 4, 16, 32}) {
            std::atomic<uint64_t> total_ops{0};
            std::atomic<bool> stop{false};
            std::vector<std::thread> workers;
            for (int t = 0; t < threads; ++t) {
                workers.emplace_back([&, t]() {
                    uint64_t ops = 0;
                    const std::string prefix =
                        "group_t" + std::to_string(t) + "_";
                    while (!stop.load(std::memory_order_relaxed)) {
                        const std::string group =
                            prefix + std::to_string(ops % 4096);
                        auto lease = group_index.AddMember(
                            group, "member_" + std::to_string(ops));
                        if (lease) {
                            ++ops;
                        }
                    }
                    total_ops.fetch_add(ops);
                });
            }
            std::this_thread::sleep_for(std::chrono::seconds(2));
            stop.store(true, std::memory_order_relaxed);
            for (auto& worker : workers) {
                worker.join();
            }
            std::cout << threads << "," << total_ops.load() / 2 << std::endl;
        }
    }
};

}  // namespace mooncake::benchmarks

int main(int argc, char** argv) {
    google::InitGoogleLogging("MetadataScanBench");
    FLAGS_logtostderr = true;
    gflags::ParseCommandLineFlags(&argc, &argv, true);

    using mooncake::benchmarks::MetadataScanBench;
    const bool ok = MetadataScanBench::RunScales();

    google::ShutdownGoogleLogging();
    return ok ? 0 : 1;
}
