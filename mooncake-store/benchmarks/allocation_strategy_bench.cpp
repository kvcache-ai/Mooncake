#include <algorithm>
#include <chrono>
#include <cmath>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <numeric>
#include <random>
#include <sstream>
#include <string>
#include <vector>
#include <sys/resource.h>

#include <gflags/gflags.h>
#include "types.h"

#include "offset_allocator/offset_allocator.hpp"
#include "allocator.h"
#include "allocation_strategy.h"

// --- gflags definitions ---
DEFINE_int64(segment_capacity, 1024,
             "Per-segment capacity in MB (base capacity for skewed mode)");
DEFINE_int32(num_allocations, 10000, "Number of allocations to benchmark");
DEFINE_int32(convergence_sample_interval, 100,
             "Sample utilization stddev every N allocations (scaleout only)");
DEFINE_bool(
    run_all, false,
    "Also run the Scale-Out matrix in addition to the default Fillup matrix");

// Scale-Out workload flags
DEFINE_string(workload, "fillup",
              "Workload type: fillup (default), scaleout, dsa");
DEFINE_int32(
    scale_out_trigger_pct, 50,
    "Pre-fill cluster to this utilization % before injecting new nodes "
    "(scaleout mode only)");
DEFINE_int32(
    alloc_percent_after_scale, 30,
    "Percentage of expanded cluster capacity to allocate after scaleout");
DEFINE_double(convergence_threshold, 0.1, "Threshold for convergence");
DEFINE_int32(
    prefill_pct, 0,
    "Pre-fill the cluster to this utilization percentage before the "
    "measured allocation loop (0 = disabled, original behavior). "
    "When >0, early exit on consecutive failures is suppressed so that "
    "all num_allocations are attempted under near-full conditions.");
DEFINE_int64(dsa_segment_capacity, 1024ULL * 1024,
             "Per-segment capacity in MB for DSA workload");
DEFINE_double(dsa_capacity_multiplier, 3.0,
              "DSA workload measures (multiplier * cluster_capacity) worth "
              "of allocations under steady-state churn. Combined with "
              "--dsa_warmup_multiplier, total per-case work is "
              "(warmup + capacity) * cluster_capacity, clamped to "
              "[kDsaMinAllocs, --dsa_max_allocs]; --num_allocations is "
              "ignored in DSA mode.");
DEFINE_double(dsa_warmup_multiplier, 1.0,
              "DSA warmup-phase write volume in units of cluster_capacity, "
              "executed BEFORE the measurement window so the cluster is in "
              "post-fill eviction churn when stats start being collected. "
              "All stats during warmup (latencies, util_ratios, evict "
              "rounds, succ/total) are discarded. Set to 0.0 to disable "
              "warmup entirely (legacy behavior).");
DEFINE_int32(dsa_max_allocs, 50000000,
             "Per-case upper bound on the auto-derived DSA allocation count. "
             "Raise this if you use very large segments (e.g. 100GB+) and "
             "want big clusters to actually reach steady-state eviction; "
             "lower it to bound benchmark runtime. Each allocation costs ~1-3 "
             "us, so the default upper bound can take minutes per saturated "
             "case.");
DEFINE_double(dsa_evict_ratio, 0.05,
              "Fraction of live objects to evict on each Allocate failure "
              "(default 0.05 = 5%). Lower values keep "
              "the steady-state cluster fill closer to the fragmentation "
              "ceiling but trigger evictions more frequently.");

using namespace mooncake;

static constexpr size_t MiB = 1024ULL * 1024;
static constexpr size_t KiB = 1024ULL;
static constexpr double GiB = 1024.0 * 1024 * 1024;

// Benchmark constants
constexpr int kNumVirtualNodes = 10;
constexpr double kSkewRatio = 0.5;  // +/- 50% capacity for skewed clusters
constexpr uint64_t kPrimaryBaseAddr = 0x100000000ULL;
constexpr uint64_t kInjectedBaseAddr = 1ULL << 52;
constexpr size_t kMaxExpectedAllocs = 600000;
constexpr double kLargeClusterThresholdGB = 500.0;
constexpr int kPreFillSampleInterval = 100;
constexpr int kMinMeasurementAllocs = 100;
constexpr int kMaxMeasurementAllocs = 50000000;
constexpr int kMinSamplesForConvergence = 10;

// DSA workload sizes (DeepSeek defaults; adjust in source for other models).
constexpr size_t kDsaKvSize = 3274752;      // ~3.12 MB per KV page
constexpr size_t kDsaIndexerSize = 658432;  // 643 KB per indexer entry
constexpr int kDsaMaxBatch = 128;           // per-round batch upper bound
constexpr int kDsaMaxRetries = 5;
// Lower bound on the auto-derived allocation count per DSA case.
constexpr int kDsaMinAllocs = 100;

enum class WorkloadType {
    FILL_UP,    // Only allocate, measure throughput/latency
    SCALE_OUT,  // Inject new nodes mid-run, measure adoption speed
    DSA,        // DSA paired KV+indexer with random fail-triggered eviction
};

struct BenchConfig {
    int num_segments;
    size_t segment_capacity;
    size_t alloc_size;
    int replica_num;
    int num_allocations;
    bool skewed;
    // Pre-fill watermark: 0 = disabled (exit early on failure); >0 = pre-fill
    // cluster to this % utilization before measuring, then run without early
    // exit.
    int prefill_pct = 0;
    std::string strategy_name;
    AllocationStrategyType strategy_type;

    // Workload
    WorkloadType workload_type;
    int scale_out_trigger_pct;
    int scale_out_new_segments;

    // DSA workload knobs (only used when workload_type == DSA).
    bool dsa_paired = false;  // false = KV-only, true = KV+indexer pair
};

struct UtilRatioStats {
    double min = 0.0;
    double p99 = 0.0;
    double p90 = 0.0;
    double p50 = 0.0;
    double max = 0.0;
    double avg = 0.0;
    bool valid = false;
};

/**
 * @brief Common base for all benchmark results.
 *
 * Contains the performance metrics shared by Fill-Up and Scale-Out modes:
 * strategy metadata, throughput, latency percentiles, and final utilization.
 */
struct BenchResultBase {
    std::string strategy_name;
    int num_segments;
    size_t alloc_size;
    int replica_num;
    bool skewed;
    double cluster_capacity_gb;  // total cluster capacity in GB

    double total_time_us;
    double throughput;  // allocs/sec
    double avg_ns;
    double p50_ns;
    double p90_ns;
    double p99_ns;

    int success_count = 0;  // successful allocations
    int total_count = 0;    // total attempted allocations

    double final_util_stddev;  // utilization stddev at run end
    double final_avg_util;     // average utilization at run end

    // Number of times fail-triggered eviction fired (DSA workload only;
    // remains 0 for fill-up / scale-out).
    int evict_count = 0;

    // DSA paired flag controls how AllocSize is rendered in print output.
    // Defaults to false so existing fill-up / scale-out output is unchanged.
    bool dsa_paired = false;

    // Temporal cluster utilization stats sampled during the measured loop.
    UtilRatioStats util_ratio_stats;

    virtual ~BenchResultBase() = default;
};

struct FillUpResult : BenchResultBase {};

struct ScaleOutResult : BenchResultBase {
    // Capacity tracking
    double initial_capacity_gb = 0.0;
    double scaled_capacity_gb = 0.0;

    // Convergence (re-used from fill-up semantics for scale-out post-injection)
    double converge_avg_util = 0.0;
    int convergence_alloc_count = -1;  // -1 if not converged

    // Scale-out adoption metrics
    double stddev_before_scale = 0.0;
    double stddev_just_after_scale = 0.0;
    int trigger_alloc_idx = -1;
    int re_converge_allocs = -1;
    std::vector<double> new_node_util_over_time;
};

static double computeClusterCapacityGB(int num_segments, size_t base_capacity,
                                       bool skewed) {
    double total = 0.0;
    for (int i = 0; i < num_segments; ++i) {
        double cap = static_cast<double>(base_capacity);
        if (skewed) {
            cap = (i % 2 == 0) ? cap * (1.0 + kSkewRatio)
                               : cap * (1.0 - kSkewRatio);
        }
        total += cap;
    }
    return total / GiB;
}

static void setupResourceLimits() {
    struct rlimit rl;
    // DSA cases simulate multi-TB clusters via virtual address space.
    rl.rlim_cur = 200ULL * 1024 * 1024 * 1024 * 1024;
    rl.rlim_max = 200ULL * 1024 * 1024 * 1024 * 1024;
    setrlimit(RLIMIT_AS, &rl);

    rl.rlim_cur = RLIM_INFINITY;
    rl.rlim_max = RLIM_INFINITY;
    setrlimit(RLIMIT_DATA, &rl);
}

/**
 * @brief Create an AllocatorManager populated with N OffsetBufferAllocators.
 *
 * Each allocator manages only offset metadata, so memory overhead is minimal
 * even for very large simulated capacities.
 *
 * @param id_offset Starting index for segment naming (for Scale-Out additions)
 */
static AllocatorManager createCluster(int num_segments, size_t base_capacity,
                                      bool skewed, int id_offset = 0) {
    AllocatorManager manager;
    // Distribute segments evenly across kNumVirtualNodes virtual nodes if
    // num_segments > kNumVirtualNodes, otherwise 1 segment per node.
    int segments_per_node = std::max(1, num_segments / kNumVirtualNodes);

    for (int i = 0; i < num_segments; ++i) {
        int global_i = i + id_offset;
        std::string name =
            "node_" + std::to_string(global_i / segments_per_node) + "_seg_" +
            std::to_string(global_i % segments_per_node);
        // Balanced skew: alternate between (1 + ratio) and (1 - ratio)
        // to keep total capacity constant for even segment counts.
        size_t capacity = base_capacity;
        if (skewed) {
            capacity = (i % 2 == 0) ? base_capacity * (1.0 + kSkewRatio)
                                    : base_capacity * (1.0 - kSkewRatio);
        }
        uint64_t base_addr = kPrimaryBaseAddr + (global_i * base_capacity);
        auto allocator = std::make_shared<OffsetBufferAllocator>(
            name, base_addr, capacity, name);
        manager.addAllocator(name, allocator);
    }
    return manager;
}

/**
 * @brief Inject `count` new, fully-empty segments into an existing manager.
 *
 * New segments are named with the id_offset to avoid collisions.
 * Each new segment has the same capacity as the original segments
 * (not skewed) to represent freshly-joined rack nodes.
 *
 * @return The shared_ptrs of the newly added allocators (for tracking util)
 */
static std::vector<std::shared_ptr<BufferAllocatorBase>> injectNewSegments(
    AllocatorManager& manager, int count, size_t capacity, int id_offset) {
    std::vector<std::shared_ptr<BufferAllocatorBase>> new_allocs;
    new_allocs.reserve(count);
    for (int i = 0; i < count; ++i) {
        std::string name = "new_node_" + std::to_string(id_offset + i);
        uint64_t base_addr = kInjectedBaseAddr +
                             (static_cast<uint64_t>(id_offset + i) * capacity);
        auto allocator = std::make_shared<OffsetBufferAllocator>(
            name, base_addr, capacity, name);
        manager.addAllocator(name, allocator);
        new_allocs.push_back(allocator);
    }
    return new_allocs;
}

/**
 * @brief Compute average utilization ratio across all segments.
 */
static double computeAverageUtilAll(const AllocatorManager& manager) {
    const auto& names = manager.getNames();
    if (names.empty()) return 0.0;
    double sum = 0.0;
    int count = 0;
    for (const auto& name : names) {
        const auto* allocators = manager.getAllocators(name);
        if (!allocators || allocators->empty()) continue;
        for (const auto& alloc : *allocators) {
            double cap = static_cast<double>(alloc->capacity());
            if (cap == 0) continue;
            sum += static_cast<double>(alloc->size()) / cap;
            ++count;
        }
    }
    return count > 0 ? sum / count : 0.0;
}

static size_t computeTotalCapacity(const AllocatorManager& manager) {
    size_t total = 0;
    for (const auto& name : manager.getNames()) {
        const auto* allocs = manager.getAllocators(name);
        if (!allocs) continue;
        for (const auto& alloc : *allocs) {
            total += alloc->capacity();
        }
    }
    return total;
}

/**
 * @brief Compute standard deviation of per-segment utilization ratios.
 */
static double computeUtilizationStdDev(const AllocatorManager& manager) {
    const auto& names = manager.getNames();
    if (names.empty()) return 0.0;

    std::vector<double> ratios;
    ratios.reserve(names.size());

    for (const auto& name : names) {
        const auto* allocators = manager.getAllocators(name);
        if (!allocators || allocators->empty()) continue;
        for (const auto& alloc : *allocators) {
            double cap = static_cast<double>(alloc->capacity());
            if (cap == 0) continue;
            double used = static_cast<double>(alloc->size());
            ratios.push_back(used / cap);
        }
    }

    if (ratios.empty()) return 0.0;

    double mean =
        std::accumulate(ratios.begin(), ratios.end(), 0.0) / ratios.size();
    double sq_sum = 0.0;
    for (double r : ratios) {
        sq_sum += (r - mean) * (r - mean);
    }
    return std::sqrt(sq_sum / ratios.size());
}

/**
 * @brief Compute average utilization ratio of a set of allocators.
 */
static double computeAverageUtil(
    const std::vector<std::shared_ptr<BufferAllocatorBase>>& allocs) {
    if (allocs.empty()) return 0.0;
    double sum = 0.0;
    int count = 0;
    for (const auto& alloc : allocs) {
        if (!alloc) continue;
        double cap = static_cast<double>(alloc->capacity());
        if (cap == 0) continue;
        sum += static_cast<double>(alloc->size()) / cap;
        ++count;
    }
    return count > 0 ? sum / count : 0.0;
}

static UtilRatioStats computeUtilRatioStats(std::vector<double>& util_ratios) {
    UtilRatioStats stats;
    if (util_ratios.empty()) return stats;

    std::sort(util_ratios.begin(), util_ratios.end());

    auto percentile = [&](double p) -> double {
        size_t idx = static_cast<size_t>(util_ratios.size() * p);
        if (idx >= util_ratios.size()) idx = util_ratios.size() - 1;
        return util_ratios[idx];
    };

    stats.min = util_ratios.front();
    stats.p99 = percentile(0.01);
    stats.p90 = percentile(0.10);
    stats.p50 = percentile(0.50);
    stats.max = util_ratios.back();
    stats.avg = std::accumulate(util_ratios.begin(), util_ratios.end(), 0.0) /
                util_ratios.size();
    stats.valid = true;
    return stats;
}

static std::string strategyName(AllocationStrategyType type) {
    switch (type) {
        case AllocationStrategyType::RANDOM:
            return "Random";
        case AllocationStrategyType::FREE_RATIO_FIRST:
            return "FreeRatioFirst";
        default:
            return "Unknown";
    }
}

/**
 * @brief Compute latency percentiles and fill the base result fields.
 *
 * Shared between Fill-Up and Scale-Out benchmark runners.
 * Sorts latencies in-place.
 */
static void computeLatencyStats(std::vector<double>& latencies, double total_us,
                                int num_allocations, BenchResultBase& res) {
    std::sort(latencies.begin(), latencies.end());
    auto percentile = [&](double p) -> double {
        if (latencies.empty()) return 0.0;
        size_t idx = static_cast<size_t>(p * latencies.size());
        if (idx >= latencies.size()) idx = latencies.size() - 1;
        return latencies[idx];
    };

    res.total_time_us = total_us;
    res.throughput =
        latencies.empty() ? 0.0 : num_allocations / (total_us / 1e6);
    res.avg_ns = latencies.empty() ? 0.0
                                   : std::accumulate(latencies.begin(),
                                                     latencies.end(), 0.0) /
                                         latencies.size();
    res.p50_ns = percentile(0.50);
    res.p90_ns = percentile(0.90);
    res.p99_ns = percentile(0.99);
}

/**
 * @brief Run Fill-Up benchmark: allocate N times, never deallocate.
 *
 * Measures pure allocation throughput, latency, and final utilization stddev.
 * No periodic sampling or convergence detection.
 */
static FillUpResult runFillUpBenchmark(const BenchConfig& cfg) {
    AllocatorManager manager =
        createCluster(cfg.num_segments, cfg.segment_capacity, cfg.skewed);
    auto strategy = CreateAllocationStrategy(cfg.strategy_type);

    std::vector<double> latencies;
    latencies.reserve(cfg.num_allocations);

    // Keep allocations alive until we compute final metrics.
    std::vector<std::vector<Replica>> active_allocations;
    active_allocations.reserve(
        std::min(cfg.num_allocations, 1 << 20 /* 1M entries max */));

    const int kMaxConsecFailures = 10;
    int consec_failures = 0;
    int success_count = 0;
    int total_count = 0;

    // --- Pre-fill phase (only when prefill_pct > 0) ---
    if (cfg.prefill_pct > 0) {
        // Use large blocks to reach target utilization fast and avoid
        // exhausting allocator metadata nodes (same strategy as ScaleOut
        // pre-fill).
        double total_cap_gb = computeClusterCapacityGB(
            cfg.num_segments, cfg.segment_capacity, cfg.skewed);
        size_t pre_fill_alloc_size =
            std::max(cfg.alloc_size, static_cast<size_t>(8ULL * MiB));
        if (total_cap_gb > kLargeClusterThresholdGB) {
            pre_fill_alloc_size =
                std::max(pre_fill_alloc_size, static_cast<size_t>(32ULL * MiB));
        }
        // Estimate total pre-fill blocks to reach 100% so we can derive an
        // adaptive sample interval. Checking every ~1% of capacity prevents
        // overshooting the target on small clusters (e.g. 1-segment 1GB).
        size_t total_cap_bytes = static_cast<size_t>(total_cap_gb * GiB);
        int estimated_total_blocks = static_cast<int>(
            total_cap_bytes / (pre_fill_alloc_size * cfg.replica_num));
        int pre_fill_sample_interval = std::max(
            1, estimated_total_blocks / 100);  // check ~every 1% of capacity

        int pre_consec_failures = 0;
        int pre_count = 0;
        while (true) {
            // Sample adaptively to avoid O(N) evaluation on every alloc,
            // while still stopping close to the target utilization.
            if (pre_count % pre_fill_sample_interval == 0) {
                if (computeAverageUtilAll(manager) * 100.0 >= cfg.prefill_pct)
                    break;
            }
            auto r = strategy->Allocate(manager, pre_fill_alloc_size,
                                        cfg.replica_num);
            if (r.has_value()) {
                active_allocations.push_back(std::move(r.value()));
                pre_consec_failures = 0;
            } else {
                if (++pre_consec_failures >= kMaxConsecFailures) break;
            }
            ++pre_count;
        }
    }

    auto total_start = std::chrono::high_resolution_clock::now();

    for (int i = 0; i < cfg.num_allocations; ++i) {
        auto t0 = std::chrono::high_resolution_clock::now();
        auto result =
            strategy->Allocate(manager, cfg.alloc_size, cfg.replica_num);
        auto t1 = std::chrono::high_resolution_clock::now();

        latencies.push_back(
            std::chrono::duration<double, std::nano>(t1 - t0).count());
        ++total_count;

        if (result.has_value()) {
            active_allocations.push_back(std::move(result.value()));
            consec_failures = 0;
            ++success_count;
        } else {
            // When prefill_pct > 0 we are measuring near-full performance;
            // do not exit early on consecutive failures.
            if (++consec_failures >= kMaxConsecFailures && cfg.prefill_pct == 0)
                break;
        }
    }

    auto total_end = std::chrono::high_resolution_clock::now();
    double total_us =
        std::chrono::duration<double, std::micro>(total_end - total_start)
            .count();

    // Compute final metrics while active_allocations is still alive
    FillUpResult res;
    res.strategy_name = cfg.strategy_name;
    res.num_segments = cfg.num_segments;
    res.alloc_size = cfg.alloc_size;
    res.replica_num = cfg.replica_num;
    res.skewed = cfg.skewed;
    res.cluster_capacity_gb = computeClusterCapacityGB(
        cfg.num_segments, cfg.segment_capacity, cfg.skewed);
    res.final_util_stddev = computeUtilizationStdDev(manager);
    res.final_avg_util = computeAverageUtilAll(manager);
    res.success_count = success_count;
    res.total_count = total_count;

    computeLatencyStats(latencies, total_us, total_count, res);
    return res;
}

/**
 * @brief Run Scale-Out benchmark: inject new nodes partway through.
 *
 * Phases:
 *   Phase 1 [0, trigger):    Run with original cluster, let it fill up.
 *   Trigger [trigger]:       Inject `scale_out_new_segments` new empty nodes.
 *   Phase 2 [trigger, end):  Continue allocating; observe how the strategy
 *                            routes traffic to the new nodes.
 *
 * Measures throughput/latency + convergence + new-node adoption speed.
 */
static ScaleOutResult runScaleOutBenchmark(const BenchConfig& cfg) {
    AllocatorManager manager =
        createCluster(cfg.num_segments, cfg.segment_capacity, cfg.skewed);
    auto strategy = CreateAllocationStrategy(cfg.strategy_type);

    const double convergence_threshold = FLAGS_convergence_threshold;

    std::vector<double> latencies;
    // Don't reserve yet, we'll reserve after calculating measurement_allocs.

    ScaleOutResult res;
    res.strategy_name = cfg.strategy_name;
    res.num_segments = cfg.num_segments;
    res.alloc_size = cfg.alloc_size;
    res.replica_num = cfg.replica_num;
    res.skewed = cfg.skewed;
    res.initial_capacity_gb = (double)computeTotalCapacity(manager) / GiB;
    res.scaled_capacity_gb =
        res.initial_capacity_gb;  // Will be updated after injection
    res.cluster_capacity_gb = res.initial_capacity_gb;

    std::vector<std::vector<Replica>> active_allocations;
    // Reserve enough to avoid frequent reallocations.
    active_allocations.reserve(kMaxExpectedAllocs);

    int success_count = 0;
    int total_count = 0;
    int consec_failures = 0;
    const int kMaxConsecFailures = 10;

    // Use larger blocks for pre-fill to reach target with less metadata
    // overhead (avoiding OOM)
    size_t pre_fill_alloc_size =
        std::max((size_t)cfg.alloc_size, (size_t)(8ULL * MiB));
    if (res.initial_capacity_gb > kLargeClusterThresholdGB) {
        pre_fill_alloc_size =
            std::max(pre_fill_alloc_size, (size_t)(32ULL * MiB));
    }
    int pre_alloc_count = 0;
    while (true) {
        // Sample periodically to avoid O(N) evaluation on every pre-fill
        // allocation
        if (pre_alloc_count % kPreFillSampleInterval == 0) {
            if (computeAverageUtilAll(manager) * 100.0 >=
                cfg.scale_out_trigger_pct) {
                break;
            }
        }
        auto result =
            strategy->Allocate(manager, pre_fill_alloc_size, cfg.replica_num);
        if (result.has_value()) {
            active_allocations.push_back(std::move(result.value()));
            consec_failures = 0;
        } else {
            if (++consec_failures >= kMaxConsecFailures) break;
        }
        pre_alloc_count++;
    }

    // --- Inject New Nodes ---
    res.stddev_before_scale = computeUtilizationStdDev(manager);
    std::vector<std::shared_ptr<BufferAllocatorBase>> new_node_allocs =
        injectNewSegments(manager, cfg.scale_out_new_segments,
                          cfg.segment_capacity, cfg.num_segments);
    res.scaled_capacity_gb = (double)computeTotalCapacity(manager) / GiB;
    res.cluster_capacity_gb = res.scaled_capacity_gb;
    res.stddev_just_after_scale = computeUtilizationStdDev(manager);
    res.trigger_alloc_idx = 0;

    // Reset counters for the benchmarking phase
    consec_failures = 0;

    // Calculate requested percentage of expanded capacity in bytes
    double pct = static_cast<double>(FLAGS_alloc_percent_after_scale) / 100.0;
    double bytes_to_allocate = res.scaled_capacity_gb * GiB * pct;
    int measurement_allocs = static_cast<int>(
        std::round(bytes_to_allocate /
                   (static_cast<double>(cfg.alloc_size) * cfg.replica_num)));
    measurement_allocs =
        std::max(kMinMeasurementAllocs,
                 std::min(measurement_allocs, kMaxMeasurementAllocs));
    latencies.reserve(measurement_allocs);

    std::vector<int> sample_points;
    std::vector<double> stddev_over_time;
    std::vector<double> avg_util_over_time;

    int sample_interval = FLAGS_convergence_sample_interval;
    // dynamically adapt interval if measurement_allocs is too small to even hit
    // enough samples
    if (measurement_allocs > 0 &&
        measurement_allocs < sample_interval * kMinSamplesForConvergence) {
        sample_interval =
            std::max(1, measurement_allocs / kMinSamplesForConvergence);
    }

    double instrumentation_time_us = 0.0;
    auto total_start = std::chrono::high_resolution_clock::now();

    for (int i = 0; i < measurement_allocs; ++i) {
        // --- Allocate ---
        auto t0 = std::chrono::high_resolution_clock::now();
        auto result =
            strategy->Allocate(manager, cfg.alloc_size, cfg.replica_num);
        auto t1 = std::chrono::high_resolution_clock::now();

        latencies.push_back(
            std::chrono::duration<double, std::nano>(t1 - t0).count());
        ++total_count;

        if (result.has_value()) {
            active_allocations.push_back(std::move(result.value()));
            consec_failures = 0;
            ++success_count;
        } else {
            if (++consec_failures >= kMaxConsecFailures) break;
        }

        // --- Sample stddev & new-node utilization ---
        if ((i + 1) % sample_interval == 0 || i == measurement_allocs - 1) {
            auto s0 = std::chrono::high_resolution_clock::now();

            sample_points.push_back(i + 1);
            stddev_over_time.push_back(computeUtilizationStdDev(manager));
            avg_util_over_time.push_back(computeAverageUtilAll(manager));

            if (!new_node_allocs.empty()) {
                res.new_node_util_over_time.push_back(
                    computeAverageUtil(new_node_allocs));
            }
            auto s1 = std::chrono::high_resolution_clock::now();
            instrumentation_time_us +=
                std::chrono::duration<double, std::micro>(s1 - s0).count();
        }
    }

    auto total_end = std::chrono::high_resolution_clock::now();
    double total_us =
        std::chrono::duration<double, std::micro>(total_end - total_start)
            .count();
    total_us -= instrumentation_time_us;

    // --- Find re-convergence point after injection ---
    res.re_converge_allocs = -1;
    for (int idx = 0; idx < static_cast<int>(stddev_over_time.size()); ++idx) {
        if (stddev_over_time[idx] < convergence_threshold) {
            // Since trigger is at 0, convergence offset is just exact alloc
            // count
            res.re_converge_allocs = sample_points[idx];
            break;
        }
    }

    // --- Find convergence point ---
    // Track the START of the last stable convergence window:
    // - When stddev drops below the threshold, record the alloc count.
    // - When stddev rises back above the threshold, reset.
    double first_converge_avg_util = -1.0;
    int converged_at = -1;
    bool in_converged_window = false;

    for (size_t i = 0; i < stddev_over_time.size(); ++i) {
        int allocs_done = sample_points[i];
        // Only evaluate convergence once the cluster is at least 1% utilized.
        const double min_util_to_check = 0.01;
        bool util_sufficient = i < avg_util_over_time.size() &&
                               avg_util_over_time[i] >= min_util_to_check;

        if (!util_sufficient) {
            // Too early — don't count
        } else if (stddev_over_time[i] < convergence_threshold) {
            if (!in_converged_window) {
                converged_at = allocs_done;
                in_converged_window = true;
                if (i < avg_util_over_time.size()) {
                    first_converge_avg_util = avg_util_over_time[i];
                }
            }
        } else {
            converged_at = -1;
            in_converged_window = false;
            first_converge_avg_util = -1.0;
        }
    }

    // Compute final metrics while active_allocations is still alive
    res.final_util_stddev = computeUtilizationStdDev(manager);
    res.final_avg_util = computeAverageUtilAll(manager);
    res.converge_avg_util = (converged_at != -1 && first_converge_avg_util >= 0)
                                ? first_converge_avg_util
                                : res.final_avg_util;
    res.convergence_alloc_count = converged_at;
    res.success_count = success_count;
    res.total_count = total_count;

    computeLatencyStats(latencies, total_us, total_count, res);
    return res;
}

static void evictRandomFraction(std::vector<std::vector<Replica>>& live,
                                double ratio, std::mt19937& rng) {
    if (live.empty()) return;

    size_t to_drop =
        std::max<size_t>(1, static_cast<size_t>(live.size() * ratio));
    if (to_drop > live.size()) to_drop = live.size();

    for (size_t i = 0; i < to_drop; ++i) {
        std::uniform_int_distribution<size_t> dist(0, live.size() - 1);
        size_t idx = dist(rng);
        std::swap(live[idx], live.back());
        live.pop_back();  // Replica destructor returns memory to allocator.
    }
}

// Try to allocate once; on failure, sample utilization before eviction.
static bool dsaAllocateWithEvict(
    const std::shared_ptr<AllocationStrategy>& strategy,
    AllocatorManager& manager, size_t size, int replica_num,
    std::vector<std::vector<Replica>>& live, std::mt19937& rng,
    int& evict_count, double evict_ratio,
    std::vector<double>* pre_evict_util = nullptr) {
    for (int attempt = 0; attempt <= kDsaMaxRetries; ++attempt) {
        auto result = strategy->Allocate(manager, size, replica_num);
        if (result.has_value()) {
            live.push_back(std::move(result.value()));
            return true;
        }

        if (live.empty()) return false;
        if (attempt == kDsaMaxRetries) return false;

        if (attempt == 0 && pre_evict_util != nullptr) {
            pre_evict_util->push_back(computeAverageUtilAll(manager));
        }

        evictRandomFraction(live, evict_ratio, rng);
        ++evict_count;
    }

    return false;
}

// Run DSA workload; the allocation count is derived from cluster capacity.
static FillUpResult runDsaBenchmark(const BenchConfig& cfg) {
    AllocatorManager manager =
        createCluster(cfg.num_segments, cfg.segment_capacity, cfg.skewed);
    auto strategy = CreateAllocationStrategy(cfg.strategy_type);

    const size_t total_capacity = computeTotalCapacity(manager);
    const size_t avg_obj_size =
        cfg.dsa_paired ? (kDsaKvSize + kDsaIndexerSize) / 2 : kDsaKvSize;
    const size_t bytes_per_alloc = avg_obj_size * cfg.replica_num;
    const double warmup_mult = std::max(0.0, FLAGS_dsa_warmup_multiplier);
    const double measure_mult = std::max(0.0, FLAGS_dsa_capacity_multiplier);
    const double total_mult = warmup_mult + measure_mult;

    int total_allocs =
        (bytes_per_alloc == 0)
            ? kDsaMinAllocs
            : static_cast<int>(total_mult * total_capacity / bytes_per_alloc);
    total_allocs =
        std::clamp(total_allocs, kDsaMinAllocs, FLAGS_dsa_max_allocs);
    int warmup_allocs =
        (total_mult > 0.0)
            ? static_cast<int>(total_allocs * (warmup_mult / total_mult))
            : 0;
    int measure_allocs = std::max(0, total_allocs - warmup_allocs);

    std::vector<double> latencies;
    latencies.reserve(measure_allocs);

    std::vector<double> util_ratios;
    util_ratios.reserve(measure_allocs);

    // Use one live set so eviction matches allocator_bench.
    std::vector<std::vector<Replica>> live_allocations;
    live_allocations.reserve(std::min(total_allocs, 1 << 20));

    std::mt19937 rng(42);
    std::uniform_int_distribution<int> batch_dist(1, kDsaMaxBatch);

    if (warmup_allocs > 0) {
        int warmup_count = 0;
        int warmup_evict_throwaway = 0;
        auto warmup_run = [&](size_t size,
                              std::vector<std::vector<Replica>>& live) {
            (void)dsaAllocateWithEvict(strategy, manager, size, cfg.replica_num,
                                       live, rng, warmup_evict_throwaway,
                                       FLAGS_dsa_evict_ratio);
            ++warmup_count;
        };

        while (warmup_count < warmup_allocs) {
            int batch = batch_dist(rng);
            for (int i = 0; i < batch && warmup_count < warmup_allocs; ++i) {
                warmup_run(kDsaKvSize, live_allocations);
            }
            if (cfg.dsa_paired) {
                for (int i = 0; i < batch && warmup_count < warmup_allocs;
                     ++i) {
                    warmup_run(kDsaIndexerSize, live_allocations);
                }
            }
        }
    }

    int success_count = 0;
    int total_count = 0;
    int evict_count = 0;

    auto run_one = [&](size_t size,
                       std::vector<std::vector<Replica>>& live) -> bool {
        auto t0 = std::chrono::high_resolution_clock::now();
        bool ok = dsaAllocateWithEvict(strategy, manager, size, cfg.replica_num,
                                       live, rng, evict_count,
                                       FLAGS_dsa_evict_ratio, &util_ratios);
        auto t1 = std::chrono::high_resolution_clock::now();

        latencies.push_back(
            std::chrono::duration<double, std::nano>(t1 - t0).count());
        ++total_count;
        if (ok) ++success_count;

        return ok;
    };

    auto total_start = std::chrono::high_resolution_clock::now();

    while (total_count < measure_allocs) {
        int batch = batch_dist(rng);

        for (int i = 0; i < batch && total_count < measure_allocs; ++i) {
            run_one(kDsaKvSize, live_allocations);
        }

        if (cfg.dsa_paired) {
            for (int i = 0; i < batch && total_count < measure_allocs; ++i) {
                run_one(kDsaIndexerSize, live_allocations);
            }
        }
    }

    auto total_end = std::chrono::high_resolution_clock::now();
    double total_us =
        std::chrono::duration<double, std::micro>(total_end - total_start)
            .count();

    FillUpResult res;
    res.strategy_name = cfg.strategy_name;
    res.num_segments = cfg.num_segments;
    res.alloc_size = cfg.alloc_size;  // KV size; AllocSize column displays it.
    res.replica_num = cfg.replica_num;
    res.skewed = cfg.skewed;
    res.cluster_capacity_gb = computeClusterCapacityGB(
        cfg.num_segments, cfg.segment_capacity, cfg.skewed);
    res.final_util_stddev = computeUtilizationStdDev(manager);
    res.final_avg_util = computeAverageUtilAll(manager);
    res.success_count = success_count;
    res.total_count = total_count;
    res.evict_count = evict_count;
    res.dsa_paired = cfg.dsa_paired;
    res.util_ratio_stats = computeUtilRatioStats(util_ratios);
    computeLatencyStats(latencies, total_us, total_count, res);
    return res;
}

static void printFillUpHeader() {
    std::cout << std::string(184, '-') << std::endl;
    std::cout << std::left << std::setw(18) << "Strategy" << std::setw(9)
              << "Replica" << std::setw(10) << "Segments" << std::setw(12)
              << "AllocSize" << std::setw(12) << "Cluster(GB)" << std::setw(8)
              << "Skewed" << std::right << std::setw(14) << "Throughput"
              << std::setw(12) << "Avg(ns)" << std::setw(12) << "P50(ns)"
              << std::setw(12) << "P90(ns)" << std::setw(12) << "P99(ns)"
              << std::setw(12) << "UtilStdDev" << std::setw(10) << "AvgUtil%"
              << std::setw(15) << "Succ/Total" << std::setw(14) << "Evictions"
              << std::endl;
    std::cout << std::string(184, '-') << std::endl;
}

static void printFillUpResult(const FillUpResult& r) {
    std::string alloc_ratio =
        std::to_string(r.success_count) + "/" + std::to_string(r.total_count);
    std::ostringstream cap_ss;
    cap_ss << std::fixed << std::setprecision(1) << r.cluster_capacity_gb;
    std::string alloc_size_str =
        r.dsa_paired ? (std::to_string(r.alloc_size / KiB) + "+" +
                        std::to_string(kDsaIndexerSize / KiB) + "KB")
                     : (std::to_string(r.alloc_size / KiB) + "KB");
    std::cout << std::left << std::setw(18) << r.strategy_name << std::setw(9)
              << r.replica_num << std::setw(10) << r.num_segments
              << std::setw(12) << alloc_size_str << std::setw(12)
              << cap_ss.str() << std::setw(8) << (r.skewed ? "yes" : "no")
              << std::right << std::fixed << std::setprecision(0)
              << std::setw(14) << r.throughput << std::setw(12) << r.avg_ns
              << std::setw(12) << r.p50_ns << std::setw(12) << r.p90_ns
              << std::setw(12) << r.p99_ns << std::setprecision(4)
              << std::setw(12) << r.final_util_stddev << std::setprecision(2)
              << std::setw(9) << (r.final_avg_util * 100.0) << "%"
              << std::setw(15) << alloc_ratio << std::setw(14) << r.evict_count
              << std::endl;
}

static void printDsaHeader() {
    std::cout << std::string(264, '-') << std::endl;
    std::cout << std::left << std::setw(18) << "Strategy" << std::setw(9)
              << "Replica" << std::setw(10) << "Segments" << std::setw(12)
              << "AllocSize" << std::setw(12) << "Cluster(GB)" << std::setw(8)
              << "Skewed" << std::right << std::setw(14) << "UtilRatio_min"
              << std::setw(14) << "UtilRatio_p99" << std::setw(14)
              << "UtilRatio_p90" << std::setw(14) << "UtilRatio_p50"
              << std::setw(14) << "UtilRatio_max" << std::setw(14)
              << "UtilRatio_avg" << std::setw(14) << "Throughput"
              << std::setw(12) << "Avg(ns)" << std::setw(12) << "P50(ns)"
              << std::setw(12) << "P90(ns)" << std::setw(12) << "P99(ns)"
              << std::setw(12) << "UtilStdDev" << std::setw(17)
              << "Evictions(round)" << std::setw(20) << "Succ/Total"
              << std::endl;
    std::cout << std::string(264, '-') << std::endl;
}

static void printDsaResult(const FillUpResult& r) {
    std::string alloc_ratio =
        std::to_string(r.success_count) + "/" + std::to_string(r.total_count);
    std::ostringstream cap_ss;
    cap_ss << std::fixed << std::setprecision(1) << r.cluster_capacity_gb;
    std::string alloc_size_str =
        r.dsa_paired ? (std::to_string(r.alloc_size / KiB) + "+" +
                        std::to_string(kDsaIndexerSize / KiB) + "KB")
                     : (std::to_string(r.alloc_size / KiB) + "KB");
    const auto& urs = r.util_ratio_stats;
    std::cout << std::left << std::setw(18) << r.strategy_name << std::setw(9)
              << r.replica_num << std::setw(10) << r.num_segments
              << std::setw(12) << alloc_size_str << std::setw(12)
              << cap_ss.str() << std::setw(8) << (r.skewed ? "yes" : "no")
              << std::right << std::fixed << std::setprecision(6)
              << std::setw(14) << urs.min << std::setw(14) << urs.p99
              << std::setw(14) << urs.p90 << std::setw(14) << urs.p50
              << std::setw(14) << urs.max << std::setw(14) << urs.avg
              << std::setprecision(0) << std::setw(14) << r.throughput
              << std::setw(12) << r.avg_ns << std::setw(12) << r.p50_ns
              << std::setw(12) << r.p90_ns << std::setw(12) << r.p99_ns
              << std::setprecision(4) << std::setw(12) << r.final_util_stddev
              << std::setprecision(0) << std::setw(17) << r.evict_count
              << std::setw(20) << alloc_ratio << std::endl;
}

static void printScaleOutHeader() {
    std::cout << std::string(210, '-') << std::endl;
    std::cout << std::left << std::setw(18) << "Strategy" << std::setw(9)
              << "Replica" << std::setw(10) << "Segments" << std::setw(12)
              << "AllocSize" << std::setw(16) << "Cluster(GB)" << std::setw(8)
              << "Skewed" << std::right << std::setw(14) << "Throughput"
              << std::setw(12) << "Avg(ns)" << std::setw(12) << "P50(ns)"
              << std::setw(12) << "P90(ns)" << std::setw(12) << "P99(ns)"
              << std::setw(12) << "UtilStdDev" << std::setw(10) << "ConvUtil%"
              << std::setw(11) << "FinalUtil%" << std::setw(14) << "Converge@"
              << std::setw(15) << "Succ/Total" << std::endl;
    std::cout << std::string(210, '-') << std::endl;
}

static void printScaleOutResult(const ScaleOutResult& r) {
    std::string converge_str = "N/A";
    std::string conv_util_str = "N/A";

    if (r.convergence_alloc_count > 0) {
        converge_str = std::to_string(r.convergence_alloc_count);
        std::ostringstream util_ss;
        util_ss << std::fixed << std::setprecision(2)
                << (r.converge_avg_util * 100.0) << "%";
        conv_util_str = util_ss.str();
    }

    std::string alloc_ratio =
        std::to_string(r.success_count) + "/" + std::to_string(r.total_count);
    std::ostringstream cap_ss;
    cap_ss << std::fixed << std::setprecision(1) << r.initial_capacity_gb
           << "->" << r.scaled_capacity_gb;

    std::cout << std::left << std::setw(18) << r.strategy_name << std::setw(9)
              << r.replica_num << std::setw(10) << r.num_segments
              << std::setw(12) << (std::to_string(r.alloc_size / KiB) + "KB")
              << std::setw(16) << cap_ss.str() << std::setw(8)
              << (r.skewed ? "yes" : "no") << std::right << std::fixed
              << std::setprecision(0) << std::setw(14) << r.throughput
              << std::setw(12) << r.avg_ns << std::setw(12) << r.p50_ns
              << std::setw(12) << r.p90_ns << std::setw(12) << r.p99_ns
              << std::setprecision(4) << std::setw(12) << r.final_util_stddev
              << std::setw(10) << conv_util_str << std::setprecision(2)
              << std::setw(9) << (r.final_avg_util * 100.0) << "%"
              << std::setw(14) << converge_str << std::setw(15) << alloc_ratio
              << std::endl;
}

static void runFillupBenchmarks() {
    std::vector<bool> skewed_options = {false, true};
    std::vector<int> segment_counts = {1, 10, 100, 512, 1024};
    std::vector<size_t> alloc_sizes = {512 * KiB, 8 * MiB, 32 * MiB};
    std::vector<int> replica_nums = {1, 2, 3};
    std::vector<AllocationStrategyType> strategies = {
        AllocationStrategyType::RANDOM,
        AllocationStrategyType::FREE_RATIO_FIRST,
    };

    std::cout
        << "\n=== AllocationStrategy Fill-Up Benchmark Matrix ===\n"
        << (FLAGS_prefill_pct > 0
                ? ("Mode: prefill_pct=" + std::to_string(FLAGS_prefill_pct) +
                   "% (cluster pre-filled before measuring; no early exit)\n")
                : "Mode: normal (exit early after 10 consecutive failures)\n")
        << "Config: num_allocations=" << FLAGS_num_allocations
        << ", segment_capacity=" << FLAGS_segment_capacity << " MB\n"
        << "Skewed setup: half nodes are (base + 50%) capacity, half are "
           "(base - 50%)\n"
        << std::endl;

    std::vector<BenchConfig> configs;
    for (auto skew : skewed_options) {
        for (auto strategy : strategies) {
            for (auto segs : segment_counts) {
                for (auto asize : alloc_sizes) {
                    for (auto rep : replica_nums) {
                        if (rep > segs) continue;
                        BenchConfig cfg;
                        cfg.num_segments = segs;
                        cfg.segment_capacity =
                            static_cast<size_t>(FLAGS_segment_capacity) * MiB;
                        cfg.alloc_size = asize;
                        cfg.replica_num = rep;
                        cfg.num_allocations = FLAGS_num_allocations;
                        cfg.skewed = skew;
                        cfg.strategy_type = strategy;
                        cfg.strategy_name = strategyName(strategy);
                        cfg.prefill_pct = FLAGS_prefill_pct;
                        cfg.workload_type = WorkloadType::FILL_UP;
                        configs.push_back(cfg);
                    }
                }
            }
        }
    }

    // Run the linear list of configs
    AllocationStrategyType current_strategy = AllocationStrategyType::RANDOM;
    bool first = true;
    for (const auto& cfg : configs) {
        // Print header when strategy
        if (first || cfg.strategy_type != current_strategy) {
            printFillUpHeader();
            current_strategy = cfg.strategy_type;
            first = false;
        }

        auto result = runFillUpBenchmark(cfg);
        printFillUpResult(result);
    }
}

static void runScaleOutMatrix() {
    std::vector<bool> skewed_options = {false, true};
    std::vector<int> segment_counts = {1, 10, 100, 512, 1024};
    std::vector<size_t> alloc_sizes = {512 * KiB, 8 * MiB, 32 * MiB};
    std::vector<int> replica_nums = {1, 2, 3};
    std::vector<AllocationStrategyType> strategies = {
        AllocationStrategyType::RANDOM,
        AllocationStrategyType::FREE_RATIO_FIRST};

    std::cout
        << "\n=== Scale-Out Workload Benchmark (Matrix) ===\n"
        << "Note: Test will exit early if 10 consecutive allocations fail.\n"
        << "Design: Pre-fill to " << FLAGS_scale_out_trigger_pct
        << "% capacity, "
        << "then measure " << FLAGS_alloc_percent_after_scale
        << "% of expanded capacity (Cap: 200k allocations).\n"
        << "Config: segment_capacity=" << FLAGS_segment_capacity << " MB, "
        << "convergence_threshold=" << FLAGS_convergence_threshold << "\n"
        << "Skewed setup: half nodes are (base + 50%) capacity, half are (base "
           "- 50%)\n"
        << std::endl;

    std::vector<BenchConfig> configs;
    for (auto skew : skewed_options) {
        for (auto strategy : strategies) {
            for (auto segs : segment_counts) {
                for (auto asize : alloc_sizes) {
                    for (auto rep : replica_nums) {
                        if (rep > segs) continue;
                        BenchConfig cfg;
                        cfg.num_segments = segs;
                        cfg.segment_capacity =
                            static_cast<size_t>(FLAGS_segment_capacity) * MiB;
                        cfg.alloc_size = asize;
                        cfg.replica_num = rep;
                        cfg.num_allocations = FLAGS_num_allocations;
                        cfg.skewed = skew;
                        cfg.strategy_type = strategy;
                        cfg.strategy_name = strategyName(strategy);
                        cfg.workload_type = WorkloadType::SCALE_OUT;

                        cfg.scale_out_trigger_pct = FLAGS_scale_out_trigger_pct;
                        cfg.scale_out_new_segments = std::max(1, segs / 2);
                        configs.push_back(cfg);
                    }
                }
            }
        }
    }

    AllocationStrategyType current_strategy = AllocationStrategyType::RANDOM;
    bool first = true;
    for (const auto& cfg : configs) {
        if (first || cfg.strategy_type != current_strategy) {
            printScaleOutHeader();
            current_strategy = cfg.strategy_type;
            first = false;
        }

        auto result = runScaleOutBenchmark(cfg);
        printScaleOutResult(result);
    }
}

// DSA matrix runner
static void runDsaMatrix() {
    std::vector<bool> skewed_options = {false, true};
    std::vector<int> segment_counts = {1, 2, 4, 8, 16};
    std::vector<int> replica_nums = {1, 2, 3};
    std::vector<bool> paired_modes = {false, true};
    std::vector<AllocationStrategyType> strategies = {
        AllocationStrategyType::RANDOM,
        AllocationStrategyType::FREE_RATIO_FIRST,
    };

    size_t seg_cap_mb = static_cast<size_t>(FLAGS_dsa_segment_capacity);

    const double total_mult = std::max(0.0, FLAGS_dsa_warmup_multiplier) +
                              std::max(0.0, FLAGS_dsa_capacity_multiplier);
    std::cout
        << "\n=== DSA Paired KV+Indexer Benchmark Matrix ===\n"
        << "Workload: per-round burst of N kvcache (" << kDsaKvSize
        << " B) allocations followed (paired mode only) by N indexer ("
        << kDsaIndexerSize << " B) allocations; N uniform in [1, "
        << kDsaMaxBatch << "].\n"
        << "Eviction: on Allocate failure, drop "
        << (FLAGS_dsa_evict_ratio * 100.0)
        << "% of live objects at random; retry up to " << kDsaMaxRetries
        << " times. Latency includes evict+retry time. Evictions(round) "
           "counts how many such drop cycles fire during measurement, "
           "NOT the number of objects evicted.\n"
        << "UtilRatio_*: cluster utilization sampled at each eviction "
           "trigger (peak fill the strategy reached before being forced "
           "to evict), NOT a time-weighted average over all allocations.\n"
        << "Phases: warmup writes " << FLAGS_dsa_warmup_multiplier
        << "x cluster_capacity worth of allocations to reach steady state "
           "(stats discarded), then measurement writes "
        << FLAGS_dsa_capacity_multiplier
        << "x cluster_capacity (stats collected). "
           "Set --dsa_warmup_multiplier=0 to disable warmup.\n"
        << "Config: dsa_segment_capacity=" << seg_cap_mb << " MB ("
        << (seg_cap_mb / 1024.0)
        << " GB), dsa_max_allocs=" << FLAGS_dsa_max_allocs << "\n"
        << "Total per-case allocations auto-derived as " << total_mult
        << "x * cluster_capacity / (avg_obj_size * replica), clamped to ["
        << kDsaMinAllocs << ", " << FLAGS_dsa_max_allocs
        << "]; warmup/measurement budgets split proportionally when clamp "
           "activates. --num_allocations is ignored in DSA mode.\n"
        << "Skewed setup: half nodes are (base + 50%) capacity, half are "
           "(base - 50%)\n"
        << std::endl;

    std::vector<BenchConfig> configs;
    for (auto paired : paired_modes) {
        for (auto skew : skewed_options) {
            for (auto strategy : strategies) {
                for (auto segs : segment_counts) {
                    for (auto rep : replica_nums) {
                        if (rep > segs) continue;
                        BenchConfig cfg;
                        cfg.num_segments = segs;
                        cfg.segment_capacity = seg_cap_mb * MiB;
                        cfg.alloc_size = kDsaKvSize;
                        cfg.replica_num = rep;
                        // num_allocations is auto-derived inside
                        // runDsaBenchmark from cluster capacity; the flag
                        // value is intentionally not propagated.
                        cfg.num_allocations = 0;
                        cfg.skewed = skew;
                        cfg.strategy_type = strategy;
                        cfg.strategy_name = strategyName(strategy);
                        cfg.workload_type = WorkloadType::DSA;
                        cfg.dsa_paired = paired;
                        configs.push_back(cfg);
                    }
                }
            }
        }
    }

    bool first = true;
    bool prev_paired = false;
    AllocationStrategyType prev_strategy = AllocationStrategyType::RANDOM;

    for (const auto& cfg : configs) {
        if (first || cfg.dsa_paired != prev_paired) {
            std::cout << "\n--- "
                      << (cfg.dsa_paired ? "Paired (KV + Indexer)" : "KV-only")
                      << " ---" << std::endl;
            prev_paired = cfg.dsa_paired;
            first = true;  // force header re-print at start of new section
        }

        if (first || cfg.strategy_type != prev_strategy) {
            printDsaHeader();
            prev_strategy = cfg.strategy_type;
            first = false;
        }

        auto result = runDsaBenchmark(cfg);
        printDsaResult(result);
    }
}

int main(int argc, char* argv[]) {
    gflags::SetUsageMessage(
        "AllocationStrategy performance benchmark.\n"
        "Usage: allocation_strategy_bench [flags]");
    gflags::ParseCommandLineFlags(&argc, &argv, true);
    setupResourceLimits();

    if (FLAGS_run_all) {
        runFillupBenchmarks();
        runScaleOutMatrix();
        runDsaMatrix();
    } else if (FLAGS_workload == "fillup") {
        runFillupBenchmarks();
    } else if (FLAGS_workload == "scaleout") {
        runScaleOutMatrix();
    } else if (FLAGS_workload == "dsa") {
        runDsaMatrix();
    } else {
        std::cout << "Invalid workload type: " << FLAGS_workload
                  << ". Use --workload=fillup, --workload=scaleout, or "
                     "--workload=dsa."
                  << std::endl;
    }

    return 0;
}
