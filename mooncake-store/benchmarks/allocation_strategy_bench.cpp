#include <algorithm>
#include <chrono>
#include <cmath>
#include <cstdlib>
#include <fstream>
#include <functional>
#include <unordered_map>
#include <iomanip>
#include <iostream>
#include <limits>
#include <numeric>
#include <random>
#include <sstream>
#include <string>
#include <vector>
#include <sys/resource.h>

#include <gflags/gflags.h>
#include "types.h"

#include "offset_allocator/offset_allocator.h"
#include "allocator.h"
#include "allocation_strategy.h"
#include "local_ssd/manager.h"

// --- gflags definitions ---
DEFINE_int64(segment_capacity, 1024,
             "Per-segment capacity in MB (base capacity for skewed mode)");
DEFINE_int32(num_allocations, 10000, "Number of allocations to benchmark");
DEFINE_int32(convergence_sample_interval, 100,
             "Sample utilization stddev or fragmentation every N allocations");
DEFINE_bool(run_all, false,
            "Run all workload matrices instead of only the selected workload");

// Scale-Out workload flags
DEFINE_string(workload, "fillup",
              "Workload type: fillup (default), scaleout, dsa, "
              "size_class_churn");
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
DEFINE_string(size_class_pattern, "kv_mixed",
              "Size-class churn pattern: kv_mixed, dsa_pair, rl, or all");
DEFINE_double(size_class_evict_ratio, 0.02,
              "Fraction of live objects to evict on each Allocate failure "
              "in size_class_churn mode.");
DEFINE_double(size_class_release_prob, 0.0,
              "Probability of releasing one random live object before each "
              "measured size_class_churn allocation. 1.0 gives a "
              "free-one/allocate-one steady state at the prefill level; 0 "
              "keeps the allocate-only legacy behavior.");
DEFINE_int64(rl_min_kib, 64,
             "Smallest object size in KiB for the rl size-class pattern.");
DEFINE_int64(rl_max_mib, 512,
             "Largest object size in MiB for the rl size-class pattern.");
DEFINE_string(rl_trace_file, "",
              "Replay allocation sizes from a trace file (one size in bytes "
              "per line, '#' starts a comment) instead of a synthetic "
              "size-class pattern. Sizes are replayed in file order and "
              "wrap around. Only used by --workload=size_class_churn.");
DEFINE_int32(probe_mib, 0,
             "At every fragmentation sample point, attempt one N MiB "
             "single-replica allocation without eviction retry, release it "
             "immediately, and report the success rate together with the "
             "free space observed at probe time (0 = disabled).");
DEFINE_string(segment_counts, "",
              "Comma-separated segment counts for the size_class_churn "
              "matrix (default: 1,10,100).");
DEFINE_string(size_class_strategies, "random,free_ratio_first",
              "Comma-separated allocation strategies for the size_class_churn "
              "matrix: random, free_ratio_first, best_fit (production "
              "strategies), largest_hole_first, reserved, hybrid (bench-only "
              "placement experiments, see "
              "--large_object_mib/--reserved_segments). hybrid = best_fit for "
              "objects below --large_object_mib, largest_hole_first above.");
DEFINE_string(replica_counts, "",
              "Comma-separated replica counts for the size_class_churn matrix "
              "(default: 1,2,3).");
DEFINE_int32(large_object_mib, 256,
             "Objects of at least this many MiB are 'large' for the reserved "
             "strategy and go to the reserved segments only.");
DEFINE_int32(best_fit_bucket_mib, 512,
             "best_fit_bucketed: segments whose slack (largest hole minus "
             "request) falls in the same bucket of this many MiB are treated "
             "as equal and chosen at random, trading a little contiguity for "
             "less traffic concentration.");
DEFINE_int32(reserved_segments, 2,
             "Number of segments (the last ones by index) reserved for large "
             "objects in the reserved strategy.");
DEFINE_string(rl_trace_events, "",
              "Replay an ordered put/remove/evict event log ('kind key size' "
              "per line, as written by extract_alloc_trace.py --events) "
              "instead of a synthetic pattern. Puts allocate, removes and "
              "evicts free; there is no eviction retry, so every failed put "
              "is reported as a failure. Only used by "
              "--workload=size_class_churn.");
DEFINE_int32(trace_events_repeat, 1,
             "Replay the event log this many times back to back (keys are "
             "suffixed per pass) to reach steady state on short traces.");

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
constexpr int kSizeClassMaxRetries = 5;
constexpr size_t kSizeClassMinPrefillAttempts = 5000;
// The theoretical prefill budget assumes every allocation succeeds. Use a
// small multiplier to absorb partial allocations, retries, and fragmentation
// without turning unreachable targets into long-running cases.
constexpr double kSizeClassPrefillAttemptMultiplier = 2.0;

enum class WorkloadType {
    FILL_UP,    // Only allocate, measure throughput/latency
    SCALE_OUT,  // Inject new nodes mid-run, measure adoption speed
    DSA,        // DSA paired KV+indexer with random fail-triggered eviction
    SIZE_CLASS_CHURN  // Mixed size classes with steady-state churn
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

    // Size-class churn knobs (only used when workload_type ==
    // SIZE_CLASS_CHURN).
    std::string size_class_pattern = "kv_mixed";
    // Bench-only strategy name for size_class_churn ("" = use strategy_type).
    std::string bench_strategy;
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

struct DistributionStats {
    double min = 0.0;
    double p10 = 0.0;
    double p50 = 0.0;
    double p90 = 0.0;
    double p99 = 0.0;
    double max = 0.0;
    double avg = 0.0;
    bool valid = false;
};

struct FragmentationSnapshot {
    uint64_t total_free_space = 0;
    uint64_t largest_free_region = 0;
    uint64_t capacity = 0;
    double fragmentation_ratio = 0.0;
    bool valid = false;
};

struct SizeClassSpec {
    std::string name;
    // Fixed object size, or the inclusive lower bound when max_size > size.
    size_t size;
    int weight;
    // Exclusive upper bound for ranged classes (sizes are drawn log-uniformly
    // from [size, max_size)); 0 means a fixed-size class.
    size_t max_size = 0;
    // Observed mean size for trace-derived classes; 0 derives it from the
    // range.
    double mean_size = 0.0;
};

struct SizeClassStat {
    std::string name;
    size_t size = 0;
    size_t max_size = 0;
    int weight = 0;
    int success_count = 0;
    int partial_count = 0;
    int failed_count = 0;
    int total_count = 0;
    DistributionStats latency_stats;
};

enum class SizeClassAllocationStatus {
    FAILED,
    PARTIAL,
    FULL,
};

struct SizeClassAllocationResult {
    SizeClassAllocationStatus status = SizeClassAllocationStatus::FAILED;
    size_t replica_count = 0;
};

struct SizeClassPrefillStats {
    size_t attempts = 0;
    size_t max_attempts = 0;
    int full_count = 0;
    int partial_count = 0;
    int failed_count = 0;
    int requested_pct = 0;
    double achieved_util_pct = 0.0;
    bool reached_target = false;
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
    int partial_count = 0;  // partial allocations, if reported
    int failed_count = 0;   // failed allocations, if reported

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

struct SizeClassChurnResult : BenchResultBase {
    std::string pattern_name;
    SizeClassPrefillStats prefill_stats;
    DistributionStats fragmentation_stats;
    DistributionStats largest_free_mb_stats;
    FragmentationSnapshot final_fragmentation;
    std::vector<SizeClassStat> size_class_stats;
    // Number of live objects released by --size_class_release_prob.
    int released_count = 0;
    // Large-allocation probe (--probe_mib); attempts == 0 when disabled.
    size_t probe_bytes = 0;
    int probe_attempts = 0;
    int probe_success = 0;
    DistributionStats probe_free_gb_stats;
    DistributionStats probe_largest_free_mb_stats;
    // Event replay: free space observed at each failed put.
    DistributionStats fail_free_gb_stats;
    DistributionStats fail_largest_free_mb_stats;
    DistributionStats fail_size_mb_stats;
    // Event replay: traffic concentration. write_skew = bytes written to the
    // busiest segment / mean bytes per segment over the whole run;
    // window_write_skew = the same ratio per sample window, averaged; the
    // utilization stddev is averaged over the sample points.
    double write_skew = 0.0;
    double window_write_skew = 0.0;
    double mean_util_stddev = 0.0;
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
        for (const auto& registration : *allocators) {
            const auto alloc = registration->GetAllocator();
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
        for (const auto& registration : *allocs) {
            total += registration->GetAllocator()->capacity();
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
        for (const auto& registration : *allocators) {
            const auto alloc = registration->GetAllocator();
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

static DistributionStats computeDistributionStats(std::vector<double>& values) {
    DistributionStats stats;
    if (values.empty()) return stats;

    std::sort(values.begin(), values.end());

    auto percentile = [&](double p) -> double {
        size_t idx = static_cast<size_t>(std::round(p * (values.size() - 1)));
        return values[idx];
    };

    stats.min = values.front();
    stats.p10 = percentile(0.10);
    stats.p50 = percentile(0.50);
    stats.p90 = percentile(0.90);
    stats.p99 = percentile(0.99);
    stats.max = values.back();
    stats.avg =
        std::accumulate(values.begin(), values.end(), 0.0) / values.size();
    stats.valid = true;
    return stats;
}

static FragmentationSnapshot computeFragmentationSnapshot(
    const AllocatorManager& manager) {
    FragmentationSnapshot snapshot;
    double weighted_fragmentation = 0.0;

    for (const auto& name : manager.getNames()) {
        const auto* allocs = manager.getAllocators(name);
        if (!allocs) continue;

        for (const auto& registration : *allocs) {
            auto offset_alloc =
                std::dynamic_pointer_cast<OffsetBufferAllocator>(
                    registration->GetAllocator());
            if (!offset_alloc) continue;

            auto allocator = offset_alloc->getOffsetAllocator();
            if (!allocator) continue;

            auto metrics = allocator->get_metrics();
            snapshot.total_free_space += metrics.total_free_space_;
            snapshot.capacity += metrics.capacity;
            snapshot.largest_free_region = std::max(
                snapshot.largest_free_region, metrics.largest_free_region_);
            if (metrics.total_free_space_ > 0) {
                double local_fragmentation =
                    1.0 - (static_cast<double>(metrics.largest_free_region_) /
                           static_cast<double>(metrics.total_free_space_));
                local_fragmentation = std::clamp(local_fragmentation, 0.0, 1.0);
                weighted_fragmentation +=
                    local_fragmentation * metrics.total_free_space_;
            }
            snapshot.valid = true;
        }
    }

    if (snapshot.valid && snapshot.total_free_space > 0) {
        snapshot.fragmentation_ratio =
            weighted_fragmentation /
            static_cast<double>(snapshot.total_free_space);
    }

    return snapshot;
}

static std::string humanSizeLabel(size_t bytes) {
    static const char* const kUnits[] = {"B", "K", "M", "G", "T"};
    int unit = 0;
    double value = static_cast<double>(bytes);
    while (value >= 1024.0 && unit < 4) {
        value /= 1024.0;
        ++unit;
    }
    std::ostringstream ss;
    if (value == std::floor(value)) {
        ss << static_cast<uint64_t>(value);
    } else {
        ss << std::fixed << std::setprecision(1) << value;
    }
    ss << kUnits[unit];
    return ss.str();
}

// Expected value of a log-uniform draw on [lo, hi).
static double logUniformMean(size_t lo, size_t hi) {
    if (hi <= lo) return static_cast<double>(lo);
    return (static_cast<double>(hi) - static_cast<double>(lo)) /
           std::log(static_cast<double>(hi) / static_cast<double>(lo));
}

static double sizeClassMeanSize(const SizeClassSpec& spec) {
    if (spec.mean_size > 0.0) return spec.mean_size;
    if (spec.max_size > spec.size)
        return logUniformMean(spec.size, spec.max_size);
    return static_cast<double>(spec.size);
}

static size_t sampleSizeClassSize(const SizeClassSpec& spec,
                                  std::mt19937& rng) {
    if (spec.max_size <= spec.size) return spec.size;
    std::uniform_real_distribution<double> dist(
        std::log(static_cast<double>(spec.size)),
        std::log(static_cast<double>(spec.max_size)));
    auto size = static_cast<size_t>(std::exp(dist(rng)));
    return std::clamp(size, spec.size, spec.max_size - 1);
}

// One equally weighted size class per octave over [min_size, max_size). With
// log-uniform sampling inside each class this gives every size octave the
// same share of allocations, which is the most hostile mix for large objects.
static std::vector<SizeClassSpec> buildOctaveSpecs(size_t min_size,
                                                   size_t max_size) {
    std::vector<SizeClassSpec> specs;
    if (min_size == 0 || max_size <= min_size) return specs;
    for (size_t lo = min_size; lo < max_size; lo *= 2) {
        size_t hi = std::min(lo * 2, max_size);
        SizeClassSpec spec;
        spec.name = humanSizeLabel(lo) + "-" + humanSizeLabel(hi);
        spec.size = lo;
        spec.max_size = hi;
        spec.weight = 1;
        specs.push_back(std::move(spec));
    }
    return specs;
}

static size_t sizeClassIndexForSize(const std::vector<SizeClassSpec>& specs,
                                    size_t size) {
    for (size_t i = 0; i < specs.size(); ++i) {
        const auto& spec = specs[i];
        if (spec.max_size > spec.size) {
            if (size >= spec.size && size < spec.max_size) return i;
        } else if (size == spec.size) {
            return i;
        }
    }
    return specs.empty() ? 0 : specs.size() - 1;
}

// Allocation sizes loaded from --rl_trace_file (one size in bytes per line).
static std::vector<size_t> g_trace_sizes;

static bool loadTraceSizes(const std::string& path, std::vector<size_t>& out) {
    std::ifstream in(path);
    if (!in) return false;
    std::string line;
    size_t malformed = 0;
    while (std::getline(in, line)) {
        auto hash = line.find('#');
        if (hash != std::string::npos) line.erase(hash);
        auto begin = line.find_first_not_of(" \t\r");
        if (begin == std::string::npos) continue;
        auto end = line.find_last_not_of(" \t\r");
        line = line.substr(begin, end - begin + 1);
        char* parse_end = nullptr;
        unsigned long long value = std::strtoull(line.c_str(), &parse_end, 10);
        if (parse_end == line.c_str() || *parse_end != '\0' || value == 0) {
            ++malformed;
            continue;
        }
        out.push_back(static_cast<size_t>(value));
    }
    if (malformed > 0) {
        std::cout << "Ignored " << malformed << " malformed trace line(s) in "
                  << path << std::endl;
    }
    return true;
}

// Trace-derived classes: one per power-of-two octave covering the observed
// range, weighted by observed counts so the per-class breakdown and the
// prefill budget reflect the recorded distribution.
static std::vector<SizeClassSpec> buildTraceSpecs(
    const std::vector<size_t>& sizes) {
    if (sizes.empty()) return {};
    const size_t min_size = *std::min_element(sizes.begin(), sizes.end());
    const size_t max_size = *std::max_element(sizes.begin(), sizes.end());
    size_t lo = 1;
    while (lo * 2 <= min_size) lo *= 2;
    size_t hi = lo;
    while (hi <= max_size) hi *= 2;
    auto octaves = buildOctaveSpecs(lo, hi);

    std::vector<double> sums(octaves.size(), 0.0);
    std::vector<int> counts(octaves.size(), 0);
    for (size_t size : sizes) {
        size_t idx = sizeClassIndexForSize(octaves, size);
        sums[idx] += static_cast<double>(size);
        ++counts[idx];
    }

    std::vector<SizeClassSpec> specs;
    for (size_t i = 0; i < octaves.size(); ++i) {
        if (counts[i] == 0) continue;
        octaves[i].weight = counts[i];
        octaves[i].mean_size = sums[i] / counts[i];
        specs.push_back(octaves[i]);
    }
    return specs;
}

static std::vector<SizeClassSpec> getSizeClassSpecs(
    const std::string& pattern_name) {
    if (pattern_name == "rl") {
        const size_t min_size =
            static_cast<size_t>(std::max<int64_t>(FLAGS_rl_min_kib, 0)) * KiB;
        const size_t max_size =
            static_cast<size_t>(std::max<int64_t>(FLAGS_rl_max_mib, 0)) * MiB;
        return buildOctaveSpecs(min_size, max_size);
    }

    if (pattern_name == "trace") {
        return buildTraceSpecs(g_trace_sizes);
    }

    if (pattern_name == "kv_mixed") {
        return {
            {"small", 4 * KiB, 70},
            {"medium", 256 * KiB, 20},
            {"large", kDsaKvSize, 10},
        };
    }

    if (pattern_name == "dsa_pair") {
        return {
            {"kv", kDsaKvSize, 50},
            {"indexer", kDsaIndexerSize, 50},
        };
    }

    return {};
}

static size_t chooseSizeClassIndex(const std::vector<SizeClassSpec>& specs,
                                   std::mt19937& rng) {
    if (specs.empty()) return 0;

    int total_weight = 0;
    for (const auto& spec : specs) {
        total_weight += spec.weight;
    }

    if (total_weight <= 0) return 0;

    std::uniform_int_distribution<int> dist(1, total_weight);
    int pick = dist(rng);
    for (size_t i = 0; i < specs.size(); ++i) {
        pick -= specs[i].weight;
        if (pick <= 0) return i;
    }

    return specs.size() - 1;
}

struct SizeClassSample {
    size_t class_idx = 0;
    size_t size = 0;
};

// Draws allocation sizes either from the weighted synthetic size classes or,
// when a trace is attached, by replaying the recorded sizes in order.
struct SizeClassSampler {
    std::vector<SizeClassSpec> specs;
    const std::vector<size_t>* trace = nullptr;
    size_t cursor = 0;

    SizeClassSample next(std::mt19937& rng) {
        if (trace != nullptr && !trace->empty()) {
            size_t size = (*trace)[cursor % trace->size()];
            ++cursor;
            return {sizeClassIndexForSize(specs, size), size};
        }
        size_t idx = chooseSizeClassIndex(specs, rng);
        if (specs.empty()) return {};
        return {idx, sampleSizeClassSize(specs[idx], rng)};
    }
};

static double computeWeightedAverageObjectSize(
    const std::vector<SizeClassSpec>& specs) {
    double weighted_size = 0.0;
    int total_weight = 0;

    for (const auto& spec : specs) {
        if (spec.weight <= 0) continue;
        weighted_size += sizeClassMeanSize(spec) * spec.weight;
        total_weight += spec.weight;
    }

    if (total_weight <= 0) return 0.0;
    return static_cast<double>(weighted_size / total_weight);
}

static size_t deriveSizeClassPrefillMaxAttempts(
    const AllocatorManager& manager, const BenchConfig& cfg,
    const std::vector<SizeClassSpec>& specs) {
    if (cfg.prefill_pct <= 0 || cfg.replica_num <= 0) return 0;

    const double avg_object_size = computeWeightedAverageObjectSize(specs);
    if (avg_object_size <= 0.0) return kSizeClassMinPrefillAttempts;

    const double target_bytes =
        static_cast<double>(computeTotalCapacity(manager)) * cfg.prefill_pct /
        100.0;
    const double bytes_per_attempt =
        avg_object_size * static_cast<double>(cfg.replica_num);
    if (target_bytes <= 0.0 || bytes_per_attempt <= 0.0) {
        return kSizeClassMinPrefillAttempts;
    }

    const double derived_attempts =
        std::ceil((target_bytes / bytes_per_attempt) *
                  kSizeClassPrefillAttemptMultiplier);
    return std::max(kSizeClassMinPrefillAttempts,
                    static_cast<size_t>(derived_attempts));
}

// Largest contiguous free region of a segment (max over its allocators).
static size_t segmentLargestFree(const AllocatorManager& manager,
                                 const std::string& name) {
    const auto* allocators = manager.getAllocators(name);
    if (!allocators) return 0;
    size_t largest = 0;
    for (const auto& registration : *allocators) {
        const auto alloc = registration->GetAllocator();
        if (!alloc) continue;
        size_t v = alloc->getLargestFreeRegion();
        if (v == kAllocatorUnknownFreeSpace) continue;
        largest = std::max(largest, v);
    }
    return largest;
}

/**
 * @brief Bench-only placement strategy: score every segment for the request
 *        and allocate in descending score order. A score of -infinity marks
 *        a segment as ineligible. Unlike RankedAllocationStrategy this ranks
 *        all segments (no candidate sampling) and has no random fallback, so
 *        the effect of the placement rule is measured in isolation.
 */
class BenchScoredStrategy : public RandomAllocationStrategy {
   public:
    using ScoreFn = std::function<double(const AllocatorManager&,
                                         const std::string&, size_t)>;
    explicit BenchScoredStrategy(ScoreFn score) : score_(std::move(score)) {}

    tl::expected<std::vector<Replica>, ErrorCode> Allocate(
        const AllocatorManager& manager, const size_t slice_length,
        const size_t replica_num = 1,
        const std::vector<std::string>& preferred_segments = {},
        const std::set<std::string>& excluded_segments = {},
        const ReplicaType replica_type = ReplicaType::MEMORY) override {
        (void)preferred_segments;
        if (slice_length == 0 || replica_num == 0) {
            return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
        }
        const auto& names = manager.getNames();
        struct Candidate {
            size_t idx;
            double score;
        };
        std::vector<Candidate> candidates;
        candidates.reserve(names.size());
        for (size_t i = 0; i < names.size(); ++i) {
            if (excluded_segments.contains(names[i])) continue;
            double s = score_(manager, names[i], slice_length);
            if (s == -std::numeric_limits<double>::infinity()) continue;
            candidates.push_back({i, s});
        }
        std::stable_sort(candidates.begin(), candidates.end(),
                         [](const Candidate& a, const Candidate& b) {
                             return a.score > b.score;
                         });
        std::vector<Replica> replicas;
        for (const auto& c : candidates) {
            if (replicas.size() >= replica_num) break;
            auto buffer = allocateSingle(manager, names[c.idx], slice_length);
            if (buffer) {
                replicas.emplace_back(std::move(buffer),
                                      ReplicaStatus::PROCESSING, replica_type);
            }
        }
        if (replicas.empty()) {
            return tl::make_unexpected(ErrorCode::NO_AVAILABLE_HANDLE);
        }
        return replicas;
    }

   private:
    ScoreFn score_;
};

static constexpr double kIneligible = -std::numeric_limits<double>::infinity();

// largest_hole_first: put the object into the segment with the largest
// contiguous free region (the fragmentation_aware idea, ranked over all
// segments).
static double scoreLargestHoleFirst(const AllocatorManager& m,
                                    const std::string& name, size_t size) {
    size_t largest = segmentLargestFree(m, name);
    return largest >= size ? static_cast<double>(largest) : kIneligible;
}

// best_fit: put the object into the segment whose largest hole is the
// smallest one that still fits, so big holes are kept for big objects.
static double scoreBestFit(const AllocatorManager& m, const std::string& name,
                           size_t size) {
    size_t largest = segmentLargestFree(m, name);
    return largest >= size ? -static_cast<double>(largest - size) : kIneligible;
}

// reserved: the last --reserved_segments segments accept only objects of at
// least --large_object_mib; all other segments accept only smaller objects.
// Within each class the choice is random, like RandomAllocationStrategy.
static double scoreReserved(const AllocatorManager& m, const std::string& name,
                            size_t size) {
    const auto& names = m.getNames();
    const size_t reserved =
        std::min<size_t>(std::max(FLAGS_reserved_segments, 0), names.size());
    size_t idx = std::find(names.begin(), names.end(), name) - names.begin();
    const bool is_reserved = idx + reserved >= names.size();
    const bool is_large =
        size >= static_cast<size_t>(std::max(FLAGS_large_object_mib, 0)) * MiB;
    if (is_reserved != is_large) return kIneligible;
    if (segmentLargestFree(m, name) < size) return kIneligible;
    return static_cast<double>(std::rand()) / RAND_MAX;
}

// best_fit_bucketed: best fit with slack quantized into buckets; ties inside
// a bucket are broken at random to spread traffic.
static double scoreBestFitBucketed(const AllocatorManager& m,
                                   const std::string& name, size_t size) {
    size_t largest = segmentLargestFree(m, name);
    if (largest < size) return kIneligible;
    const double bucket =
        static_cast<double>(std::max(FLAGS_best_fit_bucket_mib, 1)) * MiB;
    const double slack_bucket = std::floor((largest - size) / bucket);
    return -slack_bucket + static_cast<double>(std::rand()) / RAND_MAX * 0.5;
}

// hybrid: small objects best-fit into the tightest segment, large objects go
// to the segment with the largest hole.
static double scoreHybrid(const AllocatorManager& m, const std::string& name,
                          size_t size) {
    const bool is_large =
        size >= static_cast<size_t>(std::max(FLAGS_large_object_mib, 0)) * MiB;
    return is_large ? scoreLargestHoleFirst(m, name, size)
                    : scoreBestFit(m, name, size);
}

static std::shared_ptr<AllocationStrategy> createBenchStrategy(
    const BenchConfig& cfg, LocalSsdManager& local_ssd) {
    const std::string& s = cfg.bench_strategy;
    if (s.empty() || s == "random") {
        return CreateAllocationStrategy(AllocationStrategyType::RANDOM,
                                        local_ssd);
    }
    if (s == "free_ratio_first") {
        return CreateAllocationStrategy(
            AllocationStrategyType::FREE_RATIO_FIRST, local_ssd);
    }
    if (s == "largest_hole_first") {
        return std::make_shared<BenchScoredStrategy>(scoreLargestHoleFirst);
    }
    if (s == "best_fit") {
        return CreateAllocationStrategy(AllocationStrategyType::BEST_FIT,
                                        local_ssd);
    }
    if (s == "reserved") {
        return std::make_shared<BenchScoredStrategy>(scoreReserved);
    }
    if (s == "hybrid") {
        return std::make_shared<BenchScoredStrategy>(scoreHybrid);
    }
    if (s == "best_fit_bucketed") {
        return std::make_shared<BenchScoredStrategy>(scoreBestFitBucketed);
    }
    return nullptr;
}

static std::string benchStrategyLabel(const std::string& s) {
    if (s == "random") return "Random";
    if (s == "free_ratio_first") return "FreeRatioFirst";
    if (s == "largest_hole_first") return "LargestHole";
    if (s == "best_fit") return "BestFit";
    if (s == "reserved") return "Reserved";
    if (s == "hybrid") return "Hybrid";
    if (s == "best_fit_bucketed") return "BestFitBucket";
    return s;
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
    LocalSsdManager local_ssd;
    auto strategy = CreateAllocationStrategy(cfg.strategy_type, local_ssd);

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
    LocalSsdManager local_ssd;
    auto strategy = CreateAllocationStrategy(cfg.strategy_type, local_ssd);

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

// Release exactly one random live object (free-one/allocate-one churn).
static void releaseRandomOne(std::vector<std::vector<Replica>>& live,
                             std::mt19937& rng) {
    if (live.empty()) return;
    std::uniform_int_distribution<size_t> dist(0, live.size() - 1);
    size_t idx = dist(rng);
    std::swap(live[idx], live.back());
    live.pop_back();  // Replica destructor returns memory to allocator.
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

static SizeClassAllocationResult sizeClassAllocateWithEvict(
    const std::shared_ptr<AllocationStrategy>& strategy,
    AllocatorManager& manager, size_t size, int replica_num,
    std::vector<std::vector<Replica>>& live, std::mt19937& rng,
    int& evict_count, double evict_ratio) {
    for (int attempt = 0; attempt <= kSizeClassMaxRetries; ++attempt) {
        auto result = strategy->Allocate(manager, size, replica_num);
        if (result.has_value()) {
            size_t replica_count = result->size();
            if (replica_count == 0) {
                return {};
            }
            live.push_back(std::move(result.value()));
            return {
                replica_count == static_cast<size_t>(replica_num)
                    ? SizeClassAllocationStatus::FULL
                    : SizeClassAllocationStatus::PARTIAL,
                replica_count,
            };
        }

        if (live.empty()) return {};
        if (attempt == kSizeClassMaxRetries) return {};

        evictRandomFraction(live, evict_ratio, rng);
        ++evict_count;
    }

    return {};
}

static SizeClassPrefillStats prefillSizeClassChurn(
    const std::shared_ptr<AllocationStrategy>& strategy,
    AllocatorManager& manager, const BenchConfig& cfg,
    SizeClassSampler& sampler,
    std::vector<std::vector<Replica>>& live_allocations, std::mt19937& rng) {
    SizeClassPrefillStats stats;
    if (cfg.prefill_pct <= 0 || sampler.specs.empty()) return stats;
    stats.requested_pct = cfg.prefill_pct;
    stats.max_attempts =
        deriveSizeClassPrefillMaxAttempts(manager, cfg, sampler.specs);

    int consec_failures = 0;
    int evict_throwaway = 0;
    const int kMaxConsecFailures = 10;

    while (stats.attempts < stats.max_attempts) {
        if (stats.attempts % kPreFillSampleInterval == 0) {
            stats.achieved_util_pct = computeAverageUtilAll(manager) * 100.0;
            if (stats.achieved_util_pct >= cfg.prefill_pct) {
                stats.reached_target = true;
                break;
            }
        }

        auto sample = sampler.next(rng);
        auto alloc_result = sizeClassAllocateWithEvict(
            strategy, manager, sample.size, cfg.replica_num, live_allocations,
            rng, evict_throwaway, FLAGS_size_class_evict_ratio);
        ++stats.attempts;
        if (alloc_result.status == SizeClassAllocationStatus::FULL) {
            ++stats.full_count;
            consec_failures = 0;
        } else if (alloc_result.status == SizeClassAllocationStatus::PARTIAL) {
            ++stats.partial_count;
            consec_failures = 0;
        } else {
            ++stats.failed_count;
            if (++consec_failures >= kMaxConsecFailures) {
                break;
            }
        }
    }

    stats.achieved_util_pct = computeAverageUtilAll(manager) * 100.0;
    if (stats.achieved_util_pct >= cfg.prefill_pct) {
        stats.reached_target = true;
    }
    return stats;
}

// Run DSA workload; the allocation count is derived from cluster capacity.
static FillUpResult runDsaBenchmark(const BenchConfig& cfg) {
    AllocatorManager manager =
        createCluster(cfg.num_segments, cfg.segment_capacity, cfg.skewed);
    LocalSsdManager local_ssd;
    auto strategy = CreateAllocationStrategy(cfg.strategy_type, local_ssd);

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

static SizeClassChurnResult runSizeClassChurnBenchmark(const BenchConfig& cfg) {
    AllocatorManager manager =
        createCluster(cfg.num_segments, cfg.segment_capacity, cfg.skewed);
    LocalSsdManager local_ssd;
    auto strategy = createBenchStrategy(cfg, local_ssd);
    SizeClassSampler sampler;
    sampler.specs = getSizeClassSpecs(cfg.size_class_pattern);
    if (cfg.size_class_pattern == "trace") {
        sampler.trace = &g_trace_sizes;
    }
    const auto& specs = sampler.specs;

    std::vector<SizeClassStat> per_class_stats;
    per_class_stats.reserve(specs.size());
    std::vector<std::vector<double>> per_class_latencies(specs.size());
    for (const auto& spec : specs) {
        SizeClassStat stat;
        stat.name = spec.name;
        stat.size = spec.size;
        stat.max_size = spec.max_size;
        stat.weight = spec.weight;
        per_class_stats.push_back(std::move(stat));
    }

    const size_t probe_bytes =
        static_cast<size_t>(std::max(FLAGS_probe_mib, 0)) * MiB;
    const double release_prob =
        std::clamp(FLAGS_size_class_release_prob, 0.0, 1.0);
    std::uniform_real_distribution<double> release_dist(0.0, 1.0);
    std::vector<double> probe_free_gb_samples;
    std::vector<double> probe_largest_free_mb_samples;
    int probe_attempts = 0;
    int probe_success = 0;
    int released_count = 0;

    std::vector<double> latencies;
    latencies.reserve(cfg.num_allocations);
    int sample_interval = std::max(1, FLAGS_convergence_sample_interval);
    std::vector<double> fragmentation_samples;
    fragmentation_samples.reserve(cfg.num_allocations / sample_interval + 2);
    std::vector<double> largest_free_mb_samples;
    largest_free_mb_samples.reserve(fragmentation_samples.capacity());

    std::vector<std::vector<Replica>> live_allocations;
    live_allocations.reserve(std::min(cfg.num_allocations, 1 << 20));

    std::mt19937 rng(42);
    SizeClassPrefillStats prefill_stats = prefillSizeClassChurn(
        strategy, manager, cfg, sampler, live_allocations, rng);

    int success_count = 0;
    int partial_count = 0;
    int failed_count = 0;
    int total_count = 0;
    int evict_count = 0;
    double instrumentation_time_us = 0.0;

    auto total_start = std::chrono::high_resolution_clock::now();

    for (int i = 0; i < cfg.num_allocations; ++i) {
        if (release_prob > 0.0 && !live_allocations.empty() &&
            release_dist(rng) < release_prob) {
            releaseRandomOne(live_allocations, rng);
            ++released_count;
        }

        auto sample = sampler.next(rng);
        const size_t class_idx = sample.class_idx;

        auto t0 = std::chrono::high_resolution_clock::now();
        auto alloc_result = sizeClassAllocateWithEvict(
            strategy, manager, sample.size, cfg.replica_num, live_allocations,
            rng, evict_count, FLAGS_size_class_evict_ratio);
        auto t1 = std::chrono::high_resolution_clock::now();

        double latency_ns =
            std::chrono::duration<double, std::nano>(t1 - t0).count();
        latencies.push_back(latency_ns);
        per_class_latencies[class_idx].push_back(latency_ns);

        ++total_count;
        ++per_class_stats[class_idx].total_count;
        if (alloc_result.status == SizeClassAllocationStatus::FULL) {
            ++success_count;
            ++per_class_stats[class_idx].success_count;
        } else if (alloc_result.status == SizeClassAllocationStatus::PARTIAL) {
            ++partial_count;
            ++per_class_stats[class_idx].partial_count;
        } else {
            ++failed_count;
            ++per_class_stats[class_idx].failed_count;
        }

        if ((i + 1) % sample_interval == 0 || i == cfg.num_allocations - 1) {
            auto s0 = std::chrono::high_resolution_clock::now();
            auto snapshot = computeFragmentationSnapshot(manager);
            if (snapshot.valid) {
                fragmentation_samples.push_back(snapshot.fragmentation_ratio);
                largest_free_mb_samples.push_back(
                    static_cast<double>(snapshot.largest_free_region) / MiB);
            }
            if (probe_bytes > 0 && snapshot.valid) {
                // Single replica, no eviction retry; the result is dropped at
                // the end of this block so the probe never changes the pool.
                auto probe = strategy->Allocate(manager, probe_bytes, 1);
                ++probe_attempts;
                if (probe.has_value() && !probe->empty()) ++probe_success;
                probe_free_gb_samples.push_back(
                    static_cast<double>(snapshot.total_free_space) / GiB);
                probe_largest_free_mb_samples.push_back(
                    static_cast<double>(snapshot.largest_free_region) / MiB);
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
    total_us = std::max(total_us, 1.0);

    for (size_t i = 0; i < per_class_stats.size(); ++i) {
        per_class_stats[i].latency_stats =
            computeDistributionStats(per_class_latencies[i]);
    }

    SizeClassChurnResult res;
    res.strategy_name = cfg.strategy_name;
    res.num_segments = cfg.num_segments;
    res.alloc_size = 0;
    res.replica_num = cfg.replica_num;
    res.skewed = cfg.skewed;
    res.cluster_capacity_gb = computeClusterCapacityGB(
        cfg.num_segments, cfg.segment_capacity, cfg.skewed);
    res.final_util_stddev = computeUtilizationStdDev(manager);
    res.final_avg_util = computeAverageUtilAll(manager);
    res.success_count = success_count;
    res.partial_count = partial_count;
    res.failed_count = failed_count;
    res.total_count = total_count;
    res.evict_count = evict_count;
    res.pattern_name = cfg.size_class_pattern;
    res.prefill_stats = prefill_stats;
    res.fragmentation_stats = computeDistributionStats(fragmentation_samples);
    res.largest_free_mb_stats =
        computeDistributionStats(largest_free_mb_samples);
    res.final_fragmentation = computeFragmentationSnapshot(manager);
    res.size_class_stats = std::move(per_class_stats);
    res.released_count = released_count;
    res.probe_bytes = probe_bytes;
    res.probe_attempts = probe_attempts;
    res.probe_success = probe_success;
    res.probe_free_gb_stats = computeDistributionStats(probe_free_gb_samples);
    res.probe_largest_free_mb_stats =
        computeDistributionStats(probe_largest_free_mb_samples);
    computeLatencyStats(latencies, total_us, total_count, res);
    return res;
}

struct TraceEvent {
    char kind;  // 'p' put, 'r' remove, 'e' evict
    std::string key;
    size_t size;
};

static std::vector<TraceEvent> g_trace_events;

static bool loadTraceEvents(const std::string& path,
                            std::vector<TraceEvent>& out) {
    std::ifstream in(path);
    if (!in) return false;
    std::string line;
    size_t malformed = 0;
    while (std::getline(in, line)) {
        if (line.empty() || line[0] == '#') continue;
        // Format: "<kind> <key> <size>"; keys may contain spaces.
        auto first = line.find(' ');
        auto last = line.rfind(' ');
        if (first == std::string::npos || last == first) {
            ++malformed;
            continue;
        }
        std::string kind = line.substr(0, first);
        std::string key = line.substr(first + 1, last - first - 1);
        char* end = nullptr;
        unsigned long long size =
            std::strtoull(line.c_str() + last + 1, &end, 10);
        if (end == line.c_str() + last + 1 || *end != '\0') {
            ++malformed;
            continue;
        }
        char k = kind == "put"      ? 'p'
                 : kind == "remove" ? 'r'
                 : kind == "evict"  ? 'e'
                                    : 0;
        if (k == 0) {
            ++malformed;
            continue;
        }
        out.push_back({k, std::move(key), static_cast<size_t>(size)});
    }
    if (malformed > 0) {
        std::cout << "Ignored " << malformed << " malformed event line(s) in "
                  << path << std::endl;
    }
    return true;
}

// Replays put/remove/evict events with the real object lifetimes. No
// eviction retry: a failed put is counted and its free-space picture is
// recorded, exactly like action=put_start_alloc_failed in the master log.
static SizeClassChurnResult runTraceEventsBenchmark(const BenchConfig& cfg) {
    AllocatorManager manager =
        createCluster(cfg.num_segments, cfg.segment_capacity, cfg.skewed);
    LocalSsdManager local_ssd;
    auto strategy = createBenchStrategy(cfg, local_ssd);

    std::vector<size_t> put_sizes;
    for (const auto& ev : g_trace_events) {
        if (ev.kind == 'p') put_sizes.push_back(ev.size);
    }
    std::vector<SizeClassSpec> specs = buildTraceSpecs(put_sizes);
    std::vector<SizeClassStat> per_class_stats;
    std::vector<std::vector<double>> per_class_latencies(specs.size());
    for (const auto& spec : specs) {
        SizeClassStat stat;
        stat.name = spec.name;
        stat.size = spec.size;
        stat.max_size = spec.max_size;
        stat.weight = spec.weight;
        per_class_stats.push_back(std::move(stat));
    }

    const size_t probe_bytes =
        static_cast<size_t>(std::max(FLAGS_probe_mib, 0)) * MiB;
    const int sample_interval = std::max(1, FLAGS_convergence_sample_interval);
    const int repeat = std::max(1, FLAGS_trace_events_repeat);

    std::unordered_map<std::string, std::vector<Replica>> live;
    live.reserve(put_sizes.size());
    std::unordered_map<std::string, double> bytes_per_segment;
    std::unordered_map<std::string, double> window_bytes_per_segment;
    std::vector<double> window_skews;
    std::vector<double> util_stddev_samples;
    auto account = [&](const std::vector<Replica>& replicas, size_t size) {
        for (const auto& replica : replicas) {
            for (const auto& name : replica.get_segment_names()) {
                if (!name) continue;
                bytes_per_segment[*name] += static_cast<double>(size);
                window_bytes_per_segment[*name] += static_cast<double>(size);
            }
        }
    };
    auto skew_of = [&](const std::unordered_map<std::string, double>& m) {
        if (m.empty()) return 0.0;
        double total = 0.0, peak = 0.0;
        for (const auto& [k, v] : m) {
            total += v;
            peak = std::max(peak, v);
        }
        const double mean = total / static_cast<double>(cfg.num_segments);
        return mean > 0.0 ? peak / mean : 0.0;
    };
    std::vector<double> latencies;
    latencies.reserve(put_sizes.size() * repeat);
    std::vector<double> fragmentation_samples, largest_free_mb_samples;
    std::vector<double> probe_free_gb_samples, probe_largest_free_mb_samples;
    std::vector<double> fail_free_gb, fail_largest_free_mb, fail_size_mb;
    int success_count = 0, partial_count = 0, failed_count = 0, total_count = 0,
        released_count = 0, probe_attempts = 0, probe_success = 0;
    double instrumentation_time_us = 0.0;

    auto total_start = std::chrono::high_resolution_clock::now();
    for (int pass = 0; pass < repeat; ++pass) {
        const std::string suffix = repeat > 1 ? "#" + std::to_string(pass) : "";
        for (const auto& ev : g_trace_events) {
            if (ev.kind != 'p') {
                auto it = live.find(ev.key + suffix);
                if (it != live.end()) {
                    live.erase(it);  // Replica destructors free the memory.
                    ++released_count;
                }
                continue;
            }
            auto t0 = std::chrono::high_resolution_clock::now();
            auto result = strategy->Allocate(manager, ev.size, cfg.replica_num);
            auto t1 = std::chrono::high_resolution_clock::now();
            double latency_ns =
                std::chrono::duration<double, std::nano>(t1 - t0).count();
            latencies.push_back(latency_ns);
            size_t class_idx = sizeClassIndexForSize(specs, ev.size);
            per_class_latencies[class_idx].push_back(latency_ns);
            ++total_count;
            ++per_class_stats[class_idx].total_count;
            if (result.has_value() && !result->empty()) {
                if (result->size() == static_cast<size_t>(cfg.replica_num)) {
                    ++success_count;
                    ++per_class_stats[class_idx].success_count;
                } else {
                    ++partial_count;
                    ++per_class_stats[class_idx].partial_count;
                }
                account(result.value(), ev.size);
                live[ev.key + suffix] = std::move(result.value());
            } else {
                ++failed_count;
                ++per_class_stats[class_idx].failed_count;
                auto s0 = std::chrono::high_resolution_clock::now();
                auto snap = computeFragmentationSnapshot(manager);
                fail_free_gb.push_back(snap.total_free_space / GiB);
                fail_largest_free_mb.push_back(
                    static_cast<double>(snap.largest_free_region) / MiB);
                fail_size_mb.push_back(static_cast<double>(ev.size) / MiB);
                auto s1 = std::chrono::high_resolution_clock::now();
                instrumentation_time_us +=
                    std::chrono::duration<double, std::micro>(s1 - s0).count();
            }
            if (total_count % sample_interval == 0) {
                auto s0 = std::chrono::high_resolution_clock::now();
                auto snapshot = computeFragmentationSnapshot(manager);
                util_stddev_samples.push_back(
                    computeUtilizationStdDev(manager));
                window_skews.push_back(skew_of(window_bytes_per_segment));
                window_bytes_per_segment.clear();
                if (snapshot.valid) {
                    fragmentation_samples.push_back(
                        snapshot.fragmentation_ratio);
                    largest_free_mb_samples.push_back(
                        static_cast<double>(snapshot.largest_free_region) /
                        MiB);
                    if (probe_bytes > 0) {
                        auto probe =
                            strategy->Allocate(manager, probe_bytes, 1);
                        ++probe_attempts;
                        if (probe.has_value() && !probe->empty())
                            ++probe_success;
                        probe_free_gb_samples.push_back(
                            static_cast<double>(snapshot.total_free_space) /
                            GiB);
                        probe_largest_free_mb_samples.push_back(
                            static_cast<double>(snapshot.largest_free_region) /
                            MiB);
                    }
                }
                auto s1 = std::chrono::high_resolution_clock::now();
                instrumentation_time_us +=
                    std::chrono::duration<double, std::micro>(s1 - s0).count();
            }
        }
    }
    auto total_end = std::chrono::high_resolution_clock::now();
    double total_us =
        std::chrono::duration<double, std::micro>(total_end - total_start)
            .count();
    total_us = std::max(total_us - instrumentation_time_us, 1.0);

    for (size_t i = 0; i < per_class_stats.size(); ++i) {
        per_class_stats[i].latency_stats =
            computeDistributionStats(per_class_latencies[i]);
    }

    SizeClassChurnResult res;
    res.strategy_name = cfg.strategy_name;
    res.num_segments = cfg.num_segments;
    res.alloc_size = 0;
    res.replica_num = cfg.replica_num;
    res.skewed = cfg.skewed;
    res.cluster_capacity_gb = computeClusterCapacityGB(
        cfg.num_segments, cfg.segment_capacity, cfg.skewed);
    res.final_util_stddev = computeUtilizationStdDev(manager);
    res.final_avg_util = computeAverageUtilAll(manager);
    res.success_count = success_count;
    res.partial_count = partial_count;
    res.failed_count = failed_count;
    res.total_count = total_count;
    res.pattern_name = "events";
    res.fragmentation_stats = computeDistributionStats(fragmentation_samples);
    res.largest_free_mb_stats =
        computeDistributionStats(largest_free_mb_samples);
    res.final_fragmentation = computeFragmentationSnapshot(manager);
    res.size_class_stats = std::move(per_class_stats);
    res.released_count = released_count;
    res.probe_bytes = probe_bytes;
    res.probe_attempts = probe_attempts;
    res.probe_success = probe_success;
    res.probe_free_gb_stats = computeDistributionStats(probe_free_gb_samples);
    res.probe_largest_free_mb_stats =
        computeDistributionStats(probe_largest_free_mb_samples);
    res.fail_free_gb_stats = computeDistributionStats(fail_free_gb);
    res.fail_largest_free_mb_stats =
        computeDistributionStats(fail_largest_free_mb);
    res.fail_size_mb_stats = computeDistributionStats(fail_size_mb);
    res.write_skew = skew_of(bytes_per_segment);
    if (!window_skews.empty()) {
        res.window_write_skew =
            std::accumulate(window_skews.begin(), window_skews.end(), 0.0) /
            window_skews.size();
    }
    if (!util_stddev_samples.empty()) {
        res.mean_util_stddev = std::accumulate(util_stddev_samples.begin(),
                                               util_stddev_samples.end(), 0.0) /
                               util_stddev_samples.size();
    }
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

static void printSizeClassChurnHeader() {
    std::cout << std::string(260, '-') << std::endl;
    std::cout << std::left << std::setw(18) << "Strategy" << std::setw(9)
              << "Replica" << std::setw(10) << "Segments" << std::setw(14)
              << "Pattern" << std::setw(12) << "Cluster(GB)" << std::setw(8)
              << "Skewed" << std::right << std::setw(14) << "Throughput"
              << std::setw(12) << "Avg(ns)" << std::setw(12) << "P50(ns)"
              << std::setw(12) << "P90(ns)" << std::setw(12) << "P99(ns)"
              << std::setw(12) << "Frag_avg" << std::setw(12) << "Frag_p50"
              << std::setw(12) << "Frag_p90" << std::setw(12) << "Frag_p99"
              << std::setw(15) << "LargestFreeMB" << std::setw(10) << "AvgUtil%"
              << std::setw(24) << "Full/Partial/Fail/Total" << std::setw(14)
              << "Evictions" << std::endl;
    std::cout << std::string(260, '-') << std::endl;
}

static void printSizeClassChurnResult(const SizeClassChurnResult& r) {
    std::string alloc_ratio = std::to_string(r.success_count) + "/" +
                              std::to_string(r.partial_count) + "/" +
                              std::to_string(r.failed_count) + "/" +
                              std::to_string(r.total_count);
    std::ostringstream cap_ss;
    cap_ss << std::fixed << std::setprecision(1) << r.cluster_capacity_gb;

    double final_largest_free_mb =
        static_cast<double>(r.final_fragmentation.largest_free_region) / MiB;

    std::cout << std::left << std::setw(18) << r.strategy_name << std::setw(9)
              << r.replica_num << std::setw(10) << r.num_segments
              << std::setw(14) << r.pattern_name << std::setw(12)
              << cap_ss.str() << std::setw(8) << (r.skewed ? "yes" : "no")
              << std::right << std::fixed << std::setprecision(0)
              << std::setw(14) << r.throughput << std::setw(12) << r.avg_ns
              << std::setw(12) << r.p50_ns << std::setw(12) << r.p90_ns
              << std::setw(12) << r.p99_ns << std::setprecision(4)
              << std::setw(12) << r.fragmentation_stats.avg << std::setw(12)
              << r.fragmentation_stats.p50 << std::setw(12)
              << r.fragmentation_stats.p90 << std::setw(12)
              << r.fragmentation_stats.p99 << std::setprecision(1)
              << std::setw(15) << final_largest_free_mb << std::setprecision(2)
              << std::setw(9) << (r.final_avg_util * 100.0) << "%"
              << std::setw(24) << alloc_ratio << std::setw(14) << r.evict_count
              << std::endl;

    std::cout << "Prefill summary [" << r.strategy_name
              << ", pattern=" << r.pattern_name
              << ", segments=" << r.num_segments
              << ", replica=" << r.replica_num
              << "]: requested_pct=" << std::fixed << std::setprecision(2)
              << r.prefill_stats.requested_pct
              << ", achieved_pct=" << r.prefill_stats.achieved_util_pct
              << ", reached=" << (r.prefill_stats.reached_target ? "yes" : "no")
              << ", attempts=" << r.prefill_stats.attempts << "/"
              << r.prefill_stats.max_attempts
              << ", full/partial/failed=" << r.prefill_stats.full_count << "/"
              << r.prefill_stats.partial_count << "/"
              << r.prefill_stats.failed_count << std::endl;

    std::cout << "Fragmentation summary [" << r.strategy_name
              << ", pattern=" << r.pattern_name
              << ", segments=" << r.num_segments
              << ", replica=" << r.replica_num
              << ", skewed=" << (r.skewed ? "yes" : "no")
              << "]: avg=" << std::fixed << std::setprecision(4)
              << r.fragmentation_stats.avg
              << ", p50=" << r.fragmentation_stats.p50
              << ", p90=" << r.fragmentation_stats.p90
              << ", p99=" << r.fragmentation_stats.p99
              << ", max=" << r.fragmentation_stats.max
              << ", final_largest_free=" << std::setprecision(1)
              << final_largest_free_mb << " MB"
              << ", released=" << r.released_count << std::endl;

    if (r.pattern_name == "events") {
        std::cout << "Traffic summary [" << r.strategy_name
                  << ", pattern=" << r.pattern_name
                  << ", segments=" << r.num_segments
                  << ", replica=" << r.replica_num
                  << "]: write_skew(max/mean)=" << std::fixed
                  << std::setprecision(2) << r.write_skew
                  << ", window_write_skew=" << r.window_write_skew
                  << ", mean_util_stddev=" << std::setprecision(4)
                  << r.mean_util_stddev << std::endl;
    }

    if (r.failed_count > 0 && r.fail_size_mb_stats.valid) {
        std::cout << "Failure summary [" << r.strategy_name
                  << ", pattern=" << r.pattern_name
                  << ", segments=" << r.num_segments
                  << ", replica=" << r.replica_num
                  << "]: failed_puts=" << r.failed_count
                  << ", failed_size_mb min/p50/max=" << std::fixed
                  << std::setprecision(1) << r.fail_size_mb_stats.min << "/"
                  << r.fail_size_mb_stats.p50 << "/" << r.fail_size_mb_stats.max
                  << ", free_at_failure_gb avg/min/max=" << std::setprecision(2)
                  << r.fail_free_gb_stats.avg << "/" << r.fail_free_gb_stats.min
                  << "/" << r.fail_free_gb_stats.max
                  << ", largest_free_mb_at_failure p50/max="
                  << std::setprecision(1) << r.fail_largest_free_mb_stats.p50
                  << "/" << r.fail_largest_free_mb_stats.max << std::endl;
    }

    if (r.probe_bytes > 0) {
        std::cout << "Probe summary [" << r.strategy_name
                  << ", pattern=" << r.pattern_name
                  << ", segments=" << r.num_segments
                  << ", replica=" << r.replica_num
                  << "]: probe_size=" << (r.probe_bytes / MiB)
                  << " MiB, success=" << r.probe_success << "/"
                  << r.probe_attempts
                  << ", free_at_probe_gb avg/min/max=" << std::fixed
                  << std::setprecision(2) << r.probe_free_gb_stats.avg << "/"
                  << r.probe_free_gb_stats.min << "/"
                  << r.probe_free_gb_stats.max
                  << ", largest_free_mb p10/p50/max=" << std::setprecision(1)
                  << r.probe_largest_free_mb_stats.p10 << "/"
                  << r.probe_largest_free_mb_stats.p50 << "/"
                  << r.probe_largest_free_mb_stats.max << std::endl;
    }

    std::cout << "Size-class breakdown:";
    for (const auto& stat : r.size_class_stats) {
        std::string ratio = std::to_string(stat.success_count) + "/" +
                            std::to_string(stat.partial_count) + "/" +
                            std::to_string(stat.failed_count) + "/" +
                            std::to_string(stat.total_count);
        std::string size_label = stat.max_size > stat.size
                                     ? ""
                                     : std::to_string(stat.size / KiB) + "KB,";
        std::cout << " " << stat.name << "(" << size_label
                  << "w=" << stat.weight
                  << ",full/partial/failed/total=" << ratio
                  << ",p99_ns=" << std::fixed << std::setprecision(0)
                  << stat.latency_stats.p99 << ")";
    }
    std::cout << std::endl;
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

static bool parsePositiveIntList(const std::string& text,
                                 std::vector<int>& counts) {
    counts.clear();
    std::stringstream ss(text);
    std::string item;
    while (std::getline(ss, item, ',')) {
        auto begin = item.find_first_not_of(" \t");
        if (begin == std::string::npos) continue;
        auto end = item.find_last_not_of(" \t");
        item = item.substr(begin, end - begin + 1);
        char* parse_end = nullptr;
        long value = std::strtol(item.c_str(), &parse_end, 10);
        if (parse_end == item.c_str() || *parse_end != '\0' || value <= 0) {
            return false;
        }
        counts.push_back(static_cast<int>(value));
    }
    return !counts.empty();
}

static void runSizeClassChurnMatrix() {
    std::vector<bool> skewed_options = {false, true};
    std::vector<int> segment_counts = {1, 10, 100};
    std::vector<int> replica_nums = {1, 2, 3};

    std::vector<std::string> strategies;
    {
        std::stringstream ss(FLAGS_size_class_strategies);
        std::string item;
        while (std::getline(ss, item, ',')) {
            auto b = item.find_first_not_of(" \t");
            if (b == std::string::npos) continue;
            auto e = item.find_last_not_of(" \t");
            item = item.substr(b, e - b + 1);
            BenchConfig probe_cfg;
            probe_cfg.bench_strategy = item;
            LocalSsdManager probe_ssd;
            if (!createBenchStrategy(probe_cfg, probe_ssd)) {
                std::cout << "Invalid size_class_strategies entry: " << item
                          << ". Use random, free_ratio_first, "
                             "largest_hole_first, best_fit, reserved, hybrid, "
                             "or best_fit_bucketed."
                          << std::endl;
                return;
            }
            strategies.push_back(item);
        }
        if (strategies.empty()) {
            std::cout << "size_class_strategies must not be empty" << std::endl;
            return;
        }
    }

    const bool events_mode = !FLAGS_rl_trace_events.empty();
    if (events_mode) {
        g_trace_events.clear();
        if (!loadTraceEvents(FLAGS_rl_trace_events, g_trace_events)) {
            std::cout << "Cannot open rl_trace_events: "
                      << FLAGS_rl_trace_events << std::endl;
            return;
        }
        if (g_trace_events.empty()) {
            std::cout << "rl_trace_events contains no events: "
                      << FLAGS_rl_trace_events << std::endl;
            return;
        }
    }

    if (!FLAGS_segment_counts.empty() &&
        !parsePositiveIntList(FLAGS_segment_counts, segment_counts)) {
        std::cout << "Invalid segment_counts: " << FLAGS_segment_counts
                  << ". Use a comma-separated list of positive integers."
                  << std::endl;
        return;
    }
    if (!FLAGS_replica_counts.empty() &&
        !parsePositiveIntList(FLAGS_replica_counts, replica_nums)) {
        std::cout << "Invalid replica_counts: " << FLAGS_replica_counts
                  << ". Use a comma-separated list of positive integers."
                  << std::endl;
        return;
    }

    std::vector<std::string> patterns;
    if (events_mode) {
        patterns = {"events"};
    } else if (!FLAGS_rl_trace_file.empty()) {
        g_trace_sizes.clear();
        if (!loadTraceSizes(FLAGS_rl_trace_file, g_trace_sizes)) {
            std::cout << "Cannot open rl_trace_file: " << FLAGS_rl_trace_file
                      << std::endl;
            return;
        }
        if (g_trace_sizes.empty()) {
            std::cout << "rl_trace_file contains no allocation sizes: "
                      << FLAGS_rl_trace_file << std::endl;
            return;
        }
        patterns = {"trace"};
    } else if (FLAGS_size_class_pattern == "all") {
        patterns = {"kv_mixed", "dsa_pair", "rl"};
    } else {
        patterns = {FLAGS_size_class_pattern};
    }

    if (FLAGS_size_class_evict_ratio <= 0.0 ||
        FLAGS_size_class_evict_ratio > 1.0) {
        std::cout << "Invalid size_class_evict_ratio: "
                  << FLAGS_size_class_evict_ratio
                  << ". Use a value in the range (0.0, 1.0]." << std::endl;
        return;
    }

    for (const auto& pattern : patterns) {
        if (pattern == "events") continue;
        if (getSizeClassSpecs(pattern).empty()) {
            if (pattern == "rl") {
                std::cout << "Invalid rl size range: rl_min_kib="
                          << FLAGS_rl_min_kib
                          << " KiB, rl_max_mib=" << FLAGS_rl_max_mib
                          << " MiB. Both must be positive with min < max."
                          << std::endl;
            } else {
                std::cout << "Invalid size_class_pattern: " << pattern
                          << ". Use --size_class_pattern=kv_mixed, dsa_pair, "
                             "rl, or all."
                          << std::endl;
            }
            return;
        }
    }

    std::cout
        << "\n=== Size-Class Churn Fragmentation Benchmark Matrix ===\n"
        << "Workload: prefill to --prefill_pct if set, then run "
        << FLAGS_num_allocations
        << " mixed-size allocation attempts with fail-triggered random "
           "eviction and retry.\n"
        << "Fragmentation: 1 - largest_free_region / total_free_space, "
           "sampled every --convergence_sample_interval allocations.\n"
        << "Config: segment_capacity=" << FLAGS_segment_capacity
        << " MB, prefill_pct=" << FLAGS_prefill_pct
        << ", evict_ratio=" << FLAGS_size_class_evict_ratio
        << ", release_prob=" << FLAGS_size_class_release_prob
        << ", size_class_pattern=" << FLAGS_size_class_pattern
        << ", probe_mib=" << FLAGS_probe_mib << "\n"
        << "Patterns: kv_mixed = 4KB:70%, 256KB:20%, 3198KB:10%; "
           "dsa_pair = 3198KB:50%, 643KB:50%; rl = log-uniform sizes "
           "in ["
        << FLAGS_rl_min_kib << " KiB, " << FLAGS_rl_max_mib
        << " MiB], one equally weighted class per octave"
        << (g_trace_sizes.empty()
                ? std::string(".\n")
                : "; trace = sequential replay of " +
                      std::to_string(g_trace_sizes.size()) + " sizes from " +
                      FLAGS_rl_trace_file + ".\n")
        << (events_mode
                ? "Events: replaying " + std::to_string(g_trace_events.size()) +
                      " put/remove/evict events from " + FLAGS_rl_trace_events +
                      " x" +
                      std::to_string(std::max(1, FLAGS_trace_events_repeat)) +
                      " with real lifetimes and no eviction retry; "
                      "prefill and release_prob are ignored.\n"
                : std::string())
        << "Strategies: " << FLAGS_size_class_strategies
        << " (largest_hole_first/best_fit/reserved are bench-only; "
           "reserved keeps the last "
        << FLAGS_reserved_segments
        << " segment(s) for objects >= " << FLAGS_large_object_mib << " MiB).\n"
        << "Release: before each measured allocation, one random live "
           "object is released with probability release_prob.\n"
        << "Probe: when probe_mib > 0, each fragmentation sample also "
           "attempts one probe_mib single-replica allocation without "
           "eviction and releases it immediately.\n"
        << "Skewed setup: half nodes are (base + 50%) capacity, half are "
           "(base - 50%)\n"
        << std::endl;

    std::vector<BenchConfig> configs;
    for (const auto& pattern : patterns) {
        for (auto skew : skewed_options) {
            for (const auto& strategy : strategies) {
                for (auto segs : segment_counts) {
                    for (auto rep : replica_nums) {
                        if (rep > segs) continue;
                        BenchConfig cfg;
                        cfg.num_segments = segs;
                        cfg.segment_capacity =
                            static_cast<size_t>(FLAGS_segment_capacity) * MiB;
                        cfg.alloc_size = 0;
                        cfg.replica_num = rep;
                        cfg.num_allocations = FLAGS_num_allocations;
                        cfg.skewed = skew;
                        cfg.strategy_type =
                            strategy == "free_ratio_first"
                                ? AllocationStrategyType::FREE_RATIO_FIRST
                                : AllocationStrategyType::RANDOM;
                        cfg.bench_strategy = strategy;
                        cfg.strategy_name = benchStrategyLabel(strategy);
                        cfg.prefill_pct = FLAGS_prefill_pct;
                        cfg.workload_type = WorkloadType::SIZE_CLASS_CHURN;
                        cfg.size_class_pattern = pattern;
                        configs.push_back(cfg);
                    }
                }
            }
        }
    }

    bool first = true;
    std::string prev_pattern;
    std::string prev_strategy;

    for (const auto& cfg : configs) {
        if (first || cfg.size_class_pattern != prev_pattern) {
            std::cout << "\n--- Pattern: " << cfg.size_class_pattern << " ---"
                      << std::endl;
            prev_pattern = cfg.size_class_pattern;
            first = true;
        }

        if (first || cfg.bench_strategy != prev_strategy) {
            printSizeClassChurnHeader();
            prev_strategy = cfg.bench_strategy;
            first = false;
        }

        auto result = events_mode ? runTraceEventsBenchmark(cfg)
                                  : runSizeClassChurnBenchmark(cfg);
        printSizeClassChurnResult(result);
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
        runSizeClassChurnMatrix();
    } else if (FLAGS_workload == "fillup") {
        runFillupBenchmarks();
    } else if (FLAGS_workload == "scaleout") {
        runScaleOutMatrix();
    } else if (FLAGS_workload == "dsa") {
        runDsaMatrix();
    } else if (FLAGS_workload == "size_class_churn") {
        runSizeClassChurnMatrix();
    } else {
        std::cout << "Invalid workload type: " << FLAGS_workload
                  << ". Use --workload=fillup, --workload=scaleout, "
                     "--workload=dsa, or --workload=size_class_churn."
                  << std::endl;
    }

    return 0;
}
