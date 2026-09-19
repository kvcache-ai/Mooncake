// Variable-length allocation benchmark.
//
// The common KV cache workload hands the allocator a fixed object size, so
// segment choice only has to balance capacity. Other workloads - RL data-plane
// offload is the one this was written for - hand it data-dependent sizes that
// span several octaves, and there segment choice also decides whether a
// contiguous region large enough for the next big object still exists. This
// benchmark covers that second case: mixed-size churn, replay of a recorded
// allocation trace, and a large-allocation probe, across the production and a
// few bench-only placement strategies.
//
// Fixed-size workloads (fillup, scaleout, dsa, kv_mixed / dsa_pair size-class
// churn) live in allocation_strategy_bench.cpp.

#include <algorithm>
#include <chrono>
#include <cmath>
#include <cstdlib>
#include <fstream>
#include <functional>
#include <iomanip>
#include <iostream>
#include <limits>
#include <numeric>
#include <random>
#include <sstream>
#include <string>
#include <unordered_map>
#include <vector>
#include <sys/resource.h>

#include <gflags/gflags.h>
#include "types.h"

#include "offset_allocator/offset_allocator.h"
#include "allocator.h"
#include "allocation_strategy.h"
#include "local_ssd/manager.h"

// --- cluster shape ---
DEFINE_int64(segment_capacity, 1024, "Per-segment capacity in MiB");
DEFINE_string(segment_counts, "1,10,100",
              "Comma-separated segment counts to sweep");
DEFINE_string(replica_counts, "1,2,3",
              "Comma-separated replica counts to sweep");
DEFINE_string(capacity_skew, "uniform",
              "Segment capacity distribution: uniform (all segments equal), "
              "skewed (half at base + 50%, half at base - 50%), or both");

// --- what to allocate ---
DEFINE_int32(num_allocations, 10000,
             "Measured allocation attempts per case (ignored in event replay, "
             "which is driven by the trace length)");
DEFINE_string(size_pattern, "octave",
              "Object size distribution: octave (log-uniform sizes over "
              "[--min_object_kib, --max_object_mib], one equally weighted "
              "class per power-of-two octave, the most hostile mix for large "
              "objects) or trace (replay sizes from --trace_sizes)");
DEFINE_int64(min_object_kib, 64,
             "Smallest object size in KiB for the octave pattern");
DEFINE_int64(max_object_mib, 512,
             "Largest object size in MiB for the octave pattern");
DEFINE_string(trace_sizes, "",
              "Replay allocation sizes from this file (one size in bytes per "
              "line, '#' starts a comment), in file order, wrapping around. "
              "Written by extract_alloc_trace.py. Implies "
              "--size_pattern=trace.");
DEFINE_string(trace_events, "",
              "Replay an ordered put/remove/evict event log ('kind key size' "
              "per line, as written by extract_alloc_trace.py --events) with "
              "the recorded object lifetimes. Puts allocate, removes and "
              "evicts free; there is no eviction retry, so every failed put "
              "is reported as a failure, like action=put_start_alloc_failed "
              "in the master log. Overrides --size_pattern, --prefill_pct and "
              "--release_prob.");
DEFINE_int32(trace_events_repeat, 1,
             "Replay the event log this many times back to back (keys are "
             "suffixed per pass) to reach steady state on short traces");

// --- churn shape ---
DEFINE_int32(prefill_pct, 0,
             "Pre-fill the cluster to this utilization percentage before the "
             "measured loop (0 = disabled)");
DEFINE_double(release_prob, 0.0,
              "Probability of releasing one random live object before each "
              "measured allocation. 1.0 gives a free-one/allocate-one steady "
              "state at the prefill level; 0 keeps allocate-only behavior.");
DEFINE_double(evict_ratio, 0.02,
              "Fraction of live objects to evict on each allocation failure, "
              "before retrying");
DEFINE_int32(sample_interval, 100,
             "Sample fragmentation, utilization and the probe every N "
             "allocations");
DEFINE_int32(probe_mib, 0,
             "At every sample point, attempt one N MiB single-replica "
             "allocation without eviction retry, release it immediately, and "
             "report the success rate together with the free space observed at "
             "probe time (0 = disabled)");

// --- placement ---
DEFINE_string(strategies, "random,free_ratio_first,best_fit",
              "Comma-separated allocation strategies: random, "
              "free_ratio_first, best_fit (production strategies), "
              "largest_hole_first, reserved, hybrid, best_fit_bucketed "
              "(bench-only placement experiments). hybrid = best_fit for "
              "objects below --large_object_mib, largest_hole_first above.");
DEFINE_int32(large_object_mib, 256,
             "Objects of at least this many MiB are 'large' for the reserved "
             "and hybrid strategies");
DEFINE_int32(best_fit_bucket_mib, 512,
             "best_fit_bucketed: segments whose slack (largest hole minus "
             "request) falls in the same bucket of this many MiB are treated "
             "as equal and chosen at random, trading a little contiguity for "
             "less traffic concentration");
DEFINE_int32(reserved_segments, 2,
             "Number of segments (the last ones by index) reserved for large "
             "objects in the reserved strategy");

using namespace mooncake;

static constexpr size_t MiB = 1024ULL * 1024;
static constexpr size_t KiB = 1024ULL;
static constexpr double GiB = 1024.0 * 1024 * 1024;

constexpr int kNumVirtualNodes = 10;
constexpr double kSkewRatio = 0.5;  // +/- 50% capacity for skewed clusters
constexpr uint64_t kPrimaryBaseAddr = 0x100000000ULL;
constexpr int kPrefillSampleInterval = 100;
constexpr int kMaxEvictRetries = 5;
constexpr size_t kMinPrefillAttempts = 5000;
// The theoretical prefill budget assumes every allocation succeeds. Use a
// small multiplier to absorb partial allocations, retries, and fragmentation
// without turning unreachable targets into long-running cases.
constexpr double kPrefillAttemptMultiplier = 2.0;

struct BenchConfig {
    int num_segments = 1;
    size_t segment_capacity = 0;
    int replica_num = 1;
    int num_allocations = 0;
    bool skewed = false;
    int prefill_pct = 0;
    std::string pattern;   // octave, trace, or events
    std::string strategy;  // strategy flag value
    std::string strategy_label;
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
    size_t size = 0;
    int weight = 0;
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

enum class AllocationStatus {
    FAILED,
    PARTIAL,
    FULL,
};

struct AllocationResult {
    AllocationStatus status = AllocationStatus::FAILED;
    size_t replica_count = 0;
};

struct PrefillStats {
    size_t attempts = 0;
    size_t max_attempts = 0;
    int full_count = 0;
    int partial_count = 0;
    int failed_count = 0;
    int requested_pct = 0;
    double achieved_util_pct = 0.0;
    bool reached_target = false;
};

struct BenchResult {
    std::string strategy_label;
    std::string pattern_name;
    int num_segments = 0;
    int replica_num = 0;
    bool skewed = false;
    double cluster_capacity_gb = 0.0;

    double total_time_us = 0.0;
    double throughput = 0.0;  // allocs/sec
    double avg_ns = 0.0;
    double p50_ns = 0.0;
    double p90_ns = 0.0;
    double p99_ns = 0.0;

    int success_count = 0;
    int partial_count = 0;
    int failed_count = 0;
    int total_count = 0;
    int evict_count = 0;
    // Live objects released by --release_prob, or freed by a replayed
    // remove/evict event.
    int released_count = 0;

    double final_util_stddev = 0.0;
    double final_avg_util = 0.0;

    PrefillStats prefill_stats;
    DistributionStats fragmentation_stats;
    FragmentationSnapshot final_fragmentation;
    std::vector<SizeClassStat> size_class_stats;

    // Large-allocation probe (--probe_mib); attempts == 0 when disabled.
    size_t probe_bytes = 0;
    int probe_attempts = 0;
    int probe_success = 0;
    DistributionStats probe_free_gb_stats;
    DistributionStats probe_largest_free_mb_stats;

    // Free space observed at each failed allocation.
    DistributionStats fail_free_gb_stats;
    DistributionStats fail_largest_free_mb_stats;
    DistributionStats fail_size_mb_stats;

    // Traffic concentration. write_skew = bytes written to the busiest segment
    // / mean bytes per segment over the whole run; window_write_skew = the
    // same ratio per sample window, averaged; mean_util_stddev is the
    // per-segment utilization stddev averaged over the sample points.
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
    // Large clusters are simulated via virtual address space only.
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
 */
static AllocatorManager createCluster(int num_segments, size_t base_capacity,
                                      bool skewed) {
    AllocatorManager manager;
    int segments_per_node = std::max(1, num_segments / kNumVirtualNodes);

    for (int i = 0; i < num_segments; ++i) {
        std::string name = "node_" + std::to_string(i / segments_per_node) +
                           "_seg_" + std::to_string(i % segments_per_node);
        // Balanced skew: alternate between (1 + ratio) and (1 - ratio) to keep
        // total capacity constant for even segment counts.
        size_t capacity = base_capacity;
        if (skewed) {
            capacity = (i % 2 == 0) ? base_capacity * (1.0 + kSkewRatio)
                                    : base_capacity * (1.0 - kSkewRatio);
        }
        uint64_t base_addr = kPrimaryBaseAddr + (i * base_capacity);
        auto allocator = std::make_shared<OffsetBufferAllocator>(
            name, base_addr, capacity, name);
        manager.addAllocator(name, allocator);
    }
    return manager;
}

static double computeAverageUtil(const AllocatorManager& manager) {
    const auto& names = manager.getNames();
    if (names.empty()) return 0.0;
    double sum = 0.0;
    int count = 0;
    for (const auto& name : names) {
        const auto* allocators = manager.getAllocators(name);
        if (!allocators) continue;
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

static double computeUtilizationStdDev(const AllocatorManager& manager) {
    std::vector<double> ratios;
    ratios.reserve(manager.getNames().size());
    for (const auto& name : manager.getNames()) {
        const auto* allocators = manager.getAllocators(name);
        if (!allocators) continue;
        for (const auto& registration : *allocators) {
            const auto alloc = registration->GetAllocator();
            double cap = static_cast<double>(alloc->capacity());
            if (cap == 0) continue;
            ratios.push_back(static_cast<double>(alloc->size()) / cap);
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

/**
 * @brief Cluster-wide fragmentation: 1 - largest_free_region /
 *        total_free_space per segment, weighted by that segment's free space.
 */
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

// Allocation sizes loaded from --trace_sizes (one size in bytes per line).
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
    if (pattern_name == "octave") {
        const size_t min_size =
            static_cast<size_t>(std::max<int64_t>(FLAGS_min_object_kib, 0)) *
            KiB;
        const size_t max_size =
            static_cast<size_t>(std::max<int64_t>(FLAGS_max_object_mib, 0)) *
            MiB;
        return buildOctaveSpecs(min_size, max_size);
    }
    if (pattern_name == "trace") {
        return buildTraceSpecs(g_trace_sizes);
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
        if (specs.empty()) return {};
        size_t idx = chooseSizeClassIndex(specs, rng);
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
    return weighted_size / total_weight;
}

static size_t derivePrefillMaxAttempts(
    const AllocatorManager& manager, const BenchConfig& cfg,
    const std::vector<SizeClassSpec>& specs) {
    if (cfg.prefill_pct <= 0 || cfg.replica_num <= 0) return 0;

    const double avg_object_size = computeWeightedAverageObjectSize(specs);
    if (avg_object_size <= 0.0) return kMinPrefillAttempts;

    const double target_bytes =
        static_cast<double>(computeTotalCapacity(manager)) * cfg.prefill_pct /
        100.0;
    const double bytes_per_attempt =
        avg_object_size * static_cast<double>(cfg.replica_num);
    if (target_bytes <= 0.0 || bytes_per_attempt <= 0.0) {
        return kMinPrefillAttempts;
    }

    const double derived_attempts = std::ceil(
        (target_bytes / bytes_per_attempt) * kPrefillAttemptMultiplier);
    return std::max(kMinPrefillAttempts, static_cast<size_t>(derived_attempts));
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
    const std::string& name, LocalSsdManager& local_ssd) {
    if (name.empty() || name == "random") {
        return CreateAllocationStrategy(AllocationStrategyType::RANDOM,
                                        local_ssd);
    }
    if (name == "free_ratio_first") {
        return CreateAllocationStrategy(
            AllocationStrategyType::FREE_RATIO_FIRST, local_ssd);
    }
    if (name == "best_fit") {
        return CreateAllocationStrategy(AllocationStrategyType::BEST_FIT,
                                        local_ssd);
    }
    if (name == "largest_hole_first") {
        return std::make_shared<BenchScoredStrategy>(scoreLargestHoleFirst);
    }
    if (name == "reserved") {
        return std::make_shared<BenchScoredStrategy>(scoreReserved);
    }
    if (name == "hybrid") {
        return std::make_shared<BenchScoredStrategy>(scoreHybrid);
    }
    if (name == "best_fit_bucketed") {
        return std::make_shared<BenchScoredStrategy>(scoreBestFitBucketed);
    }
    return nullptr;
}

static std::string benchStrategyLabel(const std::string& s) {
    if (s == "random") return "Random";
    if (s == "free_ratio_first") return "FreeRatioFirst";
    if (s == "best_fit") return "BestFit";
    if (s == "largest_hole_first") return "LargestHole";
    if (s == "reserved") return "Reserved";
    if (s == "hybrid") return "Hybrid";
    if (s == "best_fit_bucketed") return "BestFitBucket";
    return s;
}

/**
 * @brief Compute latency percentiles and fill the shared result fields.
 *        Sorts latencies in-place.
 */
static void computeLatencyStats(std::vector<double>& latencies, double total_us,
                                int num_allocations, BenchResult& res) {
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

static AllocationResult allocateWithEvict(
    const std::shared_ptr<AllocationStrategy>& strategy,
    AllocatorManager& manager, size_t size, int replica_num,
    std::vector<std::vector<Replica>>& live, std::mt19937& rng,
    int& evict_count, double evict_ratio) {
    for (int attempt = 0; attempt <= kMaxEvictRetries; ++attempt) {
        auto result = strategy->Allocate(manager, size, replica_num);
        if (result.has_value()) {
            size_t replica_count = result->size();
            if (replica_count == 0) {
                return {};
            }
            live.push_back(std::move(result.value()));
            return {
                replica_count == static_cast<size_t>(replica_num)
                    ? AllocationStatus::FULL
                    : AllocationStatus::PARTIAL,
                replica_count,
            };
        }

        if (live.empty()) return {};
        if (attempt == kMaxEvictRetries) return {};

        evictRandomFraction(live, evict_ratio, rng);
        ++evict_count;
    }

    return {};
}

static PrefillStats prefillCluster(
    const std::shared_ptr<AllocationStrategy>& strategy,
    AllocatorManager& manager, const BenchConfig& cfg,
    SizeClassSampler& sampler,
    std::vector<std::vector<Replica>>& live_allocations, std::mt19937& rng) {
    PrefillStats stats;
    if (cfg.prefill_pct <= 0 || sampler.specs.empty()) return stats;
    stats.requested_pct = cfg.prefill_pct;
    stats.max_attempts = derivePrefillMaxAttempts(manager, cfg, sampler.specs);

    int consec_failures = 0;
    int evict_throwaway = 0;
    const int kMaxConsecFailures = 10;

    while (stats.attempts < stats.max_attempts) {
        if (stats.attempts % kPrefillSampleInterval == 0) {
            stats.achieved_util_pct = computeAverageUtil(manager) * 100.0;
            if (stats.achieved_util_pct >= cfg.prefill_pct) {
                stats.reached_target = true;
                break;
            }
        }

        auto sample = sampler.next(rng);
        auto alloc_result = allocateWithEvict(
            strategy, manager, sample.size, cfg.replica_num, live_allocations,
            rng, evict_throwaway, FLAGS_evict_ratio);
        ++stats.attempts;
        if (alloc_result.status == AllocationStatus::FULL) {
            ++stats.full_count;
            consec_failures = 0;
        } else if (alloc_result.status == AllocationStatus::PARTIAL) {
            ++stats.partial_count;
            consec_failures = 0;
        } else {
            ++stats.failed_count;
            if (++consec_failures >= kMaxConsecFailures) {
                break;
            }
        }
    }

    stats.achieved_util_pct = computeAverageUtil(manager) * 100.0;
    if (stats.achieved_util_pct >= cfg.prefill_pct) {
        stats.reached_target = true;
    }
    return stats;
}

static std::vector<SizeClassStat> buildClassStats(
    const std::vector<SizeClassSpec>& specs) {
    std::vector<SizeClassStat> stats;
    stats.reserve(specs.size());
    for (const auto& spec : specs) {
        SizeClassStat stat;
        stat.name = spec.name;
        stat.size = spec.size;
        stat.max_size = spec.max_size;
        stat.weight = spec.weight;
        stats.push_back(std::move(stat));
    }
    return stats;
}

// Mixed-size churn: prefill, then run --num_allocations attempts with
// fail-triggered random eviction and retry, optionally releasing one live
// object before each attempt.
static BenchResult runChurnBenchmark(const BenchConfig& cfg) {
    AllocatorManager manager =
        createCluster(cfg.num_segments, cfg.segment_capacity, cfg.skewed);
    LocalSsdManager local_ssd;
    auto strategy = createBenchStrategy(cfg.strategy, local_ssd);
    SizeClassSampler sampler;
    sampler.specs = getSizeClassSpecs(cfg.pattern);
    if (cfg.pattern == "trace") {
        sampler.trace = &g_trace_sizes;
    }
    const auto& specs = sampler.specs;

    auto per_class_stats = buildClassStats(specs);
    std::vector<std::vector<double>> per_class_latencies(specs.size());

    const size_t probe_bytes =
        static_cast<size_t>(std::max(FLAGS_probe_mib, 0)) * MiB;
    const double release_prob = std::clamp(FLAGS_release_prob, 0.0, 1.0);
    std::uniform_real_distribution<double> release_dist(0.0, 1.0);
    std::vector<double> probe_free_gb_samples;
    std::vector<double> probe_largest_free_mb_samples;
    int probe_attempts = 0;
    int probe_success = 0;
    int released_count = 0;

    std::vector<double> latencies;
    latencies.reserve(cfg.num_allocations);
    int sample_interval = std::max(1, FLAGS_sample_interval);
    std::vector<double> fragmentation_samples;
    fragmentation_samples.reserve(cfg.num_allocations / sample_interval + 2);

    std::vector<std::vector<Replica>> live_allocations;
    live_allocations.reserve(std::min(cfg.num_allocations, 1 << 20));

    std::mt19937 rng(42);
    PrefillStats prefill_stats =
        prefillCluster(strategy, manager, cfg, sampler, live_allocations, rng);

    int success_count = 0;
    int partial_count = 0;
    int failed_count = 0;
    int total_count = 0;
    int evict_count = 0;
    double instrumentation_time_us = 0.0;
    std::vector<double> fail_free_gb, fail_largest_free_mb, fail_size_mb;

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
        auto alloc_result = allocateWithEvict(
            strategy, manager, sample.size, cfg.replica_num, live_allocations,
            rng, evict_count, FLAGS_evict_ratio);
        auto t1 = std::chrono::high_resolution_clock::now();

        double latency_ns =
            std::chrono::duration<double, std::nano>(t1 - t0).count();
        latencies.push_back(latency_ns);
        per_class_latencies[class_idx].push_back(latency_ns);

        ++total_count;
        ++per_class_stats[class_idx].total_count;
        if (alloc_result.status == AllocationStatus::FULL) {
            ++success_count;
            ++per_class_stats[class_idx].success_count;
        } else if (alloc_result.status == AllocationStatus::PARTIAL) {
            ++partial_count;
            ++per_class_stats[class_idx].partial_count;
        } else {
            ++failed_count;
            ++per_class_stats[class_idx].failed_count;
            auto s0 = std::chrono::high_resolution_clock::now();
            auto snap = computeFragmentationSnapshot(manager);
            fail_free_gb.push_back(snap.total_free_space / GiB);
            fail_largest_free_mb.push_back(
                static_cast<double>(snap.largest_free_region) / MiB);
            fail_size_mb.push_back(static_cast<double>(sample.size) / MiB);
            auto s1 = std::chrono::high_resolution_clock::now();
            instrumentation_time_us +=
                std::chrono::duration<double, std::micro>(s1 - s0).count();
        }

        if ((i + 1) % sample_interval == 0 || i == cfg.num_allocations - 1) {
            auto s0 = std::chrono::high_resolution_clock::now();
            auto snapshot = computeFragmentationSnapshot(manager);
            if (snapshot.valid) {
                fragmentation_samples.push_back(snapshot.fragmentation_ratio);
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
    total_us = std::max(total_us - instrumentation_time_us, 1.0);

    for (size_t i = 0; i < per_class_stats.size(); ++i) {
        per_class_stats[i].latency_stats =
            computeDistributionStats(per_class_latencies[i]);
    }

    BenchResult res;
    res.strategy_label = cfg.strategy_label;
    res.pattern_name = cfg.pattern;
    res.num_segments = cfg.num_segments;
    res.replica_num = cfg.replica_num;
    res.skewed = cfg.skewed;
    res.cluster_capacity_gb = computeClusterCapacityGB(
        cfg.num_segments, cfg.segment_capacity, cfg.skewed);
    res.final_util_stddev = computeUtilizationStdDev(manager);
    res.final_avg_util = computeAverageUtil(manager);
    res.success_count = success_count;
    res.partial_count = partial_count;
    res.failed_count = failed_count;
    res.total_count = total_count;
    res.evict_count = evict_count;
    res.released_count = released_count;
    res.prefill_stats = prefill_stats;
    res.fragmentation_stats = computeDistributionStats(fragmentation_samples);
    res.final_fragmentation = computeFragmentationSnapshot(manager);
    res.size_class_stats = std::move(per_class_stats);
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

// Replays put/remove/evict events with the recorded object lifetimes. No
// eviction retry: a failed put is counted and its free-space picture is
// recorded, exactly like action=put_start_alloc_failed in the master log.
static BenchResult runEventReplayBenchmark(const BenchConfig& cfg) {
    AllocatorManager manager =
        createCluster(cfg.num_segments, cfg.segment_capacity, cfg.skewed);
    LocalSsdManager local_ssd;
    auto strategy = createBenchStrategy(cfg.strategy, local_ssd);

    std::vector<size_t> put_sizes;
    for (const auto& ev : g_trace_events) {
        if (ev.kind == 'p') put_sizes.push_back(ev.size);
    }
    std::vector<SizeClassSpec> specs = buildTraceSpecs(put_sizes);
    auto per_class_stats = buildClassStats(specs);
    std::vector<std::vector<double>> per_class_latencies(specs.size());

    const size_t probe_bytes =
        static_cast<size_t>(std::max(FLAGS_probe_mib, 0)) * MiB;
    const int sample_interval = std::max(1, FLAGS_sample_interval);
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
    std::vector<double> fragmentation_samples;
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

    BenchResult res;
    res.strategy_label = cfg.strategy_label;
    res.pattern_name = "events";
    res.num_segments = cfg.num_segments;
    res.replica_num = cfg.replica_num;
    res.skewed = cfg.skewed;
    res.cluster_capacity_gb = computeClusterCapacityGB(
        cfg.num_segments, cfg.segment_capacity, cfg.skewed);
    res.final_util_stddev = computeUtilizationStdDev(manager);
    res.final_avg_util = computeAverageUtil(manager);
    res.success_count = success_count;
    res.partial_count = partial_count;
    res.failed_count = failed_count;
    res.total_count = total_count;
    res.released_count = released_count;
    res.fragmentation_stats = computeDistributionStats(fragmentation_samples);
    res.final_fragmentation = computeFragmentationSnapshot(manager);
    res.size_class_stats = std::move(per_class_stats);
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

static void printHeader() {
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

static void printResult(const BenchResult& r) {
    std::string alloc_ratio = std::to_string(r.success_count) + "/" +
                              std::to_string(r.partial_count) + "/" +
                              std::to_string(r.failed_count) + "/" +
                              std::to_string(r.total_count);
    std::ostringstream cap_ss;
    cap_ss << std::fixed << std::setprecision(1) << r.cluster_capacity_gb;

    double final_largest_free_mb =
        static_cast<double>(r.final_fragmentation.largest_free_region) / MiB;

    std::cout << std::left << std::setw(18) << r.strategy_label << std::setw(9)
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

    const std::string tag = "[" + r.strategy_label +
                            ", pattern=" + r.pattern_name +
                            ", segments=" + std::to_string(r.num_segments) +
                            ", replica=" + std::to_string(r.replica_num) + "]";

    if (r.prefill_stats.requested_pct > 0) {
        std::cout << "Prefill summary " << tag
                  << ": requested_pct=" << std::fixed << std::setprecision(2)
                  << r.prefill_stats.requested_pct
                  << ", achieved_pct=" << r.prefill_stats.achieved_util_pct
                  << ", reached="
                  << (r.prefill_stats.reached_target ? "yes" : "no")
                  << ", attempts=" << r.prefill_stats.attempts << "/"
                  << r.prefill_stats.max_attempts
                  << ", full/partial/failed=" << r.prefill_stats.full_count
                  << "/" << r.prefill_stats.partial_count << "/"
                  << r.prefill_stats.failed_count << std::endl;
    }

    std::cout << "Fragmentation summary " << tag
              << ": skewed=" << (r.skewed ? "yes" : "no")
              << ", avg=" << std::fixed << std::setprecision(4)
              << r.fragmentation_stats.avg
              << ", p50=" << r.fragmentation_stats.p50
              << ", p90=" << r.fragmentation_stats.p90
              << ", p99=" << r.fragmentation_stats.p99
              << ", max=" << r.fragmentation_stats.max
              << ", final_largest_free=" << std::setprecision(1)
              << final_largest_free_mb << " MB"
              << ", util_stddev=" << std::setprecision(4) << r.final_util_stddev
              << ", released=" << r.released_count << std::endl;

    if (r.pattern_name == "events") {
        std::cout << "Traffic summary " << tag
                  << ": write_skew(max/mean)=" << std::fixed
                  << std::setprecision(2) << r.write_skew
                  << ", window_write_skew=" << r.window_write_skew
                  << ", mean_util_stddev=" << std::setprecision(4)
                  << r.mean_util_stddev << std::endl;
    }

    if (r.failed_count > 0 && r.fail_size_mb_stats.valid) {
        std::cout << "Failure summary " << tag
                  << ": failed_allocations=" << r.failed_count
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
        std::cout << "Probe summary " << tag
                  << ": probe_size=" << (r.probe_bytes / MiB)
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
        std::cout << " " << stat.name << "(w=" << stat.weight
                  << ",full/partial/failed/total=" << ratio
                  << ",p99_ns=" << std::fixed << std::setprecision(0)
                  << stat.latency_stats.p99 << ")";
    }
    std::cout << std::endl;
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

static std::vector<std::string> parseCommaList(const std::string& text) {
    std::vector<std::string> items;
    std::stringstream ss(text);
    std::string item;
    while (std::getline(ss, item, ',')) {
        auto b = item.find_first_not_of(" \t");
        if (b == std::string::npos) continue;
        auto e = item.find_last_not_of(" \t");
        items.push_back(item.substr(b, e - b + 1));
    }
    return items;
}

static void runMatrix() {
    std::vector<std::string> strategies;
    for (const auto& item : parseCommaList(FLAGS_strategies)) {
        LocalSsdManager probe_ssd;
        if (!createBenchStrategy(item, probe_ssd)) {
            std::cout << "Invalid --strategies entry: " << item
                      << ". Use random, free_ratio_first, best_fit, "
                         "largest_hole_first, reserved, hybrid, or "
                         "best_fit_bucketed."
                      << std::endl;
            return;
        }
        strategies.push_back(item);
    }
    if (strategies.empty()) {
        std::cout << "--strategies must not be empty" << std::endl;
        return;
    }

    std::vector<bool> skew_options;
    if (FLAGS_capacity_skew == "uniform") {
        skew_options = {false};
    } else if (FLAGS_capacity_skew == "skewed") {
        skew_options = {true};
    } else if (FLAGS_capacity_skew == "both") {
        skew_options = {false, true};
    } else {
        std::cout << "Invalid --capacity_skew: " << FLAGS_capacity_skew
                  << ". Use uniform, skewed, or both." << std::endl;
        return;
    }

    std::vector<int> segment_counts;
    if (!parsePositiveIntList(FLAGS_segment_counts, segment_counts)) {
        std::cout << "Invalid --segment_counts: " << FLAGS_segment_counts
                  << ". Use a comma-separated list of positive integers."
                  << std::endl;
        return;
    }
    std::vector<int> replica_nums;
    if (!parsePositiveIntList(FLAGS_replica_counts, replica_nums)) {
        std::cout << "Invalid --replica_counts: " << FLAGS_replica_counts
                  << ". Use a comma-separated list of positive integers."
                  << std::endl;
        return;
    }

    if (FLAGS_evict_ratio <= 0.0 || FLAGS_evict_ratio > 1.0) {
        std::cout << "Invalid --evict_ratio: " << FLAGS_evict_ratio
                  << ". Use a value in the range (0.0, 1.0]." << std::endl;
        return;
    }

    const bool events_mode = !FLAGS_trace_events.empty();
    std::string pattern;
    if (events_mode) {
        g_trace_events.clear();
        if (!loadTraceEvents(FLAGS_trace_events, g_trace_events)) {
            std::cout << "Cannot open --trace_events: " << FLAGS_trace_events
                      << std::endl;
            return;
        }
        if (g_trace_events.empty()) {
            std::cout << "--trace_events contains no events: "
                      << FLAGS_trace_events << std::endl;
            return;
        }
        pattern = "events";
    } else if (!FLAGS_trace_sizes.empty()) {
        g_trace_sizes.clear();
        if (!loadTraceSizes(FLAGS_trace_sizes, g_trace_sizes)) {
            std::cout << "Cannot open --trace_sizes: " << FLAGS_trace_sizes
                      << std::endl;
            return;
        }
        if (g_trace_sizes.empty()) {
            std::cout << "--trace_sizes contains no allocation sizes: "
                      << FLAGS_trace_sizes << std::endl;
            return;
        }
        pattern = "trace";
    } else if (FLAGS_size_pattern == "octave") {
        if (getSizeClassSpecs("octave").empty()) {
            std::cout << "Invalid octave size range: min_object_kib="
                      << FLAGS_min_object_kib
                      << " KiB, max_object_mib=" << FLAGS_max_object_mib
                      << " MiB. Both must be positive with min < max."
                      << std::endl;
            return;
        }
        pattern = "octave";
    } else {
        std::cout << "Invalid --size_pattern: " << FLAGS_size_pattern
                  << ". Use octave, or trace with --trace_sizes." << std::endl;
        return;
    }

    std::cout << "\n=== Variable-Length Allocation Benchmark Matrix ===\n"
              << "Objects span several size octaves, so segment choice decides "
                 "whether a contiguous region large enough for the next large "
                 "object still exists.\n"
              << "Fragmentation: 1 - largest_free_region / total_free_space, "
                 "sampled every --sample_interval allocations.\n"
              << "Config: segment_capacity=" << FLAGS_segment_capacity
              << " MiB, segment_counts=" << FLAGS_segment_counts
              << ", replica_counts=" << FLAGS_replica_counts
              << ", capacity_skew=" << FLAGS_capacity_skew
              << ", prefill_pct=" << FLAGS_prefill_pct
              << ", evict_ratio=" << FLAGS_evict_ratio
              << ", release_prob=" << FLAGS_release_prob
              << ", probe_mib=" << FLAGS_probe_mib << "\n";
    if (pattern == "events") {
        std::cout << "Pattern: replaying " << g_trace_events.size()
                  << " put/remove/evict events from " << FLAGS_trace_events
                  << " x" << std::max(1, FLAGS_trace_events_repeat)
                  << " with the recorded lifetimes and no eviction retry; "
                     "prefill_pct and release_prob are ignored.\n";
    } else if (pattern == "trace") {
        std::cout << "Pattern: sequential replay of " << g_trace_sizes.size()
                  << " sizes from " << FLAGS_trace_sizes
                  << ", one size class per observed octave.\n";
    } else {
        std::cout << "Pattern: log-uniform sizes in [" << FLAGS_min_object_kib
                  << " KiB, " << FLAGS_max_object_mib
                  << " MiB], one equally weighted class per octave.\n";
    }
    std::cout << "Strategies: " << FLAGS_strategies
              << " (largest_hole_first/reserved/hybrid/best_fit_bucketed are "
                 "bench-only; reserved keeps the last "
              << FLAGS_reserved_segments
              << " segment(s) for objects >= " << FLAGS_large_object_mib
              << " MiB).\n"
              << "Probe: when probe_mib > 0, each sample point also attempts "
                 "one probe_mib single-replica allocation without eviction and "
                 "releases it immediately.\n"
              << "Skewed setup: half the segments are (base + 50%) capacity, "
                 "half are (base - 50%).\n"
              << std::endl;

    std::vector<BenchConfig> configs;
    for (auto skew : skew_options) {
        for (const auto& strategy : strategies) {
            for (auto segs : segment_counts) {
                for (auto rep : replica_nums) {
                    if (rep > segs) continue;
                    BenchConfig cfg;
                    cfg.num_segments = segs;
                    cfg.segment_capacity =
                        static_cast<size_t>(FLAGS_segment_capacity) * MiB;
                    cfg.replica_num = rep;
                    cfg.num_allocations = FLAGS_num_allocations;
                    cfg.skewed = skew;
                    cfg.prefill_pct = FLAGS_prefill_pct;
                    cfg.pattern = pattern;
                    cfg.strategy = strategy;
                    cfg.strategy_label = benchStrategyLabel(strategy);
                    configs.push_back(cfg);
                }
            }
        }
    }

    bool first = true;
    std::string prev_strategy;
    for (const auto& cfg : configs) {
        if (first || cfg.strategy != prev_strategy) {
            printHeader();
            prev_strategy = cfg.strategy;
            first = false;
        }
        printResult(events_mode ? runEventReplayBenchmark(cfg)
                                : runChurnBenchmark(cfg));
    }
}

int main(int argc, char* argv[]) {
    gflags::SetUsageMessage(
        "Variable-length allocation benchmark: mixed-size churn and recorded "
        "trace replay across allocation strategies.\n"
        "Usage: variable_length_allocation_bench [flags]");
    gflags::ParseCommandLineFlags(&argc, &argv, true);
    setupResourceLimits();
    runMatrix();
    return 0;
}
