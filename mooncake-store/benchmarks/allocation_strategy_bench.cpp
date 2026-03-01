/**
 * @file allocation_strategy_bench.cpp
 * @brief Performance benchmark for AllocationStrategy implementations.
 *
 * This benchmark measures:
 * - Allocation latency (avg / P50 / P90 / P99)
 * - Throughput (allocations per second)
 * - Load balance across segments (utilization std-dev)
 * - [Scale-Out] Convergence time to balanced state
 * - [Scale-Out] Speed of load redistribution after new nodes are added
 *
 * Usage:
 *   ./allocation_strategy_bench [flags]
 *
 * Key flags:
 *   --num_segments           Number of segments (default: 10)
 *   --segment_capacity       Per-segment capacity in MB (default: 64)
 *   --alloc_size             Allocation size in KB (default: 64)
 *   --replica_num            Replicas per allocation (default: 1)
 *   --num_allocations        Total allocations to run (default: 10000)
 *   --skewed                 Use skewed segment capacities (default: false)
 *   --run_all                Run all strategies with a standard matrix
 *   --workload               Workload type: fillup, scaleout (default: fillup)
 *   --scale_out_trigger_pct  % of allocs after which new nodes are added
 *                            (scaleout mode, default: 50)
 *   --scale_out_new_segments Number of new segments to inject (default: 10)
 *   --convergence_sample_interval
 *                            Sample utilization stddev every N allocations
 *                            (scaleout mode only)
 */

#include <algorithm>
#include <chrono>
#include <cmath>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <numeric>
#include <sstream>
#include <string>
#include <vector>

#include <gflags/gflags.h>

#include "allocation_strategy.h"
#include "allocator.h"
#include "types.h"

// --- gflags definitions ---
DEFINE_int32(num_segments, 10, "Number of segments to simulate");
DEFINE_int64(segment_capacity, 1024,
             "Per-segment capacity in MB (base capacity for skewed mode)");
DEFINE_int64(alloc_size, 64, "Allocation size in KB");
DEFINE_int32(replica_num, 1, "Number of replicas per allocation");
DEFINE_int32(num_allocations, 10000, "Number of allocations to benchmark");
DEFINE_bool(skewed, false,
            "Use skewed segment capacities (capacity varies by index)");
DEFINE_string(strategy, "all",
              "Strategy to benchmark: Random, FreeRatioFirst, or all");
DEFINE_int32(convergence_sample_interval, 100,
             "Sample utilization stddev every N allocations (scaleout only)");
DEFINE_bool(run_all, false,
            "Run a standard matrix of configurations for comparison");

// Scale-Out workload flags
DEFINE_string(workload, "fillup", "Workload type: fillup (default), scaleout");
DEFINE_int32(scale_out_trigger_pct, 50,
             "Percentage of num_allocations after which new nodes are injected "
             "(scaleout mode only)");
DEFINE_int32(scale_out_new_segments, 10,
             "Number of new segments to inject in scaleout mode");

using namespace mooncake;

static constexpr size_t MiB = 1024ULL * 1024;
static constexpr size_t KiB = 1024ULL;

// ============================================================
//  Enums
// ============================================================

enum class WorkloadType {
    FILL_UP,    // Only allocate, measure throughput/latency
    SCALE_OUT,  // Inject new nodes mid-run, measure adoption speed
};

static WorkloadType parseWorkload(const std::string& s) {
    if (s == "scaleout") return WorkloadType::SCALE_OUT;
    if (s == "fillup") return WorkloadType::FILL_UP;
    std::cerr << "Unknown workload: " << s << ", falling back to fillup\n";
    return WorkloadType::FILL_UP;
}

// ============================================================
//  Config & Result structs
// ============================================================

struct BenchConfig {
    int num_segments;
    size_t segment_capacity;
    size_t alloc_size;
    int replica_num;
    int num_allocations;
    bool skewed;
    std::string strategy_name;
    AllocationStrategyType strategy_type;

    // Workload
    WorkloadType workload_type = WorkloadType::FILL_UP;
    int scale_out_trigger_pct = 50;
    int scale_out_new_segments = 10;
};

// --- Result hierarchy ---

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

    int success_count = 0;     // successful allocations
    int total_count = 0;       // total attempted allocations

    double final_util_stddev;  // utilization stddev at run end
    double final_avg_util;     // average utilization at run end

    virtual ~BenchResultBase() = default;
};

/**
 * @brief Fill-Up result: pure performance metrics, no convergence tracking.
 */
struct FillUpResult : BenchResultBase {};

/**
 * @brief Scale-Out result: performance + convergence + adoption metrics.
 */
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

static constexpr double GiB = 1024.0 * 1024 * 1024;

/**
 * @brief Compute total cluster capacity in GB.
 *
 * For skewed clusters: capacity_i = base * (1 + i%10).
 */
static double computeClusterCapacityGB(int num_segments, size_t base_capacity,
                                       bool skewed) {
    double total = 0.0;
    for (int i = 0; i < num_segments; ++i) {
        size_t cap = skewed ? base_capacity * (1 + static_cast<size_t>(i % 10))
                           : base_capacity;
        total += static_cast<double>(cap);
    }
    return total / GiB;
}

// ============================================================
//  Helpers
// ============================================================

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
    // Distribute segments evenly across 10 virtual nodes if num_segments > 10,
    // otherwise 1 segment per node.
    int segments_per_node = std::max(1, num_segments / 10);

    for (int i = 0; i < num_segments; ++i) {
        int global_i = i + id_offset;
        std::string name =
            "node_" + std::to_string(global_i / segments_per_node) + "_seg_" +
            std::to_string(global_i % segments_per_node);
        // skew is based on capacity variation by index
        size_t capacity =
            skewed ? base_capacity * (1 + static_cast<size_t>(i % 10))
                   : base_capacity;
        uint64_t base_addr = 0x100000000ULL + (global_i * base_capacity);
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
        uint64_t base_addr =
            0x800000000ULL + (static_cast<uint64_t>(id_offset + i) * capacity);
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

/**
 * @brief Parse a strategy name string to the enum type.
 */
static AllocationStrategyType parseStrategy(const std::string& name) {
    if (name == "Random") return AllocationStrategyType::RANDOM;
    if (name == "FreeRatioFirst")
        return AllocationStrategyType::FREE_RATIO_FIRST;
    std::cerr << "Unknown strategy: " << name << ", falling back to Random"
              << std::endl;
    return AllocationStrategyType::RANDOM;
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

// ============================================================
//  Latency percentile helper
// ============================================================

/**
 * @brief Compute latency percentiles and fill the base result fields.
 *
 * Shared between Fill-Up and Scale-Out benchmark runners.
 * Sorts latencies in-place.
 */
static void computeLatencyStats(std::vector<double>& latencies,
                                double total_us, int num_allocations,
                                BenchResultBase& res) {
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
    res.avg_ns = latencies.empty()
                     ? 0.0
                     : std::accumulate(latencies.begin(), latencies.end(),
                                       0.0) /
                           latencies.size();
    res.p50_ns = percentile(0.50);
    res.p90_ns = percentile(0.90);
    res.p99_ns = percentile(0.99);
}

// ============================================================
//  Fill-Up benchmark runner
// ============================================================

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
            if (++consec_failures >= kMaxConsecFailures) break;
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
    // active_allocations destructs here → memory freed
    return res;
}

// ============================================================
//  Scale-Out benchmark runner
// ============================================================

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
    // Size down per-segment capacity so original cluster fills significantly
    // by trigger point (~80% utilization).
    size_t effective_capacity = cfg.segment_capacity;
    {
        int trigger_allocs =
            cfg.num_allocations * cfg.scale_out_trigger_pct / 100;
        size_t needed = static_cast<size_t>(cfg.alloc_size) * cfg.replica_num *
                        trigger_allocs / std::max(1, cfg.num_segments);
        size_t scaleout_cap = needed * 100 / 80;
        effective_capacity = std::max(cfg.segment_capacity, scaleout_cap);
    }

    AllocatorManager manager =
        createCluster(cfg.num_segments, effective_capacity, cfg.skewed);
    auto strategy = CreateAllocationStrategy(cfg.strategy_type);

    const int sample_interval = FLAGS_convergence_sample_interval;
    const double convergence_threshold = 0.05;

    std::vector<double> latencies;
    // Don't reserve yet, we'll reserve after calculating measurement_allocs.

    std::vector<double> stddev_over_time;
    stddev_over_time.reserve(cfg.num_allocations / sample_interval + 1);
    std::vector<double> avg_util_over_time;
    avg_util_over_time.reserve(cfg.num_allocations / sample_interval + 1);

    ScaleOutResult res;
    res.strategy_name = cfg.strategy_name;
    res.num_segments = cfg.num_segments;
    res.alloc_size = cfg.alloc_size;
    res.replica_num = cfg.replica_num;
    res.skewed = cfg.skewed;
    res.initial_capacity_gb = computeClusterCapacityGB(
        cfg.num_segments, cfg.segment_capacity, cfg.skewed);
    res.scaled_capacity_gb = computeClusterCapacityGB(
        cfg.num_segments + cfg.scale_out_new_segments, effective_capacity, cfg.skewed);
    // for backward compatibility with base struct, though we print the detailed ones
    res.cluster_capacity_gb = res.scaled_capacity_gb;

    std::vector<std::vector<Replica>> active_allocations;
    // Reserve enough to avoid frequent reallocations. 
    // Measurement is 20% of expanded capacity. For 1TB cluster with 512KB blocks,
    // that's around 400k allocs. Plus pre-fill might add another 100k-200k (with 8MB blocks).
    active_allocations.reserve(600000); 

    int success_count = 0;
    int total_count = 0;
    int consec_failures = 0;
    const int kMaxConsecFailures = 10;

    // --- Pre-fill Phase ---
    // Use larger blocks for pre-fill to reach 50% target with less metadata overhead (avoiding OOM)
    size_t pre_fill_alloc_size = std::max((size_t)cfg.alloc_size, (size_t)(8ULL * MiB));
    if (res.initial_capacity_gb > 500.0) {
        pre_fill_alloc_size = std::max(pre_fill_alloc_size, (size_t)(32ULL * MiB));
    }
    int pre_alloc_count = 0;
    while (true) {
        // Sample periodically to avoid O(N) evaluation on every pre-fill allocation
        if (pre_alloc_count % 100 == 0) {
            if (computeAverageUtilAll(manager) * 100.0 >= cfg.scale_out_trigger_pct) {
                break;
            }
        }
        auto result = strategy->Allocate(manager, pre_fill_alloc_size, cfg.replica_num);
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
        injectNewSegments(
            manager, cfg.scale_out_new_segments, cfg.segment_capacity,
            /*id_offset=*/cfg.num_segments);
    res.stddev_just_after_scale = computeUtilizationStdDev(manager);
    res.trigger_alloc_idx = 0;

    // Reset counters for the benchmarking phase
    consec_failures = 0;
    
    // Calculate 20% of expanded capacity in bytes
    double bytes_to_allocate = res.scaled_capacity_gb * GiB * 0.20;
    int measurement_allocs = static_cast<int>(std::round(
        bytes_to_allocate / (static_cast<double>(cfg.alloc_size) * cfg.replica_num)));
    // Safety check: ensure at least some allocations, but cap at 500,000 to avoid OOM
    measurement_allocs = std::max(100, std::min(measurement_allocs, 500000));
    latencies.reserve(measurement_allocs);

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
        if (i % sample_interval == 0) {
            auto s0 = std::chrono::high_resolution_clock::now();
            
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
             // Since trigger is at 0, convergence offset is just idx+1 * interval
            res.re_converge_allocs = (idx + 1) * sample_interval;
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
        int allocs_done =
            static_cast<int>((i + 1) * sample_interval);
        // Only evaluate convergence once the cluster is at least 1% utilized.
        const double min_util_to_check = 0.01;
        bool util_sufficient =
            i < avg_util_over_time.size() &&
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
    res.converge_avg_util =
        (converged_at != -1 && first_converge_avg_util >= 0)
            ? first_converge_avg_util
            : res.final_avg_util;
    res.convergence_alloc_count = converged_at;
    res.success_count = success_count;
    res.total_count = total_count;

    computeLatencyStats(latencies, total_us, total_count, res);
    // active_allocations destructs here → memory freed
    return res;
}

// ============================================================
//  Pretty printing — Fill-Up
// ============================================================

static void printFillUpHeader() {
    std::cout << std::string(170, '-') << std::endl;
    std::cout << std::left << std::setw(18) << "Strategy" << std::setw(9)
              << "Replica" << std::setw(10) << "Segments" << std::setw(12)
              << "AllocSize" << std::setw(12) << "Cluster(GB)"
              << std::setw(8) << "Skewed" << std::right
              << std::setw(14) << "Throughput" << std::setw(12) << "Avg(ns)"
              << std::setw(12) << "P50(ns)" << std::setw(12) << "P90(ns)"
              << std::setw(12) << "P99(ns)" << std::setw(12)
              << "UtilStdDev" << std::setw(10) << "AvgUtil%"
              << std::setw(15) << "Succ/Total"
              << std::endl;
    std::cout << std::string(170, '-') << std::endl;
}

static void printFillUpResult(const FillUpResult& r) {
    std::string alloc_ratio = std::to_string(r.success_count) + "/" +
                              std::to_string(r.total_count);
    std::ostringstream cap_ss;
    cap_ss << std::fixed << std::setprecision(1) << r.cluster_capacity_gb;
    std::cout << std::left << std::setw(18) << r.strategy_name << std::setw(9)
              << r.replica_num << std::setw(10) << r.num_segments
              << std::setw(12)
              << (std::to_string(r.alloc_size / KiB) + "KB") << std::setw(12)
              << cap_ss.str()
              << std::setw(8) << (r.skewed ? "yes" : "no")
              << std::right << std::fixed << std::setprecision(0)
              << std::setw(14) << r.throughput << std::setw(12) << r.avg_ns
              << std::setw(12) << r.p50_ns << std::setw(12) << r.p90_ns
              << std::setw(12) << r.p99_ns << std::setprecision(4)
              << std::setw(12) << r.final_util_stddev << std::setprecision(2)
              << std::setw(9) << (r.final_avg_util * 100.0) << "%"
              << std::setw(15) << alloc_ratio
              << std::endl;
}

// ============================================================
//  Pretty printing — Scale-Out
// ============================================================

static void printScaleOutHeader() {
    std::cout << std::string(216, '-') << std::endl;
    std::cout << std::left << std::setw(18) << "Strategy" << std::setw(9)
              << "Replica" << std::setw(10) << "Segments" << std::setw(12)
              << "AllocSize" << std::setw(16) << "Cluster(GB)"
              << std::setw(8) << "Skewed" << std::right
              << std::setw(14) << "Throughput" << std::setw(12) << "Avg(ns)"
              << std::setw(12) << "P50(ns)" << std::setw(12) << "P90(ns)"
              << std::setw(12) << "P99(ns)" << std::setw(12)
              << "UtilStdDev"
              << std::setw(10) << "ConvUtil%" << std::setw(11) << "FinalUtil%"
              << std::setw(14) << "Converge@"
              << std::setw(15) << "Succ/Total"
              << std::endl;
    std::cout << std::string(216, '-') << std::endl;
}

static void printScaleOutResult(const ScaleOutResult& r) {
    std::string converge_str = "N/A";
    std::string conv_util_str = "N/A";
    
    if (r.convergence_alloc_count > 0) {
        converge_str = std::to_string(r.convergence_alloc_count);
        std::ostringstream util_ss;
        util_ss << std::fixed << std::setprecision(2) << (r.converge_avg_util * 100.0) << "%";
        conv_util_str = util_ss.str();
    }

    std::string alloc_ratio = std::to_string(r.success_count) + "/" +
                              std::to_string(r.total_count);
    std::ostringstream cap_ss;
    cap_ss << std::fixed << std::setprecision(1) << r.initial_capacity_gb 
           << "->" << r.scaled_capacity_gb;

    std::cout << std::left << std::setw(18) << r.strategy_name << std::setw(9)
              << r.replica_num << std::setw(10) << r.num_segments
              << std::setw(12)
              << (std::to_string(r.alloc_size / KiB) + "KB") << std::setw(16)
              << cap_ss.str()
              << std::setw(8) << (r.skewed ? "yes" : "no")
              << std::right << std::fixed << std::setprecision(0)
              << std::setw(14) << r.throughput << std::setw(12) << r.avg_ns
              << std::setw(12) << r.p50_ns << std::setw(12) << r.p90_ns
              << std::setw(12) << r.p99_ns << std::setprecision(4)
              << std::setw(12) << r.final_util_stddev 
              << std::setw(10) << conv_util_str
              << std::setw(11) << (std::to_string(std::round(r.final_avg_util * 10000.0) / 100.0).substr(0, std::to_string(std::round(r.final_avg_util * 10000.0) / 100.0).find('.') + 3) + "%")
              << std::setw(14) << converge_str
              << std::setw(15) << alloc_ratio << std::endl;
}

// ============================================================
//  Matrix benchmark (--run_all)
// ============================================================

// TODO:
// 加一个带deallocate的稳态运行case？但是我感觉deallocate只是在节点离开时才会出现？应该并不频繁吧？有必要吗？
static void runAllBenchmarks() { // TODO：你这个也不是runall啊
    std::vector<int> segment_counts = {1, 10, 100, 512, 1024};
    std::vector<size_t> alloc_sizes = {
        512 * KiB, 8 * MiB, 32 * MiB};
    std::vector<int> replica_nums = {1, 2, 3};
    std::vector<AllocationStrategyType> strategies = {
        AllocationStrategyType::RANDOM,
        AllocationStrategyType::FREE_RATIO_FIRST,
    };
    std::vector<bool> skewed_options = {false, true};

    std::cout << "\n=== AllocationStrategy Fill-Up Benchmark Matrix ===\n"
              << "Note: Test will exit early if " << 10
              << " consecutive allocations fail.\n"
              << "Config: num_allocations=" << FLAGS_num_allocations
              << ", segment_capacity=" << FLAGS_segment_capacity << " MB\n"
              << std::endl;

    for (auto skew : skewed_options) {
        for (auto strategy : strategies) {
            printFillUpHeader();
            for (auto segs : segment_counts) {
                for (auto asize : alloc_sizes) {
                    for (auto rep : replica_nums) {
                        // Skip impossible configs: can't have more replicas
                        // than segments
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
                        cfg.workload_type = WorkloadType::FILL_UP;

                        auto result = runFillUpBenchmark(cfg);
                        printFillUpResult(result);
                    }
                }
            }
        }
    }
}

static void runScaleOutMatrix(
    const std::vector<AllocationStrategyType>& strategies) {
    std::vector<int> segment_counts = {1, 10, 100, 512, 1024};
    std::vector<size_t> alloc_sizes = {
        512 * KiB, 8 * MiB, 32 * MiB};
    std::vector<int> replica_nums = {1, 2, 3};
    std::vector<bool> skewed_options = {false, true};

    std::cout << "\n=== Scale-Out Workload Benchmark (Matrix) ===\n"
              << "Note: Test will exit early if 10 consecutive allocations fail.\n"
              << "Design: Pre-fill to " << FLAGS_scale_out_trigger_pct << "% capacity, "
              << "then measure 20% of expanded capacity (Cap: 500k allocations).\n"
              << "Config: segment_capacity=" << FLAGS_segment_capacity << " MB\n"
              << std::endl;

    for (auto skew : skewed_options) {
        for (auto strategy : strategies) {
            printScaleOutHeader();
            for (auto segs : segment_counts) {
                for (auto asize : alloc_sizes) {
                    for (auto rep : replica_nums) {
                        // Skip impossible configs: can't have more replicas
                        // than segments
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
                        
                        // Default scale-out params for the matrix
                        cfg.scale_out_trigger_pct = 50;
                        cfg.scale_out_new_segments = std::max(1, segs / 2);

                        auto result = runScaleOutBenchmark(cfg);
                        printScaleOutResult(result);
                    }
                }
            }
        }
    }
}

// ============================================================
//  main
// ============================================================

int main(int argc, char* argv[]) {
    gflags::SetUsageMessage(
        "AllocationStrategy performance benchmark.\n"
        "Usage: allocation_strategy_bench [flags]");
    gflags::ParseCommandLineFlags(&argc, &argv, true);

    if (FLAGS_run_all) {
        runAllBenchmarks();
        return 0;
    }

    // Resolve strategies
    std::vector<AllocationStrategyType> strategies_to_run;
    if (FLAGS_strategy == "all") {
        strategies_to_run = {AllocationStrategyType::RANDOM,
                             AllocationStrategyType::FREE_RATIO_FIRST};
    } else {
        strategies_to_run = {parseStrategy(FLAGS_strategy)};
    }

    WorkloadType wl = parseWorkload(FLAGS_workload);

    if (wl == WorkloadType::SCALE_OUT) { // TODO: 怎么还是不统一啊？
        runScaleOutMatrix(strategies_to_run);
        return 0;
    }

    // Default: Fill-Up single run
    std::cout << "\n=== AllocationStrategy Benchmark ===" << std::endl;
    std::cout << "Configuration:" << std::endl;
    std::cout << "  Segments:        " << FLAGS_num_segments << std::endl;
    std::cout << "  Segment capacity:" << FLAGS_segment_capacity << " MB"
              << std::endl;
    std::cout << "  Alloc size:      " << FLAGS_alloc_size << " KB"
              << std::endl;
    std::cout << "  Replica num:     " << FLAGS_replica_num << std::endl;
    std::cout << "  Num allocations: " << FLAGS_num_allocations << std::endl;
    std::cout << "  Skewed:          " << (FLAGS_skewed ? "yes" : "no")
              << std::endl;
    std::cout << std::endl;

    printFillUpHeader();

    for (auto strategy : strategies_to_run) {
        BenchConfig cfg;
        cfg.num_segments = FLAGS_num_segments;
        cfg.segment_capacity =
            static_cast<size_t>(FLAGS_segment_capacity) * MiB;
        cfg.alloc_size = static_cast<size_t>(FLAGS_alloc_size) * KiB;
        cfg.replica_num = FLAGS_replica_num;
        cfg.num_allocations = FLAGS_num_allocations;
        cfg.skewed = FLAGS_skewed;
        cfg.strategy_type = strategy;
        cfg.strategy_name = strategyName(strategy);
        cfg.workload_type = WorkloadType::FILL_UP;

        auto result = runFillUpBenchmark(cfg);
        printFillUpResult(result);
    }

    std::cout << std::endl;
    return 0;
}
