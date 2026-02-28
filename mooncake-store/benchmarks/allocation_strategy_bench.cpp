/**
 * @file allocation_strategy_bench.cpp
 * @brief Performance benchmark for AllocationStrategy implementations.
 *
 * This benchmark measures:
 * - Allocation latency (avg / P50 / P90 / P99)
 * - Throughput (allocations per second)
 * - Load balance across segments (utilization std-dev)
 * - Convergence time to balanced state
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
 */

#include <algorithm>
#include <chrono>
#include <cmath>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <numeric>
#include <string>
#include <vector>

#include <gflags/gflags.h>

#include "allocation_strategy.h"
#include "allocator.h"
#include "types.h"

// --- gflags definitions ---
DEFINE_int32(num_segments, 10, "Number of segments to simulate");
DEFINE_int64(segment_capacity, 64,
             "Per-segment capacity in MB (base capacity for skewed mode)");
DEFINE_int64(alloc_size, 64, "Allocation size in KB");
DEFINE_int32(replica_num, 1, "Number of replicas per allocation");
DEFINE_int32(num_allocations, 10000, "Number of allocations to benchmark");
DEFINE_bool(skewed, false,
            "Use skewed segment capacities (capacity varies by index)");
DEFINE_string(strategy, "all",
              "Strategy to benchmark: Random, FreeRatioFirst, or all");
DEFINE_int32(convergence_sample_interval, 100,
             "Sample utilization stddev every N allocations");
DEFINE_bool(run_all, false,
            "Run a standard matrix of configurations for comparison");
DEFINE_string(
    dump_distribution_file, "",
    "File path to dump final segment utilization distribution (CSV format)");

// Scale-Out workload flags
DEFINE_string(workload, "fillup",
              "Workload type: fillup (default), scaleout");
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
    FILL_UP,    // Only allocate, measure throughput/latency/convergence
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
    int scale_out_trigger_pct  = 50;
    int scale_out_new_segments = 10;
};

struct BenchResult {
    std::string strategy_name;
    int num_segments;
    size_t alloc_size;
    int replica_num;
    bool skewed;

    double total_time_us;
    double throughput;  // allocs/sec
    double avg_ns;
    double p50_ns;
    double p90_ns;
    double p99_ns;

    double final_util_stddev;
    int convergence_alloc_count;  // -1 if not converged
    bool converged_in_extra_run =
        false;  // true if it converged in the lookahead loop
};

struct ScaleOutResult {
    BenchResult base;

    // stddev snapshots around the scale-out event
    double stddev_before_scale;
    double stddev_just_after_scale;

    // How many allocs (after trigger) until stddev re-converges
    // -1 = never re-converged within the remaining allocs
    int re_converge_allocs;

    // Utilization of the newly added nodes sampled over time (post-injection)
    std::vector<double> new_node_util_over_time;
};

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
        auto allocator =
            std::make_shared<OffsetBufferAllocator>(name, base_addr, capacity, name);
        manager.addAllocator(name, allocator);
        new_allocs.push_back(allocator);
    }
    return new_allocs;
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
//  CSV dump
// ============================================================

static void dumpDistribution(const BenchConfig& cfg,
                             const AllocatorManager& manager,
                             const std::string& filepath) {
    if (filepath.empty()) return;

    std::ofstream out(filepath, std::ios::app);
    if (!out.is_open()) {
        std::cerr << "Failed to open dump file: " << filepath << std::endl;
        return;
    }

    out.seekp(0, std::ios::end);
    if (out.tellp() == 0) {
        out << "Strategy,NumSegments,AllocSize,ReplicaNum,Skewed,SegmentName,"
               "Capacity,Used,Utilization\n";
    }

    for (const auto& name : manager.getNames()) {
        const auto* allocators = manager.getAllocators(name);
        if (!allocators || allocators->empty()) continue;
        for (const auto& alloc : *allocators) {
            size_t cap = alloc->capacity();
            size_t used = alloc->size();
            double util = cap > 0 ? static_cast<double>(used) / cap : 0.0;

            out << cfg.strategy_name << "," << cfg.num_segments << ","
                << cfg.alloc_size << "," << cfg.replica_num << ","
                << (cfg.skewed ? "true" : "false") << "," << name << "," << cap
                << "," << used << "," << std::fixed << std::setprecision(4)
                << util << "\n";
        }
    }
}

// ============================================================
//  WorkloadRunner abstraction
// ============================================================

/**
 * @brief Abstract base for workload generators.
 *
 * Each WorkloadRunner drives the allocation/deallocation loop and fills:
 *   - latencies: per-operation latency in nanoseconds
 *   - stddev_over_time: snapshot of utilization stddev every sample_interval ops
 */
class WorkloadRunner {
   public:
    virtual ~WorkloadRunner() = default;

    virtual void run(AllocatorManager& manager, AllocationStrategy* strategy,
                     const BenchConfig& cfg, std::vector<double>& latencies,
                     std::vector<double>& stddev_over_time) = 0;
};

// ============================================================
//  FillUpWorkloadRunner
// ============================================================

/**
 * @brief The original fill-up workload: allocate N times, never deallocate.
 *
 * Measures pure allocation throughput, latency, and convergence speed.
 */
class FillUpWorkloadRunner : public WorkloadRunner {
   public:
    void run(AllocatorManager& manager, AllocationStrategy* strategy,
             const BenchConfig& cfg, std::vector<double>& latencies,
             std::vector<double>& stddev_over_time) override {
        const int sample_interval = FLAGS_convergence_sample_interval;

        // Hold allocated replicas to prevent immediate deallocation
        std::vector<std::vector<Replica>> active_allocations;
        active_allocations.reserve(cfg.num_allocations);

        for (int i = 0; i < cfg.num_allocations; ++i) {
            auto t0 = std::chrono::high_resolution_clock::now();
            auto result =
                strategy->Allocate(manager, cfg.alloc_size, cfg.replica_num);
            auto t1 = std::chrono::high_resolution_clock::now();

            latencies.push_back(
                std::chrono::duration<double, std::nano>(t1 - t0).count());

            if (result.has_value()) {
                active_allocations.push_back(std::move(result.value()));
            }

            if (i > 0 && i % sample_interval == 0) {
                stddev_over_time.push_back(computeUtilizationStdDev(manager));
            }
        }
    }
};

// ============================================================
//  ScaleOutWorkloadRunner
// ============================================================

/**
 * @brief Scale-Out workload: partway through the run, inject new empty nodes.
 *
 * Phases:
 *   Phase 1 [0, trigger):    Run with original cluster, let it fill up.
 *   Trigger [trigger]:       Inject `scale_out_new_segments` new empty nodes.
 *   Phase 2 [trigger, end):  Continue allocating; observe how the strategy
 *                            routes traffic to the new nodes.
 *
 * The initial segment capacity is intentionally sized so that the original
 * cluster approaches ~80% utilization by the trigger point, making the
 * new-node adoption clearly visible.
 *
 * Extra results are stored in the ScaleOutResult passed by pointer; the base
 * latencies / stddev_over_time are filled normally so the common statistics
 * path remains unchanged.
 */
class ScaleOutWorkloadRunner : public WorkloadRunner {
   public:
    explicit ScaleOutWorkloadRunner(ScaleOutResult* out) : out_(out) {}

    void run(AllocatorManager& manager, AllocationStrategy* strategy,
             const BenchConfig& cfg, std::vector<double>& latencies,
             std::vector<double>& stddev_over_time) override {
        const int sample_interval = FLAGS_convergence_sample_interval;
        const int trigger_at =
            cfg.num_allocations * cfg.scale_out_trigger_pct / 100;
        const double convergence_threshold = 0.05;

        std::vector<std::vector<Replica>> active_allocations;
        active_allocations.reserve(cfg.num_allocations);

        std::vector<std::shared_ptr<BufferAllocatorBase>> new_node_allocs;
        bool injected = false;

        for (int i = 0; i < cfg.num_allocations; ++i) {
            // --- Inject new nodes at the trigger point ---
            if (!injected && i >= trigger_at) {
                out_->stddev_before_scale = computeUtilizationStdDev(manager);
                new_node_allocs = injectNewSegments(
                    manager, cfg.scale_out_new_segments, cfg.segment_capacity,
                    /*id_offset=*/cfg.num_segments);
                out_->stddev_just_after_scale =
                    computeUtilizationStdDev(manager);
                injected = true;
                std::cout << "  [ScaleOut] Injected " << cfg.scale_out_new_segments
                          << " new segments at alloc #" << i
                          << "  (stddev: " << std::fixed << std::setprecision(4)
                          << out_->stddev_before_scale << " -> "
                          << out_->stddev_just_after_scale << ")\n";
            }

            // --- Allocate ---
            auto t0 = std::chrono::high_resolution_clock::now();
            auto result =
                strategy->Allocate(manager, cfg.alloc_size, cfg.replica_num);
            auto t1 = std::chrono::high_resolution_clock::now();

            latencies.push_back(
                std::chrono::duration<double, std::nano>(t1 - t0).count());

            if (result.has_value()) {
                active_allocations.push_back(std::move(result.value()));
            }

            // --- Sample stddev & new-node utilization ---
            if (i > 0 && i % sample_interval == 0) {
                stddev_over_time.push_back(computeUtilizationStdDev(manager));

                if (injected && !new_node_allocs.empty()) {
                    out_->new_node_util_over_time.push_back(
                        computeAverageUtil(new_node_allocs));
                }
            }
        }

        // --- Find re-convergence point after injection ---
        out_->re_converge_allocs = -1;
        if (injected) {
            // Walk the stddev_over_time samples recorded after the trigger
            int trigger_sample_idx = trigger_at / sample_interval;
            for (int idx = trigger_sample_idx;
                 idx < static_cast<int>(stddev_over_time.size()); ++idx) {
                if (stddev_over_time[idx] < convergence_threshold) {
                    out_->re_converge_allocs =
                        (idx + 1) * sample_interval - trigger_at;
                    break;
                }
            }
        }
    }

   private:
    ScaleOutResult* out_;
};

// ============================================================
//  Core benchmark runner (thin dispatcher)
// ============================================================

static BenchResult runBenchmark(const BenchConfig& cfg,
                                ScaleOutResult* scaleout_result = nullptr) {
    // --- Build cluster ---
    // For ScaleOut, intentionally size down per-segment capacity so that
    // the original cluster fills up significantly by the trigger point.
    size_t effective_capacity = cfg.segment_capacity;
    if (cfg.workload_type == WorkloadType::SCALE_OUT) {
        // Target ~80% utilization at trigger point:
        //   needed = alloc_size * replica_num * trigger_allocs / num_segments
        int trigger_allocs =
            cfg.num_allocations * cfg.scale_out_trigger_pct / 100;
        size_t needed = static_cast<size_t>(cfg.alloc_size) * cfg.replica_num *
                        trigger_allocs / std::max(1, cfg.num_segments);
        // Add 25% headroom → ~80% fill at trigger point
        size_t scaleout_cap = needed * 100 / 80;
        effective_capacity = std::max(cfg.segment_capacity, scaleout_cap);
    }

    AllocatorManager manager =
        createCluster(cfg.num_segments, effective_capacity, cfg.skewed);

    auto strategy = CreateAllocationStrategy(cfg.strategy_type);

    std::vector<double> latencies;
    latencies.reserve(cfg.num_allocations);

    std::vector<double> stddev_over_time;
    stddev_over_time.reserve(cfg.num_allocations /
                                 FLAGS_convergence_sample_interval +
                             1);

    // --- Dispatch to WorkloadRunner ---
    std::unique_ptr<WorkloadRunner> runner;
    if (cfg.workload_type == WorkloadType::SCALE_OUT && scaleout_result) {
        runner = std::make_unique<ScaleOutWorkloadRunner>(scaleout_result);
    } else {
        runner = std::make_unique<FillUpWorkloadRunner>();
    }

    auto total_start = std::chrono::high_resolution_clock::now();
    runner->run(manager, strategy.get(), cfg, latencies, stddev_over_time);
    auto total_end = std::chrono::high_resolution_clock::now();

    double total_us =
        std::chrono::duration<double, std::micro>(total_end - total_start)
            .count();

    // --- Compute latency percentiles ---
    std::sort(latencies.begin(), latencies.end());
    auto percentile = [&](double p) -> double {
        if (latencies.empty()) return 0.0;
        size_t idx = static_cast<size_t>(p * latencies.size());
        if (idx >= latencies.size()) idx = latencies.size() - 1;
        return latencies[idx];
    };

    double avg_ns =
        latencies.empty()
            ? 0.0
            : std::accumulate(latencies.begin(), latencies.end(), 0.0) /
                  latencies.size();

    // --- Find convergence point (fill-up semantics) ---
    // We track the START of the last stable convergence window:
    // - When stddev drops below the threshold, record the alloc count if we
    //   haven't already started a window.
    // - When stddev rises back above the threshold, reset (the strategy
    //   diverged again, so the previous window doesn't count as stable).
    // This way converged_at reflects when the strategy *finally* settled.
    const double convergence_threshold = 0.05;
    int converged_at = -1;
    bool in_converged_window = false;

    for (size_t i = 0; i < stddev_over_time.size(); ++i) {
        int allocs_done =
            static_cast<int>((i + 1) * FLAGS_convergence_sample_interval);
        if (stddev_over_time[i] < convergence_threshold) {
            if (!in_converged_window) {
                // Record the start of this convergence window
                converged_at = allocs_done;
                in_converged_window = true;
            }
            // else: already in a stable window, keep the earlier start
        } else {
            // Diverged — reset; the next time it drops will start a new window
            converged_at = -1;
            in_converged_window = false;
        }
    }

    double final_stddev = computeUtilizationStdDev(manager);

    // For fill-up mode: if still not converged, run a lookahead to see if it
    // ever converges
    int extra_converge_allocs = -1;
    bool converged_in_extra_run = false;
    if (cfg.workload_type == WorkloadType::FILL_UP &&
        final_stddev >= convergence_threshold) {
        converged_at = -1;  // Reset false positive

        int max_extra = cfg.num_allocations * 10;
        for (int i = 0; i < max_extra; ++i) {
            strategy->Allocate(manager, cfg.alloc_size, cfg.replica_num);
            if (i > 0 && i % FLAGS_convergence_sample_interval == 0) {
                if (computeUtilizationStdDev(manager) < convergence_threshold) {
                    extra_converge_allocs = cfg.num_allocations + i;
                    converged_in_extra_run = true;
                    break;
                }
            }
        }
    }

    if (!FLAGS_dump_distribution_file.empty()) {
        dumpDistribution(cfg, manager, FLAGS_dump_distribution_file);
    }

    BenchResult res;
    res.strategy_name = cfg.strategy_name;
    res.num_segments = cfg.num_segments;
    res.alloc_size = cfg.alloc_size;
    res.replica_num = cfg.replica_num;
    res.skewed = cfg.skewed;
    res.total_time_us = total_us;
    res.throughput =
        latencies.empty() ? 0.0 : cfg.num_allocations / (total_us / 1e6);
    res.avg_ns = avg_ns;
    res.p50_ns = percentile(0.50);
    res.p90_ns = percentile(0.90);
    res.p99_ns = percentile(0.99);
    res.final_util_stddev = final_stddev;
    res.convergence_alloc_count = converged_at;
    res.converged_in_extra_run = converged_in_extra_run;

    if (converged_at == -1 && extra_converge_allocs > 0) {
        res.convergence_alloc_count = extra_converge_allocs;
    }

    return res;
}

// ============================================================
//  Pretty printing
// ============================================================

static void printHeader() {
    std::cout << std::string(145, '-') << std::endl;
    std::cout << std::left << std::setw(18) << "Strategy" << std::setw(10)
              << "Segments" << std::setw(12) << "AllocSize" << std::setw(9)
              << "Replica" << std::setw(8) << "Skewed" << std::right
              << std::setw(14) << "Throughput" << std::setw(12) << "Avg(ns)"
              << std::setw(12) << "P50(ns)" << std::setw(12) << "P90(ns)"
              << std::setw(12) << "P99(ns)" << std::setw(12) << "UtilStdDev"
              << std::setw(14) << "Converge@" << std::endl;
    std::cout << std::string(145, '-') << std::endl;
}

static void printResult(const BenchResult& r) {
    std::string converge_str = "N/A";
    if (r.convergence_alloc_count > 0) {
        converge_str = std::to_string(r.convergence_alloc_count);
        if (r.converged_in_extra_run) converge_str += "*";
    }

    std::cout << std::left << std::setw(18) << r.strategy_name << std::setw(10)
              << r.num_segments << std::setw(12)
              << (std::to_string(r.alloc_size / KiB) + "KB") << std::setw(9)
              << r.replica_num << std::setw(8) << (r.skewed ? "yes" : "no")
              << std::right << std::fixed << std::setprecision(0)
              << std::setw(14) << r.throughput << std::setw(12) << r.avg_ns
              << std::setw(12) << r.p50_ns << std::setw(12) << r.p90_ns
              << std::setw(12) << r.p99_ns << std::setprecision(4)
              << std::setw(12) << r.final_util_stddev << std::setw(14)
              << converge_str << std::endl;
}

static void printScaleOutHeader() {
    std::cout << std::string(175, '-') << std::endl;
    std::cout << std::left << std::setw(18) << "Strategy" << std::setw(9)
              << "Segs" << std::setw(14) << "AllocSize" << std::setw(9)
              << "Replica" << std::right << std::setw(14) << "Throughput"
              << std::setw(12) << "Avg(ns)" << std::setw(12) << "P99(ns)"
              << std::setw(14) << "StdDev@Trig" << std::setw(14)
              << "StdDev@Inj" << std::setw(16) << "ReConvAllocs"
              << std::setw(16) << "NewNodeUtil%" << std::endl;
    std::cout << std::string(175, '-') << std::endl;
}

static void printScaleOutResult(const ScaleOutResult& r) {
    double final_new_util = r.new_node_util_over_time.empty()
                                ? 0.0
                                : r.new_node_util_over_time.back();

    std::string reconverge_str =
        r.re_converge_allocs > 0 ? std::to_string(r.re_converge_allocs) : "N/A";

    std::cout << std::left << std::setw(18) << r.base.strategy_name
              << std::setw(9) << r.base.num_segments << std::setw(14)
              << (std::to_string(r.base.alloc_size / KiB) + "KB")
              << std::setw(9) << r.base.replica_num << std::right << std::fixed
              << std::setprecision(0) << std::setw(14) << r.base.throughput
              << std::setw(12) << r.base.avg_ns << std::setw(12)
              << r.base.p99_ns << std::setprecision(4) << std::setw(14)
              << r.stddev_before_scale << std::setw(14)
              << r.stddev_just_after_scale << std::setw(16) << reconverge_str
              << std::setprecision(1) << std::setw(16)
              << (final_new_util * 100.0) << std::endl;
}

// ============================================================
//  Matrix benchmark (--run_all)
// ============================================================

// TODO: 加一个带deallocate的稳态运行case？但是我感觉deallocate只是在节点离开时才会出现？应该并不频繁吧？有必要吗？
static void runAllBenchmarks() {
    std::vector<int> segment_counts = {1, 10, 100, 512, 1024};
    std::vector<size_t> alloc_sizes = {
        64 * KiB, 1 * MiB, 4 * MiB, 32 * MiB
        /*128 * MiB*/};  // Tested up to 128MB
                         // TODO: 有几个case,副本总容量不够分配足够多的次数;
                         //       allocate应该是静默失败了; 是否要处理?
    std::vector<int> replica_nums = {1, 2, 3};
    std::vector<AllocationStrategyType> strategies = {
        AllocationStrategyType::RANDOM,
        AllocationStrategyType::FREE_RATIO_FIRST,
    };
    std::vector<bool> skewed_options = {false, true};

    std::cout << "\n=== AllocationStrategy Benchmark Matrix ===\n" << std::endl;

    for (auto skew : skewed_options) {
        for (auto strategy : strategies) {
            printHeader();
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

                        auto result = runBenchmark(cfg);
                        printResult(result);
                    }
                }
            }
        }
    }
}

// ============================================================
//  Scale-Out benchmark runner
// ============================================================

static void runScaleOutBenchmark(
    const std::vector<AllocationStrategyType>& strategies) {
    std::cout << "\n=== Scale-Out Workload Benchmark ===\n";
    std::cout << "Configuration:\n";
    std::cout << "  Initial segments:    " << FLAGS_num_segments << "\n";
    std::cout << "  New segments:        " << FLAGS_scale_out_new_segments
              << " (injected at " << FLAGS_scale_out_trigger_pct << "%)\n";
    std::cout << "  Alloc size:          " << FLAGS_alloc_size << " KB\n";
    std::cout << "  Replica num:         " << FLAGS_replica_num << "\n";
    std::cout << "  Num allocations:     " << FLAGS_num_allocations << "\n\n";

    printScaleOutHeader();

    for (auto strategy : strategies) {
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
        cfg.workload_type = WorkloadType::SCALE_OUT;
        cfg.scale_out_trigger_pct = FLAGS_scale_out_trigger_pct;
        cfg.scale_out_new_segments = FLAGS_scale_out_new_segments;

        ScaleOutResult scaleout_res;
        scaleout_res.stddev_before_scale = 0;
        scaleout_res.stddev_just_after_scale = 0;
        scaleout_res.re_converge_allocs = -1;

        scaleout_res.base = runBenchmark(cfg, &scaleout_res);
        printScaleOutResult(scaleout_res);
    }
    std::cout << "\n* ReConvAllocs: number of allocations AFTER injection for "
                 "stddev to drop below 0.05\n";
    std::cout << "* NewNodeUtil%: final average utilization of injected nodes\n";
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

    if (wl == WorkloadType::SCALE_OUT) { // TODO: 这里函数出口也太不同意了
        runScaleOutBenchmark(strategies_to_run);
        return 0;
    }

    // Default: Fill-Up
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

    printHeader();

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

        auto result = runBenchmark(cfg);
        printResult(result);
    }

    std::cout << std::endl;
    return 0;
}
