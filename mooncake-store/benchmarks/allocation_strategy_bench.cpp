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
    int scale_out_trigger_pct = 50;
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
    // avg utilization at first-convergence point; falls back to final avg if
    // convergence was never reached
    double converge_avg_util = 0.0;
    int convergence_alloc_count;  // -1 if not converged
};

struct ScaleOutResult {
    BenchResult base;

    // stddev snapshots around the scale-out event
    double stddev_before_scale = 0.0;
    double stddev_just_after_scale = 0.0;

    // Alloc index at which injection happened
    int trigger_alloc_idx = -1;

    // How many allocs (after trigger) until stddev re-converges below 0.05
    // -1 = never re-converged within the remaining allocs
    int re_converge_allocs = -1;

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
 *   - stddev_over_time: utilization stddev snapshot every sample_interval ops
 *   - avg_util_over_time: avg utilization snapshot at the same intervals
 *   - out_final_avg_util: avg utilization across all segments at run end
 *   - out_final_stddev: utilization stddev across all segments at run end
 *
 * All out_* values and *_over_time vectors MUST be computed/populated before
 * active_allocations goes out of scope; AllocatedBuffer destructors return
 * memory to the allocators, so metrics computed after run() returns read 0.
 */
class WorkloadRunner {
   public:
    virtual ~WorkloadRunner() = default;

    virtual void run(AllocatorManager& manager, AllocationStrategy* strategy,
                     const BenchConfig& cfg, std::vector<double>& latencies,
                     std::vector<double>& stddev_over_time,
                     std::vector<double>& avg_util_over_time,
                     double& out_final_avg_util, double& out_final_stddev) = 0;
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
             std::vector<double>& stddev_over_time,
             std::vector<double>& avg_util_over_time,
             double& out_final_avg_util, double& out_final_stddev) override {
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
                avg_util_over_time.push_back(computeAverageUtilAll(manager));
            }
        }

        // Compute final metrics while active_allocations is still alive
        out_final_avg_util = computeAverageUtilAll(manager);
        out_final_stddev = computeUtilizationStdDev(manager);
        // active_allocations destructs here → memory freed
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
             std::vector<double>& stddev_over_time,
             std::vector<double>& avg_util_over_time,
             double& out_final_avg_util, double& out_final_stddev) override {
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
                out_->trigger_alloc_idx = i;
                injected = true;
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
                avg_util_over_time.push_back(computeAverageUtilAll(manager));

                if (injected && !new_node_allocs.empty()) {
                    out_->new_node_util_over_time.push_back(
                        computeAverageUtil(new_node_allocs));
                }
            }
        }

        // --- Find re-convergence point after injection ---
        out_->re_converge_allocs = -1;
        if (injected) {
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

        // Compute final metrics while active_allocations is still alive
        out_final_avg_util = computeAverageUtilAll(manager);
        out_final_stddev = computeUtilizationStdDev(manager);
        // active_allocations destructs here → memory freed
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
    stddev_over_time.reserve(
        cfg.num_allocations / FLAGS_convergence_sample_interval + 1);
    std::vector<double> avg_util_over_time;
    avg_util_over_time.reserve(
        cfg.num_allocations / FLAGS_convergence_sample_interval + 1);

    // --- Dispatch to WorkloadRunner ---
    std::unique_ptr<WorkloadRunner> runner;
    if (cfg.workload_type == WorkloadType::SCALE_OUT && scaleout_result) {
        runner = std::make_unique<ScaleOutWorkloadRunner>(scaleout_result);
    } else {
        runner = std::make_unique<FillUpWorkloadRunner>();
    }

    auto total_start = std::chrono::high_resolution_clock::now();
    double runner_avg_util = 0.0;
    double runner_final_stddev = 0.0;
    runner->run(manager, strategy.get(), cfg, latencies, stddev_over_time,
                avg_util_over_time, runner_avg_util, runner_final_stddev);
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

    double avg_ns = latencies.empty() ? 0.0
                                      : std::accumulate(latencies.begin(),
                                                        latencies.end(), 0.0) /
                                            latencies.size();

    // --- Find convergence point (fill-up semantics) ---
    // We track the START of the last stable convergence window:
    // - When stddev drops below the threshold, record the alloc count if we
    //   haven't already started a window.
    // - When stddev rises back above the threshold, reset (the strategy
    //   diverged again, so the previous window doesn't count as stable).
    // This way converged_at reflects when the strategy *finally* settled.
    const double convergence_threshold = 0.05;
    double first_converge_stddev = -1;
    double first_converge_avg_util = -1.0;  // avg util AT first convergence
    int converged_at = -1;
    bool in_converged_window = false;

    for (size_t i = 0; i < stddev_over_time.size(); ++i) {
        int allocs_done =
            static_cast<int>((i + 1) * FLAGS_convergence_sample_interval);
        // Only evaluate convergence once the cluster is at least 10% utilized.
        // Before that, stddev is naturally near 0 (all segments nearly empty)
        // which would cause a spurious early "converged" reading.
        const double min_util_to_check = 0.10;
        bool util_sufficient =
            i < avg_util_over_time.size() &&
            avg_util_over_time[i] >= min_util_to_check;

        if (!util_sufficient) {
            // Too early in the run — don't count as converged or diverged yet
        } else if (stddev_over_time[i] < convergence_threshold) {
            if (!in_converged_window) {
                // Record the start of this convergence window
                converged_at = allocs_done;
                in_converged_window = true;
                first_converge_stddev = stddev_over_time[i];
                // avg_util_over_time is sampled at the same indices
                if (i < avg_util_over_time.size()) {
                    first_converge_avg_util = avg_util_over_time[i];
                }
            }
            // else: already in a stable window, keep the earlier start
        } else {
            // Diverged — reset; the next time it drops will start a new window
            converged_at = -1;
            in_converged_window = false;
            first_converge_stddev = -1;
            first_converge_avg_util = -1.0;
        }
    }

    // final_stddev is captured inside the runner while allocations are still
    // live
    double final_stddev = runner_final_stddev;

    // For fill-up mode: if still not converged, run a lookahead to see if it
    // ever converges
    // Lookahead logic is currently disabled (see commented block below).
    // bool converged_in_extra_run = false; // TODO: disabled second conv change
    // currently if (cfg.workload_type == WorkloadType::FILL_UP &&
    //     final_stddev >= convergence_threshold) {
    //     converged_at = -1;  // Reset false positive

    //     int max_extra = cfg.num_allocations * 10;
    //     for (int i = 0; i < max_extra; ++i) {
    //         strategy->Allocate(manager, cfg.alloc_size, cfg.replica_num);
    //         if (i > 0 && i % FLAGS_convergence_sample_interval == 0) {
    //             if (computeUtilizationStdDev(manager) <
    //             convergence_threshold) {
    //                 extra_converge_allocs = cfg.num_allocations + i;
    //                 converged_in_extra_run = true;
    //                 break;
    //             }
    //         }
    //     }
    // }

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
    res.final_util_stddev =
        converged_at == -1
            ? final_stddev
            : first_converge_stddev;  // TODO: would it be better to output both
                                      // first_converge_stddev and final_stddev?
    // converge_avg_util: util at first-convergence point; fallback to final
    // util if the strategy never converged within this run
    res.converge_avg_util = (converged_at != -1 && first_converge_avg_util >= 0)
                                ? first_converge_avg_util
                                : runner_avg_util;
    res.convergence_alloc_count = converged_at;

    return res;
}

// ============================================================
//  Pretty printing
// ============================================================

static void printHeader() {
    std::cout << std::string(157, '-') << std::endl;
    std::cout << std::left << std::setw(18) << "Strategy" << std::setw(10)
              << "Segments" << std::setw(12) << "AllocSize" << std::setw(9)
              << "Replica" << std::setw(8) << "Skewed" << std::right
              << std::setw(14) << "Throughput" << std::setw(12) << "Avg(ns)"
              << std::setw(12) << "P50(ns)" << std::setw(12) << "P90(ns)"
              << std::setw(12) << "P99(ns)" << std::setw(12)
              << "UtilStdDev"  // when first converged
              << std::setw(10) << "ConvUtil%" << std::setw(14) << "Converge@"
              << std::endl;
    std::cout << std::string(157, '-') << std::endl;
}

static void printResult(const BenchResult& r) {
    std::string converge_str = "N/A";
    if (r.convergence_alloc_count > 0) {
        converge_str = std::to_string(r.convergence_alloc_count);
        // if (r.converged_in_extra_run) converge_str += "*"; // TODO: check
        // later
    }

    std::cout << std::left << std::setw(18) << r.strategy_name << std::setw(10)
              << r.num_segments << std::setw(12)
              << (std::to_string(r.alloc_size / KiB) + "KB") << std::setw(9)
              << r.replica_num << std::setw(8) << (r.skewed ? "yes" : "no")
              << std::right << std::fixed << std::setprecision(0)
              << std::setw(14) << r.throughput << std::setw(12) << r.avg_ns
              << std::setw(12) << r.p50_ns << std::setw(12) << r.p90_ns
              << std::setw(12) << r.p99_ns << std::setprecision(4)
              << std::setw(12) << r.final_util_stddev << std::setprecision(2)
              << std::setw(9) << (r.converge_avg_util * 100.0) << "%"
              << std::setw(14) << converge_str << std::endl;
}

// Print two-section Scale-Out report for a list of results.
// Section 1: Baseline perf (throughput/latency) — what the strategy costs.
// Section 2: Adoption speed — how fast new nodes get utilized.
static void printScaleOutReport(const std::vector<ScaleOutResult>& results,
                                const BenchConfig& cfg) {
    const std::string sep(100, '-');
    const std::string thin_sep(100, ' ');

    // ---- Section 1: Performance baseline --------------------------------
    std::cout << "\n  [Phase 1 & 2 : Performance Baseline]\n";
    std::cout << "  Measured over all " << cfg.num_allocations
              << " allocations (both phases)\n";
    std::cout << sep << "\n";
    std::cout << std::left << std::setw(18) << "Strategy" << std::setw(14)
              << "Throughput" << std::setw(12) << "Avg(ns)" << std::setw(12)
              << "P50(ns)" << std::setw(12) << "P90(ns)" << std::setw(12)
              << "P99(ns)" << "\n";
    std::cout << sep << "\n";
    for (const auto& r : results) {
        std::cout << std::left << std::setw(18) << r.base.strategy_name
                  << std::right << std::fixed << std::setprecision(0)
                  << std::setw(14) << r.base.throughput << std::setw(12)
                  << r.base.avg_ns << std::setw(12) << r.base.p50_ns
                  << std::setw(12) << r.base.p90_ns << std::setw(12)
                  << r.base.p99_ns << "\n";
    }

    // ---- Section 2: Scale-Out adoption metrics --------------------------
    std::cout << "\n  [Scale-Out Adoption Metrics]\n";
    std::cout << "  Injection point: alloc #" << results[0].trigger_alloc_idx
              << " (+" << cfg.scale_out_new_segments << " nodes)\n";
    std::cout << sep << "\n";
    // Column headers
    std::cout << std::left << std::setw(18) << "Strategy";
    // stddev columns
    std::cout << std::right << std::setw(16) << "StdDev(pre-inj)";
    std::cout << std::setw(16) << "StdDev(post-inj)";
    std::cout << std::setw(16) << "ReConvAllocs";
    std::cout << std::setw(16) << "NewNodeUtil%(f)";
    std::cout << "\n";
    std::cout << sep << "\n";
    for (const auto& r : results) {
        double final_new_util = r.new_node_util_over_time.empty()
                                    ? 0.0
                                    : r.new_node_util_over_time.back();
        std::string reconverge_str = r.re_converge_allocs > 0
                                         ? std::to_string(r.re_converge_allocs)
                                         : "N/A";
        std::cout << std::left << std::setw(18) << r.base.strategy_name
                  << std::right << std::fixed << std::setprecision(4)
                  << std::setw(16) << r.stddev_before_scale << std::setw(16)
                  << r.stddev_just_after_scale << std::setw(16)
                  << reconverge_str << std::setprecision(1) << std::setw(15)
                  << (final_new_util * 100.0) << "%\n";
    }
    std::cout << "\n";
    std::cout << "  StdDev(pre-inj):  utilization std-dev of original nodes at"
                 " injection\n";
    std::cout << "  StdDev(post-inj): std-dev spike right after injection"
                 " (new nodes are empty)\n";
    std::cout << "  ReConvAllocs:     allocs AFTER injection to re-settle below"
                 " stddev=0.05\n";
    std::cout << "  NewNodeUtil%(f):  final avg utilization of injected"
                 " nodes\n";
}

// ============================================================
//  Matrix benchmark (--run_all)
// ============================================================

// TODO:
// 加一个带deallocate的稳态运行case？但是我感觉deallocate只是在节点离开时才会出现？应该并不频繁吧？有必要吗？
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

// ---- ScaleOut scenario matrix ----------------------------------------
// Each row is one logical "test case".  Add new rows here to extend coverage.
// Fields: num_segments, alloc_size, replica_num, new_segments, trigger_pct
struct ScaleOutScenario {
    int num_segments;
    size_t alloc_size;  // bytes
    int replica_num;
    int new_segments;
    int trigger_pct;
    std::string label;  // human-readable description
};

// Convenience macro-style helper to convert KB/MB inline
static constexpr size_t kScenarioKiB =
    KiB;  // TODO：要你麻痹的这个奇怪命名干什么？
static constexpr size_t kScenarioMiB = MiB;

// clang-format off
static const std::vector<ScaleOutScenario> kScaleOutScenarios = { // TODO: 清理没有用的垃圾参数和注释；
    // {num_segs, alloc_size,        replica, new_segs, trigger%, label}
    // Small cluster, small allocs —— typical KV-cache shard
    { 10,  64*kScenarioKiB, 1,  5, 50, "small-cluster / small-alloc" },
    { 10,  64*kScenarioKiB, 1, 10, 50, "small-cluster / double scale-out" },
    // Medium cluster, big allocs —— model-weight sharding
    { 50,   4*kScenarioMiB, 1, 10, 50, "medium-cluster / 4MB alloc" },
    { 50,  32*kScenarioMiB, 1, 10, 50, "medium-cluster / 32MB alloc" },
    // Large cluster —— production-scale
    {100,   1*kScenarioMiB, 1, 20, 50, "large-cluster / 1MB alloc" },
    {100,   1*kScenarioMiB, 2, 20, 50, "large-cluster / 1MB alloc / replica=2" },
    // Late injection (80%) —— almost-full cluster gets new nodes
    { 50,   4*kScenarioMiB, 1, 10, 80, "near-full / late injection" },
};
// clang-format on

static void runScaleOutBenchmark(
    const std::vector<AllocationStrategyType>& strategies) {
    std::cout << "\n=== Scale-Out Workload Benchmark (Matrix) ===\n";
    std::cout << "  Total allocs per run : " << FLAGS_num_allocations << "\n";
    std::cout << "  Strategies           : ";
    for (size_t i = 0; i < strategies.size(); ++i) {
        if (i) std::cout << ", ";
        std::cout << strategyName(strategies[i]);
    }
    std::cout << "\n";

    for (const auto& scenario : kScaleOutScenarios) {
        // Build base config from scenario
        BenchConfig cfg;
        cfg.num_segments = scenario.num_segments;
        cfg.segment_capacity =
            static_cast<size_t>(FLAGS_segment_capacity) * MiB;
        cfg.alloc_size = scenario.alloc_size;
        cfg.replica_num = scenario.replica_num;
        cfg.num_allocations = FLAGS_num_allocations;
        cfg.skewed = FLAGS_skewed;
        cfg.workload_type = WorkloadType::SCALE_OUT;
        cfg.scale_out_trigger_pct = scenario.trigger_pct;
        cfg.scale_out_new_segments = scenario.new_segments;

        // Skip impossible configs
        if (scenario.replica_num > scenario.num_segments) continue;

        std::cout << "\n  Scenario: " << scenario.label << "\n";
        std::cout << "    Segments " << scenario.num_segments << "  →  +"
                  << scenario.new_segments << " new (at "
                  << scenario.trigger_pct << "% = alloc #"
                  << cfg.num_allocations * scenario.trigger_pct / 100 << ")"
                  << "  |  AllocSize " << scenario.alloc_size / KiB << " KB"
                  << "  |  Replica " << scenario.replica_num << "\n";

        // Collect results for all strategies, then print grouped
        std::vector<ScaleOutResult> all_results;
        all_results.reserve(strategies.size());

        for (auto strategy : strategies) {
            cfg.strategy_type = strategy;
            cfg.strategy_name = strategyName(strategy);

            ScaleOutResult res;
            res.base = runBenchmark(cfg, &res);
            all_results.push_back(std::move(res));
        }

        printScaleOutReport(all_results, cfg);
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

    if (wl == WorkloadType::SCALE_OUT) {  // TODO: 这里函数出口也太不统一了
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
