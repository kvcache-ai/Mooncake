/**
 * @file allocation_strategy_bench.cpp
 * @brief Performance benchmark for AllocationStrategy implementations.
 *
 * This benchmark measures:
 * - Allocation latency (avg / P50 / P90 / P99)
 * - Throughput (allocations per second)
 * - Load balance across segments (utilization std-dev)
 * - Convergence time to balanced state
 *
 * Usage:
 *   ./allocation_strategy_bench [flags]
 *
 * Key flags:
 *   --num_segments      Number of segments (default: 100)
 *   --segment_capacity  Per-segment capacity in MB (default: 64)
 *   --alloc_size        Allocation size in KB (default: 64)
 *   --replica_num       Replicas per allocation (default: 1)
 *   --num_allocations   Total allocations to run (default: 10000)
 *   --skewed            Use skewed segment capacities (default: false)
 *   --run_all           Run all strategies with a standard matrix
 */

#include <algorithm>
#include <chrono>
#include <cmath>
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
DEFINE_int32(num_segments, 100, "Number of segments to simulate");
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

using namespace mooncake;

static constexpr size_t MiB = 1024ULL * 1024;
static constexpr size_t KiB = 1024ULL;

// ============================================================
//  Helpers
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
};

/**
 * @brief Create an AllocatorManager populated with N OffsetBufferAllocators.
 *
 * Each allocator manages only offset metadata, so memory overhead is minimal
 * even for very large simulated capacities.
 */
static AllocatorManager createCluster(int num_segments, size_t base_capacity,
                                      bool skewed) {
    AllocatorManager manager;
    for (int i = 0; i < num_segments; ++i) {
        std::string name =
            "node_" + std::to_string(i / 8) + "_seg_" + std::to_string(i % 8);
        size_t capacity =
            skewed ? base_capacity * (1 + static_cast<size_t>(i % 10))
                   : base_capacity;
        uint64_t base_addr = 0x100000000ULL + (i * base_capacity);
        auto allocator = std::make_shared<OffsetBufferAllocator>(
            name, base_addr, capacity, name);
        manager.addAllocator(name, allocator);
    }
    return manager;
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
//  Core benchmark runner
// ============================================================

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
};

static BenchResult runBenchmark(const BenchConfig& cfg) {
    // Build the cluster
    AllocatorManager manager =
        createCluster(cfg.num_segments, cfg.segment_capacity, cfg.skewed);

    auto strategy = CreateAllocationStrategy(cfg.strategy_type);

    std::vector<double> latencies;
    latencies.reserve(cfg.num_allocations);

    // For convergence tracking
    std::vector<double> stddev_over_time;
    const int sample_interval = FLAGS_convergence_sample_interval;
    stddev_over_time.reserve(cfg.num_allocations / sample_interval + 1);

    // Hold allocated replicas to prevent immediate deallocation
    std::vector<std::vector<Replica>> active_allocations;
    active_allocations.reserve(cfg.num_allocations);

    // --- Main benchmark loop ---
    auto total_start = std::chrono::high_resolution_clock::now();
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

        // Periodically sample utilization stddev for convergence tracking
        if (i > 0 && i % sample_interval == 0) {
            stddev_over_time.push_back(computeUtilizationStdDev(manager));
        }
    }
    auto total_end = std::chrono::high_resolution_clock::now();

    double total_us =
        std::chrono::duration<double, std::micro>(total_end - total_start)
            .count();

    // --- Compute latency percentiles ---
    std::sort(latencies.begin(), latencies.end());
    auto percentile = [&](double p) -> double {
        size_t idx = static_cast<size_t>(p * latencies.size());
        if (idx >= latencies.size()) idx = latencies.size() - 1;
        return latencies[idx];
    };

    double avg_ns = std::accumulate(latencies.begin(), latencies.end(), 0.0) /
                    latencies.size();

    // --- Find convergence point ---
    // A strategy is considered converged if the utilization stddev is below the threshold
    // AND the overall cluster utilization is at least 10% (to ignore the initial empty state)
    int converged_at = -1;
    const double convergence_threshold = 0.05;
    const double min_utilization_to_converge = 0.10; // 10%

    double total_capacity = 0;
    for (const auto& name : manager.getNames()) {
        const auto* allocs = manager.getAllocators(name);
        if (allocs) {
            for (const auto& a : *allocs) total_capacity += a->capacity();
        }
    }

    for (size_t i = 0; i < stddev_over_time.size(); ++i) {
        int allocs_done = static_cast<int>((i + 1) * sample_interval);
        double current_utilization = (allocs_done * cfg.alloc_size * cfg.replica_num) / total_capacity;

        if (current_utilization >= min_utilization_to_converge && stddev_over_time[i] < convergence_threshold) {
            converged_at = allocs_done;
            break;
        }
    }

    double final_stddev = computeUtilizationStdDev(manager);

    BenchResult res;
    res.strategy_name = cfg.strategy_name;
    res.num_segments = cfg.num_segments;
    res.alloc_size = cfg.alloc_size;
    res.replica_num = cfg.replica_num;
    res.skewed = cfg.skewed;
    res.total_time_us = total_us;
    res.throughput = cfg.num_allocations / (total_us / 1e6);
    res.avg_ns = avg_ns;
    res.p50_ns = percentile(0.50);
    res.p90_ns = percentile(0.90);
    res.p99_ns = percentile(0.99);
    res.final_util_stddev = final_stddev;
    res.convergence_alloc_count = converged_at;
    return res;
}

// ============================================================
//  Pretty printing
// ============================================================

static void printHeader() {
    std::cout << std::left << std::setw(18) << "Strategy" << std::setw(10)
              << "Segments" << std::setw(12) << "AllocSize" << std::setw(9)
              << "Replica" << std::setw(8) << "Skewed" << std::right
              << std::setw(14) << "Throughput" << std::setw(12) << "Avg(ns)"
              << std::setw(12) << "P50(ns)" << std::setw(12) << "P90(ns)"
              << std::setw(12) << "P99(ns)" << std::setw(12) << "UtilStdDev"
              << std::setw(14) << "Converge@" << std::endl;
    std::cout << std::string(135, '-') << std::endl;
}

static void printResult(const BenchResult& r) {
    std::cout << std::left << std::setw(18) << r.strategy_name << std::setw(10)
              << r.num_segments << std::setw(12)
              << (std::to_string(r.alloc_size / KiB) + "KB") << std::setw(9)
              << r.replica_num << std::setw(8) << (r.skewed ? "yes" : "no")
              << std::right << std::fixed << std::setprecision(0)
              << std::setw(14) << r.throughput << std::setw(12) << r.avg_ns
              << std::setw(12) << r.p50_ns << std::setw(12) << r.p90_ns
              << std::setw(12) << r.p99_ns << std::setprecision(4)
              << std::setw(12) << r.final_util_stddev << std::setw(14)
              << (r.convergence_alloc_count >= 0
                      ? std::to_string(r.convergence_alloc_count)
                      : "N/A")
              << std::endl;
}

// ============================================================
//  Matrix benchmark (--run_all)
// ============================================================

static void runAllBenchmarks() {
    std::vector<int> segment_counts = {1, 10, 100, 512};
    std::vector<size_t> alloc_sizes = {4 * KiB, 64 * KiB, 1 * MiB, 4 * MiB};
    std::vector<int> replica_nums = {1, 2, 3};
    std::vector<AllocationStrategyType> strategies = {
        AllocationStrategyType::RANDOM,
        AllocationStrategyType::FREE_RATIO_FIRST,
    };
    std::vector<bool> skewed_options = {false, true};

    std::cout << "\n=== AllocationStrategy Benchmark Matrix ===\n" << std::endl;
    printHeader();

    for (auto strategy : strategies) {
        for (auto segs : segment_counts) {
            for (auto asize : alloc_sizes) {
                for (auto rep : replica_nums) {
                    // Skip impossible configs: can't have more replicas than
                    // segments
                    if (rep > segs) continue;

                    for (auto skew : skewed_options) {
                        BenchConfig cfg;
                        cfg.num_segments = segs;
                        cfg.segment_capacity = FLAGS_segment_capacity * MiB;
                        cfg.alloc_size = asize;
                        cfg.replica_num = rep;
                        cfg.num_allocations = FLAGS_num_allocations;
                        cfg.skewed = skew;
                        cfg.strategy_type = strategy;
                        cfg.strategy_name = strategyName(strategy);

                        auto result = runBenchmark(cfg);
                        printResult(result);
                    }
                }
            }
        }
    }
}

int main(int argc, char* argv[]) {
    gflags::SetUsageMessage(
        "AllocationStrategy performance benchmark.\n"
        "Usage: allocation_strategy_bench [flags]");
    gflags::ParseCommandLineFlags(&argc, &argv, true);

    if (FLAGS_run_all) {
        runAllBenchmarks();
        return 0;
    }

    // Single strategy run
    std::vector<AllocationStrategyType> strategies_to_run;
    if (FLAGS_strategy == "all") {
        strategies_to_run = {AllocationStrategyType::RANDOM,
                             AllocationStrategyType::FREE_RATIO_FIRST};
    } else {
        strategies_to_run = {parseStrategy(FLAGS_strategy)};
    }

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

        auto result = runBenchmark(cfg);
        printResult(result);
    }

    std::cout << std::endl;
    return 0;
}
