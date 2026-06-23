// CountMinSketch benchmark: measures concurrent increment+count throughput.
// Build: cmake --build builddir --target count_min_sketch_bench
// Run:   ./builddir/mooncake-store/benchmarks/count_min_sketch_bench

#include <atomic>
#include <chrono>
#include <iomanip>
#include <iostream>
#include <numeric>
#include <random>
#include <string>
#include <thread>
#include <vector>

#include "count_min_sketch.h"

using namespace mooncake;

static std::string make_key(int id) {
    return "key_" + std::to_string(id);
}

struct BenchResult {
    int num_threads;
    int ops_per_thread;
    double elapsed_ms;
    double throughput_mops;  // million ops/sec
};

// Benchmark: each thread does |ops_per_thread| increment() calls on random keys.
BenchResult bench_increment(int num_threads, int ops_per_thread, int num_keys) {
    CountMinSketch sketch(4096, 4);
    std::atomic<bool> start{false};
    std::atomic<bool> stop{false};
    std::vector<std::thread> threads;
    std::vector<uint64_t> per_thread_ops(num_threads, 0);

    for (int t = 0; t < num_threads; ++t) {
        threads.emplace_back([&, t]() {
            std::mt19937 rng(42 + t);
            std::uniform_int_distribution<int> dist(0, num_keys - 1);
            uint64_t local_ops = 0;
            while (!start.load(std::memory_order_acquire)) {
            }
            for (int i = 0; i < ops_per_thread; ++i) {
                sketch.increment(make_key(dist(rng)));
                ++local_ops;
            }
            per_thread_ops[t] = local_ops;
        });
    }

    auto t0 = std::chrono::high_resolution_clock::now();
    start.store(true, std::memory_order_release);
    for (auto &th : threads) {
        th.join();
    }
    auto t1 = std::chrono::high_resolution_clock::now();

    double elapsed_ms =
        std::chrono::duration<double, std::milli>(t1 - t0).count();
    uint64_t total_ops = 0;
    for (auto ops : per_thread_ops) total_ops += ops;

    return {num_threads, ops_per_thread, elapsed_ms,
            static_cast<double>(total_ops) / elapsed_ms / 1000.0};
}

// Benchmark: concurrent increment + count (simulates real workload).
BenchResult bench_mixed(int num_threads, int ops_per_thread, int num_keys) {
    CountMinSketch sketch(4096, 4);
    std::atomic<bool> start{false};
    std::vector<std::thread> threads;
    std::vector<uint64_t> per_thread_ops(num_threads, 0);

    for (int t = 0; t < num_threads; ++t) {
        threads.emplace_back([&, t]() {
            std::mt19937 rng(42 + t);
            std::uniform_int_distribution<int> key_dist(0, num_keys - 1);
            std::uniform_int_distribution<int> op_dist(0, 99);
            uint64_t local_ops = 0;
            while (!start.load(std::memory_order_acquire)) {
            }
            for (int i = 0; i < ops_per_thread; ++i) {
                int key = key_dist(rng);
                if (op_dist(rng) < 80) {
                    sketch.increment(make_key(key));  // 80% increment
                } else {
                    (void)sketch.count(make_key(key));  // 20% count
                }
                ++local_ops;
            }
            per_thread_ops[t] = local_ops;
        });
    }

    auto t0 = std::chrono::high_resolution_clock::now();
    start.store(true, std::memory_order_release);
    for (auto &th : threads) {
        th.join();
    }
    auto t1 = std::chrono::high_resolution_clock::now();

    double elapsed_ms =
        std::chrono::duration<double, std::milli>(t1 - t0).count();
    uint64_t total_ops = 0;
    for (auto ops : per_thread_ops) total_ops += ops;

    return {num_threads, ops_per_thread, elapsed_ms,
            static_cast<double>(total_ops) / elapsed_ms / 1000.0};
}

void print_result(const char *label, const BenchResult &r) {
    std::cout << std::setw(30) << label << " | threads=" << r.num_threads
              << " | " << std::fixed << std::setprecision(1) << r.elapsed_ms
              << " ms | " << std::setprecision(2) << r.throughput_mops
              << " M ops/s" << std::endl;
}

int main() {
    const int NUM_KEYS = 100000;
    const int OPS_PER_THREAD = 200000;
    std::vector<int> thread_counts = {1, 2, 4, 8, 16};

    std::cout << "=== CountMinSketch Benchmark ===" << std::endl;
    std::cout << "Keys: " << NUM_KEYS
              << ", Ops/thread: " << OPS_PER_THREAD << std::endl;
    std::cout << std::string(80, '-') << std::endl;

    std::cout << "\n--- Increment Only ---" << std::endl;
    for (int t : thread_counts) {
        auto r = bench_increment(t, OPS_PER_THREAD, NUM_KEYS);
        print_result("increment", r);
    }

    std::cout << "\n--- Mixed (80% increment, 20% count) ---" << std::endl;
    for (int t : thread_counts) {
        auto r = bench_mixed(t, OPS_PER_THREAD, NUM_KEYS);
        print_result("mixed", r);
    }

    std::cout << "\n=== Benchmark Complete ===" << std::endl;
    return 0;
}
