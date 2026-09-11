// CountMinSketch benchmark: measures concurrent increment+count throughput
// for both lock-free (CAS) and mutex-based implementations.
// Build: cmake --build builddir --target count_min_sketch_bench
// Run:   ./builddir/mooncake-store/benchmarks/count_min_sketch_bench

#include <atomic>
#include <chrono>
#include <iomanip>
#include <iostream>
#include <mutex>
#include <numeric>
#include <random>
#include <string>
#include <thread>
#include <vector>
#include <functional>

#include "count_min_sketch.h"

using namespace mooncake;

// Mutex-based CountMinSketch for baseline comparison.
// Uses a single global mutex to protect all increment/count operations,
// simulating the original implementation before lock-free CAS optimization.
class MutexCountMinSketch {
   public:
    MutexCountMinSketch(size_t width, size_t depth)
        : width_(width), depth_(depth), table_(width * depth, 0) {
        // Use same hash seeds as the CAS version for fair comparison
        std::mt19937 rng(42);
        std::uniform_int_distribution<uint64_t> dist;
        seeds_.resize(depth);
        for (size_t i = 0; i < depth; ++i) {
            seeds_[i] = dist(rng);
        }
    }

    void increment(const std::string& key) {
        std::lock_guard<std::mutex> lock(mutex_);
        uint64_t h = std::hash<std::string>{}(key);
        for (size_t i = 0; i < depth_; ++i) {
            size_t idx = ((h ^ seeds_[i]) % width_) + i * width_;
            if (table_[idx] < 255) {
                table_[idx]++;
            }
        }
    }

    uint8_t count(const std::string& key) const {
        std::lock_guard<std::mutex> lock(mutex_);
        uint64_t h = std::hash<std::string>{}(key);
        uint8_t min_val = 255;
        for (size_t i = 0; i < depth_; ++i) {
            size_t idx = ((h ^ seeds_[i]) % width_) + i * width_;
            min_val = std::min(min_val, table_[idx]);
        }
        return min_val;
    }

   private:
    size_t width_;
    size_t depth_;
    std::vector<uint8_t> table_;
    std::vector<uint64_t> seeds_;
    mutable std::mutex mutex_;
};

static std::string make_key(int id) { return "key_" + std::to_string(id); }

struct BenchResult {
    int num_threads;
    int ops_per_thread;
    double elapsed_ms;
    double throughput_mops;  // million ops/sec
};

template <typename CMS>
BenchResult bench_increment(int num_threads, int ops_per_thread, int num_keys,
                            CMS& sketch) {
    std::atomic<bool> start{false};
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
    for (auto& th : threads) {
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

template <typename CMS>
BenchResult bench_mixed(int num_threads, int ops_per_thread, int num_keys,
                        CMS& sketch) {
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
    for (auto& th : threads) {
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

void print_result(const char* label, const BenchResult& r) {
    std::cout << std::setw(30) << label << " | threads=" << r.num_threads
              << " | " << std::fixed << std::setprecision(1) << r.elapsed_ms
              << " ms | " << std::setprecision(2) << r.throughput_mops
              << " M ops/s" << std::endl;
}

int main() {
    const int NUM_KEYS = 100000;
    const int OPS_PER_THREAD = 200000;
    std::vector<int> thread_counts = {1, 2, 4, 8, 16};

    std::cout << "=== CountMinSketch Benchmark: CAS vs Mutex ===" << std::endl;
    std::cout << "Keys: " << NUM_KEYS << ", Ops/thread: " << OPS_PER_THREAD
              << std::endl;
    std::cout << std::string(80, '=') << std::endl;

    // --- MUTEX BASELINE ---
    std::cout << "\n=== MUTEX BASELINE (single mutex for all operations) ==="
              << std::endl;
    {
        MutexCountMinSketch sketch(4096, 4);

        std::cout << "\n--- Increment Only ---" << std::endl;
        std::vector<BenchResult> mutex_inc_results;
        for (int t : thread_counts) {
            auto r = bench_increment(t, OPS_PER_THREAD, NUM_KEYS, sketch);
            mutex_inc_results.push_back(r);
            print_result("mutex-increment", r);
        }

        std::cout << "\n--- Mixed (80% increment, 20% count) ---" << std::endl;
        std::vector<BenchResult> mutex_mix_results;
        for (int t : thread_counts) {
            auto r = bench_mixed(t, OPS_PER_THREAD, NUM_KEYS, sketch);
            mutex_mix_results.push_back(r);
            print_result("mutex-mixed", r);
        }
    }

    // --- CAS LOCK-FREE ---
    std::cout << "\n=== CAS LOCK-FREE (atomic<uint8_t> + compare_exchange) ==="
              << std::endl;
    {
        CountMinSketch sketch(4096, 4);

        std::cout << "\n--- Increment Only ---" << std::endl;
        std::vector<BenchResult> cas_inc_results;
        for (int t : thread_counts) {
            auto r = bench_increment(t, OPS_PER_THREAD, NUM_KEYS, sketch);
            cas_inc_results.push_back(r);
            print_result("cas-increment", r);
        }

        std::cout << "\n--- Mixed (80% increment, 20% count) ---" << std::endl;
        std::vector<BenchResult> cas_mix_results;
        for (int t : thread_counts) {
            auto r = bench_mixed(t, OPS_PER_THREAD, NUM_KEYS, sketch);
            cas_mix_results.push_back(r);
            print_result("cas-mixed", r);
        }
    }

    // --- COMPARISON TABLE ---
    std::cout << "\n" << std::string(80, '=') << std::endl;
    std::cout << "  COMPARISON: CAS vs Mutex (throughput in M ops/s)"
              << std::endl;
    std::cout << std::string(80, '=') << std::endl;
    std::cout << std::setw(10) << "Threads" << std::setw(16) << "Mutex-Inc"
              << std::setw(16) << "CAS-Inc" << std::setw(12) << "Speedup"
              << std::setw(16) << "Mutex-Mix" << std::setw(16) << "CAS-Mix"
              << std::setw(12) << "Speedup" << std::endl;
    std::cout << std::string(80, '-') << std::endl;

    // Rerun to get fresh data for comparison (same conditions)
    {
        MutexCountMinSketch m_sketch(4096, 4);
        CountMinSketch c_sketch(4096, 4);

        for (size_t i = 0; i < thread_counts.size(); ++i) {
            int t = thread_counts[i];
            auto m_inc = bench_increment(t, OPS_PER_THREAD, NUM_KEYS, m_sketch);
            auto c_inc = bench_increment(t, OPS_PER_THREAD, NUM_KEYS, c_sketch);
            auto m_mix = bench_mixed(t, OPS_PER_THREAD, NUM_KEYS, m_sketch);
            auto c_mix = bench_mixed(t, OPS_PER_THREAD, NUM_KEYS, c_sketch);

            double inc_speedup = c_inc.throughput_mops / m_inc.throughput_mops;
            double mix_speedup = c_mix.throughput_mops / m_mix.throughput_mops;

            std::cout << std::setw(10) << t << std::setw(16) << std::fixed
                      << std::setprecision(2) << m_inc.throughput_mops
                      << std::setw(16) << c_inc.throughput_mops << std::setw(11)
                      << std::setprecision(1) << inc_speedup << "x"
                      << std::setw(16) << std::setprecision(2)
                      << m_mix.throughput_mops << std::setw(16)
                      << c_mix.throughput_mops << std::setw(11)
                      << std::setprecision(1) << mix_speedup << "x"
                      << std::endl;
        }
    }

    std::cout << "\n=== Benchmark Complete ===" << std::endl;
    return 0;
}
