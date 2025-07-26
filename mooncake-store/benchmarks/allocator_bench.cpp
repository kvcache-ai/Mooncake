#include <iostream>
#include <random>
#include <vector>
#include <chrono>
#include <algorithm>
#include <numeric>
#include <iomanip>

#include "offset_allocator/offset_allocator.hpp"

using namespace mooncake::offset_allocator;

class OffsetAllocatorBenchHelper {
   public:
    OffsetAllocatorBenchHelper(uint64_t baseAddress, uint32_t poolSize,
                               uint32_t maxAllocs)
        : pool_size_(poolSize),
          allocated_size_(0),
          allocator_(OffsetAllocator::create(baseAddress, poolSize, maxAllocs)),
          rd_(),
          gen_(rd_()) {}

    void allocate(uint32_t size) {
        while (true) {
            auto handle = allocator_->allocate(size);
            if (handle.has_value()) {
                allocated_.push_back(std::move(*handle));
                allocated_sizes_.push_back(size);
                allocated_size_ += size;
                break;
            }
            if (allocated_.size() == 0) {
                break;
            }
            std::uniform_int_distribution<uint32_t> dist(0,
                                                         allocated_.size() - 1);
            auto index = dist(gen_);
            std::swap(allocated_[index], allocated_.back());
            std::swap(allocated_sizes_[index], allocated_sizes_.back());
            allocated_size_ -= allocated_sizes_.back();
            allocated_.pop_back();
            allocated_sizes_.pop_back();
        }
    }

    double get_allocated_ratio() const {
        return static_cast<double>(allocated_size_) / pool_size_;
    }

   private:
    uint64_t pool_size_;
    uint64_t allocated_size_;
    std::shared_ptr<OffsetAllocator> allocator_;
    std::vector<OffsetAllocationHandle> allocated_;
    std::vector<uint32_t> allocated_sizes_;
    std::random_device rd_;
    std::mt19937 gen_;
};

template <typename BenchHelper>
void uniform_size_allocation_benchmark() {
    std::cout << std::endl
              << "=== Uniform Size Allocation Benchmark ===" << std::endl;
    const size_t max_pool_size = 2ull * 1024 * 1024 * 1024;
    std::vector<uint32_t> allocation_sizes;
    for (uint32_t i = 32; i < (1 << 26); i *= 4) {
        allocation_sizes.push_back(i);
    }
    for (uint32_t i = 32; i < (1 << 26); i *= 4) {
        allocation_sizes.push_back(i - 17);
    }
    for (uint32_t i = 32; i < (1 << 26); i *= 4) {
        allocation_sizes.push_back(i + 17);
    }
    for (uint32_t i = 32; i < (1 << 26); i *= 4) {
        allocation_sizes.push_back(i * 0.9);
    }
    for (uint32_t i = 32; i < (1 << 26); i *= 4) {
        allocation_sizes.push_back(i * 1.1);
    }

    for (auto alloc_size : allocation_sizes) {
        // For small allocation sizes, use a smaller pool size to avoid
        // benchmark runs too slow.
        size_t pool_size =
            alloc_size < 1024 ? max_pool_size / 16 : max_pool_size;
        size_t max_allocs = pool_size / alloc_size + 10;
        BenchHelper bench_helper(0x1000, pool_size, max_allocs);
        int warmup_num = pool_size / alloc_size;
        for (int i = 0; i < warmup_num; i++) {
            bench_helper.allocate(alloc_size);
        }

        // START
        auto start_time = std::chrono::high_resolution_clock::now();
        double min_util_ratio = 1.0;
        double total_util_ratio = 0.0;
        int benchmark_num = 1000000;
        for (int i = 0; i < benchmark_num; i++) {
            bench_helper.allocate(alloc_size);
            double util_ratio = bench_helper.get_allocated_ratio();
            if (util_ratio < min_util_ratio) {
                min_util_ratio = util_ratio;
            }
            total_util_ratio += util_ratio;
        }
        auto end_time = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::nanoseconds>(
            end_time - start_time);
        // END
        double avg_util_ratio = total_util_ratio / benchmark_num;
        std::cout << "Alloc size: " << alloc_size
                  << ", min util ratio: " << min_util_ratio
                  << ", avg util ratio: " << avg_util_ratio
                  << ", time: " << duration.count() / benchmark_num << " ns"
                  << std::endl;
    }
}

template <typename BenchHelper>
void random_size_allocation_benchmark() {
    std::cout << std::endl
              << "=== Random Size Allocation Benchmark ===" << std::endl;
    const size_t pool_size = 2ull * 1024 * 1024 * 1024;
    const size_t max_alloc_size = 1ull << 26;
    const size_t min_alloc_size = 1024;

    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<uint32_t> dist(min_alloc_size,
                                                 max_alloc_size);

    // Warmup
    size_t max_allocs = pool_size / min_alloc_size + 10;
    BenchHelper bench_helper(0x1000, pool_size, max_allocs);
    for (size_t warmup_size = 0; warmup_size < pool_size;) {
        size_t alloc_size = dist(gen);
        bench_helper.allocate(alloc_size);
        warmup_size += alloc_size;
    }

    int benchmark_num = 1000000;
    std::vector<double> util_ratios;
    util_ratios.reserve(benchmark_num);

    // Run benchmark
    auto start_time = std::chrono::high_resolution_clock::now();
    for (int i = 0; i < benchmark_num; i++) {
        size_t alloc_size = dist(gen);
        bench_helper.allocate(alloc_size);
        util_ratios.push_back(bench_helper.get_allocated_ratio());
    }
    auto end_time = std::chrono::high_resolution_clock::now();

    // Calculate metrics
    const double avg_time_ns =
        std::chrono::duration_cast<std::chrono::nanoseconds>(end_time -
                                                             start_time)
            .count() /
        static_cast<double>(benchmark_num);

    std::sort(util_ratios.begin(), util_ratios.end());

    const double min_util = util_ratios.front();
    const double max_util = util_ratios.back();
    const double p50 = util_ratios[util_ratios.size() * 0.50];
    const double p90 = util_ratios[util_ratios.size() * 0.10];
    const double p99 = util_ratios[util_ratios.size() * 0.01];

    const double mean_util =
        std::accumulate(util_ratios.begin(), util_ratios.end(), 0.0) /
        util_ratios.size();

    std::cout << std::fixed << std::setprecision(6);
    std::cout << "util ratio (min / p99 / p90 / p50 / max / avg): " << min_util
              << " / " << p99 << " / " << p90 << " / " << p50 << " / "
              << max_util << " / " << mean_util << std::endl;
    std::cout << "avg alloc time: " << avg_time_ns << " ns/op" << std::endl;
}

int main() {
    std::cout << "=== OffsetAllocator Benchmark ===" << std::endl;
    uniform_size_allocation_benchmark<OffsetAllocatorBenchHelper>();
    random_size_allocation_benchmark<OffsetAllocatorBenchHelper>();
}