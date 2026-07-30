#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <iomanip>
#include <iostream>
#include <string>
#include <thread>
#include <vector>

#include <gflags/gflags.h>

#include "offset_allocator/offset_allocator.h"

DEFINE_uint32(threads, 16, "Number of allocator worker threads");
DEFINE_uint64(iterations, 100000, "Operations per worker thread");

namespace {

using Clock = std::chrono::steady_clock;
using mooncake::offset_allocator::OffsetAllocator;

constexpr uint64_t kMiB = 1024ULL * 1024;
constexpr uint64_t kPoolSize = 1024 * kMiB;
constexpr uint64_t kHighWaterFillSize = 960 * kMiB;
constexpr uint64_t kFailedRequestSize = 128 * kMiB;
constexpr uint64_t kSuccessfulRequestSize = 4 * 1024;

struct Result {
    std::string name;
    uint64_t operations;
    uint64_t unexpected_results;
    double seconds;
    std::vector<uint64_t> latencies_ns;
};

template <typename Operation>
Result runConcurrentBenchmark(const std::string& name, Operation operation) {
    std::atomic<uint32_t> ready{0};
    std::atomic<bool> start{false};
    std::atomic<uint64_t> unexpected_results{0};
    std::vector<std::vector<uint64_t>> per_thread_latencies(FLAGS_threads);
    std::vector<std::thread> workers;
    workers.reserve(FLAGS_threads);

    for (uint32_t thread_index = 0; thread_index < FLAGS_threads;
         ++thread_index) {
        workers.emplace_back([&, thread_index] {
            auto& latencies = per_thread_latencies[thread_index];
            latencies.reserve(FLAGS_iterations);
            ready.fetch_add(1, std::memory_order_release);
            while (!start.load(std::memory_order_acquire)) {
                std::this_thread::yield();
            }

            for (uint64_t i = 0; i < FLAGS_iterations; ++i) {
                const auto begin = Clock::now();
                if (!operation()) {
                    unexpected_results.fetch_add(1, std::memory_order_relaxed);
                }
                const auto end = Clock::now();
                latencies.push_back(
                    std::chrono::duration_cast<std::chrono::nanoseconds>(end -
                                                                         begin)
                        .count());
            }
        });
    }

    while (ready.load(std::memory_order_acquire) != FLAGS_threads) {
        std::this_thread::yield();
    }
    const auto begin = Clock::now();
    start.store(true, std::memory_order_release);
    for (auto& worker : workers) {
        worker.join();
    }
    const auto end = Clock::now();

    Result result{
        .name = name,
        .operations = FLAGS_iterations * FLAGS_threads,
        .unexpected_results =
            unexpected_results.load(std::memory_order_relaxed),
        .seconds = std::chrono::duration<double>(end - begin).count(),
        .latencies_ns = {},
    };
    result.latencies_ns.reserve(result.operations);
    for (auto& latencies : per_thread_latencies) {
        result.latencies_ns.insert(result.latencies_ns.end(), latencies.begin(),
                                   latencies.end());
    }
    return result;
}

uint64_t percentile(const std::vector<uint64_t>& values, double percentile) {
    const size_t index = static_cast<size_t>(
        percentile * static_cast<double>(values.size() - 1));
    return values[index];
}

void printResult(Result result) {
    std::sort(result.latencies_ns.begin(), result.latencies_ns.end());
    const double qps = static_cast<double>(result.operations) / result.seconds;

    std::cout << std::fixed << std::setprecision(2) << result.name
              << ": operations=" << result.operations << ", qps=" << qps
              << ", p50_ns=" << percentile(result.latencies_ns, 0.50)
              << ", p99_ns=" << percentile(result.latencies_ns, 0.99)
              << ", max_ns=" << result.latencies_ns.back()
              << ", unexpected_results=" << result.unexpected_results
              << std::endl;
}

}  // namespace

int main(int argc, char** argv) {
    gflags::ParseCommandLineFlags(&argc, &argv, true);
    if (FLAGS_threads == 0 || FLAGS_iterations == 0) {
        std::cerr << "threads and iterations must be greater than zero"
                  << std::endl;
        return 1;
    }

    auto high_water_allocator = OffsetAllocator::create(
        0, kPoolSize, FLAGS_threads + 16, FLAGS_threads + 16);
    auto high_water_fill = high_water_allocator->allocate(kHighWaterFillSize);
    if (!high_water_fill.has_value()) {
        std::cerr << "failed to prepare high-water allocator" << std::endl;
        return 1;
    }
    std::cout << "threads=" << FLAGS_threads
              << ", iterations_per_thread=" << FLAGS_iterations
              << ", high_water_percent="
              << 100.0 * static_cast<double>(kHighWaterFillSize) / kPoolSize
              << ", largest_free_region="
              << high_water_allocator->storageReport().largestFreeRegion
              << ", failed_request_size=" << kFailedRequestSize << std::endl;

    printResult(runConcurrentBenchmark("high_water_failed_allocate", [&] {
        return !high_water_allocator->allocate(kFailedRequestSize).has_value();
    }));

    auto success_allocator = OffsetAllocator::create(
        0, kPoolSize, FLAGS_threads + 16, FLAGS_threads + 16);
    printResult(runConcurrentBenchmark("successful_allocate_free", [&] {
        return success_allocator->allocate(kSuccessfulRequestSize).has_value();
    }));
    return 0;
}
