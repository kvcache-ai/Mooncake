// Benchmarks allocator and PutStart allocation stages for concurrent puts to
// one segment. QPS and latency use separate phases; RPC, metadata, and transfer
// work are intentionally excluded.

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <iomanip>
#include <iostream>
#include <limits>
#include <memory>
#include <optional>
#include <string>
#include <thread>
#include <vector>

#include <gflags/gflags.h>

#include "allocation_strategy.h"
#include "allocator.h"
#include "offset_allocator/offset_allocator.h"

DEFINE_uint32(threads, 16, "Number of concurrent worker threads");
DEFINE_uint64(iterations, 100000, "Measured operations per worker and phase");
DEFINE_uint64(warmup_iterations, 1000,
              "Warmup operations per worker before each measured phase");
DEFINE_uint64(pool_size_mb, 1024, "Allocator capacity in MiB");
DEFINE_double(high_water_ratio, 0.90,
              "Used-space ratio for the capacity-exhaustion scenario");
DEFINE_uint64(failed_request_mb, 128,
              "Request size in MiB for expected-failure scenarios");
DEFINE_uint64(success_request_kb, 4,
              "Request size in KiB for the successful allocate/free scenario");
DEFINE_uint64(fragmentation_block_mb, 4,
              "Block size in MiB used to create deterministic fragmentation");
DEFINE_uint32(fragmentation_stride, 5,
              "Free every Nth block; 5 leaves approximately 80% used");
DEFINE_double(mixed_failure_ratio, 0.05,
              "Expected-failure ratio for the high-water mixed scenario");
DEFINE_string(layer, "all",
              "Benchmark layer: all, offset_allocator, or put_allocation");
DEFINE_string(scenario, "all",
              "Scenario: all, capacity, fragmentation, mixed, or success");

namespace {

using Clock = std::chrono::steady_clock;
using mooncake::AllocatedBuffer;
using mooncake::AllocatorManager;
using mooncake::OffsetBufferAllocator;
using mooncake::RandomAllocationStrategy;
using mooncake::ReplicaType;
using mooncake::offset_allocator::OffsetAllocationHandle;
using mooncake::offset_allocator::OffsetAllocator;
using mooncake::offset_allocator::OffsetAllocStorageReport;

constexpr uint64_t kKiB = 1024;
constexpr uint64_t kMiB = 1024 * kKiB;
constexpr uint64_t kMixedPatternSize = 10000;
constexpr uintptr_t kBenchmarkBaseAddress = 0x100000000ULL;
constexpr char kSegmentName[] = "benchmark-segment";

struct PhaseResult {
    uint64_t operations = 0;
    uint64_t unexpected_results = 0;
    double seconds = 0;
    std::vector<uint64_t> latencies_ns;
};

struct Result {
    std::string layer;
    std::string scenario;
    uint64_t operations = 0;
    uint64_t throughput_unexpected_results = 0;
    uint64_t latency_unexpected_results = 0;
    double seconds = 0;
    std::vector<uint64_t> latencies_ns;
    uint64_t capacity = 0;
    uint64_t total_free_space = 0;
    uint64_t largest_free_region = 0;
    uint64_t request_size = 0;
    uint64_t success_request_size = 0;
    double expected_failure_ratio = 0;
};

template <typename Operation>
PhaseResult runConcurrentPhase(Operation& operation, bool collect_latency) {
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
            if (collect_latency) {
                latencies.reserve(FLAGS_iterations);
            }

            for (uint64_t i = 0; i < FLAGS_warmup_iterations; ++i) {
                operation(thread_index, i);
            }

            ready.fetch_add(1, std::memory_order_release);
            while (!start.load(std::memory_order_acquire)) {
                std::this_thread::yield();
            }

            if (collect_latency) {
                for (uint64_t i = 0; i < FLAGS_iterations; ++i) {
                    const auto begin = Clock::now();
                    const bool expected_result = operation(thread_index, i);
                    const auto end = Clock::now();
                    if (!expected_result) {
                        unexpected_results.fetch_add(1,
                                                     std::memory_order_relaxed);
                    }
                    latencies.push_back(
                        std::chrono::duration_cast<std::chrono::nanoseconds>(
                            end - begin)
                            .count());
                }
            } else {
                for (uint64_t i = 0; i < FLAGS_iterations; ++i) {
                    if (!operation(thread_index, i)) {
                        unexpected_results.fetch_add(1,
                                                     std::memory_order_relaxed);
                    }
                }
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

    PhaseResult result;
    result.operations = FLAGS_iterations * FLAGS_threads;
    result.unexpected_results =
        unexpected_results.load(std::memory_order_relaxed);
    result.seconds = std::chrono::duration<double>(end - begin).count();
    if (collect_latency) {
        result.latencies_ns.reserve(result.operations);
        for (auto& latencies : per_thread_latencies) {
            result.latencies_ns.insert(result.latencies_ns.end(),
                                       latencies.begin(), latencies.end());
        }
    }
    return result;
}

template <typename Operation>
Result runConcurrentBenchmark(const std::string& layer,
                              const std::string& scenario,
                              const OffsetAllocStorageReport& report,
                              uint64_t capacity, uint64_t request_size,
                              uint64_t success_request_size,
                              double expected_failure_ratio,
                              Operation operation) {
    auto throughput = runConcurrentPhase(operation, false);
    auto latency = runConcurrentPhase(operation, true);

    Result result;
    result.layer = layer;
    result.scenario = scenario;
    result.operations = throughput.operations;
    result.throughput_unexpected_results = throughput.unexpected_results;
    result.latency_unexpected_results = latency.unexpected_results;
    result.seconds = throughput.seconds;
    result.latencies_ns = std::move(latency.latencies_ns);
    result.capacity = capacity;
    result.total_free_space = report.totalFreeSpace;
    result.largest_free_region = report.largestFreeRegion;
    result.request_size = request_size;
    result.success_request_size = success_request_size;
    result.expected_failure_ratio = expected_failure_ratio;
    return result;
}

uint64_t percentile(const std::vector<uint64_t>& values, double quantile) {
    const size_t index =
        static_cast<size_t>(quantile * static_cast<double>(values.size() - 1));
    return values[index];
}

void printCsvHeader() {
    std::cout
        << "layer,scenario,threads,iterations_per_thread,operations,"
           "throughput_seconds,qps,p50_ns,p99_ns,"
           "throughput_unexpected_results,latency_unexpected_results,"
           "capacity_bytes,used_percent,total_free_bytes,"
           "largest_free_region_bytes,request_bytes,success_request_bytes,"
           "expected_failure_ratio"
        << std::endl;
}

void printResult(Result result) {
    std::sort(result.latencies_ns.begin(), result.latencies_ns.end());
    const double qps = static_cast<double>(result.operations) / result.seconds;
    const double used_percent =
        100.0 * static_cast<double>(result.capacity - result.total_free_space) /
        static_cast<double>(result.capacity);

    std::cout << result.layer << ',' << result.scenario << ',' << FLAGS_threads
              << ',' << FLAGS_iterations << ',' << result.operations << ','
              << std::fixed << std::setprecision(6) << result.seconds << ','
              << std::setprecision(2) << qps << ','
              << percentile(result.latencies_ns, 0.50) << ','
              << percentile(result.latencies_ns, 0.99) << ','
              << result.throughput_unexpected_results << ','
              << result.latency_unexpected_results << ',' << result.capacity
              << ',' << used_percent << ',' << result.total_free_space << ','
              << result.largest_free_region << ',' << result.request_size << ','
              << result.success_request_size << ','
              << result.expected_failure_ratio << std::endl;
}

bool isSelected(const std::string& selected, const std::string& value) {
    return selected == "all" || selected == value;
}

template <typename Allocation, typename Allocate>
bool fillCapacityPressure(uint64_t capacity, double used_ratio,
                          Allocate allocate, std::vector<Allocation>& held) {
    auto allocation = allocate(
        static_cast<uint64_t>(static_cast<double>(capacity) * used_ratio));
    if (!allocation) {
        return false;
    }
    held.emplace_back(std::move(allocation));
    return true;
}

template <typename Allocation, typename Allocate>
bool createFragmentation(uint64_t block_size, uint32_t stride,
                         Allocate allocate, std::vector<Allocation>& held) {
    while (auto allocation = allocate(block_size)) {
        held.emplace_back(std::move(allocation));
    }
    if (held.size() < stride) {
        return false;
    }
    for (size_t i = 0; i < held.size(); i += stride) {
        held[i].reset();
    }
    return true;
}

class OffsetAllocatorFixture {
   public:
    OffsetAllocatorFixture(uint64_t capacity, uint64_t /* unused */)
        : capacity_(capacity),
          allocator_(OffsetAllocator::create(0, capacity)) {}

    bool prepareCapacityPressure(double used_ratio) {
        return fillCapacityPressure(
            capacity_, used_ratio,
            [&](uint64_t size) { return allocator_->allocate(size); }, held_);
    }

    bool prepareFragmentation(uint64_t block_size, uint32_t stride) {
        return createFragmentation(
            block_size, stride,
            [&](uint64_t size) { return allocator_->allocate(size); }, held_);
    }

    bool expectAllocationFailure(uint64_t request_size) {
        return !allocator_->allocate(request_size).has_value();
    }

    bool allocateAndFree(uint64_t request_size) {
        return allocator_->allocate(request_size).has_value();
    }

    OffsetAllocStorageReport report() const {
        return allocator_->storageReport();
    }

   private:
    uint64_t capacity_;
    std::shared_ptr<OffsetAllocator> allocator_;
    std::vector<std::optional<OffsetAllocationHandle>> held_;
};

class PutAllocationFixture {
   public:
    PutAllocationFixture(uint64_t capacity, uint64_t /* unused */)
        : capacity_(capacity),
          allocator_(std::make_shared<OffsetBufferAllocator>(
              kSegmentName, kBenchmarkBaseAddress, capacity,
              "benchmark-endpoint", ReplicaType::MEMORY)) {
        allocator_manager_.addAllocator(kSegmentName, allocator_);
    }

    bool prepareCapacityPressure(double used_ratio) {
        return fillCapacityPressure(
            capacity_, used_ratio,
            [&](uint64_t size) { return allocator_->allocate(size); }, held_);
    }

    bool prepareFragmentation(uint64_t block_size, uint32_t stride) {
        return createFragmentation(
            block_size, stride,
            [&](uint64_t size) { return allocator_->allocate(size); }, held_);
    }

    bool expectAllocationFailure(uint64_t request_size) {
        auto result = strategy_.Allocate(allocator_manager_, request_size, 1);
        return !result.has_value();
    }

    bool allocateAndFree(uint64_t request_size) {
        auto result = strategy_.Allocate(allocator_manager_, request_size, 1);
        return result.has_value();
    }

    OffsetAllocStorageReport report() const {
        return allocator_->getOffsetAllocator()->storageReport();
    }

   private:
    uint64_t capacity_;
    std::shared_ptr<OffsetBufferAllocator> allocator_;
    AllocatorManager allocator_manager_;
    RandomAllocationStrategy strategy_;
    std::vector<std::unique_ptr<AllocatedBuffer>> held_;
};

template <typename Fixture>
bool runFixtureScenarios(const std::string& layer, uint64_t capacity,
                         uint64_t failed_request_size,
                         uint64_t success_request_size,
                         uint64_t fragmentation_block_size) {
    if (isSelected(FLAGS_scenario, "capacity")) {
        Fixture fixture(capacity, fragmentation_block_size);
        if (!fixture.prepareCapacityPressure(FLAGS_high_water_ratio)) {
            std::cerr << "failed to prepare capacity-pressure fixture for "
                      << layer << std::endl;
            return false;
        }
        const auto report = fixture.report();
        if (report.largestFreeRegion >= failed_request_size) {
            std::cerr << "capacity scenario request must exceed the largest "
                         "free region"
                      << std::endl;
            return false;
        }
        auto operation = [&](uint32_t, uint64_t) {
            return fixture.expectAllocationFailure(failed_request_size);
        };
        printResult(runConcurrentBenchmark(layer, "capacity_failed", report,
                                           capacity, failed_request_size, 0,
                                           1.0, operation));
    }

    if (isSelected(FLAGS_scenario, "fragmentation")) {
        Fixture fixture(capacity, fragmentation_block_size);
        if (!fixture.prepareFragmentation(fragmentation_block_size,
                                          FLAGS_fragmentation_stride)) {
            std::cerr << "failed to prepare fragmented fixture for " << layer
                      << std::endl;
            return false;
        }
        const auto report = fixture.report();
        if (report.totalFreeSpace < failed_request_size ||
            report.largestFreeRegion >= failed_request_size) {
            std::cerr << "fragmentation scenario requires total free space >= "
                         "request size and largest free region < request size"
                      << std::endl;
            return false;
        }
        auto operation = [&](uint32_t, uint64_t) {
            return fixture.expectAllocationFailure(failed_request_size);
        };
        printResult(runConcurrentBenchmark(
            layer, "fragmentation_failed", report, capacity,
            failed_request_size, 0, 1.0, operation));
    }

    if (isSelected(FLAGS_scenario, "mixed")) {
        Fixture fixture(capacity, fragmentation_block_size);
        if (!fixture.prepareCapacityPressure(FLAGS_high_water_ratio)) {
            std::cerr << "failed to prepare mixed high-water fixture for "
                      << layer << std::endl;
            return false;
        }
        const auto report = fixture.report();
        if (report.largestFreeRegion >= failed_request_size) {
            std::cerr << "mixed scenario failure request must exceed the "
                         "largest free region"
                      << std::endl;
            return false;
        }

        const uint64_t failure_slots = static_cast<uint64_t>(
            FLAGS_mixed_failure_ratio * static_cast<double>(kMixedPatternSize) +
            0.5);
        auto operation = [&](uint32_t thread_index, uint64_t operation_index) {
            const uint64_t pattern_slot =
                (operation_index * 7919 +
                 static_cast<uint64_t>(thread_index) * 104729) %
                kMixedPatternSize;
            if (pattern_slot < failure_slots) {
                return fixture.expectAllocationFailure(failed_request_size);
            }
            return fixture.allocateAndFree(success_request_size);
        };
        printResult(
            runConcurrentBenchmark(layer, "high_water_mixed", report, capacity,
                                   failed_request_size, success_request_size,
                                   static_cast<double>(failure_slots) /
                                       static_cast<double>(kMixedPatternSize),
                                   operation));
    }

    if (isSelected(FLAGS_scenario, "success")) {
        Fixture fixture(capacity, fragmentation_block_size);
        const auto report = fixture.report();
        auto operation = [&](uint32_t, uint64_t) {
            return fixture.allocateAndFree(success_request_size);
        };
        printResult(runConcurrentBenchmark(
            layer, "successful_allocate_free", report, capacity,
            success_request_size, success_request_size, 0.0, operation));
    }
    return true;
}

bool validateFlags() {
    const bool valid_layer = FLAGS_layer == "all" ||
                             FLAGS_layer == "offset_allocator" ||
                             FLAGS_layer == "put_allocation";
    const bool valid_scenario =
        FLAGS_scenario == "all" || FLAGS_scenario == "capacity" ||
        FLAGS_scenario == "fragmentation" || FLAGS_scenario == "mixed" ||
        FLAGS_scenario == "success";
    if (!valid_layer || !valid_scenario || FLAGS_threads == 0 ||
        FLAGS_iterations == 0 || FLAGS_pool_size_mb == 0 ||
        FLAGS_failed_request_mb == 0 || FLAGS_success_request_kb == 0 ||
        FLAGS_fragmentation_block_mb == 0 || FLAGS_fragmentation_stride < 2 ||
        FLAGS_high_water_ratio <= 0.0 || FLAGS_high_water_ratio >= 1.0) {
        return false;
    }
    const uint64_t mixed_failure_slots = static_cast<uint64_t>(
        FLAGS_mixed_failure_ratio * static_cast<double>(kMixedPatternSize) +
        0.5);
    if (mixed_failure_slots == 0 || mixed_failure_slots >= kMixedPatternSize) {
        return false;
    }
    constexpr uint64_t max_size = std::numeric_limits<size_t>::max();
    return FLAGS_pool_size_mb <= max_size / kMiB &&
           FLAGS_failed_request_mb <= max_size / kMiB &&
           FLAGS_success_request_kb <= max_size / kKiB &&
           FLAGS_fragmentation_block_mb <= max_size / kMiB;
}

}  // namespace

int main(int argc, char** argv) {
    gflags::ParseCommandLineFlags(&argc, &argv, true);
    if (!validateFlags()) {
        std::cerr << "invalid benchmark flags; use --help for valid values"
                  << std::endl;
        return 1;
    }

    const uint64_t capacity = FLAGS_pool_size_mb * kMiB;
    const uint64_t failed_request_size = FLAGS_failed_request_mb * kMiB;
    const uint64_t success_request_size = FLAGS_success_request_kb * kKiB;
    const uint64_t fragmentation_block_size =
        FLAGS_fragmentation_block_mb * kMiB;
    if (failed_request_size >= capacity || success_request_size >= capacity ||
        fragmentation_block_size >= capacity) {
        std::cerr << "request and fragmentation block sizes must be smaller "
                     "than the allocator capacity"
                  << std::endl;
        return 1;
    }

    printCsvHeader();

    if (isSelected(FLAGS_layer, "offset_allocator") &&
        !runFixtureScenarios<OffsetAllocatorFixture>(
            "offset_allocator", capacity, failed_request_size,
            success_request_size, fragmentation_block_size)) {
        return 1;
    }
    if (isSelected(FLAGS_layer, "put_allocation") &&
        !runFixtureScenarios<PutAllocationFixture>(
            "put_allocation", capacity, failed_request_size,
            success_request_size, fragmentation_block_size)) {
        return 1;
    }
    return 0;
}
