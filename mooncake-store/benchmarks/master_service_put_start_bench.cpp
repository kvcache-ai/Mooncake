#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <iomanip>
#include <iostream>
#include <latch>
#include <string>
#include <thread>
#include <vector>

#include <gflags/gflags.h>
#include <glog/logging.h>

#include "master_service.h"

DEFINE_uint32(threads, 16, "Number of concurrent PutStart workers");
DEFINE_uint64(iterations, 10000, "Measured operations per worker");
DEFINE_uint64(warmup_iterations, 1000, "Warmup operations per worker");
DEFINE_uint32(logical_groups, 1, "Number of logical placement groups");
DEFINE_uint32(targets_per_group, 1,
              "Physical allocation targets sharing each logical name");
DEFINE_uint64(value_size, 4096, "PutStart value size in bytes");
DEFINE_bool(preferred_only, false,
            "Resolve every request through its logical group preference");

namespace {

using Clock = std::chrono::steady_clock;
using mooncake::BufferAllocatorType;
using mooncake::ErrorCode;
using mooncake::generate_uuid;
using mooncake::MasterService;
using mooncake::MasterServiceConfig;
using mooncake::ReplicateConfig;
using mooncake::Segment;
using mooncake::TenantId;
using mooncake::UUID;

constexpr uintptr_t kBase = 0x100000000ULL;
constexpr uint64_t kTargetCapacity = 128ULL * 1024 * 1024 * 1024;

struct WorkerResult {
    uint64_t failures{0};
    std::vector<uint64_t> put_start_latencies_ns;
};

uint64_t Percentile(const std::vector<uint64_t>& sorted, double quantile) {
    if (sorted.empty()) {
        return 0;
    }
    const size_t index =
        static_cast<size_t>(quantile * static_cast<double>(sorted.size() - 1));
    return sorted[index];
}

bool RunOperation(MasterService& service, const UUID& client_id,
                  uint32_t thread_index, uint64_t operation_index,
                  std::string_view phase, WorkerResult* result) {
    const uint32_t group =
        static_cast<uint32_t>(operation_index % FLAGS_logical_groups);
    ReplicateConfig config;
    config.replica_num = 1;
    if (FLAGS_preferred_only) {
        config.preferred_segment = "group-" + std::to_string(group);
    }
    const std::string key = std::string(phase) + '-' +
                            std::to_string(thread_index) + '-' +
                            std::to_string(operation_index);

    const auto start = Clock::now();
    auto put = service.PutStart(client_id, key, TenantId::Default(),
                                FLAGS_value_size, config);
    const auto finish = Clock::now();
    if (result) {
        result->put_start_latencies_ns.push_back(
            std::chrono::duration_cast<std::chrono::nanoseconds>(finish - start)
                .count());
    }
    if (!put) {
        if (result) {
            ++result->failures;
        }
        return false;
    }
    return true;
}

}  // namespace

int main(int argc, char** argv) {
    gflags::SetUsageMessage(
        "In-process MasterService PutStart/PutRevoke benchmark");
    gflags::ParseCommandLineFlags(&argc, &argv, true);
    google::InitGoogleLogging(argv[0]);
    FLAGS_minloglevel = 2;

    if (FLAGS_threads == 0 || FLAGS_iterations == 0 ||
        FLAGS_logical_groups == 0 || FLAGS_targets_per_group == 0 ||
        FLAGS_value_size == 0) {
        std::cerr << "all numeric flags must be nonzero" << std::endl;
        return 2;
    }

    auto config = MasterServiceConfig::builder()
                      .set_memory_allocator(BufferAllocatorType::OFFSET)
                      .build();
    MasterService service(config);
    const UUID client_id = generate_uuid();
    uint64_t target_index = 0;
    for (uint32_t group = 0; group < FLAGS_logical_groups; ++group) {
        for (uint32_t target = 0; target < FLAGS_targets_per_group; ++target) {
            Segment segment;
            segment.id = generate_uuid();
            segment.name = "group-" + std::to_string(group);
            segment.base = kBase + target_index * kTargetCapacity;
            segment.size = kTargetCapacity;
            segment.te_endpoint =
                segment.name + "-target-" + std::to_string(target);
            auto mounted = service.MountSegment(segment, client_id);
            if (!mounted) {
                std::cerr << "mount failed: "
                          << mooncake::toString(mounted.error()) << std::endl;
                return 2;
            }
            ++target_index;
        }
    }

    std::vector<WorkerResult> results(FLAGS_threads);
    std::latch ready(FLAGS_threads);
    std::latch start(1);
    std::vector<std::thread> workers;
    workers.reserve(FLAGS_threads);
    for (uint32_t thread_index = 0; thread_index < FLAGS_threads;
         ++thread_index) {
        workers.emplace_back([&, thread_index] {
            for (uint64_t i = 0; i < FLAGS_warmup_iterations; ++i) {
                RunOperation(service, client_id, thread_index, i, "warmup",
                             nullptr);
            }
            auto& result = results[thread_index];
            result.put_start_latencies_ns.reserve(FLAGS_iterations);
            ready.count_down();
            start.wait();
            for (uint64_t i = 0; i < FLAGS_iterations; ++i) {
                RunOperation(service, client_id, thread_index, i, "measured",
                             &result);
            }
        });
    }

    ready.wait();
    const auto begin = Clock::now();
    start.count_down();
    for (auto& worker : workers) {
        worker.join();
    }
    const auto end = Clock::now();

    uint64_t failures = 0;
    std::vector<uint64_t> latencies;
    latencies.reserve(FLAGS_threads * FLAGS_iterations);
    for (auto& result : results) {
        failures += result.failures;
        latencies.insert(latencies.end(), result.put_start_latencies_ns.begin(),
                         result.put_start_latencies_ns.end());
    }
    std::sort(latencies.begin(), latencies.end());
    const uint64_t operations = FLAGS_threads * FLAGS_iterations;
    const double seconds = std::chrono::duration<double>(end - begin).count();

    std::cout
        << "threads,logical_groups,targets_per_group,preferred_only,operations,"
           "seconds,qps,p50_ns,p99_ns,failures\n"
        << FLAGS_threads << ',' << FLAGS_logical_groups << ','
        << FLAGS_targets_per_group << ',' << FLAGS_preferred_only << ','
        << operations << ',' << std::fixed << std::setprecision(6) << seconds
        << ',' << std::setprecision(2)
        << static_cast<double>(operations) / seconds << ','
        << Percentile(latencies, 0.50) << ',' << Percentile(latencies, 0.99)
        << ',' << failures << std::endl;
    return failures == 0 ? 0 : 1;
}
