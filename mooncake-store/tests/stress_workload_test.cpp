#include <gflags/gflags.h>
#include <glog/logging.h>
#include <numa.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cmath>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include "allocator.h"
#include "client.h"
#include "types.h"
#include "utils.h"

// Configuration flags
DEFINE_string(protocol, "rdma", "Transfer protocol: rdma|tcp");
DEFINE_string(device_name, "erdma_0",
              "Device name to use, valid if protocol=rdma");
DEFINE_string(master_address, "localhost:50051", "Address of master server");
DEFINE_int32(num_threads, 8, "Number of concurrent worker threads");
DEFINE_int32(test_operation_nums, 100, "Number of operations per thread");

DEFINE_int32(value_size, 1048576, "Size of values in bytes (default: 1MB)");

// Memory configuration flags
DEFINE_uint64(ram_buffer_size_gb, 15,
              "RAM buffer size in GB for segment allocation");
DEFINE_uint64(client_buffer_allocator_size_mb, 256,
              "Client buffer allocator size in MB");

// Network configuration flags
DEFINE_string(local_hostname, "localhost:12345", "Local hostname for client");
DEFINE_string(metadata_connection_string, "P2PHANDSHAKE",
              "Metadata connection string");

namespace mooncake {
namespace benchmark {

// Global client and allocator instances
std::shared_ptr<Client> g_client = nullptr;
std::unique_ptr<SimpleAllocator> g_client_buffer_allocator = nullptr;
void* g_segment_ptr = nullptr;
size_t g_ram_buffer_size = 0;

// Performance measurement structures
struct OperationResult {
    double latency_us;  // Latency in microseconds
    bool is_put;        // true for PUT, false for GET
    bool success;       // Operation success status
};

struct ThreadStats {
    std::vector<OperationResult> operations;
    uint64_t total_operations = 0;
    uint64_t successful_operations = 0;
    uint64_t put_operations = 0;
    uint64_t get_operations = 0;
};

bool initialize_segment() {
    // Use gflags configuration for RAM buffer size
    g_ram_buffer_size = FLAGS_ram_buffer_size_gb * 1024ull * 1024 * 1024;
    g_segment_ptr = allocate_buffer_allocator_memory(g_ram_buffer_size);
    if (!g_segment_ptr) {
        LOG(ERROR) << "Failed to allocate segment memory of size "
                   << FLAGS_ram_buffer_size_gb << "GB";
        return false;
    }

    auto result = g_client->MountSegment(g_segment_ptr, g_ram_buffer_size);
    if (!result.has_value()) {
        LOG(ERROR) << "Failed to mount segment: " << toString(result.error());
        return false;
    }

    LOG(INFO) << "Segment initialized successfully with "
              << FLAGS_ram_buffer_size_gb << "GB RAM buffer";
    return true;
}

void cleanup_segment() {
    if (g_segment_ptr && g_client) {
        auto result =
            g_client->UnmountSegment(g_segment_ptr, g_ram_buffer_size);
        if (!result.has_value()) {
            LOG(ERROR) << "Failed to unmount segment: "
                       << toString(result.error());
        }
    }
}

bool initialize_client() {
    auto client_opt = Client::Create(
        FLAGS_local_hostname,              // Local hostname
        FLAGS_metadata_connection_string,  // Metadata connection string
        FLAGS_protocol, FLAGS_master_address);

    if (!client_opt.has_value()) {
        LOG(ERROR) << "Failed to create client";
        return false;
    }

    LOG(INFO) << "Create client successfully";

    g_client = *client_opt;

    // Use gflags configuration for client buffer allocator size
    auto client_buffer_allocator_size =
        FLAGS_client_buffer_allocator_size_mb * 1024 * 1024;
    g_client_buffer_allocator =
        std::make_unique<SimpleAllocator>(client_buffer_allocator_size);

    auto result = g_client->RegisterLocalMemory(
        g_client_buffer_allocator->getBase(), client_buffer_allocator_size,
        "cpu:0", false, false);

    if (!result.has_value()) {
        LOG(ERROR) << "Failed to register local memory: "
                   << toString(result.error());
        return false;
    }

    // Verify that the buffer allocator has enough space for all threads
    size_t total_required_memory = FLAGS_num_threads * FLAGS_value_size;
    if (total_required_memory > client_buffer_allocator_size) {
        LOG(ERROR) << "Insufficient buffer allocator memory. Required: "
                   << total_required_memory / (1024 * 1024)
                   << "MB, Available: " << FLAGS_client_buffer_allocator_size_mb
                   << "MB";
        return false;
    }

    LOG(INFO) << "Client initialized successfully with "
              << FLAGS_client_buffer_allocator_size_mb << "MB buffer allocator";
    return true;
}

void cleanup_client() {
    if (g_client) {
        g_client.reset();
    }
    g_client_buffer_allocator.reset();
}

std::string generate_key(int thread_id, uint64_t operation_id) {
    return "key_" + std::to_string(thread_id) + "_" +
           std::to_string(operation_id);
}

void worker_thread(int thread_id, std::atomic<bool>& stop_flag,
                   ThreadStats& stats) {
    // Allocate thread-local buffer
    void* write_buffer = g_client_buffer_allocator->allocate(FLAGS_value_size);
    if (!write_buffer) {
        LOG(ERROR) << "Thread " << thread_id
                   << ": Failed to allocate write buffer";
        return;
    }

    // Fill buffer with simple pattern
    memset(write_buffer, 'A' + (thread_id % 26), FLAGS_value_size);

    std::vector<Slice> slices;
    slices.emplace_back(
        Slice{write_buffer, static_cast<size_t>(FLAGS_value_size)});

    ReplicateConfig config;
    config.replica_num = 1;

    std::vector<std::string> stored_keys;  // Track keys for GET operations

    // Phase 1: Perform PUT operations
    for (int i = 0; i < FLAGS_test_operation_nums && !stop_flag.load(); ++i) {
        std::string key = generate_key(thread_id, i);

        auto start_time = std::chrono::high_resolution_clock::now();
        auto result = g_client->Put(key.data(), slices, config);
        auto end_time = std::chrono::high_resolution_clock::now();

        auto latency_us = std::chrono::duration_cast<std::chrono::microseconds>(
                              end_time - start_time)
                              .count();

        bool success = result.has_value();
        stats.operations.push_back(
            {static_cast<double>(latency_us), true, success});

        if (success) {
            stored_keys.push_back(key);
            stats.put_operations++;
            stats.successful_operations++;
        }

        stats.total_operations++;
    }

    // Phase 2: Perform GET operations on the stored keys
    for (int i = 0; i < FLAGS_test_operation_nums && !stop_flag.load() &&
                    !stored_keys.empty();
         ++i) {
        // Use modulo to cycle through stored keys deterministically
        size_t key_index = i % stored_keys.size();
        std::string key = stored_keys[key_index];

        auto start_time = std::chrono::high_resolution_clock::now();
        auto result = g_client->Get(key.data(), slices);
        auto end_time = std::chrono::high_resolution_clock::now();

        auto latency_us = std::chrono::duration_cast<std::chrono::microseconds>(
                              end_time - start_time)
                              .count();

        bool success = result.has_value();
        stats.operations.push_back(
            {static_cast<double>(latency_us), false, success});

        if (success) {
            stats.get_operations++;
            stats.successful_operations++;
        }

        stats.total_operations++;
    }

    g_client_buffer_allocator->deallocate(write_buffer, FLAGS_value_size);
}

void calculate_percentiles(std::vector<double>& latencies, double& p50,
                           double& p90, double& p95, double& p99) {
    if (latencies.empty()) {
        p50 = p90 = p95 = p99 = 0.0;
        return;
    }

    std::sort(latencies.begin(), latencies.end());
    size_t size = latencies.size();

    // Use explicit parentheses and floating-point arithmetic for safe
    // percentile calculation This avoids integer division order issues and
    // ensures correct indices
    p50 = latencies[static_cast<size_t>(std::ceil((size * 0.50) - 1))];
    p90 = latencies[static_cast<size_t>(std::ceil((size * 0.90) - 1))];
    p95 = latencies[static_cast<size_t>(std::ceil((size * 0.95) - 1))];
    p99 = latencies[static_cast<size_t>(std::ceil((size * 0.99) - 1))];
}

void print_results(const std::vector<ThreadStats>& thread_stats,
                   double duration_s) {
    // Aggregate statistics
    uint64_t total_ops = 0;
    uint64_t successful_ops = 0;
    uint64_t total_put_ops = 0;
    uint64_t total_get_ops = 0;

    std::vector<double> all_latencies;
    std::vector<double> put_latencies;
    std::vector<double> get_latencies;

    for (const auto& stats : thread_stats) {
        total_ops += stats.total_operations;
        successful_ops += stats.successful_operations;
        total_put_ops += stats.put_operations;
        total_get_ops += stats.get_operations;

        for (const auto& op : stats.operations) {
            if (op.success) {
                all_latencies.push_back(op.latency_us);
                if (op.is_put) {
                    put_latencies.push_back(op.latency_us);
                } else {
                    get_latencies.push_back(op.latency_us);
                }
            }
        }
    }

    // Calculate percentiles
    double all_p50, all_p90, all_p95, all_p99;
    double put_p50, put_p90, put_p95, put_p99;
    double get_p50, get_p90, get_p95, get_p99;

    calculate_percentiles(all_latencies, all_p50, all_p90, all_p95, all_p99);
    calculate_percentiles(put_latencies, put_p50, put_p90, put_p95, put_p99);
    calculate_percentiles(get_latencies, get_p50, get_p90, get_p95, get_p99);

    // Calculate throughput
    double ops_per_second = successful_ops / duration_s;
    double put_ops_per_second = total_put_ops / duration_s;
    double get_ops_per_second = total_get_ops / duration_s;

    // Calculate data throughput separately for PUT and GET operations
    double put_data_throughput_mb_s =
        (total_put_ops * FLAGS_value_size) / (duration_s * 1024 * 1024);
    double get_data_throughput_mb_s =
        (total_get_ops * FLAGS_value_size) / (duration_s * 1024 * 1024);
    double total_data_throughput_mb_s =
        put_data_throughput_mb_s + get_data_throughput_mb_s;

    // Print results
    LOG(INFO) << "=== Benchmark Results ===";
    LOG(INFO) << "Test Duration: " << duration_s << " seconds";
    LOG(INFO) << "Threads: " << FLAGS_num_threads;
    LOG(INFO) << "Key Size: 128 bytes";
    LOG(INFO) << "Value Size: " << FLAGS_value_size << " bytes";
    LOG(INFO) << "Operations per thread: " << FLAGS_test_operation_nums;
    LOG(INFO) << "";
    LOG(INFO) << "=== Operation Statistics ===";
    LOG(INFO) << "Total Operations: " << total_ops;
    LOG(INFO) << "Successful Operations: " << successful_ops;
    LOG(INFO) << "PUT Operations: " << total_put_ops;
    LOG(INFO) << "GET Operations: " << total_get_ops;
    LOG(INFO) << "Success Rate: " << (100.0 * successful_ops / total_ops)
              << "%";
    LOG(INFO) << "";
    LOG(INFO) << "=== Throughput ===";
    LOG(INFO) << "Total Operations/sec: " << ops_per_second;
    LOG(INFO) << "PUT Operations/sec: " << put_ops_per_second;
    LOG(INFO) << "GET Operations/sec: " << get_ops_per_second;
    LOG(INFO) << "Total Data Throughput (MB/s): " << total_data_throughput_mb_s;
    LOG(INFO) << "PUT Data Throughput (MB/s): " << put_data_throughput_mb_s;
    LOG(INFO) << "GET Data Throughput (MB/s): " << get_data_throughput_mb_s;
    LOG(INFO) << "";
    LOG(INFO) << "=== Latency (microseconds) ===";
    LOG(INFO) << "All Operations - P50: " << all_p50 << ", P90: " << all_p90
              << ", P95: " << all_p95 << ", P99: " << all_p99;

    if (!put_latencies.empty()) {
        LOG(INFO) << "PUT Operations - P50: " << put_p50 << ", P90: " << put_p90
                  << ", P95: " << put_p95 << ", P99: " << put_p99;
    }

    if (!get_latencies.empty()) {
        LOG(INFO) << "GET Operations - P50: " << get_p50 << ", P90: " << get_p90
                  << ", P95: " << get_p95 << ", P99: " << get_p99;
    }
}

}  // namespace benchmark
}  // namespace mooncake

int main(int argc, char** argv) {
    // Initialize gflags and glog
    gflags::ParseCommandLineFlags(&argc, &argv, true);
    google::InitGoogleLogging(argv[0]);
    FLAGS_logtostderr = 1;

    using namespace mooncake::benchmark;

    LOG(INFO) << "Starting Mooncake Store Stress Benchmark";
    LOG(INFO) << "Protocol: " << FLAGS_protocol
              << ", Device: " << FLAGS_device_name;
    LOG(INFO) << "Local hostname: " << FLAGS_local_hostname;
    LOG(INFO) << "Metadata connection: " << FLAGS_metadata_connection_string;
    LOG(INFO) << "Operations per thread: " << FLAGS_test_operation_nums;
    LOG(INFO) << "RAM buffer size: " << FLAGS_ram_buffer_size_gb << "GB";
    LOG(INFO) << "Client buffer allocator size: "
              << FLAGS_client_buffer_allocator_size_mb << "MB";

    // Initialize client and segment
    if (!initialize_client()) {
        LOG(ERROR) << "Failed to initialize client";
        return 1;
    }

    if (!initialize_segment()) {
        LOG(ERROR) << "Failed to initialize segment";
        cleanup_client();
        return 1;
    }

    // Prepare worker threads
    std::vector<std::thread> workers;
    std::vector<ThreadStats> thread_stats(FLAGS_num_threads);
    std::atomic<bool> stop_flag{false};

    LOG(INFO) << "Starting " << FLAGS_num_threads << " worker threads with "
              << FLAGS_test_operation_nums << " operations each";

    // Start worker threads
    auto start_time = std::chrono::high_resolution_clock::now();

    for (int i = 0; i < FLAGS_num_threads; ++i) {
        workers.emplace_back(worker_thread, i, std::ref(stop_flag),
                             std::ref(thread_stats[i]));
    }

    // Wait for all threads to complete (they will finish after completing their
    // operations)
    for (auto& worker : workers) {
        worker.join();
    }

    auto end_time = std::chrono::high_resolution_clock::now();
    double actual_duration_s =
        std::chrono::duration_cast<std::chrono::milliseconds>(end_time -
                                                              start_time)
            .count() /
        1000.0;

    // Print results
    print_results(thread_stats, actual_duration_s);

    // Cleanup
    cleanup_segment();
    cleanup_client();
    google::ShutdownGoogleLogging();

    LOG(INFO) << "Benchmark completed successfully";
    return 0;
}
