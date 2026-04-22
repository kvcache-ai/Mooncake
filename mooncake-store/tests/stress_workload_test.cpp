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
#include <mutex>
#include <condition_variable>

#include "allocator.h"
#include "centralized_client_service.h"
#include "p2p_client_service.h"
#include "types.h"
#include "utils.h"

// Configuration flags
DEFINE_string(protocol, "rdma", "Transfer protocol: rdma|tcp");
DEFINE_string(device_name, "", "Device name to use, valid if protocol=rdma");
DEFINE_string(master_address, "localhost:50051", "Address of master server");
DEFINE_int32(num_threads, 8, "Number of concurrent worker threads");
DEFINE_int32(test_operation_nums, 100, "Number of batch operations per thread");
DEFINE_int32(batch_size, 1, "Batch size for PUT/GET operations");

// Client configurations
DEFINE_string(client_type, "Centralized", "Type of client: Centralized | P2P");
DEFINE_string(tiered_backend_config,
              "{\"tiers\": [{\"type\": \"DRAM\", \"capacity\": 16106127360, "
              "\"priority\": 10, \"allocator_type\": \"OFFSET\" }]}",
              "Tiered backend config json for P2P mode");

DEFINE_int32(value_size, 1048576, "Size of values in bytes (default: 1MB)");

// Memory configuration flags
DEFINE_uint64(ram_buffer_size_gb, 15,
              "RAM buffer size in GB for segment allocation");

// Network configuration flags
DEFINE_string(local_hostname, "localhost:12345", "Local hostname for client");
DEFINE_string(metadata_connection_string, "P2PHANDSHAKE",
              "Metadata connection string");
DEFINE_string(p2p_local_transfer_mode, "te",
              "Local transfer mode for P2P local Get/Put path: memcpy|te");
DEFINE_uint64(local_memcpy_async_worker_num, 32,
              "If set p2p_local_transfer_mode=memcpy, Worker number for async "
              "local memcpy executor (P2P)");
DEFINE_uint64(route_cache_max_memory_mb, 300,
              "Max memory for RouteCache in MB (P2P mode)");
DEFINE_uint64(route_cache_ttl_ms, 300000,
              "TTL for RouteCache entries in ms (P2P mode)");
namespace mooncake {
namespace benchmark {

// Global client and allocator instances
std::shared_ptr<ClientService> g_client = nullptr;
void* g_segment_ptr = nullptr;
size_t g_ram_buffer_size = 0;
void* g_worker_buffer_base = nullptr;
size_t g_per_thread_buffer_stride =
    0;  // (1 + batch_size) * value_size per thread

// Synchronization primitives for coordinated start
std::mutex g_sync_mutex;
std::condition_variable g_sync_cv;
int g_ready_threads_count = 0;
int g_put_done_count = 0;
bool g_start_signal_flag = false;
bool g_get_start_signal_flag = false;
std::condition_variable g_put_sync_cv;

// Performance measurement structures
struct OperationResult {
    double latency_us;  // Latency in microseconds
    bool is_put;        // true for PUT, false for GET
    bool success;       // Operation success status
};

struct alignas(64) ThreadStats {
    std::vector<OperationResult> operations;
    std::vector<double> batch_put_latency_us;  // per-batch completion latency
    std::vector<double> batch_get_latency_us;  // per-batch completion latency
    uint64_t total_operations = 0;
    uint64_t successful_operations = 0;
    uint64_t put_operations = 0;
    uint64_t get_operations = 0;
    uint64_t put_failures = 0;
    uint64_t get_failures = 0;
};

bool initialize_segment() {
    if (FLAGS_client_type == "P2P") {
        LOG(INFO) << "Skipping segment initialization for P2P mode";
        return true;
    }

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
    if (FLAGS_client_type == "P2P") {
        return;
    }

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
    std::optional<std::string> device_names;
    if (!FLAGS_device_name.empty()) {
        device_names = FLAGS_device_name;
    }
    std::optional<std::shared_ptr<ClientService>> client_opt;

    if (FLAGS_client_type == "P2P") {
        // Build tiered_backend_config from ram_buffer_size_gb so the P2P
        // storage capacity is consistent with the Centralization segment size.
        // FLAGS_tiered_backend_config can still override this if explicitly
        // set.
        std::string tiered_config = FLAGS_tiered_backend_config;
        if (tiered_config.find("16106127360") != std::string::npos) {
            // Default value is still 15GB placeholder — replace with actual
            // flag
            uint64_t capacity_bytes =
                FLAGS_ram_buffer_size_gb * 1024ull * 1024 * 1024;
            tiered_config =
                "{\"tiers\": [{\"type\": \"DRAM\", \"capacity\": " +
                std::to_string(capacity_bytes) +
                ", \"priority\": 10, \"allocator_type\": \"OFFSET\"}]}";
        }
        auto config = ClientConfigBuilder::build_p2p_real_client(
            FLAGS_local_hostname, FLAGS_metadata_connection_string,
            FLAGS_protocol, device_names, FLAGS_master_address, tiered_config,
            0, nullptr, "", 12345,
            /*rpc_thread_num=*/2, /*lock_shard_count=*/1024,
            /*route_cache_max_memory_bytes=*/FLAGS_route_cache_max_memory_mb *
                1024 * 1024,
            /*route_cache_ttl_ms=*/FLAGS_route_cache_ttl_ms,
            FLAGS_p2p_local_transfer_mode, FLAGS_local_memcpy_async_worker_num);
        client_opt = ClientService::Create(config);
    } else {
        auto config = ClientConfigBuilder::build_centralized_real_client(
            FLAGS_local_hostname, FLAGS_metadata_connection_string,
            FLAGS_protocol, device_names, FLAGS_master_address);
        client_opt = ClientService::Create(config);
    }

    if (!client_opt.has_value()) {
        LOG(ERROR) << "Failed to create client";
        return false;
    }

    LOG(INFO) << "Create client successfully";

    g_client = client_opt.value();

    // Pre-allocate the worker buffer per thread, bypassing the slab allocator
    // (SimpleAllocator/CacheLib) to avoid fragmentation.
    // Layout: [write_buffer | dst_0 | dst_1 | ... | dst_{batch_size-1}]
    // Stride = (1 + batch_size) * value_size.
    if (FLAGS_batch_size <= 0) {
        LOG(ERROR) << "batch_size must be greater than 0";
        return false;
    }

    g_per_thread_buffer_stride = static_cast<size_t>(FLAGS_value_size) *
                                 (1 + static_cast<size_t>(FLAGS_batch_size));
    size_t total_buffer_size = FLAGS_num_threads * g_per_thread_buffer_stride;

    if (total_buffer_size < 4096) {
        LOG(ERROR) << "total_buffer_size (" << total_buffer_size
                   << ") is less than minimum alignment (4096). "
                      "Consider increasing value_size, batch_size, or "
                      "num_threads.";
        return false;
    }

    size_t read_working_set = static_cast<size_t>(FLAGS_num_threads) *
                              static_cast<size_t>(FLAGS_batch_size) *
                              static_cast<size_t>(FLAGS_value_size);
    LOG(INFO) << "Worker buffer per thread: "
              << g_per_thread_buffer_stride / (1024 * 1024)
              << " MiB (1 write + " << FLAGS_batch_size << " read destinations"
              << "), total read working set: "
              << read_working_set / (1024 * 1024) << " MiB";
    if (read_working_set < 128ull * 1024 * 1024) {
        LOG(WARNING) << "Read working set is " << read_working_set / 1024
                     << " KiB (< 128 MiB); may fit in L3 and bias memcpy read "
                        "latency/throughput optimistically. Increase "
                        "num_threads, batch_size, or value_size for "
                        "DRAM-realistic numbers.";
    }

    // Align to page boundary for RDMA registration.
    const size_t kAlignment = 4096;
    g_worker_buffer_base = std::aligned_alloc(kAlignment, total_buffer_size);
    if (!g_worker_buffer_base) {
        LOG(ERROR) << "Failed to allocate worker buffer of size "
                   << total_buffer_size / (1024 * 1024) << "MB";
        return false;
    }

    auto result = g_client->RegisterLocalMemory(
        g_worker_buffer_base, total_buffer_size, "cpu:0", false, false);

    if (!result.has_value()) {
        std::free(g_worker_buffer_base);
        g_worker_buffer_base = nullptr;
        LOG(ERROR) << "Failed to register local memory: "
                   << toString(result.error());
        return false;
    }

    LOG(INFO) << "Client initialized successfully with "
              << total_buffer_size / (1024 * 1024) << "MB worker buffer";
    return true;
}

void cleanup_client() {
    if (g_client) {
        g_client.reset();
    }
    if (g_worker_buffer_base) {
        std::free(g_worker_buffer_base);
        g_worker_buffer_base = nullptr;
    }
}

std::string generate_key(int thread_id, uint64_t operation_id) {
    return "key_" + std::to_string(thread_id) + "_" +
           std::to_string(operation_id);
}

void worker_thread(int thread_id, std::atomic<bool>& stop_flag,
                   ThreadStats& stats) {
    int total_keys = FLAGS_test_operation_nums * FLAGS_batch_size;
    stats.operations.reserve(total_keys * 2);  // 2 for PUT and GET
    stats.batch_put_latency_us.reserve(FLAGS_test_operation_nums);
    stats.batch_get_latency_us.reserve(FLAGS_test_operation_nums);

    // Each thread gets a dedicated slice of the pre-registered worker buffer.
    // Layout: [write_buffer | dst_0 | dst_1 | ... | dst_{batch_size-1}]
    // thread_base + 0             * value_size → write buffer (PUT source)
    // thread_base + (1 + j)       * value_size → GET destination for key j
    char* thread_base =
        static_cast<char*>(g_worker_buffer_base) +
        static_cast<size_t>(thread_id) * g_per_thread_buffer_stride;
    void* write_buffer = thread_base;

    if (!g_worker_buffer_base || g_per_thread_buffer_stride == 0) {
        LOG(ERROR) << "Thread " << thread_id
                   << ": worker buffer not initialized";
        return;
    }

    // Fill buffer with simple pattern
    memset(write_buffer, 'A' + (thread_id % 26), FLAGS_value_size);

    std::vector<Slice> slices;
    slices.emplace_back(
        Slice{write_buffer, static_cast<size_t>(FLAGS_value_size)});

    WriteConfig config = [&]() -> WriteConfig {
        if (FLAGS_client_type == "P2P") {
            WriteRouteRequestConfig p2p_config;
            p2p_config.max_candidates = 1;
            return p2p_config;
        } else {
            ReplicateConfig rep_config;
            rep_config.replica_num = 1;
            return rep_config;
        }
    }();

    std::vector<std::string> stored_keys;  // Track keys for GET operations
    stored_keys.reserve(total_keys);

    // Warmup phase: Establish RPC/RDMA connections before measuring latency
    constexpr int kWarmupBatches = 10;
    int warmup_successes = 0;
    auto warmup_start = std::chrono::high_resolution_clock::now();
    for (int w = 0; w < kWarmupBatches; ++w) {
        int warmup_batch = FLAGS_batch_size;
        std::vector<ObjectKey> wkeys;
        wkeys.reserve(warmup_batch);
        std::vector<std::vector<Slice>> wslices(warmup_batch, slices);
        for (int j = 0; j < warmup_batch; ++j) {
            wkeys.push_back("warmup_" + std::to_string(thread_id) + "_" +
                            std::to_string(w) + "_" + std::to_string(j));
        }
        auto wres = g_client->BatchPut(wkeys, wslices, config);
        for (auto& r : wres) {
            if (r.has_value()) ++warmup_successes;
        }
    }
    auto warmup_end = std::chrono::high_resolution_clock::now();
    LOG(INFO) << "Thread " << thread_id << " warmup: " << kWarmupBatches
              << " batches, " << warmup_successes << " successful puts, "
              << std::chrono::duration_cast<std::chrono::microseconds>(
                     warmup_end - warmup_start)
                     .count()
              << " us total";

    // Signal ready and wait for all threads
    {
        std::unique_lock<std::mutex> lock(g_sync_mutex);
        g_ready_threads_count++;
        if (g_ready_threads_count == FLAGS_num_threads) {
            g_sync_cv.notify_all();
        }
        g_sync_cv.wait(lock, [] { return g_start_signal_flag; });
    }

    // Phase 1: Perform PUT operations
    for (int i = 0; i < FLAGS_test_operation_nums && !stop_flag.load(); ++i) {
        std::vector<ObjectKey> batch_keys;
        std::vector<std::vector<Slice>> batch_slices(FLAGS_batch_size, slices);
        for (int j = 0; j < FLAGS_batch_size; ++j) {
            batch_keys.push_back(
                generate_key(thread_id, i * FLAGS_batch_size + j));
        }

        auto start_time = std::chrono::high_resolution_clock::now();
        auto results = g_client->BatchPut(batch_keys, batch_slices, config);
        auto end_time = std::chrono::high_resolution_clock::now();

        auto latency_us = std::chrono::duration_cast<std::chrono::microseconds>(
                              end_time - start_time)
                              .count();

        std::vector<bool> key_success(FLAGS_batch_size, false);
        for (size_t j = 0; j < results.size(); ++j) {
            if (results[j].has_value()) {
                key_success[j] = true;
                stored_keys.push_back(batch_keys[j]);
                stats.put_operations++;
                stats.successful_operations++;
            } else {
                stats.put_failures++;
            }
        }
        double per_key_latency_us =
            static_cast<double>(latency_us) / FLAGS_batch_size;
        for (int j = 0; j < FLAGS_batch_size; ++j) {
            stats.operations.push_back(
                {per_key_latency_us, true, key_success[j]});
        }
        stats.batch_put_latency_us.push_back(static_cast<double>(latency_us));
        stats.total_operations += FLAGS_batch_size;
    }

    // Completion signal of PUT phase and wait for threads to start GET phase
    {
        std::unique_lock<std::mutex> lock(g_sync_mutex);
        g_put_done_count++;
        if (g_put_done_count == FLAGS_num_threads) {
            g_put_sync_cv.notify_all();
        }
        g_put_sync_cv.wait(lock, [] { return g_get_start_signal_flag; });
    }

    // Phase 2: Perform GET operations
    for (int i = 0; i < FLAGS_test_operation_nums && !stop_flag.load() &&
                    !stored_keys.empty();
         ++i) {
        std::vector<std::string> batch_keys;
        std::vector<std::vector<void*>> all_buffers(FLAGS_batch_size);
        std::vector<std::vector<size_t>> all_sizes(
            FLAGS_batch_size, {static_cast<size_t>(FLAGS_value_size)});

        for (int j = 0; j < FLAGS_batch_size; ++j) {
            batch_keys.push_back(
                stored_keys[(i * FLAGS_batch_size + j) % stored_keys.size()]);
            all_buffers[j] = {thread_base +
                              (1 + j) * static_cast<size_t>(FLAGS_value_size)};
        }

        auto start_time = std::chrono::high_resolution_clock::now();
        auto results = g_client->BatchGet(batch_keys, all_buffers, all_sizes);
        auto end_time = std::chrono::high_resolution_clock::now();

        auto latency_us = std::chrono::duration_cast<std::chrono::microseconds>(
                              end_time - start_time)
                              .count();

        std::vector<bool> key_success(results.size(), false);
        for (size_t j = 0; j < results.size(); ++j) {
            if (results[j].has_value()) {
                key_success[j] = true;
                stats.get_operations++;
                stats.successful_operations++;
            } else {
                stats.get_failures++;
            }
        }
        double per_key_latency_us =
            static_cast<double>(latency_us) / FLAGS_batch_size;
        for (int j = 0; j < FLAGS_batch_size; ++j) {
            stats.operations.push_back(
                {per_key_latency_us, false, key_success[j]});
        }
        stats.batch_get_latency_us.push_back(static_cast<double>(latency_us));
        stats.total_operations += FLAGS_batch_size;
    }

    // Buffers are fixed slices of g_worker_buffer_base; no deallocation needed.
}

void calculate_percentiles(std::vector<double>& latencies, double& p50,
                           double& p70, double& p90, double& p95, double& p99) {
    if (latencies.empty()) {
        p50 = p70 = p90 = p95 = p99 = 0.0;
        return;
    }

    std::sort(latencies.begin(), latencies.end());
    size_t size = latencies.size();

    // Use explicit parentheses and floating-point arithmetic for safe
    // percentile calculation This avoids integer division order issues and
    // ensures correct indices
    p50 = latencies[static_cast<size_t>(std::ceil((size * 0.50) - 1))];
    p70 = latencies[static_cast<size_t>(std::ceil((size * 0.70) - 1))];
    p90 = latencies[static_cast<size_t>(std::ceil((size * 0.90) - 1))];
    p95 = latencies[static_cast<size_t>(std::ceil((size * 0.95) - 1))];
    p99 = latencies[static_cast<size_t>(std::ceil((size * 0.99) - 1))];
}

void print_results(const std::vector<ThreadStats>& thread_stats,
                   double put_duration_s, double get_duration_s) {
    double total_duration_s = put_duration_s + get_duration_s;
    uint64_t total_ops = 0;
    uint64_t successful_ops = 0;
    uint64_t total_put_ops = 0;
    uint64_t total_get_ops = 0;
    uint64_t total_put_failures = 0;
    uint64_t total_get_failures = 0;

    std::vector<double> put_latencies;
    std::vector<double> get_latencies;
    std::vector<double> put_batch_latencies;
    std::vector<double> get_batch_latencies;

    for (const auto& stats : thread_stats) {
        total_ops += stats.total_operations;
        successful_ops += stats.successful_operations;
        total_put_ops += stats.put_operations;
        total_get_ops += stats.get_operations;
        total_put_failures += stats.put_failures;
        total_get_failures += stats.get_failures;

        for (const auto& op : stats.operations) {
            if (op.success) {
                if (op.is_put) {
                    put_latencies.push_back(op.latency_us);
                } else {
                    get_latencies.push_back(op.latency_us);
                }
            }
        }
        put_batch_latencies.insert(put_batch_latencies.end(),
                                   stats.batch_put_latency_us.begin(),
                                   stats.batch_put_latency_us.end());
        get_batch_latencies.insert(get_batch_latencies.end(),
                                   stats.batch_get_latency_us.begin(),
                                   stats.batch_get_latency_us.end());
    }

    // Calculate percentiles: per-key (latency of individual op, batch time
    // divided by batch_size) and per-batch (whole-batch completion latency).
    double put_p50, put_p70, put_p90, put_p95, put_p99;
    double get_p50, get_p70, get_p90, get_p95, get_p99;
    double putb_p50, putb_p70, putb_p90, putb_p95, putb_p99;
    double getb_p50, getb_p70, getb_p90, getb_p95, getb_p99;

    calculate_percentiles(put_latencies, put_p50, put_p70, put_p90, put_p95,
                          put_p99);
    calculate_percentiles(get_latencies, get_p50, get_p70, get_p90, get_p95,
                          get_p99);
    calculate_percentiles(put_batch_latencies, putb_p50, putb_p70, putb_p90,
                          putb_p95, putb_p99);
    calculate_percentiles(get_batch_latencies, getb_p50, getb_p70, getb_p90,
                          getb_p95, getb_p99);

    double put_ops_per_second =
        put_duration_s > 0 ? total_put_ops / put_duration_s : 0.0;
    double get_ops_per_second =
        get_duration_s > 0 ? total_get_ops / get_duration_s : 0.0;

    double put_data_throughput_mb_s =
        put_duration_s > 0 ? (total_put_ops * FLAGS_value_size) /
                                 (put_duration_s * 1024.0 * 1024.0)
                           : 0.0;
    double get_data_throughput_mb_s =
        get_duration_s > 0 ? (total_get_ops * FLAGS_value_size) /
                                 (get_duration_s * 1024.0 * 1024.0)
                           : 0.0;

    // Report PUT and GET success rates separately. A single combined rate
    // hid cases where one phase was healthy and the other was failing.
    uint64_t put_attempts = total_put_ops + total_put_failures;
    uint64_t get_attempts = total_get_ops + total_get_failures;
    double put_success_rate =
        put_attempts > 0 ? 100.0 * total_put_ops / put_attempts : 0.0;
    double get_success_rate =
        get_attempts > 0 ? 100.0 * total_get_ops / get_attempts : 0.0;

    // Machine-readable failure markers for the runner. Anything less than
    // 100% success means the config is not comparable to a clean run and
    // should be excluded from averaging.
    if (put_success_rate < 100.0) {
        LOG(WARNING) << "!! WARN: PUT_SUCCESS_RATE_NOT_FULL rate="
                     << put_success_rate << " failures=" << total_put_failures;
    }
    if (get_success_rate < 100.0) {
        LOG(WARNING) << "!! WARN: GET_SUCCESS_RATE_NOT_FULL rate="
                     << get_success_rate << " failures=" << total_get_failures;
    }

    LOG(INFO) << "=== Benchmark Results ===";
    LOG(INFO) << "Total Test Duration: " << total_duration_s << " seconds";
    LOG(INFO) << "PUT Duration: " << put_duration_s << " seconds";
    LOG(INFO) << "GET Duration: " << get_duration_s << " seconds";
    LOG(INFO) << "Threads: " << FLAGS_num_threads;
    LOG(INFO) << "Key Size: 128 bytes";
    LOG(INFO) << "Value Size: " << FLAGS_value_size << " bytes";
    LOG(INFO) << "Batch Size: " << FLAGS_batch_size;
    LOG(INFO) << "Batches per thread: " << FLAGS_test_operation_nums;
    LOG(INFO) << "";
    LOG(INFO) << "=== Operation Statistics ===";
    LOG(INFO) << "Total Operations: " << total_ops;
    LOG(INFO) << "Successful Operations: " << successful_ops;
    LOG(INFO) << "PUT Operations: " << total_put_ops
              << " (failures=" << total_put_failures << ")";
    LOG(INFO) << "GET Operations: " << total_get_ops
              << " (failures=" << total_get_failures << ")";
    LOG(INFO) << "PUT Success Rate: " << put_success_rate << "%";
    LOG(INFO) << "GET Success Rate: " << get_success_rate << "%";
    LOG(INFO) << "";
    LOG(INFO) << "=== Throughput ===";
    LOG(INFO) << "PUT Operations/sec: " << put_ops_per_second;
    LOG(INFO) << "GET Operations/sec: " << get_ops_per_second;
    LOG(INFO) << "PUT Data Throughput (MB/s): " << put_data_throughput_mb_s;
    LOG(INFO) << "GET Data Throughput (MB/s): " << get_data_throughput_mb_s;
    LOG(INFO) << "";
    LOG(INFO) << "=== Latency (microseconds) ===";

    // Per-key latency
    if (!put_latencies.empty()) {
        LOG(INFO) << "PUT Operations - P50: " << put_p50 << ", P70: " << put_p70
                  << ", P90: " << put_p90 << ", P95: " << put_p95
                  << ", P99: " << put_p99;
    }

    if (!get_latencies.empty()) {
        LOG(INFO) << "GET Operations - P50: " << get_p50 << ", P70: " << get_p70
                  << ", P90: " << get_p90 << ", P95: " << get_p95
                  << ", P99: " << get_p99;
    }

    // Batch-level latency
    if (!put_batch_latencies.empty()) {
        LOG(INFO) << "PUT Batch - P50: " << putb_p50 << ", P70: " << putb_p70
                  << ", P90: " << putb_p90 << ", P95: " << putb_p95
                  << ", P99: " << putb_p99;
    }

    if (!get_batch_latencies.empty()) {
        LOG(INFO) << "GET Batch - P50: " << getb_p50 << ", P70: " << getb_p70
                  << ", P90: " << getb_p90 << ", P95: " << getb_p95
                  << ", P99: " << getb_p99;
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
    LOG(INFO) << "Batches per thread: " << FLAGS_test_operation_nums;
    LOG(INFO) << "RAM buffer size: " << FLAGS_ram_buffer_size_gb << "GB";

    // Capacity precheck: PUT phase writes num_threads * ops unique keys at
    // value_size bytes each. If this exceeds the mounted segment, PUTs start
    // failing mid-run, making that config's results incomparable with
    // successful configs. Refuse to start instead of failing silently.
    // Skip for P2P mode where the segment is provisioned internally by tiered
    // backend config, not by ram_buffer_size_gb alone.
    if (FLAGS_client_type != "P2P") {
        uint64_t put_working_set = static_cast<uint64_t>(FLAGS_num_threads) *
                                   FLAGS_test_operation_nums *
                                   static_cast<uint64_t>(FLAGS_batch_size) *
                                   static_cast<uint64_t>(FLAGS_value_size);
        uint64_t segment_cap = static_cast<uint64_t>(FLAGS_ram_buffer_size_gb) *
                               1024 * 1024 * 1024;
        uint64_t usable_cap = segment_cap;
        if (put_working_set > usable_cap) {
            LOG(FATAL) << "PUT working set " << put_working_set / (1024 * 1024)
                       << " MiB (num_threads=" << FLAGS_num_threads
                       << " * batches=" << FLAGS_test_operation_nums
                       << " * batch_size=" << FLAGS_batch_size
                       << " * value=" << FLAGS_value_size
                       << ") exceeds ram_buffer_size_gb="
                       << FLAGS_ram_buffer_size_gb
                       << "GB. Increase --ram_buffer_size_gb or reduce "
                          "batches/threads/batch_size/value_size.";
        }
    }

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
    for (int i = 0; i < FLAGS_num_threads; ++i) {
        workers.emplace_back(worker_thread, i, std::ref(stop_flag),
                             std::ref(thread_stats[i]));
    }

    // Wait for all workers to finish warming up
    {
        std::unique_lock<std::mutex> lock(g_sync_mutex);
        g_sync_cv.wait(
            lock, [&] { return g_ready_threads_count == FLAGS_num_threads; });
    }

    // Wait for all workers to finish PUT phase
    auto put_start_time = std::chrono::high_resolution_clock::now();
    {
        std::lock_guard<std::mutex> lock(g_sync_mutex);
        g_start_signal_flag = true;
    }
    g_sync_cv.notify_all();

    {
        std::unique_lock<std::mutex> lock(g_sync_mutex);
        g_put_sync_cv.wait(
            lock, [&] { return g_put_done_count == FLAGS_num_threads; });
    }
    auto put_end_time = std::chrono::high_resolution_clock::now();

    // Start Phase 2: GET
    auto get_start_time = std::chrono::high_resolution_clock::now();
    {
        std::lock_guard<std::mutex> lock(g_sync_mutex);
        g_get_start_signal_flag = true;
    }
    g_put_sync_cv.notify_all();

    // Wait for all threads to complete
    for (auto& worker : workers) {
        worker.join();
    }

    auto get_end_time = std::chrono::high_resolution_clock::now();

    double put_duration_s =
        std::chrono::duration_cast<std::chrono::milliseconds>(put_end_time -
                                                              put_start_time)
            .count() /
        1000.0;
    double get_duration_s =
        std::chrono::duration_cast<std::chrono::milliseconds>(get_end_time -
                                                              get_start_time)
            .count() /
        1000.0;

    // Print results
    print_results(thread_stats, put_duration_s, get_duration_s);

    // Cleanup
    cleanup_segment();
    cleanup_client();
    google::ShutdownGoogleLogging();

    LOG(INFO) << "Benchmark completed successfully";
    return 0;
}
