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
DEFINE_int32(test_operation_nums, 100, "Number of operations per thread");
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
DEFINE_uint64(local_memcpy_async_queue_depth, 2048,
              "If set p2p_local_transfer_mode=memcpy, Queue depth for async "
              "local memcpy executor (P2P)");
namespace mooncake {
namespace benchmark {

// Global client and allocator instances
std::shared_ptr<ClientService> g_client = nullptr;
void* g_segment_ptr = nullptr;
size_t g_ram_buffer_size = 0;
void* g_worker_buffer_base = nullptr;
size_t g_per_thread_buffer_stride = 0;  // write_buf + read_pool per thread

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

struct ThreadStats {
    std::vector<OperationResult> operations;
    uint64_t total_operations = 0;
    uint64_t successful_operations = 0;
    uint64_t put_operations = 0;
    uint64_t get_operations = 0;
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
        // FLAGS_tiered_backend_config can still override this if explicitly set.
        std::string tiered_config = FLAGS_tiered_backend_config;
        if (tiered_config.find("16106127360") != std::string::npos) {
            // Default value is still 15GB placeholder — replace with actual flag
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
            /*route_cache_max_memory_bytes=*/300 * 1024 * 1024,
            /*route_cache_ttl_ms=*/5 * 60 * 1000, FLAGS_p2p_local_transfer_mode,
            FLAGS_local_memcpy_async_worker_num,
            FLAGS_local_memcpy_async_queue_depth);
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

    // Pre-allocate the worker buffer pool with exact sizing, bypassing the
    // slab allocator (SimpleAllocator/CacheLib) to avoid fragmentation.
    // Each thread gets: 1 write_buffer + num_read_slots read slots.
    // Both P2P and Centralization BatchGet submit all transfers in parallel,
    // so each key in a batch needs its own destination slot to avoid concurrent
    // RDMA/memcpy writes targeting the same buffer region.
    size_t per_thread_read_slots =
        static_cast<size_t>(std::max(1, FLAGS_batch_size));
    g_per_thread_buffer_stride =
        static_cast<size_t>(FLAGS_value_size) * (1 + per_thread_read_slots);
    size_t total_buffer_size = FLAGS_num_threads * g_per_thread_buffer_stride;

    // Align to page boundary for RDMA registration.
    g_worker_buffer_base = std::aligned_alloc(4096, total_buffer_size);
    if (!g_worker_buffer_base) {
        LOG(ERROR) << "Failed to allocate worker buffer pool of size "
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
              << total_buffer_size / (1024 * 1024) << "MB worker buffer pool";
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
    // Reserve memory for operations to avoid reallocations during benchmark
    stats.operations.reserve(FLAGS_test_operation_nums * 2);

    // Each thread gets a dedicated slice of the pre-registered worker buffer
    // pool. The stride is computed once in initialize_client() so all threads
    // use the same formula and regions never overlap.
    // Layout per thread: [write_buffer | read_pool_slot_0 | ... | slot_N-1]
    int num_read_slots = std::max(1, FLAGS_batch_size);
    char* thread_base =
        static_cast<char*>(g_worker_buffer_base) +
        static_cast<size_t>(thread_id) * g_per_thread_buffer_stride;
    void* write_buffer = thread_base;
    void* read_pool = thread_base + FLAGS_value_size;

    if (!g_worker_buffer_base || g_per_thread_buffer_stride == 0) {
        LOG(ERROR) << "Thread " << thread_id
                   << ": worker buffer pool not initialized";
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

    // Warmup phase: Establish RPC/RDMA connections before measuring latency
    std::string warmup_key = "warmup_" + std::to_string(thread_id);
    auto warmup_start = std::chrono::high_resolution_clock::now();
    auto warmup_res = g_client->Put(warmup_key.data(), slices, config);
    auto warmup_end = std::chrono::high_resolution_clock::now();
    LOG(INFO) << "Thread " << thread_id << " warmup put completed in "
              << std::chrono::duration_cast<std::chrono::microseconds>(
                     warmup_end - warmup_start)
                     .count()
              << " us. Success: " << warmup_res.has_value();

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
    for (int i = 0; i < FLAGS_test_operation_nums && !stop_flag.load();
         i += FLAGS_batch_size) {
        int actual_batch =
            std::min(FLAGS_batch_size, FLAGS_test_operation_nums - i);
        std::vector<ObjectKey> batch_keys;
        std::vector<std::vector<Slice>> batch_slices(actual_batch, slices);

        for (int j = 0; j < actual_batch; ++j) {
            batch_keys.push_back(generate_key(thread_id, i + j));
        }

        auto start_time = std::chrono::high_resolution_clock::now();
        auto results = g_client->BatchPut(batch_keys, batch_slices, config);
        auto end_time = std::chrono::high_resolution_clock::now();

        auto latency_us = std::chrono::duration_cast<std::chrono::microseconds>(
                              end_time - start_time)
                              .count();

        // Record metrics. Latency is divided across keys so that percentiles
        // reflect per-key cost rather than batch completion time.
        bool any_success = false;
        for (size_t j = 0; j < results.size(); ++j) {
            bool success = results[j].has_value();
            if (success) {
                stored_keys.push_back(batch_keys[j]);
                stats.put_operations++;
                stats.successful_operations++;
                any_success = true;
            }
        }
        double per_key_latency_us =
            static_cast<double>(latency_us) / actual_batch;
        for (int j = 0; j < actual_batch; ++j) {
            stats.operations.push_back({per_key_latency_us, true, any_success});
        }
        stats.total_operations += actual_batch;
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

    // Phase 2: Perform GET operations on the stored keys
    for (int i = 0; i < FLAGS_test_operation_nums && !stop_flag.load() &&
                    !stored_keys.empty();
         i += FLAGS_batch_size) {
        int actual_batch =
            std::min(FLAGS_batch_size, (int)stored_keys.size() - i);
        if (actual_batch <= 0) break;

        std::vector<std::string> batch_keys;
        std::vector<std::vector<void*>> all_buffers(actual_batch);
        std::vector<std::vector<size_t>> all_sizes(
            actual_batch, {static_cast<size_t>(FLAGS_value_size)});

        for (int j = 0; j < actual_batch; ++j) {
            batch_keys.push_back(stored_keys[(i + j) % stored_keys.size()]);
            // Each key uses a distinct slot so that concurrent async copies
            // write to separate memory regions and do not race.
            void* slot = static_cast<char*>(read_pool) +
                         (j % num_read_slots) * FLAGS_value_size;
            all_buffers[j] = {slot};
        }

        auto start_time = std::chrono::high_resolution_clock::now();
        auto results = g_client->BatchGet(batch_keys, all_buffers, all_sizes);
        auto end_time = std::chrono::high_resolution_clock::now();

        auto latency_us = std::chrono::duration_cast<std::chrono::microseconds>(
                              end_time - start_time)
                              .count();

        bool any_success = false;
        for (const auto& res : results) {
            if (res.has_value()) {
                stats.get_operations++;
                stats.successful_operations++;
                any_success = true;
            }
        }
        double per_key_latency_us =
            static_cast<double>(latency_us) / actual_batch;
        for (int j = 0; j < actual_batch; ++j) {
            stats.operations.push_back(
                {per_key_latency_us, false, any_success});
        }
        stats.total_operations += actual_batch;
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
    double all_p50, all_p70, all_p90, all_p95, all_p99;
    double put_p50, put_p70, put_p90, put_p95, put_p99;
    double get_p50, get_p70, get_p90, get_p95, get_p99;

    calculate_percentiles(all_latencies, all_p50, all_p70, all_p90, all_p95,
                          all_p99);
    calculate_percentiles(put_latencies, put_p50, put_p70, put_p90, put_p95,
                          put_p99);
    calculate_percentiles(get_latencies, get_p50, get_p70, get_p90, get_p95,
                          get_p99);

    // Calculate throughput using phase-specific durations
    double put_ops_per_second = total_put_ops / put_duration_s;
    double get_ops_per_second = total_get_ops / get_duration_s;
    double total_ops_per_second = successful_ops / total_duration_s;

    // Calculate data throughput separately for PUT and GET operations
    double put_data_throughput_mb_s =
        (total_put_ops * FLAGS_value_size) / (put_duration_s * 1024 * 1024);
    double get_data_throughput_mb_s =
        (total_get_ops * FLAGS_value_size) / (get_duration_s * 1024 * 1024);

    // Print results
    LOG(INFO) << "=== Benchmark Results ===";
    LOG(INFO) << "Total Test Duration: " << total_duration_s << " seconds";
    LOG(INFO) << "PUT Duration: " << put_duration_s << " seconds";
    LOG(INFO) << "GET Duration: " << get_duration_s << " seconds";
    LOG(INFO) << "Threads: " << FLAGS_num_threads;
    LOG(INFO) << "Key Size: 128 bytes";
    LOG(INFO) << "Value Size: " << FLAGS_value_size << " bytes";
    LOG(INFO) << "Batch Size: " << FLAGS_batch_size;
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
    LOG(INFO) << "Total Operations/sec: " << total_ops_per_second;
    LOG(INFO) << "PUT Operations/sec: " << put_ops_per_second;
    LOG(INFO) << "GET Operations/sec: " << get_ops_per_second;
    LOG(INFO) << "PUT Data Throughput (MB/s): " << put_data_throughput_mb_s;
    LOG(INFO) << "GET Data Throughput (MB/s): " << get_data_throughput_mb_s;
    LOG(INFO) << "";
    LOG(INFO) << "=== Latency (microseconds) ===";

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
