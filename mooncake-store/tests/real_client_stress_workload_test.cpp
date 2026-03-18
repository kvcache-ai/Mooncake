#include <gflags/gflags.h>
#include <glog/logging.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cmath>
#include <filesystem>
#include <fstream>
#include <memory>
#include <mutex>
#include <numeric>
#include <optional>
#include <random>
#include <span>
#include <string>
#include <thread>
#include <vector>

#include "client_config_builder.h"
#include "p2p_rpc_types.h"
#include "real_client.h"
#include "rpc_types.h"
#include "types.h"

using namespace mooncake;

DEFINE_string(protocol, "tcp", "Transfer protocol: tcp|rdma");
DEFINE_string(master_address, "127.0.0.1:50051", "Master server address");
DEFINE_string(local_hostname, "127.0.0.1", "Local hostname or IP");
DEFINE_string(metadata_connection_string, "P2PHANDSHAKE",
              "Metadata connection string");
DEFINE_string(device_names, "", "RDMA device names");
DEFINE_string(
    tiered_backend_config,
    R"({"tiers":[{"type":"DRAM","capacity":1073741824,"priority":10}]})",
    "Tiered backend config json");

DEFINE_uint32(client_rpc_port, 12345, "Client RPC port in P2P mode");
DEFINE_uint32(rpc_thread_num, 16, "RPC thread number in P2P mode");

DEFINE_int32(node_id, 1, "Current node ID (1-based)");
DEFINE_int32(num_nodes, 1, "Total number of nodes in the test cluster");
DEFINE_int32(key_count, 1000, "Number of keys to preload on this node");
DEFINE_int32(num_threads, 8, "Number of worker threads");
DEFINE_int32(test_operation_nums, 1000, "Operations per thread");
DEFINE_int32(value_size, 1048576, "Value size in bytes");
DEFINE_int32(warmup_ops, 100, "Warmup ops per thread");
DEFINE_double(remote_read_ratio, 0.5,
              "Ratio of remote reads in stress-read mode");
DEFINE_int32(random_seed, 12345, "Random seed");
DEFINE_int64(start_timestamp_ms, 0,
             "Global start timestamp (ms since epoch) for read phase in "
             "preload-then-read mode. If 0, start immediately after preload.");

DEFINE_string(client_mode, "p2p",
              "Client mode: p2p | centralized");
DEFINE_int64(global_segment_size, 2147483648LL,
             "Global segment size in bytes for centralized mode "
             "(default 2GB, must be >= key_count * value_size).");

namespace {

std::shared_ptr<RealClient> g_client;

struct OperationResult {
    double latency_us = 0.0;
    bool success = false;
    bool expected_remote = false;
    bool query_success = false;
};

struct ThreadStats {
    std::vector<OperationResult> reads;
    uint64_t total_reads = 0;
    uint64_t successful_reads = 0;
    uint64_t query_failures = 0;
    uint64_t get_failures = 0;
    uint64_t local_reads = 0;
    uint64_t remote_reads = 0;
    uint64_t local_successful_reads = 0;
    uint64_t remote_successful_reads = 0;
};



std::string generate_key(int node_id, int idx) {
    return "node_" + std::to_string(node_id) + "_obj_" + std::to_string(idx);
}

bool initialize_real_client() {
    auto client = RealClient::create();

    int rc = 0;
    if (FLAGS_client_mode == "p2p") {
        auto config = ClientConfigBuilder::build_p2p_real_client(
            FLAGS_local_hostname,
            FLAGS_metadata_connection_string,
            FLAGS_protocol,
            FLAGS_device_names.empty()
                ? std::nullopt
                : std::optional<std::string>(FLAGS_device_names),
            FLAGS_master_address,
            FLAGS_tiered_backend_config,
            /*local_buffer_size=*/0,
            /*transfer_engine=*/nullptr,
            /*ipc_socket_path=*/"",
            static_cast<uint16_t>(FLAGS_client_rpc_port),
            static_cast<uint32_t>(FLAGS_rpc_thread_num));
        rc = client->setup(config);
    } else if (FLAGS_client_mode == "centralized") {
        auto config = ClientConfigBuilder::build_centralized_real_client(
            FLAGS_local_hostname,
            FLAGS_metadata_connection_string,
            FLAGS_protocol,
            FLAGS_device_names.empty()
                ? std::nullopt
                : std::optional<std::string>(FLAGS_device_names),
            FLAGS_master_address,
            static_cast<uint64_t>(FLAGS_global_segment_size),
            /*local_buffer_size=*/0,
            /*transfer_engine=*/nullptr,
            /*ipc_socket_path=*/"",
            /*enable_offload=*/false);
        rc = client->setup(config);
    } else {
        LOG(ERROR) << "Unknown client_mode: " << FLAGS_client_mode
                   << ", expected 'p2p' or 'centralized'";
        return false;
    }
    if (rc != 0) {
        LOG(ERROR) << "RealClient setup failed, rc=" << rc;
        return false;
    }

    g_client = std::move(client);
    return true;
}

void calculate_percentiles(std::vector<double>& latencies, double& p50,
                           double& p90, double& p95, double& p99) {
    if (latencies.empty()) {
        p50 = p90 = p95 = p99 = 0.0;
        return;
    }

    std::sort(latencies.begin(), latencies.end());
    const size_t size = latencies.size();

    auto idx = [&](double ratio) -> size_t {
        return static_cast<size_t>(std::ceil((size * ratio) - 1));
    };

    p50 = latencies[idx(0.50)];
    p90 = latencies[idx(0.90)];
    p95 = latencies[idx(0.95)];
    p99 = latencies[idx(0.99)];
}

void print_read_results(const std::vector<ThreadStats>& thread_stats,
                        double duration_s) {
    uint64_t total_reads = 0;
    uint64_t successful_reads = 0;
    uint64_t query_failures = 0;
    uint64_t get_failures = 0;
    uint64_t local_reads = 0;
    uint64_t remote_reads = 0;
    uint64_t local_success = 0;
    uint64_t remote_success = 0;

    std::vector<double> all_latencies;
    std::vector<double> local_latencies;
    std::vector<double> remote_latencies;

    for (const auto& stats : thread_stats) {
        total_reads += stats.total_reads;
        successful_reads += stats.successful_reads;
        query_failures += stats.query_failures;
        get_failures += stats.get_failures;
        local_reads += stats.local_reads;
        remote_reads += stats.remote_reads;
        local_success += stats.local_successful_reads;
        remote_success += stats.remote_successful_reads;

        for (const auto& op : stats.reads) {
            if (!op.success) continue;
            all_latencies.push_back(op.latency_us);
            if (op.expected_remote) {
                remote_latencies.push_back(op.latency_us);
            } else {
                local_latencies.push_back(op.latency_us);
            }
        }
    }

    double all_p50, all_p90, all_p95, all_p99;
    double local_p50, local_p90, local_p95, local_p99;
    double remote_p50, remote_p90, remote_p95, remote_p99;

    calculate_percentiles(all_latencies, all_p50, all_p90, all_p95, all_p99);
    calculate_percentiles(local_latencies, local_p50, local_p90, local_p95,
                          local_p99);
    calculate_percentiles(remote_latencies, remote_p50, remote_p90, remote_p95,
                          remote_p99);

    const double reads_per_sec =
        duration_s > 0 ? successful_reads / duration_s : 0.0;
    const double data_mb_per_sec =
        duration_s > 0
            ? (successful_reads * FLAGS_value_size) / (duration_s * 1024 * 1024)
            : 0.0;

    LOG(INFO) << "=== RealClient Multi-Node Stress Results ===";
    LOG(INFO) << "Node ID: " << FLAGS_node_id;
    LOG(INFO) << "Duration(s): " << duration_s;
    LOG(INFO) << "Threads: " << FLAGS_num_threads;
    LOG(INFO) << "Value size(bytes): " << FLAGS_value_size;
    LOG(INFO) << "Total reads: " << total_reads;
    LOG(INFO) << "Successful reads: " << successful_reads;
    LOG(INFO) << "Query failures: " << query_failures;
    LOG(INFO) << "Get failures: " << get_failures;
    LOG(INFO) << "Success rate(%): "
              << (total_reads == 0 ? 0.0
                                   : 100.0 * successful_reads / total_reads);

    LOG(INFO) << "Local reads: " << local_reads
              << ", local successful reads: " << local_success;
    LOG(INFO) << "Remote reads: " << remote_reads
              << ", remote successful reads: " << remote_success;

    LOG(INFO) << "Reads/sec: " << reads_per_sec;
    LOG(INFO) << "Data throughput(MB/s): " << data_mb_per_sec;

    LOG(INFO) << "All latency(us) p50/p90/p95/p99 = "
              << all_p50 << "/" << all_p90 << "/" << all_p95 << "/" << all_p99;
    LOG(INFO) << "Local latency(us) p50/p90/p95/p99 = "
              << local_p50 << "/" << local_p90 << "/" << local_p95 << "/"
              << local_p99;
    LOG(INFO) << "Remote latency(us) p50/p90/p95/p99 = "
              << remote_p50 << "/" << remote_p90 << "/" << remote_p95 << "/"
              << remote_p99;
}

bool preload_keys() {
    std::vector<char> buffer(FLAGS_value_size, 'A');
    if (g_client->register_buffer(buffer.data(), buffer.size()) != 0) {
        LOG(ERROR) << "register_buffer failed in preload";
        return false;
    }

    // Choose write configuration based on client_mode:
    // - P2P mode: use WriteRouteRequestConfig (write via routing service)
    // - Centralized mode: use ReplicateConfig (centralized replication)
    // put_from is a zero-copy style API: it reads directly from the user-registered
    // buffer; both modes support it.
    WriteConfig write_cfg = [&]() -> WriteConfig {
        if (FLAGS_client_mode == "p2p") {
            WriteRouteRequestConfig cfg;
            cfg.max_candidates = 1;
            cfg.allow_local = true;
            cfg.prefer_local = true;
            cfg.early_return = true;
            return cfg;
        } else {
            ReplicateConfig cfg;
            cfg.replica_num = 1;
            return cfg;
        }
    }();

    for (int i = 0; i < FLAGS_key_count; ++i) {
        auto key = generate_key(FLAGS_node_id, i);
        std::fill(buffer.begin(), buffer.end(),
                  static_cast<char>('A' + (i % 26)));

        int rc = g_client->put_from(key, buffer.data(), buffer.size(),
                                    write_cfg);
        if (rc != 0) {
            LOG(ERROR) << "put_from failed for key=" << key << ", rc=" << rc;
            g_client->unregister_buffer(buffer.data());
            return false;
        }
    }

    g_client->unregister_buffer(buffer.data());

    LOG(INFO) << "Preload complete, node=" << FLAGS_node_id
              << ", keys=" << FLAGS_key_count;
    return true;
}

void stress_read_worker(int thread_id, const std::vector<std::string>& local_keys,
                        const std::vector<int>& remote_node_ids,
                        std::atomic<bool>& stop_flag, ThreadStats& stats) {
    std::vector<char> buffer(FLAGS_value_size, 0);
    if (g_client->register_buffer(buffer.data(), buffer.size()) != 0) {
        LOG(ERROR) << "Thread " << thread_id << ": register_buffer failed";
        return;
    }

    std::mt19937 rng(FLAGS_random_seed + thread_id);
    std::uniform_real_distribution<double> ratio_dist(0.0, 1.0);

    auto pick_index = [&](size_t size) -> size_t {
        std::uniform_int_distribution<size_t> dist(0, size - 1);
        return dist(rng);
    };

    ReadRouteConfig read_cfg;

    const int total_ops = FLAGS_warmup_ops + FLAGS_test_operation_nums;
    const bool has_remote_nodes = !remote_node_ids.empty();
    const int total_remote_keys =
        has_remote_nodes ? static_cast<int>(remote_node_ids.size()) * FLAGS_key_count : 0;

    for (int i = 0; i < total_ops && !stop_flag.load(); ++i) {
        const bool choose_remote =
            has_remote_nodes &&
            (ratio_dist(rng) < FLAGS_remote_read_ratio);

        int target_node = FLAGS_node_id;
        int key_idx = 0;

        if (choose_remote) {
            // Perform a global random selection over the key space across all
            // remote nodes:
            // total_remote_keys = remote_node_ids.size() * key_count
            if (total_remote_keys <= 0) {
                // Should not happen, but keep a defensive guard.
                continue;
            }
            std::uniform_int_distribution<int> remote_key_dist(0, total_remote_keys - 1);
            int gidx = remote_key_dist(rng);  // Global remote-key index

            int remote_node_slot = gidx / FLAGS_key_count;   // Which remote node (slot)
            int remote_key_idx = gidx % FLAGS_key_count;     // Key index within that node

            target_node = remote_node_ids[static_cast<size_t>(remote_node_slot)];
            key_idx = remote_key_idx;
        } else {
            // Pure local read: pick a random key from this node's key space.
            key_idx = static_cast<int>(
                pick_index(static_cast<size_t>(FLAGS_key_count)));
        }
        const std::string key = generate_key(target_node, key_idx);

        auto start = std::chrono::high_resolution_clock::now();
        int64_t size = g_client->get_into(key, buffer.data(), buffer.size(),
                                          read_cfg);
        auto end = std::chrono::high_resolution_clock::now();

        if (i >= FLAGS_warmup_ops) {
            OperationResult result;
            result.latency_us =
                std::chrono::duration_cast<std::chrono::microseconds>(end - start)
                    .count();
            result.expected_remote = choose_remote;
            result.success = size >= 0;
            result.query_success = result.success;  // get_into already covers query+get
            stats.reads.push_back(result);

            stats.total_reads++;
            if (choose_remote) {
                stats.remote_reads++;
            } else {
                stats.local_reads++;
            }

            if (result.success) {
                stats.successful_reads++;
                if (choose_remote) {
                    stats.remote_successful_reads++;
                } else {
                    stats.local_successful_reads++;
                }
            } else {
                stats.get_failures++;
            }
        }
    }

    g_client->unregister_buffer(buffer.data());
}

bool stress_read() {
    // Local keys: node_<node_id>_obj_0..key_count-1
    // Remote node list: derived from num_nodes and node_id.
    std::vector<int> remote_node_ids;
    remote_node_ids.reserve(std::max(0, FLAGS_num_nodes - 1));
    for (int nid = 1; nid <= FLAGS_num_nodes; ++nid) {
        if (nid == FLAGS_node_id) continue;
        remote_node_ids.push_back(nid);
    }

    LOG(INFO) << "Stress-read config: node_id=" << FLAGS_node_id
              << ", num_nodes=" << FLAGS_num_nodes
              << ", local_keys=" << FLAGS_key_count
              << ", remote_nodes=" << remote_node_ids.size();

    std::vector<std::thread> workers;
    std::vector<ThreadStats> thread_stats(FLAGS_num_threads);
    std::atomic<bool> stop_flag{false};

    auto start = std::chrono::high_resolution_clock::now();

    // local_keys is not used at the moment; keep it as a placeholder to avoid
    // larger signature changes.
    std::vector<std::string> dummy_local_keys;

    for (int i = 0; i < FLAGS_num_threads; ++i) {
        workers.emplace_back(stress_read_worker, i, std::cref(dummy_local_keys),
                             std::cref(remote_node_ids), std::ref(stop_flag),
                             std::ref(thread_stats[i]));
    }

    for (auto& t : workers) {
        t.join();
    }

    auto end = std::chrono::high_resolution_clock::now();
    const double duration_s =
        std::chrono::duration_cast<std::chrono::milliseconds>(end - start)
            .count() /
        1000.0;

    print_read_results(thread_stats, duration_s);
    return true;
}

}  // namespace

int main(int argc, char** argv) {
    gflags::ParseCommandLineFlags(&argc, &argv, true);
    google::InitGoogleLogging(argv[0]);
    FLAGS_logtostderr = 1;

    LOG(INFO) << "Starting RealClient stress workload test"
              << ", node_id=" << FLAGS_node_id
              << ", master=" << FLAGS_master_address
              << ", local_hostname=" << FLAGS_local_hostname;

    if (FLAGS_remote_read_ratio < 0.0 || FLAGS_remote_read_ratio > 1.0) {
        LOG(ERROR) << "remote_read_ratio must be in [0, 1]";
        return 1;
    }

    if (FLAGS_num_nodes <= 0) {
        LOG(ERROR) << "num_nodes must be > 0";
        return 1;
    }
    if (FLAGS_node_id <= 0 || FLAGS_node_id > FLAGS_num_nodes) {
        LOG(ERROR) << "node_id must be in [1, num_nodes], got node_id="
                   << FLAGS_node_id << ", num_nodes=" << FLAGS_num_nodes;
        return 1;
    }

    if (!initialize_real_client()) {
        LOG(ERROR) << "Failed to initialize RealClient";
        return 1;
    }

    // In the same process: preload this node's keys first, then start the stress
    // read phase.
    bool ok = preload_keys();
    if (!ok) {
        LOG(ERROR) << "preload-then-read: preload phase failed";
    } else {
        // If a global start time is provided, align the read phase across nodes.
        if (FLAGS_start_timestamp_ms > 0) {
            using clock = std::chrono::system_clock;
            auto now = clock::now();
            auto now_ms =
                std::chrono::duration_cast<std::chrono::milliseconds>(
                    now.time_since_epoch())
                    .count();
            if (FLAGS_start_timestamp_ms > now_ms) {
                auto wait_ms = FLAGS_start_timestamp_ms - now_ms;
                LOG(INFO) << "preload-then-read: preload done, waiting "
                          << wait_ms
                          << " ms to align global start time (start_ts_ms="
                          << FLAGS_start_timestamp_ms << ", now_ms="
                          << now_ms << ")";
                std::this_thread::sleep_for(
                    std::chrono::milliseconds(wait_ms));
            } else {
                LOG(WARNING)
                    << "preload-then-read: start_timestamp_ms ("
                    << FLAGS_start_timestamp_ms
                    << ") is in the past, starting stress-read immediately";
            }
        }
        LOG(INFO) << "preload-then-read: starting stress-read phase";
        ok = stress_read();
    }

    // After finishing, keep the process alive for 120 seconds before cleanup.
    // This reduces the chance that other pods still doing remote reads will fail
    // because this node exits too early.
    if (ok) {
        LOG(INFO) << "Test finished successfully, keeping process alive for 120s "
                     "before cleanup to allow in-flight remote reads to finish";
        std::this_thread::sleep_for(std::chrono::seconds(120));
    } else {
        LOG(WARNING) << "Test failed, skipping post-test delay.";
    }

    g_client.reset();
    google::ShutdownGoogleLogging();
    return ok ? 0 : 1;
}