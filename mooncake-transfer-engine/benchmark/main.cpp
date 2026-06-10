// Copyright 2025 KVCache.AI
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "utils.h"
#include "common.h"

#include "bench_runner.h"
#include "te_backend.h"
#ifdef USE_TENT
#include "tent_backend.h"
#include "tent/transfer_engine.h"
#include "tent/common/types.h"
#include "coordinator.h"
#endif

#include <thread>
#include <chrono>
#include <sstream>
#include <iomanip>
#include <fstream>
#include <iostream>
#include <map>
#include <set>

using namespace mooncake::tent;

#ifdef USE_TENT
/**
 * Structure to hold benchmark results for serialization
 */
struct BenchmarkResult {
    int node_rank;
    size_t block_size;
    size_t batch_size;
    int num_threads;
    size_t num_targets;
    uint64_t total_samples;
    double total_duration_avg;
    double transfer_duration_avg;
    double transfer_duration_min;
    double transfer_duration_max;
    double transfer_duration_p99;
    double transfer_duration_p999;

    // Serialize to string for transmission
    std::string serialize() const {
        std::ostringstream oss;
        oss << node_rank << "," << block_size << "," << batch_size << ","
            << num_threads << "," << num_targets << "," << total_samples << ","
            << total_duration_avg << "," << transfer_duration_avg << ","
            << transfer_duration_min << "," << transfer_duration_max << ","
            << transfer_duration_p99 << "," << transfer_duration_p999;
        return oss.str();
    }

    // Deserialize from string
    static BenchmarkResult deserialize(const std::string& str) {
        BenchmarkResult result{};
        std::istringstream iss(str);
        char comma;
        iss >> result.node_rank >> comma >> result.block_size >> comma >>
            result.batch_size >> comma >> result.num_threads >> comma >>
            result.num_targets >> comma >> result.total_samples >> comma >>
            result.total_duration_avg >> comma >>
            result.transfer_duration_avg >> comma >>
            result.transfer_duration_min >> comma >>
            result.transfer_duration_max >> comma >>
            result.transfer_duration_p99 >> comma >>
            result.transfer_duration_p999;
        return result;
    }

    // Calculate aggregate bandwidth in GB/s
    double getBandwidthGBs() const {
        size_t total_data_transferred =
            (block_size * batch_size) * total_samples;
        double duration_sec = total_duration_avg / 1e6;
        return (total_data_transferred / (1000.0 * 1000.0 * 1000.0)) /
               duration_sec;
    }
};

/**
 * Create BenchmarkResult from XferBenchStats
 */
static BenchmarkResult createResult(int node_rank, size_t block_size,
                                    size_t batch_size, int num_threads,
                                    size_t num_targets,
                                    const XferBenchStats& stats) {
    BenchmarkResult result;
    result.node_rank = node_rank;
    result.block_size = block_size;
    result.batch_size = batch_size;
    result.num_threads = num_threads;
    result.num_targets = num_targets;
    result.total_samples = stats.transfer_duration.count();
    result.total_duration_avg = stats.total_duration.avg();
    result.transfer_duration_avg = stats.transfer_duration.avg();
    result.transfer_duration_min = stats.transfer_duration.min();
    result.transfer_duration_max = stats.transfer_duration.max();
    result.transfer_duration_p99 = stats.transfer_duration.p99();
    result.transfer_duration_p999 = stats.transfer_duration.p999();
    return result;
}

/**
 * Receive all results from a single node
 */
static std::vector<BenchmarkResult> receiveResultsFromNode(
    const std::string& msg) {
    std::vector<BenchmarkResult> results;

    std::istringstream iss(msg);
    size_t count;
    iss >> count;
    iss.ignore();  // Skip the delimiter

    for (size_t i = 0; i < count; ++i) {
        std::string result_str;
        std::getline(iss, result_str, '|');
        try {
            results.push_back(BenchmarkResult::deserialize(result_str));
        } catch (const std::exception& e) {
            LOG(WARNING) << "Failed to deserialize result: " << e.what();
        }
    }

    return results;
}

/**
 * Receive results from all other nodes
 */
static std::map<int, std::vector<BenchmarkResult>> receiveAllResultsFromNodes(
    TransferEngine* engine, int expected_nodes) {
    std::map<int, std::vector<BenchmarkResult>>
        all_results;  // node_rank -> results
    std::set<int> received_nodes;
    auto start_time = std::chrono::steady_clock::now();
    int timeout_sec = 60;  // 60 seconds timeout for result collection

    while ((int)received_nodes.size() < expected_nodes) {
        auto now = std::chrono::steady_clock::now();
        auto elapsed =
            std::chrono::duration_cast<std::chrono::seconds>(now - start_time)
                .count();

        if (elapsed >= timeout_sec) {
            LOG(WARNING) << "Timeout waiting for results from all nodes. "
                         << "Received from " << received_nodes.size() << " of "
                         << expected_nodes << " nodes";
            break;
        }

        std::vector<Notification> notifications;
        auto status = engine->receiveNotification(notifications);
        if (!status.ok()) {
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
            continue;
        }

        for (const auto& notif : notifications) {
            if (notif.name == "benchmark_results") {
                try {
                    auto node_results = receiveResultsFromNode(notif.msg);
                    if (!node_results.empty()) {
                        int node_rank = node_results[0].node_rank;
                        all_results[node_rank] = node_results;
                        received_nodes.insert(node_rank);
                        LOG(INFO) << "Received " << node_results.size()
                                  << " results from node " << node_rank;
                    }
                } catch (const std::exception& e) {
                    LOG(WARNING)
                        << "Failed to deserialize results: " << e.what();
                }
            }
        }

        if (notifications.empty()) {
            std::this_thread::sleep_for(std::chrono::milliseconds(50));
        }
    }

    return all_results;
}

/**
 * Print aggregated statistics for a single test configuration
 */
static void printSingleConfigAggregatedStats(
    const std::vector<BenchmarkResult>& results, int num_nodes) {
    if (results.empty()) {
        LOG(WARNING) << "No results to aggregate";
        return;
    }

    // Aggregate statistics across all nodes
    double total_bandwidth = 0;
    double avg_latency = 0;
    double avg_transfer = 0;
    double p99_transfer = 0;
    double p999_transfer = 0;

    for (const auto& result : results) {
        total_bandwidth += result.getBandwidthGBs();
        avg_latency += result.total_duration_avg;
        avg_transfer += result.transfer_duration_avg;
        p99_transfer += result.transfer_duration_p99;
        p999_transfer += result.transfer_duration_p999;
    }

    avg_latency /= results.size();
    avg_transfer /= results.size();
    p99_transfer /= results.size();
    p999_transfer /= results.size();

    // Get configuration from first result
    size_t block_size = results[0].block_size;
    size_t batch_size = results[0].batch_size;
    int num_threads = results[0].num_threads;
    size_t flows_per_node = results[0].num_targets * num_threads;
    size_t total_flows = flows_per_node * num_nodes;

    // clang-format off
    std::cout << std::left << std::fixed << std::setprecision(6)
              << std::setw(14) << block_size
              << std::setw(8) << batch_size
              << std::setw(10) << num_threads
              << std::setw(20) << total_bandwidth
              << std::setprecision(1)
              << std::setw(14) << avg_latency
              << std::setw(14) << avg_transfer
              << std::setw(14) << p99_transfer
              << std::setw(14) << p999_transfer
              << std::endl;
    // clang-format on
}

/**
 * Print header for aggregated results
 */
static void printAggregatedHeader() {
    // clang-format off
    std::cout << std::left
              << std::setw(14) << "BlkSize (B)"
              << std::setw(8) << "Batch"
              << std::setw(10) << "Threads"
              << std::setw(20) << "Total BW (GB/S)"
              << std::setw(14) << "Avg Lat (us)"
              << std::setw(14) << "Avg Tx (us)"
              << std::setw(14) << "P99 Tx (us)"
              << std::setw(14) << "P999 Tx (us)"
              << std::endl;
    std::cout << std::string(160, '-') << std::endl;
    // clang-format on
}
#endif  // USE_TENT

/**
 * Process a single test configuration for single-target mode
 */
int processBatchSizes(BenchRunner& runner, size_t block_size, size_t batch_size,
                      int num_threads) {
    bool mixed_opcode = false;
    OpCode opcode = READ;
    if (XferBenchConfig::check_consistency || XferBenchConfig::op_type == "mix")
        mixed_opcode = true;
    else if (XferBenchConfig::op_type == "read")
        opcode = READ;
    else if (XferBenchConfig::op_type == "write")
        opcode = WRITE;
    else {
        LOG(ERROR) << "Invalid args: workload only support read|write|mix";
        exit(EXIT_FAILURE);
    }

    XferBenchStats stats;
    std::mutex mutex;
    int rc = runner.runInitiatorTasks([&](int thread_id) -> int {
        runner.pinThread(thread_id);
        auto max_block_size = XferBenchConfig::max_block_size;
        auto max_batch_size = XferBenchConfig::max_batch_size;
        auto local_gpu_offset = std::max(0, XferBenchConfig::local_gpu_id);
        auto target_gpu_offset = std::max(0, XferBenchConfig::target_gpu_id);
        uint64_t local_addr = runner.getLocalBufferBase(
            local_gpu_offset + thread_id, max_block_size, max_batch_size);
        uint64_t target_addr = runner.getTargetBufferBase(
            target_gpu_offset + thread_id, max_block_size, max_batch_size);

        XferBenchTimer timer;
        while (timer.lap_us(false) < 1000000ull) {
            runner.runSingleTransfer(local_addr, target_addr, block_size,
                                     batch_size, opcode);
        }
        timer.reset();
        std::vector<double> transfer_duration;
        if (mixed_opcode) {
            while (timer.lap_us(false) <
                   XferBenchConfig::duration * 1000000ull) {
                uint8_t pattern = 0;
                if (XferBenchConfig::check_consistency)
                    pattern =
                        fillData((void*)local_addr, block_size * batch_size);
                auto val = runner.runSingleTransfer(
                    local_addr, target_addr, block_size, batch_size, WRITE);
                transfer_duration.push_back(val);
                fillData((void*)local_addr, block_size * batch_size);
                val = runner.runSingleTransfer(local_addr, target_addr,
                                               block_size, batch_size, READ);
                if (XferBenchConfig::check_consistency)
                    verifyData((void*)local_addr, block_size * batch_size,
                               pattern);
                transfer_duration.push_back(val);
            }
        } else {
            while (timer.lap_us(false) <
                   XferBenchConfig::duration * 1000000ull) {
                auto val = runner.runSingleTransfer(
                    local_addr, target_addr, block_size, batch_size, opcode);
                transfer_duration.push_back(val);
            }
        }
        auto total_duration = timer.lap_us();
        mutex.lock();
        stats.total_duration.add(total_duration);
        for (auto val : transfer_duration) stats.transfer_duration.add(val);
        mutex.unlock();
        return 0;
    });

    if (rc != 0) return -1;
    printStats(block_size, batch_size, stats, num_threads);
    return 0;
}

/**
 * Process a single test configuration for all-to-all mode
 * Returns the statistics instead of printing them directly
 */
static XferBenchStats processBatchSizesAllToAll(BenchRunner& runner,
                                                size_t block_size,
                                                size_t batch_size,
                                                int num_threads) {
    bool mixed_opcode = false;
    OpCode opcode = READ;
    if (XferBenchConfig::check_consistency || XferBenchConfig::op_type == "mix")
        mixed_opcode = true;
    else if (XferBenchConfig::op_type == "read")
        opcode = READ;
    else if (XferBenchConfig::op_type == "write")
        opcode = WRITE;
    else {
        LOG(ERROR) << "Invalid args: workload only support read|write|mix";
        exit(EXIT_FAILURE);
    }

    XferBenchStats stats;
    std::mutex mutex;

    size_t num_targets = runner.getTargetCount();
    if (num_targets == 0) {
        LOG(WARNING) << "No targets connected, skipping test";
        return stats;
    }

    LOG(INFO) << "Running all-to-all test: " << num_threads << " threads, "
              << num_targets << " targets per thread";

    int rc = runner.runInitiatorTasks([&](int thread_id) -> int {
        runner.pinThread(thread_id);

        auto max_block_size = XferBenchConfig::max_block_size;
        auto max_batch_size = XferBenchConfig::max_batch_size;
        auto local_gpu_offset = std::max(0, XferBenchConfig::local_gpu_id);

        uint64_t local_addr = runner.getLocalBufferBase(
            local_gpu_offset + thread_id, max_block_size, max_batch_size);

        XferBenchTimer timer;

        // Warmup phase - transfer to each target once
        for (size_t target_idx = 0; target_idx < num_targets; ++target_idx) {
            runner.runTransferToTarget(local_addr, target_idx, block_size,
                                       batch_size, opcode);
        }

        timer.reset();
        std::vector<double> transfer_duration;
        uint64_t total_transfers = 0;

        if (mixed_opcode) {
            while (timer.lap_us(false) <
                   XferBenchConfig::duration * 1000000ull) {
                for (size_t target_idx = 0; target_idx < num_targets;
                     ++target_idx) {
                    uint8_t pattern = 0;
                    if (XferBenchConfig::check_consistency)
                        pattern = fillData((void*)local_addr,
                                           block_size * batch_size);

                    auto val = runner.runTransferToTarget(
                        local_addr, target_idx, block_size, batch_size, WRITE);
                    transfer_duration.push_back(val);
                    total_transfers++;

                    fillData((void*)local_addr, block_size * batch_size);
                    val = runner.runTransferToTarget(
                        local_addr, target_idx, block_size, batch_size, READ);
                    if (XferBenchConfig::check_consistency)
                        verifyData((void*)local_addr, block_size * batch_size,
                                   pattern);
                    transfer_duration.push_back(val);
                    total_transfers++;
                }
            }
        } else {
            while (timer.lap_us(false) <
                   XferBenchConfig::duration * 1000000ull) {
                // Round-robin through all targets for balanced all-to-all
                // traffic
                for (size_t target_idx = 0; target_idx < num_targets;
                     ++target_idx) {
                    auto val = runner.runTransferToTarget(
                        local_addr, target_idx, block_size, batch_size, opcode);
                    transfer_duration.push_back(val);
                    total_transfers++;
                }
            }
        }

        auto total_duration = timer.lap_us();
        mutex.lock();
        stats.total_duration.add(total_duration);
        for (auto val : transfer_duration) stats.transfer_duration.add(val);
        mutex.unlock();

        return 0;
    });

    if (rc != 0) {
        stats.transfer_duration.clear();
        stats.total_duration.clear();
    }

    return stats;
}

#ifdef USE_TENT
// Helper function to get current time in nanoseconds
static inline int64_t getCurrentTimeNs() {
    auto ret = std::chrono::steady_clock::now().time_since_epoch();
    return std::chrono::duration_cast<std::chrono::nanoseconds>(ret).count();
}

/**
 * Run all-to-all benchmark using coordinator mode
 * This uses a coordinator server for node discovery and result aggregation
 * Node rank is automatically assigned by the coordinator
 */
int runAllToAllBenchmarkWithCoordinator(BenchRunner& runner) {
    using namespace mooncake::bench;

    LOG(INFO) << "=== All-to-All with Coordinator ===";
    LOG(INFO) << "Coordinator: " << XferBenchConfig::coordinator;

    // Get local segment name (IP:Port in P2P mode)
    auto* tent_runner = dynamic_cast<TENTBenchRunner*>(&runner);
    if (!tent_runner) {
        LOG(ERROR) << "Coordinator mode requires TENT backend";
        return -1;
    }

    std::string my_segment_name = tent_runner->getSegmentName();
    std::string my_hostname = mooncake::getHostname();

    LOG(INFO) << "My segment name: " << my_segment_name;
    LOG(INFO) << "My hostname: " << my_hostname;

    // Connect to coordinator
    auto client =
        std::make_unique<CoordinatorClient>(XferBenchConfig::coordinator);
    if (!client->Connect()) {
        LOG(ERROR) << "Failed to connect to coordinator";
        return -1;
    }

    // Register with coordinator (will be assigned a rank)
    RegisterRequest reg_req;
    reg_req.rpc_address = my_segment_name;
    reg_req.hostname = my_hostname;

    auto reg_result = client->RegisterNode(reg_req);
    if (!reg_result.has_value()) {
        LOG(ERROR) << "Failed to register with coordinator";
        return -1;
    }

    // Get assigned rank and total nodes
    int32_t my_rank = reg_result->assigned_rank;
    int32_t total_nodes = reg_result->total_nodes;

    LOG(INFO) << "Registered as node " << my_rank << " of " << total_nodes;

    // Wait for all nodes with timeout
    int wait_timeout = 300;  // 5 minutes default wait timeout
    if (XferBenchConfig::wait_timeout > 0) {
        wait_timeout = XferBenchConfig::wait_timeout;
    }

    auto nodes_result = client->WaitForAll(my_rank, wait_timeout);
    if (!nodes_result.has_value()) {
        auto error = nodes_result.error();
        if (error == ErrorCode::TIMEOUT) {
            LOG(ERROR) << "Timeout waiting for all nodes (waited "
                       << wait_timeout << " seconds)";
            LOG(ERROR) << "This usually means:";
            LOG(ERROR) << "  1. Not all nodes have started - check that all "
                       << total_nodes << " nodes are running";
            LOG(ERROR) << "  2. Some nodes may have crashed - check their logs";
            LOG(ERROR) << "  3. Network connectivity issues - check firewall "
                          "and routing";
        } else {
            LOG(ERROR) << "Failed waiting for all nodes, error code: "
                       << static_cast<int>(error);
        }
        return -1;
    }

    auto& all_nodes = nodes_result.value();
    LOG(INFO) << "All nodes ready! Got " << all_nodes.size() << " nodes:";

    // Build target segments list (all except my own)
    std::vector<std::string> target_segments;
    for (const auto& node : all_nodes) {
        if (node.node_rank != my_rank) {
            target_segments.push_back(node.rpc_address);
            LOG(INFO) << "  - Node " << node.node_rank << ": "
                      << node.rpc_address << " (" << node.hostname << ")";
        }
    }

    if (target_segments.empty()) {
        LOG(WARNING) << "No target segments (single node test)";
        return 0;
    }

    // Connect to all targets (skip sync since coordinator already handled it)
    LOG(INFO) << "Connecting to " << target_segments.size() << " targets...";

    // Use connectToAllTargets with 0 timeout to skip sync
    int rc = runner.connectToAllTargets(target_segments, 0);
    if (rc != 0) {
        LOG(ERROR) << "Failed to connect to targets";
        return rc;
    }

    LOG(INFO) << "Connected to all targets, starting tests...";

    // Print header for local results
    std::cout << "\n\033[32m===== Local Node Results (Node " << my_rank << "/"
              << total_nodes << ") =====\033[0m" << std::endl;
    printStatsAllToAllHeader();

    // Run tests
    bool interrupted = false;

    for (int num_threads = XferBenchConfig::start_num_threads;
         !interrupted && num_threads <= XferBenchConfig::max_num_threads;
         num_threads *= 2) {
        runner.startInitiator(num_threads);

        for (size_t block_size = XferBenchConfig::start_block_size;
             !interrupted && block_size <= XferBenchConfig::max_block_size;
             block_size *= 2) {
            for (size_t batch_size = XferBenchConfig::start_batch_size;
                 !interrupted && batch_size <= XferBenchConfig::max_batch_size;
                 batch_size *= 2) {
                if (block_size * batch_size * num_threads >
                    XferBenchConfig::total_buffer_size) {
                    LOG(INFO) << "Skipped for block_size " << block_size
                              << " batch_size " << batch_size;
                } else {
                    // Run the test
                    auto stats = processBatchSizesAllToAll(
                        runner, block_size, batch_size, num_threads);

                    if (stats.transfer_duration.count() == 0) {
                        interrupted = true;
                        break;
                    }

                    // Report result to coordinator
                    ResultReport report;
                    report.node_rank = my_rank;
                    report.num_threads = num_threads;
                    report.block_size = block_size;
                    report.batch_size = batch_size;
                    report.total_samples = stats.transfer_duration.count();
                    report.total_duration_avg = stats.total_duration.avg();
                    report.transfer_duration_avg =
                        stats.transfer_duration.avg();
                    report.transfer_duration_min =
                        stats.transfer_duration.min();
                    report.transfer_duration_max =
                        stats.transfer_duration.max();
                    report.transfer_duration_p99 =
                        stats.transfer_duration.p99();
                    report.transfer_duration_p999 =
                        stats.transfer_duration.p999();

                    // Print local result immediately
                    printStatsAllToAll(my_rank, total_nodes,
                                       target_segments.size(), block_size,
                                       batch_size, stats, num_threads);

                    auto ack = client->ReportResult(report);
                    if (!ack.has_value() || !ack->success) {
                        LOG(WARNING) << "Failed to report result for config "
                                     << "(threads=" << num_threads
                                     << ", block=" << block_size
                                     << ", batch=" << batch_size << ")";
                    }
                }
            }
        }
        runner.stopInitiator();
    }

    LOG(INFO)
        << "All tests complete. Results will be displayed by coordinator.";
    LOG(INFO) << "Waiting for coordinator to exit...";

    // Wait for coordinator to exit by pinging periodically
    while (true) {
        std::this_thread::sleep_for(std::chrono::seconds(1));
        PingRequest req{my_rank};
        auto result = client->Ping(req);
        if (!result.has_value() || !result->alive) {
            LOG(INFO) << "Coordinator has exited, shutting down.";
            break;
        }
    }

    return 0;
}
#endif  // USE_TENT

int main(int argc, char* argv[]) {
    gflags::SetUsageMessage(
        "Mooncake Transfer Engine Benchmarking Tool\n"
        "Usage: ./tebench [options]\n\n"
        "Modes:\n"
        "  1. Single-target (default): Point-to-point transfer test\n"
        "     Start target: ./tebench --seg_name=my_segment\n"
        "     Start initiator: ./tebench --target_seg_name=my_segment\n\n"
        "  2. All-to-All: Multi-node full mesh test\n"
        "     First start coordinator: ./coordinator --num_nodes=4\n"
        "     Then on each node (all nodes use same command):\n"
        "     ./tebench --coordinator=IP:PORT\n\n"
        "  When --coordinator is set, all-to-all mode is automatically "
        "enabled.");

    gflags::ParseCommandLineFlags(&argc, &argv, true);
    XferBenchConfig::loadFromFlags();
    google::InitGoogleLogging(argv[0]);
    google::InstallFailureSignalHandler();

    std::unique_ptr<BenchRunner> runner;
    if (XferBenchConfig::backend == "classic") {
        runner = std::make_unique<TEBenchRunner>();
    } else {
#ifdef USE_TENT
        runner = std::make_unique<TENTBenchRunner>();
#else
        LOG(ERROR) << "Backend '" << XferBenchConfig::backend
                   << "' requires building with -DUSE_TENT=ON";
        return EXIT_FAILURE;
#endif
    }

    // Coordinator mode (implies all-to-all)
    if (!XferBenchConfig::coordinator.empty()) {
        if (XferBenchConfig::backend != "tent") {
            LOG(ERROR) << "Coordinator mode requires TENT backend (use "
                          "--backend=tent)";
            return EXIT_FAILURE;
        }

#ifdef USE_TENT
        return runAllToAllBenchmarkWithCoordinator(*runner);
#else
        LOG(ERROR) << "Coordinator mode requires building with -DUSE_TENT=ON";
        return EXIT_FAILURE;
#endif
    }

    // Single-target mode (default)
    if (XferBenchConfig::target_seg_name.empty()) {
        std::cout << "\033[33mTo start initiators, run " << std::endl
                  << "  ./tebench --target_seg_name="
                  << runner->getSegmentName()
                  << " --seg_type=" << XferBenchConfig::seg_type
                  << " --backend=" << XferBenchConfig::backend << std::endl
                  << "Press Ctrl-C to terminate\033[0m" << std::endl;
        return runner->runTarget();
    }

    printStatsHeader();
    bool interrupted = false;
    for (int num_threads = XferBenchConfig::start_num_threads;
         !interrupted && num_threads <= XferBenchConfig::max_num_threads;
         num_threads *= 2) {
        runner->startInitiator(num_threads);
        for (size_t block_size = XferBenchConfig::start_block_size;
             !interrupted && block_size <= XferBenchConfig::max_block_size;
             block_size *= 2) {
            for (size_t batch_size = XferBenchConfig::start_batch_size;
                 !interrupted && batch_size <= XferBenchConfig::max_batch_size;
                 batch_size *= 2) {
                if (block_size * batch_size * num_threads >
                    XferBenchConfig::total_buffer_size) {
                    LOG(INFO) << "Skipped for block_size " << block_size
                              << " batch_size " << batch_size;
                } else {
                    if (processBatchSizes(*runner, block_size, batch_size,
                                          num_threads) != 0)
                        interrupted = true;
                }
            }
        }
        runner->stopInitiator();
    }
    return 0;
}
