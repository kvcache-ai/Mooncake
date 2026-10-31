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
#include "split_output.h"
#include "common.h"
#include "bench_config.h"

#include "bench_runner.h"
#include "qos_metrics_adapter.h"
#include "target_metrics.h"
#include "te_backend.h"
#include "workload_config.h"
#ifdef USE_TENT
#include "tent_backend.h"
#endif

#include <atomic>
#include <mutex>
#include <random>
#include <thread>

using namespace mooncake::tent;

uint64_t steadyClockNs() {
    const auto now = std::chrono::steady_clock::now().time_since_epoch();
    return std::chrono::duration_cast<std::chrono::nanoseconds>(now).count();
}

double gbPerSecond(uint64_t bytes, double duration_us) {
    if (duration_us <= 0.0) return 0.0;
    return static_cast<double>(bytes) / (1000.0 * duration_us);
}

int processBatchSizes(
    BenchRunner& runner, size_t block_size, size_t batch_size, int num_threads,
    const OpMode& op_mode, const std::vector<QosClassConfig>& qos_classes,
    const std::vector<WorkloadClassConfig>& workload_classes = {}) {
    const bool mixed_opcode = op_mode.mixed_opcode;
    const bool write_seed = op_mode.write_seed;
    const bool read_verify = op_mode.read_verify;
    const OpCode opcode = op_mode.opcode;
    const bool random_access = op_mode.random_access;

    XferBenchStats stats;
    std::vector<XferBenchStats> qos_stats(qos_classes.size());
    std::vector<TargetBenchStats> target_stats(runner.getTargetCount());
    const auto target_names =
        splitCommaSeparated(XferBenchConfig::target_seg_name);
    LOG_ASSERT(target_names.size() == target_stats.size());
    for (size_t i = 0; i < target_stats.size(); ++i)
        target_stats[i].segment_name = target_names[i];
    XferBenchStats tight_stats;
    XferBenchStats loose_stats;
    std::mutex mutex;
    std::atomic<int> measurement_ready{0};
    std::atomic<bool> measurement_started{false};
    auto paceRequest = [&]() {
        if (XferBenchConfig::request_interval_us == 0) return;
        const uint64_t interval_ns =
            XferBenchConfig::request_interval_us * 1000ull;
        const uint64_t target_ns = steadyClockNs() + interval_ns;
        while (steadyClockNs() < target_ns) {
            std::this_thread::yield();
        }
    };
    size_t address_stride_bytes =
        XferBenchConfig::max_block_size * XferBenchConfig::max_batch_size;
    if (!workload_classes.empty()) {
        address_stride_bytes = 0;
        for (const auto& config : workload_classes) {
            address_stride_bytes =
                std::max(address_stride_bytes,
                         checkedMul(config.block_size, config.batch_size,
                                    "workload address stride"));
        }
    }
    const size_t target_window_bytes = XferBenchConfig::target_window_size != 0
                                           ? XferBenchConfig::target_window_size
                                           : address_stride_bytes;
    if (target_window_bytes < address_stride_bytes) {
        LOG(ERROR) << "--target_window_size (" << target_window_bytes
                   << ") must be at least the largest operation size, "
                      "max_block_size x max_batch_size ("
                   << address_stride_bytes << ")";
        exit(EXIT_FAILURE);
    }
    int rc = runner.runInitiatorTasks([&](int thread_id) -> int {
        runner.pinThread(thread_id);
        auto local_gpu_offset = std::max(0, XferBenchConfig::local_gpu_id);
        auto target_gpu_offset = std::max(0, XferBenchConfig::target_gpu_id);
        const int target_thread_id =
            static_cast<int>(target_gpu_offset * runner.getTargetCount()) +
            thread_id;
        uint64_t local_addr = runner.getLocalBufferBase(
            local_gpu_offset + thread_id, address_stride_bytes, 1);
        uint64_t target_addr = runner.getTargetBufferBase(
            target_thread_id, target_window_bytes, 1);
        uint64_t target_id = runner.getTargetSegmentId(target_thread_id);
        const size_t target_index = runner.getTargetIndex(target_thread_id);
        const bool qos_enabled = !qos_classes.empty();
        const size_t qos_class =
            qos_enabled ? qosClassForThread(qos_classes, thread_id) : 0;
        const auto* workload =
            workload_classes.empty() ? nullptr : &workload_classes[qos_class];
        const size_t thread_block_size =
            workload ? workload->block_size : block_size;
        const size_t thread_batch_size =
            workload ? workload->batch_size : batch_size;
        const IntentType intent_type =
            workload ? workload->intent_type : IntentType::INTENT_UNSPEC;
        const uint64_t op_bytes = checkedMul(
            thread_block_size, thread_batch_size, "operation payload size");
        const uint64_t target_slots = target_window_bytes / op_bytes;
        uint64_t prev_slot = target_slots;  // none yet
        std::mt19937_64 rng(thread_id + 1);
        // A window holding a single operation stays at a fixed offset.
        auto nextTargetAddr = [&]() -> uint64_t {
            if (!random_access || target_slots < 2) return target_addr;
            prev_slot = pickTargetSlot(rng, target_slots, prev_slot);
            return target_addr + prev_slot * op_bytes;
        };
        const bool tight = XferBenchConfig::deadline_us > 0 &&
                           thread_id < XferBenchConfig::deadline_tight_threads;
        auto deadlineNs = [&]() -> uint64_t {
            const uint64_t deadline_us =
                workload ? workload->deadline_us
                         : (tight ? XferBenchConfig::deadline_us : 0);
            if (deadline_us == 0) return 0;
            const auto now =
                std::chrono::steady_clock::now().time_since_epoch();
            return std::chrono::duration_cast<std::chrono::nanoseconds>(now)
                       .count() +
                   deadline_us * 1000ull;
        };

        auto failTask = [&]() {
            measurement_started.store(true, std::memory_order_release);
            return -1;
        };

        XferBenchTimer timer;
        while (timer.lap_us(false) < 1000000ull) {
            if (runner.runSingleTransfer(
                    local_addr, target_id, nextTargetAddr(), thread_block_size,
                    thread_batch_size, opcode, deadlineNs(), intent_type) < 0) {
                return failTask();
            }
        }
        if (measurement_ready.fetch_add(1, std::memory_order_acq_rel) + 1 ==
            num_threads) {
            measurement_started.store(true, std::memory_order_release);
        } else {
            while (!measurement_started.load(std::memory_order_acquire)) {
                std::this_thread::yield();
            }
        }
        timer.reset();
        std::vector<double> transfer_duration;
        std::vector<double> thread_instant_bandwidth;
        if (mixed_opcode) {
            while (timer.lap_us(false) <
                   XferBenchConfig::duration * 1000000ull) {
                const size_t bytes =
                    checkedMul(thread_block_size, thread_batch_size,
                               "consistency payload size");
                const uint64_t batch_bytes = bytes;
                uint8_t pattern = 0;
                if (XferBenchConfig::check_consistency)
                    pattern = fillData((void*)local_addr, bytes);
                // Read back from where this iteration wrote.
                const uint64_t op_target_addr = nextTargetAddr();
                paceRequest();
                auto val = runner.runSingleTransfer(
                    local_addr, target_id, op_target_addr, thread_block_size,
                    thread_batch_size, WRITE, deadlineNs(), intent_type);
                if (val < 0) return failTask();
                thread_instant_bandwidth.push_back(
                    gbPerSecond(batch_bytes, val));
                transfer_duration.push_back(val);
                fillData((void*)local_addr, bytes);
                paceRequest();
                val = runner.runSingleTransfer(
                    local_addr, target_id, op_target_addr, thread_block_size,
                    thread_batch_size, READ, deadlineNs(), intent_type);
                if (val < 0) return failTask();
                thread_instant_bandwidth.push_back(
                    gbPerSecond(batch_bytes, val));
                if (XferBenchConfig::check_consistency)
                    verifyData((void*)local_addr,
                               checkedMul(thread_block_size, thread_batch_size,
                                          "consistency payload size"),
                               pattern);
                transfer_duration.push_back(val);
            }
        } else {
            while (timer.lap_us(false) <
                   XferBenchConfig::duration * 1000000ull) {
                const size_t bytes =
                    checkedMul(thread_block_size, thread_batch_size,
                               "operation payload size");
                const uint64_t batch_bytes = bytes;
                if (write_seed) {
                    fillData((void*)local_addr, bytes,
                             stableDataSeed(target_addr));
                }
                paceRequest();
                auto val = runner.runSingleTransfer(
                    local_addr, target_id, nextTargetAddr(), thread_block_size,
                    thread_batch_size, opcode, deadlineNs(), intent_type);
                if (val < 0) return failTask();
                thread_instant_bandwidth.push_back(
                    gbPerSecond(batch_bytes, val));
                if (read_verify) {
                    verifyData((void*)local_addr, bytes,
                               stableDataSeed(target_addr));
                }
                transfer_duration.push_back(val);
            }
        }
        auto total_duration = timer.lap_us();
        const uint64_t bytes_per_operation = checkedMul(
            thread_block_size, thread_batch_size, "operation payload size");
        const uint64_t transferred_bytes =
            checkedMul(bytes_per_operation, transfer_duration.size(),
                       "thread transferred bytes");
        std::lock_guard<std::mutex> lock(mutex);
        stats.total_duration.add(total_duration);
        stats.transfer_duration.add(transfer_duration);
        stats.instant_bandwidth.add(thread_instant_bandwidth);
        auto& target = target_stats[target_index];
        ++target.threads;
        target.transferred_bytes =
            checkedAdd(target.transferred_bytes, transferred_bytes,
                       "target transferred bytes");
        target.stats.total_duration.add(total_duration);
        target.stats.transfer_duration.add(transfer_duration);
        target.stats.instant_bandwidth.add(thread_instant_bandwidth);
        if (qos_enabled) {
            qos_stats[qos_class].total_duration.add(total_duration);
            qos_stats[qos_class].transfer_duration.add(transfer_duration);
            qos_stats[qos_class].instant_bandwidth.add(
                thread_instant_bandwidth);
        }
        auto& group_stats = tight ? tight_stats : loose_stats;
        group_stats.total_duration.add(total_duration);
        group_stats.transfer_duration.add(transfer_duration);
        return 0;
    });

    if (rc != 0) return -1;
    if (workload_classes.empty())
        printStats(block_size, batch_size, stats, num_threads);
    auto target_report = calculateTargetMetrics(
        block_size, batch_size, num_threads, XferBenchConfig::backend,
        mixed_opcode ? "mix" : XferBenchConfig::op_type, &target_stats);
    if (runner.getTargetCount() > 1) printTargetMetrics(target_report);
    if (!XferBenchConfig::result_output_jsonl.empty()) {
        std::string error;
        if (!appendTargetMetricsJsonl(XferBenchConfig::result_output_jsonl,
                                      target_report, &error)) {
            LOG(ERROR) << error;
            return -1;
        }
    }
    if (!qos_classes.empty()) {
        std::vector<uint64_t> bytes_per_operation;
        for (const auto& config : workload_classes) {
            bytes_per_operation.push_back(config.block_size *
                                          config.batch_size);
        }
        auto report = calculateQosMetricsFromBenchStats(
            block_size, batch_size, num_threads, qos_classes, &qos_stats,
            XferBenchConfig::qos_link_capacity_gbps, bytes_per_operation);
        printQosMetrics(report);
        if (!XferBenchConfig::qos_output_jsonl.empty()) {
            std::string error;
            if (!appendQosMetricsJsonl(XferBenchConfig::qos_output_jsonl,
                                       report, &error)) {
                LOG(ERROR) << error;
                return -1;
            }
        }
    }
    if (XferBenchConfig::deadline_us > 0 && workload_classes.empty()) {
        const int tight_threads =
            std::min(num_threads, XferBenchConfig::deadline_tight_threads);
        printDeadlineGroupStats("tight", block_size, batch_size, tight_stats,
                                tight_threads, XferBenchConfig::deadline_us);
        printDeadlineGroupStats("loose", block_size, batch_size, loose_stats,
                                num_threads - tight_threads, 0);
    }
    return 0;
}

int main(int argc, char* argv[]) {
    gflags::SetUsageMessage(
        "Mooncake Transfer Engine Benchmarking Tool\n"
        "Usage: ./tebench [options]");
    gflags::ParseCommandLineFlags(&argc, &argv, true);
    XferBenchConfig::loadFromFlags();
    if (!XferBenchConfig::split_output_jsonl.empty()) {
        std::string error;
        if (!openSplitOutput(XferBenchConfig::split_output_jsonl, &error)) {
            LOG(ERROR) << error;
            return EXIT_FAILURE;
        }
    }
    BenchConfig config;
    std::string config_error;
    if (!validateConfig(&config, &config_error)) {
        LOG(ERROR) << config_error;
        return EXIT_FAILURE;
    }
    const auto& qos_classes = config.qos_classes;
    const auto& workload_classes = config.workload_classes;
    std::unique_ptr<BenchRunner> runner;
    if (XferBenchConfig::backend == "classic") {
        runner = std::make_unique<TEBenchRunner>();
    } else {
#ifdef USE_TENT
        runner = std::make_unique<TENTBenchRunner>();
#else
        LOG(ERROR) << "Backend '" << XferBenchConfig::backend
                   << "' requires building with -DUSE_TENT=ON; only the "
                      "'classic' backend is available in this build";
        return EXIT_FAILURE;
#endif
    }
    if (XferBenchConfig::target_seg_name.empty()) {
        std::cout << "\033[33mTo start initiators, run " << std::endl
                  << "  ./tebench --target_seg_name="
                  << runner->getSegmentName()
                  << " --seg_type=" << XferBenchConfig::seg_type
                  << " --backend=" << XferBenchConfig::backend << std::endl
                  << "Press Ctrl-C to terminate\033[0m" << std::endl;
        return runner->runTarget();
    }
    if (!workload_classes.empty()) {
        const int num_threads = XferBenchConfig::start_num_threads;
        if (runner->startInitiator(num_threads) != 0) return EXIT_FAILURE;
        const int rc =
            processBatchSizes(*runner, 0, 0, num_threads, config.op_mode,
                              qos_classes, workload_classes);
        runner->stopInitiator();
        return rc == 0 ? EXIT_SUCCESS : EXIT_FAILURE;
    }
    printStatsHeader();
    bool interrupted = false;
    for (int num_threads = XferBenchConfig::start_num_threads;
         !interrupted && num_threads <= XferBenchConfig::max_num_threads;
         num_threads *= 2) {
        if (runner->startInitiator(num_threads) != 0) {
            interrupted = true;
            break;
        }
        for (size_t block_size = XferBenchConfig::start_block_size;
             !interrupted && block_size <= XferBenchConfig::max_block_size;
             block_size *= 2) {
            for (size_t batch_size = XferBenchConfig::start_batch_size;
                 !interrupted && batch_size <= XferBenchConfig::max_batch_size;
                 batch_size *= 2) {
                const size_t bytes_per_thread = checkedMul(
                    block_size, batch_size, "benchmark operation size");
                if (XferBenchConfig::target_range_size != 0 &&
                    bytes_per_thread > XferBenchConfig::target_range_size) {
                    LOG(ERROR) << "target range is smaller than one operation";
                    interrupted = true;
                } else if (num_threads != 0 &&
                           bytes_per_thread >
                               XferBenchConfig::total_buffer_size /
                                   num_threads) {
                    LOG(INFO) << "Skipped for block_size " << block_size
                              << " batch_size " << batch_size;
                } else {
                    if (processBatchSizes(*runner, block_size, batch_size,
                                          num_threads, config.op_mode,
                                          qos_classes) != 0)
                        interrupted = true;
                }
            }
        }
        runner->stopInitiator();
    }
    return interrupted ? EXIT_FAILURE : EXIT_SUCCESS;
}
