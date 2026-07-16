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

#include "bench_runner.h"
#include "qos_metrics_adapter.h"
#include "te_backend.h"
#ifdef USE_TENT
#include "tent_backend.h"
#endif

using namespace mooncake::tent;

int processBatchSizes(BenchRunner& runner, size_t block_size, size_t batch_size,
                      int num_threads,
                      const std::vector<QosClassConfig>& qos_classes) {
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
    std::vector<XferBenchStats> qos_stats(qos_classes.size());
    XferBenchStats tight_stats;
    XferBenchStats loose_stats;
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
        const bool qos_enabled = !qos_classes.empty();
        const size_t qos_class =
            qos_enabled ? qosClassForThread(qos_classes, thread_id) : 0;
        const bool tight = XferBenchConfig::deadline_us > 0 &&
                           thread_id < XferBenchConfig::deadline_tight_threads;
        auto deadlineNs = [&]() -> uint64_t {
            if (!tight) return 0;
            const auto now =
                std::chrono::steady_clock::now().time_since_epoch();
            return std::chrono::duration_cast<std::chrono::nanoseconds>(now)
                       .count() +
                   XferBenchConfig::deadline_us * 1000ull;
        };

        XferBenchTimer timer;
        while (timer.lap_us(false) < 1000000ull) {
            runner.runSingleTransfer(local_addr, target_addr, block_size,
                                     batch_size, opcode, deadlineNs());
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
                auto val = runner.runSingleTransfer(local_addr, target_addr,
                                                    block_size, batch_size,
                                                    WRITE, deadlineNs());
                transfer_duration.push_back(val);
                fillData((void*)local_addr, block_size * batch_size);
                val = runner.runSingleTransfer(local_addr, target_addr,
                                               block_size, batch_size, READ,
                                               deadlineNs());
                if (XferBenchConfig::check_consistency)
                    verifyData((void*)local_addr, block_size * batch_size,
                               pattern);
                transfer_duration.push_back(val);
            }
        } else {
            while (timer.lap_us(false) <
                   XferBenchConfig::duration * 1000000ull) {
                auto val = runner.runSingleTransfer(local_addr, target_addr,
                                                    block_size, batch_size,
                                                    opcode, deadlineNs());
                transfer_duration.push_back(val);
            }
        }
        auto total_duration = timer.lap_us();
        std::lock_guard<std::mutex> lock(mutex);
        stats.total_duration.add(total_duration);
        stats.transfer_duration.add(transfer_duration);
        if (qos_enabled) {
            qos_stats[qos_class].total_duration.add(total_duration);
            qos_stats[qos_class].transfer_duration.add(transfer_duration);
        }
        auto& group_stats = tight ? tight_stats : loose_stats;
        group_stats.total_duration.add(total_duration);
        group_stats.transfer_duration.add(transfer_duration);
        return 0;
    });

    if (rc != 0) return -1;
    printStats(block_size, batch_size, stats, num_threads);
    if (!qos_classes.empty()) {
        auto report = calculateQosMetricsFromBenchStats(
            block_size, batch_size, num_threads, qos_classes, &qos_stats,
            XferBenchConfig::qos_link_capacity_gbps);
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
    if (XferBenchConfig::deadline_us > 0) {
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
    std::vector<QosClassConfig> qos_classes;
    if (!XferBenchConfig::qos_classes.empty() &&
        !XferBenchConfig::qos_classes_json.empty()) {
        LOG(ERROR) << "Use only one of --qos_classes or --qos_classes_json";
        return EXIT_FAILURE;
    }
    if (!XferBenchConfig::qos_classes_json.empty()) {
        std::string error;
        if (!parseQosClassesJson(XferBenchConfig::qos_classes_json,
                                 &qos_classes, &error)) {
            LOG(ERROR) << "Invalid --qos_classes_json: " << error;
            return EXIT_FAILURE;
        }
    } else if (!XferBenchConfig::qos_classes.empty()) {
        std::string error;
        if (!parseQosClasses(XferBenchConfig::qos_classes, &qos_classes,
                             &error)) {
            LOG(ERROR) << "Invalid --qos_classes: " << error;
            return EXIT_FAILURE;
        }
    }
    if (!qos_classes.empty()) {
        std::string error;
        if (XferBenchConfig::start_num_threads !=
            XferBenchConfig::max_num_threads) {
            LOG(ERROR)
                << "QoS metrics require start_num_threads == max_num_threads";
            return EXIT_FAILURE;
        }
        if (!validateQosClasses(qos_classes, XferBenchConfig::start_num_threads,
                                &error)) {
            LOG(ERROR) << "Invalid QoS classes: " << error;
            return EXIT_FAILURE;
        }
    }
    if (XferBenchConfig::qos_link_capacity_gbps < 0.0 ||
        !std::isfinite(XferBenchConfig::qos_link_capacity_gbps)) {
        LOG(ERROR) << "qos_link_capacity_gbps must be finite and non-negative";
        return EXIT_FAILURE;
    }
    if (XferBenchConfig::deadline_tight_threads < 0 ||
        XferBenchConfig::deadline_tight_threads >
            XferBenchConfig::max_num_threads) {
        LOG(ERROR) << "deadline_tight_threads must be in [0, max_num_threads]";
        return EXIT_FAILURE;
    }
    if (XferBenchConfig::deadline_us > 0 &&
        XferBenchConfig::backend != "tent") {
        LOG(ERROR) << "deadline tagging is supported only by the tent backend";
        return EXIT_FAILURE;
    }
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
                if (block_size * batch_size * num_threads >
                    XferBenchConfig::total_buffer_size) {
                    LOG(INFO) << "Skipped for block_size " << block_size
                              << " batch_size " << batch_size;
                } else {
                    if (processBatchSizes(*runner, block_size, batch_size,
                                          num_threads, qos_classes) != 0)
                        interrupted = true;
                }
            }
        }
        runner->stopInitiator();
    }
    return 0;
}
