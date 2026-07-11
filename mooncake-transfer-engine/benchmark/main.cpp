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
#include "te_backend.h"
#ifdef USE_TENT
#include "tent_backend.h"
#endif

using namespace mooncake::tent;

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
        if (XferBenchConfig::receiver_credit_mode == "disabled") {
            while (timer.lap_us(false) < 1000000ull) {
                runner.runSingleTransfer(local_addr, target_addr, block_size,
                                         batch_size, opcode);
            }
        }
        timer.reset();
        std::vector<double> transfer_duration;
        if (mixed_opcode) {
            const auto runConsistencyPair = [&]() {
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
            };
            if (XferBenchConfig::receiver_credit_mode != "disabled" &&
                XferBenchConfig::receiver_credit_operations > 0) {
                for (uint64_t operation = 0;
                     operation < XferBenchConfig::receiver_credit_operations;
                     operation += 2) {
                    runConsistencyPair();
                }
            } else {
                while (timer.lap_us(false) <
                       XferBenchConfig::duration * 1000000ull) {
                    runConsistencyPair();
                }
            }
        } else {
            const auto runOperation = [&]() {
                auto val = runner.runSingleTransfer(local_addr, target_addr,
                                                    block_size, batch_size,
                                                    opcode);
                transfer_duration.push_back(val);
            };
            if (XferBenchConfig::receiver_credit_mode != "disabled" &&
                XferBenchConfig::receiver_credit_operations > 0) {
                for (uint64_t operation = 0;
                     operation < XferBenchConfig::receiver_credit_operations;
                     ++operation) {
                    runOperation();
                }
            } else {
                while (timer.lap_us(false) <
                       XferBenchConfig::duration * 1000000ull) {
                    runOperation();
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

    if (rc != 0) return -1;
    printStats(block_size, batch_size, stats, num_threads);
    return 0;
}

int main(int argc, char* argv[]) {
    gflags::SetUsageMessage(
        "Mooncake Transfer Engine Benchmarking Tool\n"
        "Usage: ./tebench [options]");
    gflags::ParseCommandLineFlags(&argc, &argv, true);
    XferBenchConfig::loadFromFlags();
    const auto& receiver_credit_mode = XferBenchConfig::receiver_credit_mode;
    if (receiver_credit_mode != "disabled" && receiver_credit_mode != "fixed" &&
        receiver_credit_mode != "credit") {
        LOG(ERROR) << "receiver_credit_mode must be disabled, fixed, or credit";
        return EXIT_FAILURE;
    }
    if (receiver_credit_mode != "disabled") {
        if (XferBenchConfig::backend != "tent" ||
            XferBenchConfig::xport_type != "rdma") {
            LOG(ERROR) << "receiver-credit experiments require tent RDMA";
            return EXIT_FAILURE;
        }
        if (XferBenchConfig::receiver_capacity_bytes == 0 ||
            XferBenchConfig::receiver_capacity_slots == 0) {
            LOG(ERROR) << "receiver capacity bytes and slots must be positive";
            return EXIT_FAILURE;
        }
        if (XferBenchConfig::receiver_credit_grant_batch == 0) {
            LOG(ERROR) << "receiver_credit_grant_batch must be positive";
            return EXIT_FAILURE;
        }
        if (!XferBenchConfig::target_seg_name.empty() &&
            receiver_credit_mode == "credit" &&
            XferBenchConfig::receiver_credit_operations == 0) {
            LOG(ERROR) << "credit mode requires receiver_credit_operations";
            return EXIT_FAILURE;
        }
        if (!XferBenchConfig::target_seg_name.empty() &&
            receiver_credit_mode == "credit" &&
            XferBenchConfig::receiver_credit_operations %
                    XferBenchConfig::receiver_credit_grant_batch !=
                0) {
            LOG(ERROR) << "receiver_credit_operations must be divisible by "
                          "receiver_credit_grant_batch";
            return EXIT_FAILURE;
        }
        if (!XferBenchConfig::target_seg_name.empty() &&
            (XferBenchConfig::start_block_size !=
                 XferBenchConfig::max_block_size ||
             XferBenchConfig::start_batch_size !=
                 XferBenchConfig::max_batch_size)) {
            LOG(ERROR) << "receiver-credit runs require one block and batch "
                          "size";
            return EXIT_FAILURE;
        }
        if (!XferBenchConfig::target_seg_name.empty() &&
            (XferBenchConfig::start_num_threads != 1 ||
             XferBenchConfig::max_num_threads != 1)) {
            LOG(ERROR)
                << "the benchmark-only receiver-credit protocol requires "
                   "one worker per sender process";
            return EXIT_FAILURE;
        }
        if (!XferBenchConfig::target_seg_name.empty() &&
            XferBenchConfig::op_type != "write" &&
            XferBenchConfig::op_type != "mix") {
            LOG(ERROR) << "receiver-credit capacity runs require "
                          "--op_type=write or mix";
            return EXIT_FAILURE;
        }
        if (!XferBenchConfig::target_seg_name.empty() &&
            XferBenchConfig::op_type == "mix" &&
            XferBenchConfig::receiver_credit_operations % 2 != 0) {
            LOG(ERROR) << "receiver_credit_operations must be even for mix";
            return EXIT_FAILURE;
        }
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
                                          num_threads) != 0)
                        interrupted = true;
                }
            }
        }
        runner->stopInitiator();
    }
    return 0;
}
