// Copyright 2024 KVCache.AI
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
#include "tev0_backend.h"
#include "tev1_backend.h"

using namespace mooncake::v1;

void processBatchSizes(BenchRunner &runner, size_t block_size,
                       size_t batch_size) {
    bool mixed_opcode = false;
    OpCode opcode;
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
    runner.runInitiatorTasks([&](int thread_id) -> int {
        runner.pinThread(thread_id);
        uint64_t local_addr =
            runner.getLocalBufferBase(thread_id, block_size, batch_size);
        uint64_t target_addr =
            runner.getTargetBufferBase(thread_id, block_size, batch_size);

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
                        fillData((void *)local_addr, block_size * batch_size);
                auto val = runner.runSingleTransfer(local_addr, target_addr,
                                                    block_size, batch_size,
                                                    WRITE);
                transfer_duration.push_back(val);
                val = runner.runSingleTransfer(local_addr, target_addr,
                                               block_size, batch_size,
                                               READ);
                if (XferBenchConfig::check_consistency)
                    verifyData((void *)local_addr, block_size * batch_size,
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

    printStats(block_size, batch_size, stats);
}

int main(int argc, char *argv[]) {
    gflags::SetUsageMessage(
        "Transfer Engine Benchmarking Tool\nUsage: ./tebench [options]");
    gflags::ParseCommandLineFlags(&argc, &argv, true);
    XferBenchConfig::loadFromFlags();
    std::unique_ptr<BenchRunner> runner;
    if (XferBenchConfig::backend == "v0")
        runner = std::make_unique<TEv0BenchRunner>();
    else
        runner = std::make_unique<TEv1BenchRunner>();
    if (XferBenchConfig::target_seg_name.empty()) {
        std::cout << "\033[33mTo start initiators, run " << std::endl
                  << "  ./tebench --target_seg_name="
                  << runner->getSegmentName() << std::endl
                  << "Press Ctrl-C to terminate\033[0m" << std::endl;
        return runner->runTarget();
    }
    runner->startInitiator();
    printStatsHeader();
    for (size_t block_size = XferBenchConfig::start_block_size;
         block_size <= XferBenchConfig::max_block_size; block_size *= 2) {
        for (size_t batch_size = XferBenchConfig::start_batch_size;
             batch_size <= XferBenchConfig::max_batch_size; batch_size *= 2) {
            if (block_size * batch_size * XferBenchConfig::num_threads >
                XferBenchConfig::total_buffer_size) {
                LOG(INFO) << "Skipped for block_size " << block_size
                          << " batch_size " << batch_size;
            } else {
                processBatchSizes(*runner, block_size, batch_size);
            }
        }
    }

    runner->stopInitiator();
    return 0;
}
