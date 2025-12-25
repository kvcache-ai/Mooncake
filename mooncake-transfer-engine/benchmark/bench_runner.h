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

#ifndef BENCH_RUNNER_H
#define BENCH_RUNNER_H

#include "utils.h"

#include <string>
#include <functional>
#include <vector>
#include <fcntl.h>
#include <gflags/gflags.h>
#include <glog/logging.h>
#include <signal.h>
#include <sys/time.h>

namespace mooncake {
namespace tent {
class BenchRunner {
   public:
    BenchRunner() {}
    virtual ~BenchRunner() {}

    BenchRunner(const BenchRunner&) = delete;
    BenchRunner& operator=(const BenchRunner&) = delete;

    virtual void pinThread(int thread_id) = 0;

    virtual int runTarget() = 0;

    virtual int startInitiator(int num_threads) = 0;

    virtual int stopInitiator() = 0;

    virtual int runInitiatorTasks(
        const std::function<int(int /* thread_id */)>& func) = 0;

    virtual std::string getSegmentName() const = 0;

    virtual uint64_t getLocalBufferBase(int thread_id, uint64_t block_size,
                                        uint64_t batch_size) const = 0;

    virtual uint64_t getTargetBufferBase(int thread_id, uint64_t block_size,
                                         uint64_t batch_size) const = 0;

    virtual double runSingleTransfer(uint64_t local_addr, uint64_t target_addr,
                                     uint64_t block_size, uint64_t batch_size,
                                     OpCode opcode) = 0;
};

}  // namespace tent
}  // namespace mooncake

#endif  // BENCH_RUNNER_H