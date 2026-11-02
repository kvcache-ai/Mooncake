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

// runSingleTransfer() result for a transfer the engine dropped on purpose.
// Counted separately; never mixed into latency or bandwidth.
inline constexpr double kTransferDropped = -2.0;

struct TransferResult {
    double duration_us = -1.0;
    uint64_t transferred_bytes = 0;
    uint64_t completed = 0;
    uint64_t dropped = 0;
    // Populated only for mixed completed/canceled batches, for data checks.
    std::vector<size_t> completed_indices;
};

enum class IntentType : int;

class BenchRunner {
   public:
    BenchRunner() {}
    virtual ~BenchRunner() {}

    BenchRunner(const BenchRunner&) = delete;
    BenchRunner& operator=(const BenchRunner&) = delete;

    virtual double deadlineDropThreshold() const { return 0.0; }

    virtual TransferResult runTransfer(uint64_t local_addr, uint64_t target_id,
                                       uint64_t target_addr,
                                       uint64_t block_size, uint64_t batch_size,
                                       OpCode opcode, uint64_t deadline_ns,
                                       IntentType intent_type) {
        const double duration =
            runSingleTransfer(local_addr, target_id, target_addr, block_size,
                              batch_size, opcode, deadline_ns, intent_type);
        return {duration,
                duration >= 0 ? block_size * batch_size : 0,
                duration >= 0 ? batch_size : 0,
                0,
                {}};
    }

    virtual void pinThread(int thread_id) = 0;

    virtual int runTarget() = 0;

    virtual int startInitiator(int num_threads) = 0;

    virtual int stopInitiator() = 0;

    virtual int runInitiatorTasks(
        const std::function<int(int /* thread_id */)>& func) = 0;

    virtual std::string getSegmentName() const = 0;

    virtual uint64_t getLocalBufferBase(int thread_id, uint64_t block_size,
                                        uint64_t batch_size) const = 0;

    virtual size_t getTargetCount() const = 0;

    virtual size_t getTargetIndex(int thread_id) const = 0;

    virtual uint64_t getTargetSegmentId(int thread_id) const = 0;

    virtual uint64_t getTargetBufferBase(int thread_id, uint64_t block_size,
                                         uint64_t batch_size) const = 0;

    // Returns elapsed microseconds on success, kTransferDropped if the
    // admission queue dropped the transfer as deadline-infeasible
    // (--mlu_local_threshold), or another negative value if the transfer
    // failed. Implementations must not call exit() from worker threads.
    virtual double runSingleTransfer(uint64_t local_addr, uint64_t target_id,
                                     uint64_t target_addr, uint64_t block_size,
                                     uint64_t batch_size, OpCode opcode,
                                     uint64_t deadline_ns,
                                     IntentType intent_type) = 0;
};

}  // namespace tent
}  // namespace mooncake

#endif  // BENCH_RUNNER_H
