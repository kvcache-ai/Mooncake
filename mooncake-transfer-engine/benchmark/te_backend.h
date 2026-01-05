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

#ifndef TEV0_BACKEND_H
#define TEV0_BACKEND_H

#include "bench_runner.h"
#include "utils.h"

#include <string>
#include <vector>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <functional>
#include <fcntl.h>
#include <gflags/gflags.h>
#include <glog/logging.h>
#include <signal.h>
#include <sys/time.h>

#include "transfer_engine.h"

namespace mooncake {
namespace tent {
class TEBenchRunner : public BenchRunner {
   public:
    TEBenchRunner();
    ~TEBenchRunner();

    TEBenchRunner(const TEBenchRunner&) = delete;
    TEBenchRunner& operator=(const TEBenchRunner&) = delete;

    void pinThread(int thread_id);

    int runTarget();

    int startInitiator(int num_threads);

    int stopInitiator();

    int runInitiatorTasks(const std::function<int(int /* thread_id */)>& func);

    std::string getSegmentName() const { return engine_->getLocalIpAndPort(); }

    uint64_t getLocalBufferBase(int thread_id, uint64_t block_size,
                                uint64_t batch_size) const {
        const size_t num_buffers = pinned_buffer_list_.size();
        return (uint64_t)pinned_buffer_list_[thread_id % num_buffers] +
               block_size * batch_size * (thread_id / num_buffers);
    }

    uint64_t getTargetBufferBase(int thread_id, uint64_t block_size,
                                 uint64_t batch_size) const {
        return info_->buffers[thread_id % info_->buffers.size()].addr +
               block_size * batch_size * (thread_id / info_->buffers.size());
    }

    double runSingleTransfer(uint64_t local_addr, uint64_t target_addr,
                             uint64_t block_size, uint64_t batch_size,
                             OpCode opcode);

   private:
    int allocateBuffers();

    int freeBuffers();

    int runner(int thread_id);

   private:
    std::unique_ptr<mooncake::TransferEngine> engine_;
    std::vector<void*> pinned_buffer_list_;
    SegmentID handle_;
    std::shared_ptr<TransferMetadata::SegmentDesc> info_;

    std::vector<std::function<int(int)>> current_task_;
    std::vector<std::thread> threads_;
    std::mutex mtx_;
    std::condition_variable cv_task_;
    std::condition_variable cv_done_;
    int pending_ = 0;
};

}  // namespace tent
}  // namespace mooncake

#endif  // TEV0_BACKEND_H