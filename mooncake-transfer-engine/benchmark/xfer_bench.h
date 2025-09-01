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

#ifndef XFER_BENCH_H
#define XFER_BENCH_H

#include "utils.h"

#include <string>
#include <bits/stdc++.h>
#include <fcntl.h>
#include <gflags/gflags.h>
#include <glog/logging.h>
#include <signal.h>
#include <sys/time.h>

#include "v1/transfer_engine.h"
#include "v1/utility/random.h"
#include "v1/utility/system.h"

namespace mooncake {
namespace v1 {
class XferTERunner {
   public:
    XferTERunner();
    ~XferTERunner();

    XferTERunner(const XferTERunner &) = delete;
    XferTERunner &operator=(const XferTERunner &) = delete;

    void pinThread(int thread_id);

    int runTarget();

    int startInitiator();

    int stopInitiator();

    int runInitiatorTasks(const std::function<int(int /* thread_id */)> &func);

    std::string getSegmentName() const { return engine_->getSegmentName(); }

    uint64_t getLocalBufferBase(int thread_id, uint64_t block_size,
                                uint64_t batch_size) const {
        const size_t num_buffers = pinned_buffer_list_.size();
        return (uint64_t)pinned_buffer_list_[thread_id % num_buffers] +
               block_size * batch_size * (thread_id / num_buffers);
    }

    uint64_t getTargetBufferBase(int thread_id, uint64_t block_size,
                                 uint64_t batch_size) const {
        return info_.buffers[thread_id % info_.buffers.size()].base +
               block_size * batch_size * (thread_id / info_.buffers.size());
    }

    double runSingleTransfer(uint64_t local_addr, uint64_t target_addr,
                             uint64_t block_size, uint64_t batch_size,
                             Request::OpCode opcode);

   private:
    int allocateBuffers();

    int freeBuffers();

    int runner(int thread_id);

   private:
    std::unique_ptr<TransferEngine> engine_;
    std::vector<void *> pinned_buffer_list_;
    SegmentID handle_;
    SegmentInfo info_;

    std::vector<std::function<int(int)>> current_task_;
    std::vector<std::thread> threads_;
    std::mutex mtx_;
    std::condition_variable cv_task_;
    std::condition_variable cv_done_;
    int pending_ = 0;
};

}  // namespace v1
}  // namespace mooncake

#endif  // XFER_BENCH_H