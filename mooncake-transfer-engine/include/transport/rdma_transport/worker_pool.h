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

#ifndef WORKER_H
#define WORKER_H

#include <queue>
#include <unordered_set>

#include "rdma_context.h"

namespace mooncake {
class WorkerPool {
   public:
    WorkerPool(RdmaContext &context, int numa_socket_id = 0);

    ~WorkerPool();

    // Add slices to queue, called by Transport
    int submitPostSend(const std::vector<Transport::Slice *> &slice_list);

   private:
    void performPostSend(int thread_id);

    void performPollCq(int thread_id);

    void redispatch(std::vector<Transport::Slice *> &slice_list, int thread_id);

    void transferWorker(int thread_id);

    void monitorWorker();

    int doProcessContextEvents();

   private:
    RdmaContext &context_;
    const int numa_socket_id_;

    std::vector<std::thread> worker_thread_;
    std::atomic<bool> workers_running_;
    std::atomic<int> suspended_flag_;

    std::atomic<int> redispatch_counter_;

    std::mutex cond_mutex_;
    std::condition_variable cond_var_;

    using SliceList = std::vector<Transport::Slice *>;

    const static int kShardCount = 8;
    std::unordered_map<std::string, SliceList> slice_queue_[kShardCount];
    std::atomic<uint64_t> slice_queue_count_[kShardCount];
    TicketLock slice_queue_lock_[kShardCount];

    std::vector<std::unordered_map<std::string, SliceList>>
        collective_slice_queue_;

    std::atomic<uint64_t> submitted_slice_count_, processed_slice_count_;

    uint64_t success_nr_polls = 0, failed_nr_polls = 0;
};
}  // namespace mooncake

#endif  // WORKER_H
