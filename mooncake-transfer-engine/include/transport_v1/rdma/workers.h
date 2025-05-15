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

#ifndef RDMA_WORKERS_H
#define RDMA_WORKERS_H

#include <future>
#include <queue>
#include <thread>
#include <unordered_set>
#include <vector>

#include "context.h"

namespace mooncake {
namespace v1 {

class RdmaTransport;
class Workers {
   public:
    struct SliceQueue {
        RdmaSlice *head, *tail;
        RWSpinlock lock;
        std::atomic<size_t> num_entries;

        SliceQueue() : head(nullptr), tail(nullptr), num_entries(0) {}

        void push(RdmaSlice *first, RdmaSlice *last, size_t count);

        RdmaSlice *pop(size_t max_count);
    };

   public:
    Workers(RdmaTransport *transport);

    ~Workers();

    int start();

    int stop();

    int submit(RdmaSlice *slice);

    int submit(RdmaSliceList &slice_list);

    int cancel(RdmaSliceList &slice_list);

   private:
    using Task = std::function<void()>;

    void workerThread(int thread_id);

    void asyncPostSend(int thread_id);

    void asyncPollCq(int thread_id);

    int doHandshake(std::shared_ptr<RdmaEndPoint> &endpoint,
                    const std::string &peer_server_name,
                    const std::string &peer_nic_name);

    void monitorThread();

    int handleContextEvents(std::shared_ptr<RdmaContext> &context);

   private:
    RdmaTransport *transport_;
    size_t num_workers_;

    std::vector<std::thread> workers_;
    std::thread monitor_;

    std::atomic<int64_t> inflight_slices_;

    std::mutex mutex_;
    std::condition_variable cv_;
    std::atomic<bool> running_;
    bool stop_flag_;
    struct WorkerEnv {
        SliceQueue slice_queue;
        RemoteBufferManager remote_buffer;
    };
    WorkerEnv *worker_env_;
};
}  // namespace v1
}  // namespace mooncake

#endif  // WORKER_H
