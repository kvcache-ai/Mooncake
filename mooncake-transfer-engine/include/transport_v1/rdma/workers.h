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
struct RdmaResources {
    using DeviceID = std::string;

    struct ContextLocal {
        std::shared_ptr<RdmaContext> context;
        std::shared_ptr<LocalBuffers> local_buffers;
    };

    std::string local_segment_name;
    std::shared_ptr<Topology> local_topology;
    std::unordered_map<DeviceID, ContextLocal> context_group;

    // TODO will move out
    std::shared_ptr<mooncake::TransferMetadata> metadata_manager;
};

class Workers {
   public:
    Workers(std::shared_ptr<RdmaResources> &resources);

    ~Workers();

    int start();

    int stop();

    int submit(RdmaSlice *slices, size_t count);

    int cancel(RdmaSlice *slice);

   private:
    using Task = std::function<void()>;

    void workerThread();

    void asyncPostSend();

    void asyncPollCq();

    int doHandshake(std::shared_ptr<RdmaEndPoint> &endpoint,
                    const std::string &peer_server_name,
                    const std::string &peer_nic_name);

   private:
    std::shared_ptr<RdmaResources> resources_;
    const size_t num_workers_;

    std::vector<std::thread> workers_;
    std::queue<RdmaSlice *> slices_;
    std::queue<Task> tasks_;
    std::mutex mutex_;
    std::condition_variable cv_;
    std::atomic<bool> running_;
    bool stop_flag_;
};
}  // namespace v1
}  // namespace mooncake

#endif  // WORKER_H
