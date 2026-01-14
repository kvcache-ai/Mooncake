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

#ifndef PROXY_MANAGER_H_
#define PROXY_MANAGER_H_

#include "tent/common/types.h"
#include "tent/common/status.h"
#include "tent/common/concurrent/thread_pool.h"

#include "tent/runtime/transport.h"

// Beta version -- use with own risk

namespace mooncake {
namespace tent {
class TransferEngineImpl;
class TaskInfo;
struct StageBufferCache;

struct StagingTask {
    TaskInfo* native{nullptr};
    std::vector<std::string> params;
};

class ProxyManager {
    struct StageBuffers {
        void* chunks;
        std::atomic_flag* bitmap;
        uint64_t local_stage_buffer = 0;
        uint64_t remote_stage_buffer = 0;
    };

   public:
    explicit ProxyManager(TransferEngineImpl* impl,
                          size_t chunk_size = 8 * 1024 * 1024,
                          size_t chunk_count = 32);

    ~ProxyManager();

    Status deconstruct();

    Status submit(TaskInfo* task, const std::vector<std::string>& params);

    Status getStatus(TaskInfo* task, TransferStatus& task_status);

    Status pinStageBuffer(const std::string& location, uint64_t& addr);

    Status unpinStageBuffer(uint64_t addr);

   private:
    void runner(size_t id);

    Status transferEventLoop(StagingTask& task, StageBufferCache* cache);

    Status transferSync(StagingTask& task, StageBufferCache* cache);

    Status allocateStageBuffers(const std::string& location);

    Status freeStageBuffers(const std::string& location);

    BatchID submitCrossStage(const Request& request,
                             uint64_t local_stage_buffer,
                             uint64_t remote_stage_buffer,
                             uint64_t chunk_length);

    BatchID submitLocalStage(const Request& request,
                             uint64_t local_stage_buffer, uint64_t chunk_length,
                             uint64_t offset);

    Status waitCrossStage(const Request& request, uint64_t local_stage_buffer,
                          uint64_t remote_stage_buffer, uint64_t chunk_length);

    Status waitLocalStage(const Request& request, uint64_t local_stage_buffer,
                          uint64_t chunk_length, uint64_t offset);

    Status waitRemoteStage(const std::string& server_addr,
                           const Request& request, uint64_t remote_stage_buffer,
                           uint64_t chunk_length, uint64_t offset);

    void submitRemoteStage(const std::string& server_addr,
                           const Request& request, uint64_t remote_stage_buffer,
                           uint64_t chunk_length, uint64_t offset,
                           std::future<Status>& handle);

   private:
    const size_t chunk_size_;
    const size_t chunk_count_;
    TransferEngineImpl* impl_;
    std::unordered_map<std::string, StageBuffers> stage_buffers_;
    std::atomic<bool> running_;
    struct WorkerShard {
        std::thread thread;
        std::mutex mu;
        std::condition_variable cv;
        std::queue<StagingTask> queue;
    };
    const static size_t kShards = 8;
    WorkerShard shards_[kShards];
    ThreadPool delegate_pool_;
};
}  // namespace tent
}  // namespace mooncake

#endif  // PROXY_MANAGER_H_