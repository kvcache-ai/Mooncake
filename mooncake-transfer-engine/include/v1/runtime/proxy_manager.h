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

#include "v1/common/types.h"
#include "v1/common/status.h"

#include "v1/runtime/transport.h"

namespace mooncake {
namespace v1 {
class TransferEngineImpl;
class TaskInfo;

struct StagingTask {
    TaskInfo* native{nullptr};
    std::vector<std::string> params;
};

class ProxyManager {
    struct StageBuffers {
        ~StageBuffers() { delete[] bitmap; }
        void* chunks;
        std::atomic_flag* bitmap;
    };

   public:
    explicit ProxyManager(TransferEngineImpl* impl,
                          size_t chunk_size = 4 * 1024 * 1024,
                          size_t chunk_count = 8);

    ~ProxyManager();

    Status deconstruct();

    Status submit(TaskInfo* task, const std::vector<std::string>& params);

    Status getStatus(TaskInfo* task, TransferStatus& task_status);

    Status pinStageBuffer(const std::string& location, uint64_t& addr);

    Status unpinStageBuffer(uint64_t addr);

   private:
    void runner();

    Status transferSync(StagingTask task);

    Status allocateStageBuffers(const std::string& location);

    Status freeStageBuffers(const std::string& location);

    Status waitCrossStage(const Request& request, uint64_t local_stage_buffer,
                          uint64_t remote_stage_buffer, uint64_t chunk_length);

    Status waitLocalStage(const Request& request, uint64_t local_stage_buffer,
                          uint64_t chunk_length, uint64_t offset);

    Status waitRemoteStage(const std::string& server_addr,
                           const Request& request, uint64_t remote_stage_buffer,
                           uint64_t chunk_length, uint64_t offset);

   private:
    const size_t chunk_size_;
    const size_t chunk_count_;
    TransferEngineImpl* impl_;
    std::unordered_map<std::string, StageBuffers> stage_buffers_;
    std::atomic<bool> running_;
    std::thread worker_thread_;
    std::mutex queue_mu_;
    std::condition_variable queue_cv_;
    std::queue<StagingTask> task_queue_;
};
}  // namespace v1
}  // namespace mooncake

#endif  // PROXY_MANAGER_H_