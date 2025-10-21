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

#include "v1/runtime/proxy_manager.h"
#include "v1/runtime/transfer_engine_impl.h"
#include <algorithm>
#include <cstring>
#include <sstream>
#include <mutex>

namespace mooncake {
namespace v1 {
ProxyManager::ProxyManager(TransferEngineImpl* impl, size_t chunk_size,
                           size_t chunk_count)
    : chunk_size_(chunk_size), chunk_count_(chunk_count), impl_(impl) {
    running_ = true;
    worker_thread_ = std::thread(&ProxyManager::runner, this);
}

ProxyManager::~ProxyManager() { deconstruct(); }

Status ProxyManager::deconstruct() {
    running_ = false;
    queue_cv_.notify_all();
    if (worker_thread_.joinable()) worker_thread_.join();
    for (auto entry : stage_buffers_) {
        impl_->unregisterLocalMemory(entry.second.chunks);
        impl_->freeLocalMemory(entry.second.chunks);
    }
    stage_buffers_.clear();
    return Status::OK();
}

Status ProxyManager::waitLocalStage(const Request& request,
                                    uint64_t local_stage_buffer,
                                    uint64_t chunk_length, uint64_t offset) {
    Request local_stage;
    local_stage.opcode = request.opcode;
    local_stage.source = (uint8_t*)request.source + offset;
    local_stage.length = chunk_length;
    local_stage.target_id = LOCAL_SEGMENT_ID;
    local_stage.target_offset = local_stage_buffer;
    return impl_->transferSync({local_stage});
}

Status ProxyManager::waitRemoteStage(const std::string& server_addr,
                                     const Request& request,
                                     uint64_t remote_stage_buffer,
                                     uint64_t chunk_length, uint64_t offset) {
    Request remote_stage;
    remote_stage.opcode = request.opcode;
    remote_stage.source = (void*)remote_stage_buffer;
    remote_stage.length = chunk_length;
    remote_stage.target_id = LOCAL_SEGMENT_ID;
    remote_stage.target_offset = request.target_offset + offset;
    return ControlClient::delegate(server_addr, remote_stage);
}

Status ProxyManager::waitCrossStage(const Request& request,
                                    uint64_t local_stage_buffer,
                                    uint64_t remote_stage_buffer,
                                    uint64_t chunk_length) {
    Request inter_stage;
    inter_stage.opcode = request.opcode;
    inter_stage.source = (void*)local_stage_buffer;
    inter_stage.length = chunk_length;
    inter_stage.target_id = request.target_id;
    inter_stage.target_offset = remote_stage_buffer;
    return impl_->transferSync({inter_stage});
}

Status ProxyManager::submit(TaskInfo* task,
                            const std::vector<std::string>& params) {
    StagingTask staging_task;
    staging_task.native = task;
    staging_task.params = params;
    {
        std::lock_guard<std::mutex> lk(queue_mu_);
        task_queue_.push(staging_task);
    }
    queue_cv_.notify_one();
    return Status::OK();
}

Status ProxyManager::getStatus(TaskInfo* task, TransferStatus& task_status) {
    if (!task || !task->staging) return Status::InvalidArgument("Invalid task");
    task_status.s = task->staging_status;
    if (task_status.s == COMPLETED)
        task_status.transferred_bytes = task->request.length;
    return Status::OK();
}

void ProxyManager::runner() {
    while (running_) {
        StagingTask task;
        {
            std::unique_lock<std::mutex> lk(queue_mu_);
            queue_cv_.wait(lk,
                           [&] { return !running_ || !task_queue_.empty(); });

            if (!running_) break;
            if (task_queue_.empty()) continue;

            task = task_queue_.front();
            task_queue_.pop();
        }

        if (!task.native) continue;
        auto status = transferSync(task);
        if (status.ok())
            task.native->staging_status = COMPLETED;
        else
            task.native->staging_status = FAILED;
    }
}

Status ProxyManager::transferSync(StagingTask task) {
    auto& request = task.native->request;
    uint64_t local_stage_buffer = 0, remote_stage_buffer = 0;
    auto server_addr = task.params[0];
    bool local_staging = !task.params[1].empty();
    bool remote_staging = !task.params[2].empty();

    if (local_staging) {
        CHECK_STATUS(pinStageBuffer(task.params[1], local_stage_buffer));
    }
    if (remote_staging) {
        CHECK_STATUS(ControlClient::pinStageBuffer(server_addr, task.params[2],
                                                   remote_stage_buffer));
    }

    for (size_t offset = 0; offset < request.length; offset += chunk_size_) {
        size_t chunk_length = std::min(chunk_size_, request.length - offset);

        if (!local_staging)
            local_stage_buffer = (uint64_t)request.source + offset;
        if (!remote_staging)
            remote_stage_buffer = request.target_offset + offset;

        if (request.opcode == Request::WRITE) {
            if (local_staging) {
                CHECK_STATUS(waitLocalStage(request, local_stage_buffer,
                                            chunk_length, offset));
            }

            CHECK_STATUS(waitCrossStage(request, local_stage_buffer,
                                        remote_stage_buffer, chunk_length));

            if (remote_staging) {
                CHECK_STATUS(waitRemoteStage(server_addr, request,
                                             remote_stage_buffer, chunk_length,
                                             offset));
            }

        } else {
            if (remote_staging) {
                CHECK_STATUS(waitRemoteStage(server_addr, request,
                                             remote_stage_buffer, chunk_length,
                                             offset));
            }

            CHECK_STATUS(waitCrossStage(request, local_stage_buffer,
                                        remote_stage_buffer, chunk_length));

            if (local_staging) {
                CHECK_STATUS(waitLocalStage(request, local_stage_buffer,
                                            chunk_length, offset));
            }
        }
    }

    if (remote_staging) {
        CHECK_STATUS(
            ControlClient::unpinStageBuffer(server_addr, remote_stage_buffer));
    }

    if (local_staging) {
        CHECK_STATUS(unpinStageBuffer(local_stage_buffer));
    }

    return Status::OK();
}

Status ProxyManager::allocateStageBuffers(const std::string& location) {
    if (stage_buffers_.count(location)) return Status::OK();
    StageBuffers buf;
    auto total_size = chunk_size_ * chunk_count_;
    CHECK_STATUS(impl_->allocateLocalMemory(&buf.chunks, total_size, location));
    CHECK_STATUS(impl_->registerLocalMemory(buf.chunks, total_size));
    buf.bitmap = new std::atomic_flag[chunk_count_];
    for (size_t i = 0; i < chunk_count_; ++i)
        buf.bitmap[i].clear(std::memory_order_relaxed);
    stage_buffers_[location] = std::move(buf);
    return Status::OK();
}

Status ProxyManager::freeStageBuffers(const std::string& location) {
    auto it = stage_buffers_.find(location);
    if (it == stage_buffers_.end())
        return Status::InvalidArgument("Stage buffer not allocated" LOC_MARK);
    impl_->unregisterLocalMemory(it->second.chunks);
    impl_->freeLocalMemory(it->second.chunks);
    stage_buffers_.erase(it);
    return Status::OK();
}

Status ProxyManager::pinStageBuffer(const std::string& location,
                                    uint64_t& addr) {
    auto it = stage_buffers_.find(location);
    if (it == stage_buffers_.end()) {
        CHECK_STATUS(allocateStageBuffers(location));
        it = stage_buffers_.find(location);
    }

    auto& buf = it->second;
    for (size_t i = 0; i < chunk_count_; ++i) {
        if (!buf.bitmap[i].test_and_set(std::memory_order_acquire)) {
            addr = reinterpret_cast<uint64_t>(static_cast<char*>(buf.chunks) +
                                              i * chunk_size_);
            return Status::OK();
        }
    }

    return Status::TooManyRequests("No available stage buffer in " + location);
}

Status ProxyManager::unpinStageBuffer(uint64_t addr) {
    for (auto& [location, buf] : stage_buffers_) {
        auto base = reinterpret_cast<uint64_t>(buf.chunks);
        auto end = base + chunk_size_ * chunk_count_;
        if (addr >= base && addr < end) {
            size_t index = (addr - base) / chunk_size_;
            if (index >= chunk_count_)
                return Status::InvalidArgument("Invalid buffer index");
            buf.bitmap[index].clear(std::memory_order_release);
            return Status::OK();
        }
    }
    return Status::InvalidArgument("Address not found in any stage buffer");
}

}  // namespace v1
}  // namespace mooncake