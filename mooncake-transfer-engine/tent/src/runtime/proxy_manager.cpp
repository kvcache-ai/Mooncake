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

#include "tent/runtime/proxy_manager.h"
#include "tent/runtime/transfer_engine_impl.h"
#include "tent/common/log_rate_limiter.h"
#include <algorithm>
#include <cstring>
#include <sstream>
#include <mutex>
#include <glog/logging.h>

namespace mooncake {
namespace tent {
ProxyManager::ProxyManager(TransferEngineImpl* impl, size_t chunk_size,
                           size_t chunk_count)
    : chunk_size_(chunk_size), chunk_count_(chunk_count), impl_(impl) {
    running_ = true;
    for (size_t i = 0; i < kShards; ++i) {
        shards_[i].thread = std::thread(&ProxyManager::runner, this, i);
    }
    VLOG(1) << "ProxyManager initialized: chunk_size=" << chunk_size
            << ", chunk_count=" << chunk_count << ", shards=" << kShards;
}

ProxyManager::~ProxyManager() { deconstruct(); }

Status ProxyManager::deconstruct() {
    running_ = false;
    for (size_t i = 0; i < kShards; ++i) {
        shards_[i].cv.notify_all();
        shards_[i].thread.join();
    }
    for (auto entry : stage_buffers_) {
        impl_->unregisterLocalMemory(entry.second.chunks);
        impl_->freeLocalMemory(entry.second.chunks);
        delete[] entry.second.bitmap;
    }
    stage_buffers_.clear();
    VLOG(1) << "ProxyManager shutdown completed";
    return Status::OK();
}

BatchID ProxyManager::submitCrossStage(const Request& request,
                                       uint64_t local_stage_buffer,
                                       uint64_t remote_stage_buffer,
                                       uint64_t chunk_length) {
    Request inter_stage;
    inter_stage.opcode = request.opcode;
    inter_stage.source = (void*)local_stage_buffer;
    inter_stage.length = chunk_length;
    inter_stage.target_id = request.target_id;
    inter_stage.target_offset = remote_stage_buffer;
    auto batch = impl_->allocateBatch(1);
    impl_->submitTransfer(batch, {inter_stage});
    return batch;
}

BatchID ProxyManager::submitLocalStage(const Request& request,
                                       uint64_t local_stage_buffer,
                                       uint64_t chunk_length, uint64_t offset) {
    Request local_stage;
    local_stage.opcode = request.opcode;
    local_stage.source = (uint8_t*)request.source + offset;
    local_stage.length = chunk_length;
    local_stage.target_id = LOCAL_SEGMENT_ID;
    local_stage.target_offset = local_stage_buffer;
    auto batch = impl_->allocateBatch(1);
    impl_->submitTransfer(batch, {local_stage});
    return batch;
}

Status ProxyManager::waitLocalStage(const Request& request,
                                    uint64_t local_stage_buffer,
                                    uint64_t chunk_length, uint64_t offset) {
    auto batch =
        submitLocalStage(request, local_stage_buffer, chunk_length, offset);
    return impl_->waitTransferCompletion(batch);
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

void ProxyManager::submitRemoteStage(const std::string& server_addr,
                                     const Request& request,
                                     uint64_t remote_stage_buffer,
                                     uint64_t chunk_length, uint64_t offset,
                                     std::future<Status>& handle) {
    Request remote_stage;
    remote_stage.opcode = request.opcode;
    remote_stage.source = (void*)remote_stage_buffer;
    remote_stage.length = chunk_length;
    remote_stage.target_id = LOCAL_SEGMENT_ID;
    remote_stage.target_offset = request.target_offset + offset;
    handle = delegate_pool_.enqueue([server_addr, remote_stage]() {
        return ControlClient::delegate(server_addr, remote_stage);
    });
}

Status ProxyManager::waitCrossStage(const Request& request,
                                    uint64_t local_stage_buffer,
                                    uint64_t remote_stage_buffer,
                                    uint64_t chunk_length) {
    auto batch = submitCrossStage(request, local_stage_buffer,
                                  remote_stage_buffer, chunk_length);
    return impl_->waitTransferCompletion(batch);
}

Status ProxyManager::submit(TaskInfo* task,
                            const std::vector<std::string>& params) {
    if (!task) {
        LOG(ERROR) << "Failed to submit task: null task pointer";
        return Status::InvalidArgument("Invalid task");
    }
    StagingTask staging_task;
    staging_task.native = task;
    staging_task.params = params;
    task->staging_status = PENDING;
    static std::atomic<size_t> next_queue_index(0);
    thread_local size_t id = next_queue_index.fetch_add(1) % kShards;
    {
        std::lock_guard<std::mutex> lk(shards_[id].mu);
        shards_[id].queue.push(staging_task);
    }
    shards_[id].cv.notify_one();
    VLOG(1) << "Task submitted to ProxyManager shard " << id
            << ", opcode=" << task->request.opcode
            << ", length=" << task->request.length
            << ", target_id=" << task->request.target_id;
    return Status::OK();
}

Status ProxyManager::getStatus(TaskInfo* task, TransferStatus& task_status) {
    if (!task || !task->staging) return Status::InvalidArgument("Invalid task");
    task_status.s = task->staging_status;
    if (task_status.s == COMPLETED) {
        task_status.transferred_bytes = task->request.length;
    }
    return Status::OK();
}

struct StageBufferCache {
    StageBufferCache(ProxyManager& mgr) : mgr(mgr) {}

    uint64_t allocateLocal(const std::string& location, int idx = 0) {
        auto key = location + "-" + std::to_string(idx);
        if (local_stage_buffers.count(key)) {
            return local_stage_buffers[key];
        }
        uint64_t addr = 0;
        auto status = mgr.pinStageBuffer(location, addr);
        if (!status.ok()) {
            LOG(ERROR) << "Failed to pin local stage buffer: " << status
                       << ", location " << location;
            return 0;
        }
        local_stage_buffers[key] = addr;
        return addr;
    }

    uint64_t allocateRemote(const std::string& server_addr,
                            const std::string& location, int idx = 0) {
        auto key = location + "-" + std::to_string(idx);
        if (remote_stage_buffers[server_addr].count(key)) {
            return remote_stage_buffers[server_addr][key];
        }
        uint64_t addr = 0;
        auto status =
            ControlClient::pinStageBuffer(server_addr, location, addr);
        if (!status.ok()) {
            LOG(ERROR) << "Failed to pin remote stage buffer: " << status
                       << ", location " << location;
            return 0;
        }
        remote_stage_buffers[server_addr][key] = addr;
        return addr;
    }

    void reset() {
        for (auto& entry : local_stage_buffers) {
            mgr.unpinStageBuffer(entry.second);
        }
        local_stage_buffers.clear();
        for (auto& server_entry : remote_stage_buffers) {
            for (auto& entry : server_entry.second) {
                ControlClient::unpinStageBuffer(server_entry.first,
                                                entry.second);
            }
        }
        remote_stage_buffers.clear();
    }

    std::unordered_map<std::string, uint64_t> local_stage_buffers;
    std::unordered_map<std::string, std::unordered_map<std::string, uint64_t>>
        remote_stage_buffers;
    ProxyManager& mgr;
};

void ProxyManager::runner(size_t id) {
    StageBufferCache cache(*this);
    auto& shard = shards_[id];
    size_t completed_tasks = 0;
    size_t failed_tasks = 0;

    while (running_) {
        StagingTask task;
        {
            std::unique_lock<std::mutex> lk(shard.mu);
            if (shard.queue.empty()) {
                shard.cv.wait(
                    lk, [&] { return !running_ || !shard.queue.empty(); });
            }

            if (!running_) break;
            if (shard.queue.empty()) continue;

            task = shard.queue.front();
            shard.queue.pop();
        }

        if (!task.native) continue;
        auto status = transferEventLoop(task, &cache);
        auto staging_status = status.ok() ? COMPLETED : FAILED;
        __atomic_store(&task.native->staging_status, &staging_status,
                       __ATOMIC_RELEASE);

        if (status.ok()) {
            completed_tasks++;
        } else {
            failed_tasks++;
            // Rate limit error logging to avoid log spam
            thread_local LogRateLimiter rate_limiter(LOG_RATE_LIMIT_1S);
            if (rate_limiter.shouldLog()) {
                VLOG(1) << "Shard " << id
                        << " errors: completed=" << completed_tasks
                        << ", failed=" << failed_tasks;
            }
        }
    }

    cache.reset();
    VLOG(1) << "ProxyManager worker " << id
            << " exiting: completed=" << completed_tasks
            << ", failed=" << failed_tasks;
}

Status ProxyManager::transferEventLoop(StagingTask& task,
                                       StageBufferCache* cache) {
    auto& request = task.native->request;
    auto server_addr = task.params[0];
    bool local_staging = !task.params[1].empty();
    bool remote_staging = !task.params[2].empty();
    const static size_t kStageBuffers = 4;

    VLOG(1) << "transferEventLoop started: opcode=" << request.opcode
            << ", length=" << request.length
            << ", local_staging=" << local_staging
            << ", remote_staging=" << remote_staging
            << ", server=" << server_addr;
    uint64_t local_stage_buffer[kStageBuffers],
        remote_stage_buffer[kStageBuffers];
    if (local_staging) {
        for (size_t i = 0; i < kStageBuffers; ++i) {
            local_stage_buffer[i] =
                cache->allocateLocal(task.params[1], static_cast<int>(i));
            if (local_stage_buffer[i] == 0)
                return Status::InternalError(
                    "Failed to pin local stage buffer");
        }
    }
    if (remote_staging) {
        for (size_t i = 0; i < kStageBuffers; ++i) {
            remote_stage_buffer[i] = cache->allocateRemote(
                server_addr, task.params[2], static_cast<int>(i));
            if (remote_stage_buffer[i] == 0)
                return Status::InternalError(
                    "Failed to pin remote stage buffer");
        }
    }

    enum class StageState {
        PRE,
        CROSS,
        POST,
        INFLIGHT,
        FINISH,
        FAILED,
        INFLIGHT_REMOTE
    };

    struct Chunk {
        size_t offset;
        size_t length;
        uint64_t local_buf;
        uint64_t remote_buf;
        StageState prev_state;
        StageState state;
        BatchID batch;
    };

    std::queue<size_t> event_queue;
    std::vector<Chunk> chunks;
    std::unordered_set<uint64_t> local_locked;
    std::unordered_set<uint64_t> remote_locked;

    for (size_t offset = 0; offset < request.length; offset += chunk_size_) {
        size_t id = chunks.size();
        Chunk chunk{offset,
                    std::min(chunk_size_, request.length - offset),
                    local_staging ? local_stage_buffer[id % kStageBuffers]
                                  : (uint64_t)request.source + offset,
                    remote_staging ? remote_stage_buffer[id % kStageBuffers]
                                   : request.target_offset + offset,
                    StageState::PRE,
                    StageState::PRE,
                    0};
        chunks.push_back(chunk);
    }

    for (size_t i = 0; i < chunks.size(); ++i) event_queue.push(i);
    std::vector<std::future<Status>> remote_futures(chunks.size());

    while (!event_queue.empty()) {
        auto id = event_queue.front();
        auto& chunk = chunks[id];
        event_queue.pop();
        switch (chunk.state) {
            case StageState::PRE: {
                if (request.opcode == Request::WRITE && local_staging) {
                    if (local_locked.count(chunk.local_buf)) {
                        event_queue.push(id);
                        break;
                    }
                    local_locked.insert(chunk.local_buf);
                    chunk.batch = submitLocalStage(request, chunk.local_buf,
                                                   chunk.length, chunk.offset);
                    chunk.prev_state = chunk.state;
                    chunk.state = StageState::INFLIGHT;
                } else if (request.opcode == Request::READ && remote_staging) {
                    if (remote_locked.count(chunk.remote_buf)) {
                        event_queue.push(id);
                        break;
                    }
                    remote_locked.insert(chunk.remote_buf);
                    submitRemoteStage(server_addr, request, chunk.remote_buf,
                                      chunk.length, chunk.offset,
                                      remote_futures[id]);
                    chunk.prev_state = chunk.state;
                    chunk.state = StageState::INFLIGHT_REMOTE;
                } else {
                    chunk.state = StageState::CROSS;
                }
                event_queue.push(id);
                break;
            }

            case StageState::CROSS: {
                if (request.opcode == Request::READ && local_staging) {
                    if (local_locked.count(chunk.local_buf)) {
                        event_queue.push(id);
                        break;
                    }
                    local_locked.insert(chunk.local_buf);
                }
                if (request.opcode == Request::WRITE && remote_staging) {
                    if (remote_locked.count(chunk.remote_buf)) {
                        event_queue.push(id);
                        break;
                    }
                    remote_locked.insert(chunk.remote_buf);
                }
                chunk.batch = submitCrossStage(request, chunk.local_buf,
                                               chunk.remote_buf, chunk.length);
                chunk.prev_state = chunk.state;
                chunk.state = StageState::INFLIGHT;
                event_queue.push(id);
                break;
            }

            case StageState::POST: {
                if (request.opcode == Request::WRITE && remote_staging) {
                    submitRemoteStage(server_addr, request, chunk.remote_buf,
                                      chunk.length, chunk.offset,
                                      remote_futures[id]);
                    chunk.prev_state = chunk.state;
                    chunk.state = StageState::INFLIGHT_REMOTE;
                } else if (request.opcode == Request::READ && local_staging) {
                    chunk.batch = submitLocalStage(request, chunk.local_buf,
                                                   chunk.length, chunk.offset);
                    chunk.prev_state = chunk.state;
                    chunk.state = StageState::INFLIGHT;
                    event_queue.push(id);
                }
                break;
            }

            case StageState::INFLIGHT: {
                TransferStatus xfer_status;
                CHECK_STATUS(
                    impl_->getTransferStatus(chunk.batch, xfer_status));
                if (xfer_status.s == PENDING) {
                    event_queue.push(id);
                    break;
                }
                if (xfer_status.s == COMPLETED) {
                    if (chunk.prev_state == StageState::PRE)
                        chunk.state = StageState::CROSS;
                    else if (chunk.prev_state == StageState::CROSS) {
                        chunk.state = StageState::POST;
                        if (request.opcode == Request::WRITE && local_staging)
                            local_locked.erase(chunk.local_buf);
                        if (request.opcode == Request::READ && remote_staging)
                            remote_locked.erase(chunk.remote_buf);
                    } else if (chunk.prev_state == StageState::POST) {
                        chunk.state = StageState::FINISH;
                        if (request.opcode == Request::READ && local_staging)
                            local_locked.erase(chunk.local_buf);
                        if (request.opcode == Request::WRITE && remote_staging)
                            remote_locked.erase(chunk.remote_buf);
                    }
                    impl_->freeBatch(chunk.batch);
                    chunk.batch = 0;
                } else if (xfer_status.s != PENDING) {
                    chunk.state = StageState::FAILED;
                    impl_->freeBatch(chunk.batch);
                    chunk.batch = 0;
                }
                event_queue.push(id);
                break;
            }

            case StageState::FINISH: {
                break;
            }

            case StageState::FAILED: {
                LOG(ERROR) << "Transfer event loop failed at chunk " << id
                           << ", offset=" << chunk.offset
                           << ", state=" << static_cast<int>(chunk.prev_state);
                return Status::InternalError(
                    "Proxy event loop in failed state");
            }

            case StageState::INFLIGHT_REMOTE: {
                auto& fut = remote_futures[id];
                if (!fut.valid()) {
                    chunk.state = StageState::FAILED;
                    break;
                }
                if (fut.wait_for(std::chrono::seconds(0)) ==
                    std::future_status::ready) {
                    Status rs = fut.get();
                    if (!rs.ok()) {
                        chunk.state = StageState::FAILED;
                        break;
                    }
                    if (chunk.prev_state == StageState::PRE) {
                        chunk.state = StageState::CROSS;
                        event_queue.push(id);
                    } else if (chunk.prev_state == StageState::POST) {
                        chunk.state = StageState::FINISH;
                        if (request.opcode == Request::WRITE &&
                            remote_staging) {
                            remote_locked.erase(chunk.remote_buf);
                        }
                    }
                } else {
                    event_queue.push(id);
                }
                break;
            }
        }
    }

    VLOG(1) << "transferEventLoop completed successfully for " << chunks.size()
            << " chunks, total_length=" << request.length;
    return Status::OK();
}

Status ProxyManager::transferSync(StagingTask& task, StageBufferCache* cache) {
    auto& request = task.native->request;
    uint64_t local_stage_buffer = 0, remote_stage_buffer = 0;
    auto server_addr = task.params[0];
    bool local_staging = !task.params[1].empty();
    bool remote_staging = !task.params[2].empty();

    if (local_staging) {
        local_stage_buffer = cache->allocateLocal(task.params[1]);
        if (local_stage_buffer == 0)
            return Status::InternalError("Failed to pin local stage buffer");
    }
    if (remote_staging) {
        remote_stage_buffer =
            cache->allocateRemote(server_addr, task.params[2]);
        if (remote_stage_buffer == 0)
            return Status::InternalError("Failed to pin remote stage buffer");
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

    return Status::OK();
}

Status ProxyManager::allocateStageBuffers(const std::string& location) {
    if (stage_buffers_.count(location)) {
        return Status::OK();
    }
    StageBuffers buf;
    auto total_size = chunk_size_ * chunk_count_;
    CHECK_STATUS(
        impl_->allocateLocalMemory(&buf.chunks, total_size, location, true));
    CHECK_STATUS(impl_->registerLocalMemory(buf.chunks, total_size));
    buf.bitmap = new std::atomic_flag[chunk_count_];
    for (size_t i = 0; i < chunk_count_; ++i)
        buf.bitmap[i].clear(std::memory_order_relaxed);
    stage_buffers_[location] = std::move(buf);
    VLOG(1) << "Stage buffer allocated: location=" << location
            << ", size=" << total_size;
    return Status::OK();
}

Status ProxyManager::freeStageBuffers(const std::string& location) {
    auto it = stage_buffers_.find(location);
    if (it == stage_buffers_.end()) {
        return Status::InvalidArgument("Stage buffer not allocated" LOC_MARK);
    }
    impl_->unregisterLocalMemory(it->second.chunks);
    impl_->freeLocalMemory(it->second.chunks);
    delete[] it->second.bitmap;
    stage_buffers_.erase(it);
    VLOG(1) << "Stage buffer freed: location=" << location;
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
    // Rate limit warnings for buffer exhaustion
    thread_local LogRateLimiter rate_limiter(LOG_RATE_LIMIT_1S);
    if (rate_limiter.shouldLog()) {
        LOG(WARNING) << "Stage buffer exhausted: location=" << location
                     << ", all " << chunk_count_ << " chunks in use";
    }
    return Status::TooManyRequests("No available stage buffer in " + location);
}

Status ProxyManager::unpinStageBuffer(uint64_t addr) {
    for (auto& [location, buf] : stage_buffers_) {
        auto base = reinterpret_cast<uint64_t>(buf.chunks);
        auto end = base + chunk_size_ * chunk_count_;
        if (addr >= base && addr < end) {
            size_t index = (addr - base) / chunk_size_;
            if (index >= chunk_count_) {
                return Status::InvalidArgument("Invalid buffer index");
            }
            buf.bitmap[index].clear(std::memory_order_release);
            return Status::OK();
        }
    }
    return Status::InvalidArgument("Address not found in any stage buffer");
}

}  // namespace tent
}  // namespace mooncake