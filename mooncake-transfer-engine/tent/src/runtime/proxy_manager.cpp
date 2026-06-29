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
#include <algorithm>
#include <cstring>
#include <sstream>
#include <mutex>

namespace mooncake {
namespace tent {
ProxyManager::ProxyManager(TransferEngineImpl* impl,
                           size_t max_queued_tasks_per_shard)
    : ProxyManager(impl, max_queued_tasks_per_shard, true) {}

std::unique_ptr<ProxyManager> ProxyManager::createForTest(
    TransferEngineImpl* impl, size_t max_queued_tasks_per_shard) {
    return std::unique_ptr<ProxyManager>(
        new ProxyManager(impl, max_queued_tasks_per_shard, false));
}

ProxyManager::ProxyManager(TransferEngineImpl* impl,
                           size_t max_queued_tasks_per_shard,
                           bool start_workers)
    : max_queued_tasks_per_shard_(max_queued_tasks_per_shard), impl_(impl) {
    running_ = true;
    if (start_workers) {
        for (size_t i = 0; i < kShards; ++i) {
            shards_[i].thread = std::thread(&ProxyManager::runner, this, i);
        }
    }
}

ProxyManager::~ProxyManager() { deconstruct(); }

Status ProxyManager::deconstruct() {
    running_ = false;
    for (size_t i = 0; i < kShards; ++i) {
        shards_[i].cv.notify_all();
        if (shards_[i].thread.joinable()) shards_[i].thread.join();
    }

    std::unordered_map<std::string, StageBuffers> stage_buffers;
    {
        std::lock_guard<std::mutex> lk(stage_buffers_mu_);
        stage_buffers = std::move(stage_buffers_);
    }
    for (auto& entry : stage_buffers) {
        impl_->unregisterLocalMemory(entry.second.chunks);
        impl_->freeLocalMemory(entry.second.chunks);
        delete[] entry.second.bitmap;
    }
    return Status::OK();
}

Request ProxyManager::makeCrossStageRequest(const Request& request,
                                            uint64_t local_stage_buffer,
                                            uint64_t remote_stage_buffer,
                                            uint64_t chunk_length) {
    Request inter_stage;
    inter_stage.opcode = request.opcode;
    inter_stage.source = (void*)local_stage_buffer;
    inter_stage.length = chunk_length;
    inter_stage.target_id = request.target_id;
    inter_stage.target_offset = remote_stage_buffer;
    inter_stage.priority = request.priority;
    return inter_stage;
}

Request ProxyManager::makeLocalStageRequest(const Request& request,
                                            uint64_t local_stage_buffer,
                                            uint64_t chunk_length,
                                            uint64_t offset) {
    Request local_stage;
    local_stage.opcode = request.opcode;
    local_stage.source = (uint8_t*)request.source + offset;
    local_stage.length = chunk_length;
    local_stage.target_id = LOCAL_SEGMENT_ID;
    local_stage.target_offset = local_stage_buffer;
    local_stage.priority = request.priority;
    return local_stage;
}

Request ProxyManager::makeRemoteStageRequest(const Request& request,
                                             uint64_t remote_stage_buffer,
                                             uint64_t chunk_length,
                                             uint64_t offset) {
    Request remote_stage;
    remote_stage.opcode = request.opcode;
    remote_stage.source = (void*)remote_stage_buffer;
    remote_stage.length = chunk_length;
    remote_stage.target_id = LOCAL_SEGMENT_ID;
    remote_stage.target_offset = request.target_offset + offset;
    remote_stage.priority = request.priority;
    return remote_stage;
}

BatchID ProxyManager::submitCrossStage(const Request& request,
                                       uint64_t local_stage_buffer,
                                       uint64_t remote_stage_buffer,
                                       uint64_t chunk_length) {
    auto inter_stage = makeCrossStageRequest(request, local_stage_buffer,
                                             remote_stage_buffer, chunk_length);
    auto batch = impl_->allocateBatch(1);
    auto status = impl_->submitStagingTransfer(batch, {inter_stage});
    if (!status.ok()) {
        LOG(WARNING) << "failed to submit cross-stage transfer: "
                     << status.ToString();
        if (batch) impl_->freeBatch(batch);
        return 0;
    }
    return batch;
}

BatchID ProxyManager::submitLocalStage(const Request& request,
                                       uint64_t local_stage_buffer,
                                       uint64_t chunk_length, uint64_t offset) {
    auto local_stage = makeLocalStageRequest(request, local_stage_buffer,
                                             chunk_length, offset);
    auto batch = impl_->allocateBatch(1);
    auto status = impl_->submitStagingTransfer(batch, {local_stage});
    if (!status.ok()) {
        LOG(WARNING) << "failed to submit local-stage transfer: "
                     << status.ToString();
        if (batch) impl_->freeBatch(batch);
        return 0;
    }
    return batch;
}

Status ProxyManager::waitLocalStage(const Request& request,
                                    uint64_t local_stage_buffer,
                                    uint64_t chunk_length, uint64_t offset) {
    auto batch =
        submitLocalStage(request, local_stage_buffer, chunk_length, offset);
    if (!batch) return Status::TooManyRequests("submit local stage failed");
    return impl_->waitTransferCompletion(batch);
}

Status ProxyManager::waitRemoteStage(const std::string& server_addr,
                                     const Request& request,
                                     uint64_t remote_stage_buffer,
                                     uint64_t chunk_length, uint64_t offset) {
    auto remote_stage = makeRemoteStageRequest(request, remote_stage_buffer,
                                               chunk_length, offset);
    return ControlClient::delegate(server_addr, remote_stage);
}

void ProxyManager::submitRemoteStage(const std::string& server_addr,
                                     const Request& request,
                                     uint64_t remote_stage_buffer,
                                     uint64_t chunk_length, uint64_t offset,
                                     std::future<Status>& handle) {
    auto remote_stage = makeRemoteStageRequest(request, remote_stage_buffer,
                                               chunk_length, offset);
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
    if (!batch) return Status::TooManyRequests("submit cross stage failed");
    return impl_->waitTransferCompletion(batch);
}

Status ProxyManager::submit(TaskInfo* task, BatchID batch,
                            const std::vector<std::string>& params) {
    if (!task || batch == 0) {
        return Status::InvalidArgument("invalid staging task" LOC_MARK);
    }
    StagingTask staging_task;
    staging_task.native = task;
    staging_task.batch = batch;
    staging_task.params = params;
    static std::atomic<size_t> next_queue_index(0);
    thread_local size_t id = next_queue_index.fetch_add(1) % kShards;
    {
        std::lock_guard<std::mutex> lk(shards_[id].mu);
        if (max_queued_tasks_per_shard_ > 0 &&
            shards_[id].queued_tasks >= max_queued_tasks_per_shard_) {
            return Status::TooManyRequests(
                "staging proxy shard queue is full" LOC_MARK);
        }
        shards_[id].queue.push(staging_task);
        ++shards_[id].queued_tasks;
        task->staging_status = PENDING;
    }
    shards_[id].cv.notify_one();
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

    Status allocateLocal(const std::string& location, uint64_t& addr,
                         int idx = 0) {
        auto key = location + "-" + std::to_string(idx);
        if (local_stage_buffers.count(key)) {
            addr = local_stage_buffers[key];
            return Status::OK();
        }
        auto status = mgr.pinStageBuffer(location, addr);
        if (!status.ok()) {
            addr = 0;
            return status;
        }
        local_stage_buffers[key] = addr;
        return Status::OK();
    }

    Status allocateRemote(const std::string& server_addr,
                          const std::string& location, uint64_t& addr,
                          int idx = 0) {
        auto key = location + "-" + std::to_string(idx);
        if (remote_stage_buffers[server_addr].count(key)) {
            addr = remote_stage_buffers[server_addr][key];
            return Status::OK();
        }
        auto status =
            ControlClient::pinStageBuffer(server_addr, location, addr);
        if (!status.ok()) {
            addr = 0;
            return status;
        }
        remote_stage_buffers[server_addr][key] = addr;
        return Status::OK();
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
            --shard.queued_tasks;
        }

        if (!task.native) continue;
        auto status = transferEventLoop(task, &cache);
        auto staging_status = status.ok() ? COMPLETED : FAILED;
        __atomic_store(&task.native->staging_status, &staging_status,
                       __ATOMIC_RELEASE);
        impl_->notifyBatchMaybeReady(task.batch);
    }
    cache.reset();
}

Status ProxyManager::transferEventLoop(StagingTask& task,
                                       StageBufferCache* cache) {
    auto& request = task.native->request;
    auto server_addr = task.params[0];
    bool local_staging = !task.params[1].empty();
    bool remote_staging = !task.params[2].empty();
    const size_t kStageBuffers = std::min(kChunkCount, static_cast<size_t>(16));
    uint64_t local_stage_buffer[kStageBuffers],
        remote_stage_buffer[kStageBuffers];
    if (local_staging) {
        for (size_t i = 0; i < kStageBuffers; ++i) {
            CHECK_STATUS(cache->allocateLocal(
                task.params[1], local_stage_buffer[i], static_cast<int>(i)));
            if (local_stage_buffer[i] == 0)
                return Status::InternalError(
                    "Failed to pin local stage buffer");
        }
    }
    if (remote_staging) {
        for (size_t i = 0; i < kStageBuffers; ++i) {
            CHECK_STATUS(cache->allocateRemote(server_addr, task.params[2],
                                               remote_stage_buffer[i],
                                               static_cast<int>(i)));
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

    for (size_t offset = 0; offset < request.length; offset += kChunkSize) {
        size_t id = chunks.size();
        Chunk chunk{offset,
                    std::min(kChunkSize, request.length - offset),
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
                    if (!chunk.batch) {
                        chunk.state = StageState::FAILED;
                        event_queue.push(id);
                        break;
                    }
                    chunk.prev_state = chunk.state;
                    chunk.state = StageState::INFLIGHT;
                    event_queue.push(id);
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
                    event_queue.push(id);
                } else {
                    chunk.state = StageState::CROSS;
                    event_queue.push(id);
                }
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
                if (!chunk.batch) {
                    chunk.state = StageState::FAILED;
                    event_queue.push(id);
                    break;
                }
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
                    event_queue.push(id);
                } else if (request.opcode == Request::READ && local_staging) {
                    chunk.batch = submitLocalStage(request, chunk.local_buf,
                                                   chunk.length, chunk.offset);
                    if (!chunk.batch) {
                        chunk.state = StageState::FAILED;
                        event_queue.push(id);
                        break;
                    }
                    chunk.prev_state = chunk.state;
                    chunk.state = StageState::INFLIGHT;
                    event_queue.push(id);
                } else {
                    // No staging needed, mark as finished
                    chunk.state = StageState::FINISH;
                }
                break;
            }

            case StageState::INFLIGHT: {
                TransferStatus xfer_status;
                CHECK_STATUS(impl_->progressBatch(chunk.batch, xfer_status));
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
                // Drain the queue to avoid losing chunks
                while (!event_queue.empty()) {
                    event_queue.pop();
                }
                return Status::InternalError(
                    "Proxy event loop in failed state");
            }

            case StageState::INFLIGHT_REMOTE: {
                auto& fut = remote_futures[id];
                if (!fut.valid()) {
                    chunk.state = StageState::FAILED;
                    event_queue.push(id);
                    break;
                }
                if (fut.wait_for(std::chrono::seconds(0)) ==
                    std::future_status::ready) {
                    Status rs = fut.get();
                    if (!rs.ok()) {
                        chunk.state = StageState::FAILED;
                        event_queue.push(id);
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
                        event_queue.push(id);
                    }
                } else {
                    event_queue.push(id);
                }
                break;
            }
        }
    }

    return Status::OK();
}

Status ProxyManager::transferSync(StagingTask& task, StageBufferCache* cache) {
    auto& request = task.native->request;
    uint64_t local_stage_buffer = 0, remote_stage_buffer = 0;
    auto server_addr = task.params[0];
    bool local_staging = !task.params[1].empty();
    bool remote_staging = !task.params[2].empty();

    if (local_staging) {
        CHECK_STATUS(cache->allocateLocal(task.params[1], local_stage_buffer));
        if (local_stage_buffer == 0)
            return Status::InternalError("Failed to pin local stage buffer");
    }
    if (remote_staging) {
        CHECK_STATUS(cache->allocateRemote(server_addr, task.params[2],
                                           remote_stage_buffer));
        if (remote_stage_buffer == 0)
            return Status::InternalError("Failed to pin remote stage buffer");
    }

    for (size_t offset = 0; offset < request.length; offset += kChunkSize) {
        size_t chunk_length = std::min(kChunkSize, request.length - offset);

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

Status ProxyManager::allocateStageBuffersLocked(const std::string& location) {
    if (stage_buffers_.count(location)) return Status::OK();
    StageBuffers buf;
    auto total_size = kChunkSize * kChunkCount;
    CHECK_STATUS(
        impl_->allocateLocalMemory(&buf.chunks, total_size, location, true));
    CHECK_STATUS(impl_->registerLocalMemory(buf.chunks, total_size));
    buf.bitmap = new std::atomic_flag[kChunkCount];
    for (size_t i = 0; i < kChunkCount; ++i)
        buf.bitmap[i].clear(std::memory_order_relaxed);
    stage_buffers_[location] = std::move(buf);
    return Status::OK();
}

Status ProxyManager::freeStageBuffers(const std::string& location) {
    std::lock_guard<std::mutex> lk(stage_buffers_mu_);
    auto it = stage_buffers_.find(location);
    if (it == stage_buffers_.end())
        return Status::InvalidArgument("Stage buffer not allocated" LOC_MARK);
    impl_->unregisterLocalMemory(it->second.chunks);
    impl_->freeLocalMemory(it->second.chunks);
    delete[] it->second.bitmap;
    stage_buffers_.erase(it);
    return Status::OK();
}

Status ProxyManager::pinStageBuffer(const std::string& location,
                                    uint64_t& addr) {
    std::lock_guard<std::mutex> lk(stage_buffers_mu_);
    auto it = stage_buffers_.find(location);
    if (it == stage_buffers_.end()) {
        CHECK_STATUS(allocateStageBuffersLocked(location));
        it = stage_buffers_.find(location);
    }

    auto& buf = it->second;
    for (size_t i = 0; i < kChunkCount; ++i) {
        if (!buf.bitmap[i].test_and_set(std::memory_order_acquire)) {
            addr = reinterpret_cast<uint64_t>(static_cast<char*>(buf.chunks) +
                                              i * kChunkSize);
            return Status::OK();
        }
    }
    return Status::TooManyRequests("No available stage buffer in " + location);
}

Status ProxyManager::unpinStageBuffer(uint64_t addr) {
    std::lock_guard<std::mutex> lk(stage_buffers_mu_);
    for (auto& [location, buf] : stage_buffers_) {
        auto base = reinterpret_cast<uint64_t>(buf.chunks);
        auto end = base + kChunkSize * kChunkCount;
        if (addr >= base && addr < end) {
            size_t index = (addr - base) / kChunkSize;
            if (index >= kChunkCount)
                return Status::InvalidArgument("Invalid buffer index");
            buf.bitmap[index].clear(std::memory_order_release);
            return Status::OK();
        }
    }
    return Status::InvalidArgument("Address not found in any stage buffer");
}

}  // namespace tent
}  // namespace mooncake
