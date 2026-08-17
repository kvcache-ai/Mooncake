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
#include "remote_stage_operation.h"
#include <algorithm>
#include <cstring>
#include <sstream>
#include <mutex>

namespace mooncake {
namespace tent {
ProxyManager::ProxyManager(TransferEngineImpl* impl, size_t chunk_size,
                           size_t chunk_count)
    : chunk_size_(chunk_size), chunk_count_(chunk_count), impl_(impl) {
    running_ = true;
    for (size_t i = 0; i < kShards; ++i) {
        shards_[i].thread = std::thread(&ProxyManager::runner, this, i);
    }
}

ProxyManager::~ProxyManager() { deconstruct(); }

void ProxyManager::flushPendingCleanups(bool unpin_remote_buffers) {
    if (!has_pending_cleanups_.load(std::memory_order_relaxed)) return;
    std::vector<Batch*> batches_to_release;
    std::vector<uint64_t> local_addrs_to_unpin;
    std::vector<std::pair<std::string, uint64_t>> remote_addrs_to_unpin;
    {
        std::lock_guard<std::mutex> lk(pending_cleanups_mu_);
        if (pending_cleanups_.empty()) {
            has_pending_cleanups_.store(false, std::memory_order_relaxed);
            return;
        }
        for (auto it = pending_cleanups_.begin();
             it != pending_cleanups_.end();) {
            for (auto batch_it = it->batches.begin();
                 batch_it != it->batches.end();) {
                TransferStatus status;
                auto poll_status = impl_->progressBatch(*batch_it, status);
                if (!poll_status.ok()) {
                    LOG_EVERY_N(WARNING, 100)
                        << "Failed to poll deferred staging batch: "
                        << poll_status;
                    ++batch_it;
                    continue;
                }
                if (status.s == PENDING) {
                    ++batch_it;
                    continue;
                }
                batches_to_release.push_back((Batch*)*batch_it);
                batch_it = it->batches.erase(batch_it);
            }

            internal::pollRemoteOperations(it->remote_operations);
            if (it->batches.empty()) {
                local_addrs_to_unpin.insert(local_addrs_to_unpin.end(),
                                            it->local_addrs.begin(),
                                            it->local_addrs.end());
                it->local_addrs.clear();
                if (it->remote_operations.empty() && unpin_remote_buffers) {
                    remote_addrs_to_unpin.insert(remote_addrs_to_unpin.end(),
                                                 it->remote_addrs.begin(),
                                                 it->remote_addrs.end());
                    it->remote_addrs.clear();
                }
            }

            if (it->local_addrs.empty() && it->remote_addrs.empty() &&
                it->batches.empty() && it->remote_operations.empty()) {
                it = pending_cleanups_.erase(it);
                continue;
            }
            ++it;
        }
        if (pending_cleanups_.empty()) {
            has_pending_cleanups_.store(false, std::memory_order_relaxed);
        }
    }

    for (auto batch : batches_to_release) impl_->releaseBatch(batch);
    for (auto addr : local_addrs_to_unpin) unpinStageBuffer(addr);
    for (auto& [server, addr] : remote_addrs_to_unpin) {
        ControlClient::unpinStageBuffer(server, addr);
    }
}

Status ProxyManager::deconstruct() {
    if (!running_.exchange(false, std::memory_order_acq_rel)) {
        return Status::OK();
    }
    for (size_t i = 0; i < kShards; ++i) {
        shards_[i].cv.notify_all();
        if (shards_[i].thread.joinable()) shards_[i].thread.join();
    }

    auto has_pending_batches = [this]() {
        std::lock_guard<std::mutex> lk(pending_cleanups_mu_);
        return std::any_of(
            pending_cleanups_.begin(), pending_cleanups_.end(),
            [](const PendingBufferCleanup& p) { return !p.batches.empty(); });
    };

    flushPendingCleanups(false);
    while (has_pending_batches()) {
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
        flushPendingCleanups(false);
    }

    flushPendingCleanups(false);
    std::vector<PendingBufferCleanup> remaining;
    {
        std::lock_guard<std::mutex> lk(pending_cleanups_mu_);
        remaining = std::move(pending_cleanups_);
        pending_cleanups_.clear();
        has_pending_cleanups_.store(false, std::memory_order_relaxed);
    }
    for (auto& pending : remaining) {
        if (!pending.remote_operations.empty()) {
            LOG(WARNING)
                << "Leaving " << pending.remote_addrs.size()
                << " remote stage buffers pinned because "
                << pending.remote_operations.size()
                << " remote staging requests have not reached a confirmed "
                   "terminal state";
            continue;
        }
        for (auto& [server, addr] : pending.remote_addrs) {
            ControlClient::unpinStageBuffer(server, addr);
        }
    }
    std::unique_lock<std::shared_mutex> guard(stage_buffers_mutex_);
    for (auto entry : stage_buffers_) {
        impl_->unregisterLocalMemory(entry.second.chunks);
        impl_->freeLocalMemory(entry.second.chunks);
        delete[] entry.second.bitmap;
    }
    stage_buffers_.clear();
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
    Request local_stage;
    local_stage.opcode = request.opcode;
    local_stage.source = (uint8_t*)request.source + offset;
    local_stage.length = chunk_length;
    local_stage.target_id = LOCAL_SEGMENT_ID;
    local_stage.target_offset = local_stage_buffer;
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
    Request remote_stage;
    remote_stage.opcode = request.opcode;
    remote_stage.source = (void*)remote_stage_buffer;
    remote_stage.length = chunk_length;
    remote_stage.target_id = LOCAL_SEGMENT_ID;
    remote_stage.target_offset = request.target_offset + offset;
    return ControlClient::delegate(server_addr, remote_stage);
}

void ProxyManager::submitRemoteStage(
    const std::string& server_addr, const Request& request,
    uint64_t remote_stage_buffer, uint64_t chunk_length, uint64_t offset,
    std::shared_ptr<internal::RemoteStageOperation>& operation) {
    Request remote_stage;
    remote_stage.opcode = request.opcode;
    remote_stage.source = (void*)remote_stage_buffer;
    remote_stage.length = chunk_length;
    remote_stage.target_id = LOCAL_SEGMENT_ID;
    remote_stage.target_offset = request.target_offset + offset;
    operation = std::make_shared<internal::RemoteStageOperation>();
    ControlClient::delegateAsync(
        server_addr, remote_stage,
        [operation](Status status) { operation->complete(std::move(status)); });
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
    StagingTask staging_task;
    staging_task.native = task;
    staging_task.batch = batch;
    staging_task.params = params;
    task->staging_status.store(PENDING, std::memory_order_relaxed);
    static std::atomic<size_t> next_queue_index(0);
    thread_local size_t id = next_queue_index.fetch_add(1) % kShards;
    {
        std::lock_guard<std::mutex> lk(shards_[id].mu);
        shards_[id].queue.push(staging_task);
    }
    shards_[id].cv.notify_one();
    return Status::OK();
}

Status ProxyManager::getStatus(TaskInfo* task, TransferStatus& task_status) {
    if (!task || !task->staging) return Status::InvalidArgument("Invalid task");
    task_status.s = task->staging_status.load(std::memory_order_acquire);
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
    auto& shard = shards_[id];
    while (running_.load(std::memory_order_acquire)) {
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
        flushPendingCleanups();
        StageBufferCache cache(*this);
        bool buffers_safe_to_release = true;
        std::vector<BatchID> undrained_batches;
        std::vector<std::shared_ptr<internal::RemoteStageOperation>>
            undrained_remote_operations;
        auto status =
            transferEventLoop(task, &cache, buffers_safe_to_release,
                              undrained_batches, undrained_remote_operations);
        if (buffers_safe_to_release) {
            cache.reset();
        } else {
            LOG(WARNING) << "Staging cleanup could not drain all in-flight "
                            "operations; deferring stage buffer cleanup";
            PendingBufferCleanup pending;
            for (auto& entry : cache.local_stage_buffers) {
                pending.local_addrs.push_back(entry.second);
            }
            for (auto& server_entry : cache.remote_stage_buffers) {
                for (auto& entry : server_entry.second) {
                    pending.remote_addrs.push_back(
                        {server_entry.first, entry.second});
                }
            }
            pending.batches = std::move(undrained_batches);
            pending.remote_operations = std::move(undrained_remote_operations);
            {
                std::lock_guard<std::mutex> lk(pending_cleanups_mu_);
                pending_cleanups_.push_back(std::move(pending));
                has_pending_cleanups_.store(true, std::memory_order_relaxed);
            }
        }
        task.native->staging_status.store(status.ok() ? COMPLETED : FAILED,
                                          std::memory_order_release);
        impl_->notifyBatchMaybeReady(task.batch);
    }
}

Status ProxyManager::transferEventLoop(
    StagingTask& task, StageBufferCache* cache, bool& buffers_safe_to_release,
    std::vector<BatchID>& undrained_batches,
    std::vector<std::shared_ptr<internal::RemoteStageOperation>>&
        undrained_remote_operations) {
    buffers_safe_to_release = true;
    auto& request = task.native->request;
    auto server_addr = task.params[0];
    bool local_staging = !task.params[1].empty();
    bool remote_staging = !task.params[2].empty();
    const size_t kStageBuffers =
        std::min(chunk_count_, static_cast<size_t>(16));
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
    std::vector<std::shared_ptr<internal::RemoteStageOperation>>
        remote_operations(chunks.size());
    auto drain_batch = [&](BatchID batch) {
        TransferStatus xfer_status;
        auto status = impl_->progressBatch(batch, xfer_status);
        if (!status.ok()) {
            LOG(ERROR) << "Failed to poll in-flight staging batch: " << status;
            impl_->freeBatch(batch);
            return false;
        }
        auto free_status = impl_->freeBatch(batch);
        if (!free_status.ok()) {
            LOG(WARNING) << "Failed to free drained staging batch: "
                         << free_status;
        }
        return xfer_status.s != PENDING;
    };
    auto cleanup_inflight = [&]() {
        bool all_drained = true;
        for (auto& chunk : chunks) {
            if (!chunk.batch) continue;
            Batch* bptr = nullptr;
            auto retain_status = impl_->retainBatch(chunk.batch, bptr);
            if (drain_batch(chunk.batch)) {
                if (retain_status.ok() && bptr) {
                    impl_->releaseBatch(bptr);
                }
                chunk.batch = 0;
            } else {
                all_drained = false;
                if (retain_status.ok() && bptr) {
                    undrained_batches.push_back(chunk.batch);
                }
            }
        }
        if (!internal::pollRemoteOperations(remote_operations)) {
            all_drained = false;
            for (auto& operation : remote_operations) {
                if (operation) {
                    undrained_remote_operations.push_back(std::move(operation));
                }
            }
            remote_operations.clear();
        }
        return all_drained;
    };

    // The loop below can leave through the CHECK_STATUS on progressBatch or
    // through the FAILED branch, which only drains the queue. Either way the
    // chunks still in flight own a batch that nobody would free.
    struct PendingBatches {
        TransferEngineImpl* impl;
        std::vector<Chunk>& chunks;
        ~PendingBatches() {
            for (auto& chunk : chunks) {
                if (!chunk.batch) continue;
                auto status = impl->freeBatch(chunk.batch);
                if (!status.ok())
                    LOG(WARNING)
                        << "failed to free chunk batch: " << status.ToString();
                chunk.batch = 0;
            }
        }
    } pending_batches{impl_, chunks};

    // An in-flight chunk goes straight back on the queue, so this loop spins
    // with nothing to do -- and the INFLIGHT case takes progress_mutex_ every
    // pass. Back off only after a whole sweep advanced no chunk, so a queue
    // that is progressing still runs at full speed.
    size_t sweep_remaining = event_queue.size();
    bool swept_progress = false;
    uint64_t idle_sweeps = 0;

    while (!event_queue.empty()) {
        if (!running_.load(std::memory_order_acquire)) {
            buffers_safe_to_release = cleanup_inflight();
            return Status::InternalError("Proxy manager is shutting down");
        }
        auto id = event_queue.front();
        auto& chunk = chunks[id];
        const auto state_before = chunk.state;
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
                                      remote_operations[id]);
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
                                      remote_operations[id]);
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
                auto status = impl_->progressBatch(chunk.batch, xfer_status);
                if (!status.ok()) {
                    buffers_safe_to_release = cleanup_inflight();
                    return status;
                }
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
                buffers_safe_to_release = cleanup_inflight();
                return Status::InternalError(
                    "Proxy event loop in failed state");
            }

            case StageState::INFLIGHT_REMOTE: {
                auto& operation = remote_operations[id];
                if (!operation) {
                    chunk.state = StageState::FAILED;
                    event_queue.push(id);
                    break;
                }
                auto result = operation->tryTakeResult();
                if (result) {
                    operation.reset();
                    if (!result->ok()) {
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

        if (chunk.state != state_before) swept_progress = true;
        if (sweep_remaining > 0) --sweep_remaining;
        if (sweep_remaining == 0) {
            if (swept_progress)
                idle_sweeps = 0;
            else
                waitBeforeNextPoll(idle_sweeps++);
            swept_progress = false;
            sweep_remaining = event_queue.size();
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
    // Held across the slow allocate + register, which run once per location.
    // Racing instead would register the same hundreds of MB twice, then throw
    // one away.
    std::unique_lock<std::shared_mutex> guard(stage_buffers_mutex_);
    if (stage_buffers_.count(location)) return Status::OK();
    StageBuffers buf;
    auto total_size = chunk_size_ * chunk_count_;
    CHECK_STATUS(
        impl_->allocateLocalMemory(&buf.chunks, total_size, location, true));
    CHECK_STATUS(impl_->registerLocalMemory(buf.chunks, total_size));
    buf.bitmap = new std::atomic_flag[chunk_count_];
    for (size_t i = 0; i < chunk_count_; ++i)
        buf.bitmap[i].clear(std::memory_order_relaxed);
    stage_buffers_[location] = std::move(buf);
    return Status::OK();
}

Status ProxyManager::freeStageBuffers(const std::string& location) {
    std::unique_lock<std::shared_mutex> guard(stage_buffers_mutex_);
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
    {
        std::shared_lock<std::shared_mutex> guard(stage_buffers_mutex_);
        auto it = stage_buffers_.find(location);
        if (it == stage_buffers_.end()) {
            guard.unlock();
            CHECK_STATUS(allocateStageBuffers(location));
            guard.lock();
            it = stage_buffers_.find(location);
            if (it == stage_buffers_.end())
                return Status::InternalError(
                    "Stage buffer disappeared" LOC_MARK);
        }

        auto& buf = it->second;
        for (size_t i = 0; i < chunk_count_; ++i) {
            if (!buf.bitmap[i].test_and_set(std::memory_order_acquire)) {
                addr = reinterpret_cast<uint64_t>(
                    static_cast<char*>(buf.chunks) + i * chunk_size_);
                return Status::OK();
            }
        }
    }

    if (has_pending_cleanups_.load(std::memory_order_relaxed)) {
        flushPendingCleanups();
        std::shared_lock<std::shared_mutex> guard(stage_buffers_mutex_);
        auto it = stage_buffers_.find(location);
        if (it != stage_buffers_.end()) {
            auto& buf = it->second;
            for (size_t i = 0; i < chunk_count_; ++i) {
                if (!buf.bitmap[i].test_and_set(std::memory_order_acquire)) {
                    addr = reinterpret_cast<uint64_t>(
                        static_cast<char*>(buf.chunks) + i * chunk_size_);
                    return Status::OK();
                }
            }
        }
    }

    return Status::TooManyRequests("No available stage buffer in " + location);
}

Status ProxyManager::unpinStageBuffer(uint64_t addr) {
    std::shared_lock<std::shared_mutex> guard(stage_buffers_mutex_);
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

}  // namespace tent
}  // namespace mooncake
