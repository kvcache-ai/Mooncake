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

#ifndef MULTI_TRANSPORT_H_
#define MULTI_TRANSPORT_H_

#include <mutex>
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>

#include "trace_context.h"
#include "transport/transport.h"

namespace mooncake {
class MultiTransportTraceRegistry {
   public:
    using BatchID = Transport::BatchID;

    enum class SliceTerminalState {
        kPending,
        kCompleted,
        kFailed,
        kTimedOut,
    };

    struct TaskState {
        tracing::TraceContext context;
        std::string transport_name;
        size_t slice_count{0};
        size_t total_bytes{0};
        bool terminal_recorded{false};
        std::vector<bool> slice_queued;
        std::vector<SliceTerminalState> slice_terminal_states;
    };

    struct BatchState {
        tracing::TraceContext context;
        bool terminal_recorded{false};
        std::unordered_map<size_t, TaskState> tasks;
    };

    void RegisterBatch(BatchID batch_id, const tracing::TraceContext& context) {
        std::lock_guard<std::mutex> guard(mutex_);
        auto& batch = batches_[batch_id];
        if (!batch.context.valid()) {
            batch.context = context;
        }
    }

    void ClearBatch(BatchID batch_id) {
        std::lock_guard<std::mutex> guard(mutex_);
        batches_.erase(batch_id);
    }

    std::optional<tracing::TraceContext> LookupBatchContext(
        BatchID batch_id) const {
        std::lock_guard<std::mutex> guard(mutex_);
        auto it = batches_.find(batch_id);
        if (it == batches_.end()) {
            return std::nullopt;
        }
        return it->second.context;
    }

    void RegisterTask(BatchID batch_id, size_t task_id,
                      const tracing::TraceContext& context,
                      const std::string& transport_name, size_t slice_count,
                      size_t total_bytes) {
        std::lock_guard<std::mutex> guard(mutex_);
        auto& task = batches_[batch_id].tasks[task_id];
        task.context = context;
        task.transport_name = transport_name;
        task.slice_count = slice_count;
        task.total_bytes = total_bytes;
        task.terminal_recorded = false;
        task.slice_queued.assign(slice_count, false);
        task.slice_terminal_states.assign(slice_count,
                                          SliceTerminalState::kPending);
    }

    std::optional<TaskState> LookupTask(BatchID batch_id,
                                        size_t task_id) const {
        std::lock_guard<std::mutex> guard(mutex_);
        auto batch_it = batches_.find(batch_id);
        if (batch_it == batches_.end()) {
            return std::nullopt;
        }
        auto task_it = batch_it->second.tasks.find(task_id);
        if (task_it == batch_it->second.tasks.end()) {
            return std::nullopt;
        }
        return task_it->second;
    }

    bool MarkSliceQueued(BatchID batch_id, size_t task_id, size_t slice_id) {
        std::lock_guard<std::mutex> guard(mutex_);
        auto* task = FindTaskState(batch_id, task_id);
        if (!task || slice_id >= task->slice_queued.size()) {
            return false;
        }
        if (task->slice_queued[slice_id]) {
            return false;
        }
        task->slice_queued[slice_id] = true;
        return true;
    }

    bool MarkSliceTerminal(BatchID batch_id, size_t task_id, size_t slice_id,
                           Transport::Slice::SliceStatus status) {
        std::lock_guard<std::mutex> guard(mutex_);
        auto* task = FindTaskState(batch_id, task_id);
        if (!task || slice_id >= task->slice_terminal_states.size()) {
            return false;
        }
        if (task->slice_terminal_states[slice_id] !=
            SliceTerminalState::kPending) {
            return false;
        }
        task->slice_terminal_states[slice_id] = MapSliceStatus(status);
        return true;
    }

    bool MarkTaskTerminal(BatchID batch_id, size_t task_id) {
        std::lock_guard<std::mutex> guard(mutex_);
        auto* task = FindTaskState(batch_id, task_id);
        if (!task || task->terminal_recorded) {
            return false;
        }
        task->terminal_recorded = true;
        return true;
    }

    bool MarkBatchTerminal(BatchID batch_id) {
        std::lock_guard<std::mutex> guard(mutex_);
        auto it = batches_.find(batch_id);
        if (it == batches_.end() || it->second.terminal_recorded) {
            return false;
        }
        it->second.terminal_recorded = true;
        return true;
    }

   private:
    static SliceTerminalState MapSliceStatus(Transport::Slice::SliceStatus status) {
        switch (status) {
            case Transport::Slice::SUCCESS:
                return SliceTerminalState::kCompleted;
            case Transport::Slice::FAILED:
                return SliceTerminalState::kFailed;
            case Transport::Slice::TIMEOUT:
                return SliceTerminalState::kTimedOut;
            default:
                return SliceTerminalState::kPending;
        }
    }

    TaskState* FindTaskState(BatchID batch_id, size_t task_id) {
        auto batch_it = batches_.find(batch_id);
        if (batch_it == batches_.end()) {
            return nullptr;
        }
        auto task_it = batch_it->second.tasks.find(task_id);
        if (task_it == batch_it->second.tasks.end()) {
            return nullptr;
        }
        return &task_it->second;
    }

    mutable std::mutex mutex_;
    std::unordered_map<BatchID, BatchState> batches_;
};

class MultiTransport {
   public:
    using BatchID = Transport::BatchID;
    using TransferRequest = Transport::TransferRequest;
    using TransferStatus = Transport::TransferStatus;
    using BatchDesc = Transport::BatchDesc;

    MultiTransport(std::shared_ptr<TransferMetadata> metadata,
                   std::string &local_server_name);

    ~MultiTransport();

    BatchID allocateBatchID(size_t batch_size);

    Status freeBatchID(BatchID batch_id);

    Status submitTransfer(BatchID batch_id,
                          const std::vector<TransferRequest> &entries);

#ifdef ENABLE_MULTI_PROTOCOL
    Status mp_submitTransfer(BatchID batch_id,
                             const std::vector<TransferRequest> &entries,
                             std::string &proto);
#endif

    Status getTransferStatus(BatchID batch_id, size_t task_id,
                             TransferStatus &status);

    Status getBatchTransferStatus(BatchID batch_id, TransferStatus &status);

    void SetBatchTraceContext(BatchID batch_id,
                              const tracing::TraceContext& context) {
        trace_registry_.RegisterBatch(batch_id, context);
    }

    std::optional<tracing::TraceContext> LookupBatchTraceContext(
        BatchID batch_id) const {
        return trace_registry_.LookupBatchContext(batch_id);
    }

    std::optional<MultiTransportTraceRegistry::TaskState> LookupTaskTraceState(
        BatchID batch_id, size_t task_id) const {
        return trace_registry_.LookupTask(batch_id, task_id);
    }

    Transport *installTransport(const std::string &proto,
                                std::shared_ptr<Topology> topo);

    Transport *getTransport(const std::string &proto);

    std::vector<Transport *> listTransports();

    void *getBaseAddr();

   private:
    Status selectTransport(const TransferRequest &entry, Transport *&transport);

#ifdef ENABLE_MULTI_PROTOCOL
    Status mp_selectTransport(const TransferRequest &entry,
                              Transport *&transport,
                              std::string &preferred_proto);
#endif

   private:
    std::shared_ptr<TransferMetadata> metadata_;
    std::string local_server_name_;
    std::map<std::string, std::shared_ptr<Transport>> transport_map_;
    RWSpinlock batch_desc_lock_;
    std::unordered_map<BatchID, std::shared_ptr<BatchDesc>> batch_desc_set_;
    MultiTransportTraceRegistry trace_registry_;
};
}  // namespace mooncake

#endif  // MULTI_TRANSPORT_H_
