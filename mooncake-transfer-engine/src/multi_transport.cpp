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

#include "multi_transport.h"

#include "transport/rdma_transport/rdma_transport.h"
#include "transport/tcp_transport/tcp_transport.h"
#include "transport/transport.h"
#ifdef USE_NVMEOF
#include "transport/nvmeof_transport/nvmeof_transport.h"
#endif

namespace mooncake {
MultiTransport::MultiTransport(std::shared_ptr<TransferMetadata> metadata,
                               std::string &local_server_name)
    : metadata_(metadata), local_server_name_(local_server_name) {
    // ...
}

MultiTransport::~MultiTransport() {
    // ...
}

MultiTransport::BatchID MultiTransport::allocateBatchID(size_t batch_size) {
    auto batch_desc = new BatchDesc();
    if (!batch_desc) return ERR_MEMORY;
    batch_desc->id = BatchID(batch_desc);
    batch_desc->batch_size = batch_size;
    batch_desc->task_list.reserve(batch_size);
    batch_desc->context = NULL;
#ifdef CONFIG_USE_BATCH_DESC_SET
    batch_desc_lock_.lock();
    batch_desc_set_[batch_desc->id] = batch_desc;
    batch_desc_lock_.unlock();
#endif
    return batch_desc->id;
}

Status MultiTransport::freeBatchID(BatchID batch_id) {
    auto &batch_desc = *((BatchDesc *)(batch_id));
    const size_t task_count = batch_desc.task_list.size();
    for (size_t task_id = 0; task_id < task_count; task_id++) {
        if (!batch_desc.task_list[task_id].is_finished) {
            LOG(ERROR) << "BatchID cannot be freed until all tasks are done";
            return Status::BatchBusy(
                "BatchID cannot be freed until all tasks are done");
        }
    }
    delete &batch_desc;
#ifdef CONFIG_USE_BATCH_DESC_SET
    RWSpinlock::WriteGuard guard(batch_desc_lock_);
    batch_desc_set_.erase(batch_id);
#endif
    return Status::OK();
}

Status MultiTransport::submitTransfer(
    BatchID batch_id, const std::vector<TransferRequest> &entries) {
    auto &batch_desc = *((BatchDesc *)(batch_id));
    if (batch_desc.task_list.size() + entries.size() > batch_desc.batch_size) {
        LOG(ERROR) << "MultiTransport: Exceed the limitation of batch capacity";
        return Status::TooManyRequests(
            "Exceed the limitation of batch capacity");
    }

    size_t task_id = batch_desc.task_list.size();
    batch_desc.task_list.resize(task_id + entries.size());
    struct SubmitTasks {
        std::vector<TransferRequest *> request_list;
        std::vector<Transport::TransferTask *> task_list;
    };
    std::unordered_map<Transport *, SubmitTasks> submit_tasks;
    for (auto &request : entries) {
        auto transport = selectTransport(request);
        if (!transport) {
            return Status::InvalidArgument(
                "SelectTransport failed for SegmentID: " +
                std::to_string(request.target_id));
        }
        auto &task = batch_desc.task_list[task_id];
        task.batch_id = batch_id;
        ++task_id;
        submit_tasks[transport].request_list.push_back(
            (TransferRequest *)&request);
        submit_tasks[transport].task_list.push_back(&task);
    }
    for (auto &entry : submit_tasks) {
        auto status = entry.first->submitTransferTask(entry.second.request_list,
                                                      entry.second.task_list);
        if (!status.ok()) {
            LOG(ERROR) << "MultiTransport: Failed to submit transfer task to "
                       << entry.first->getName();
            return status;
        }
    }
    return Status::OK();
}

Status MultiTransport::getTransferStatus(BatchID batch_id, size_t task_id,
                                         TransferStatus &status) {
    auto &batch_desc = *((BatchDesc *)(batch_id));
    const size_t task_count = batch_desc.task_list.size();
    if (task_id >= task_count) {
        return Status::InvalidArgument(
            "MultiTransport: task id is equal to or larger than task_count");
    }
    auto &task = batch_desc.task_list[task_id];
    status.transferred_bytes = task.transferred_bytes;
    uint64_t success_slice_count = task.success_slice_count;
    uint64_t failed_slice_count = task.failed_slice_count;
    if (success_slice_count + failed_slice_count == task.slice_count) {
        if (failed_slice_count) {
            status.s = Transport::TransferStatusEnum::FAILED;
        } else {
            status.s = Transport::TransferStatusEnum::COMPLETED;
        }
        task.is_finished = true;
    } else {
        status.s = Transport::TransferStatusEnum::WAITING;
    }
    return Status::OK();
}

Transport *MultiTransport::installTransport(const std::string &proto,
                                            std::shared_ptr<Topology> topo) {
    Transport *transport = nullptr;
    if (std::string(proto) == "rdma") {
        transport = new RdmaTransport();
    } else if (std::string(proto) == "tcp") {
        transport = new TcpTransport();
    }
#ifdef USE_NVMEOF
    else if (std::string(proto) == "nvmeof") {
        transport = new NVMeoFTransport();
    }
#endif

    if (!transport) {
        LOG(ERROR) << "MultiTransport: Failed to initialize transport "
                   << proto;
        return nullptr;
    }

    Status status = transport->install(local_server_name_, metadata_, topo);
    if (!status.ok()) {
        return nullptr;
    }

    transport_map_[proto] = std::shared_ptr<Transport>(transport);
    return transport;
}

Transport *MultiTransport::selectTransport(const TransferRequest &entry) {
    if (entry.target_id == LOCAL_SEGMENT_ID && transport_map_.count("local"))
        return transport_map_["local"].get();
    auto target_segment_desc = metadata_->getSegmentDescByID(entry.target_id);
    if (!target_segment_desc) {
        LOG(ERROR) << "MultiTransport: Incorrect target segment id "
                   << entry.target_id;
        return nullptr;
    }
    auto proto = target_segment_desc->protocol;
    if (!transport_map_.count(proto)) {
        LOG(ERROR) << "MultiTransport: Transport " << proto << " not installed";
        return nullptr;
    }
    return transport_map_[proto].get();
}

Transport *MultiTransport::getTransport(const std::string &proto) {
    if (!transport_map_.count(proto)) return nullptr;
    return transport_map_[proto].get();
}

std::vector<Transport *> MultiTransport::listTransports() {
    std::vector<Transport *> transport_list;
    for (auto &entry : transport_map_)
        transport_list.push_back(entry.second.get());
    return transport_list;
}

}  // namespace mooncake