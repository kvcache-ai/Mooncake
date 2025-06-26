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

#include "v1/transport/tcp/tcp_transport.h"

#include <bits/stdint-uintn.h>
#include <glog/logging.h>

#include <algorithm>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <iomanip>
#include <memory>

#include "v1/common.h"
#include "v1/metadata/metadata.h"

namespace mooncake {
namespace v1 {
TcpTransport::TcpTransport() : installed_(false) {}

TcpTransport::~TcpTransport() { uninstall(); }

Status TcpTransport::install(std::string &local_segment_name,
                             std::shared_ptr<MetadataService> metadata,
                             std::shared_ptr<Topology> local_topology,
                             std::shared_ptr<ConfigManager> conf) {
    if (installed_) {
        return Status::InvalidArgument(
            "TCP transport has been installed" LOC_MARK);
    }

    metadata_ = metadata;
    local_segment_name_ = local_segment_name;
    local_topology_ = local_topology;
    auto status = setupLocalSegment();
    if (!status.ok()) return status;

    installed_ = true;
    return Status::OK();
}

Status TcpTransport::uninstall() {
    if (installed_) {
        metadata_.reset();
        installed_ = false;
    }
    return Status::OK();
}

Status TcpTransport::allocateSubBatch(SubBatchRef &batch, size_t max_size) {
    auto tcp_batch = new TcpSubBatch();
    batch = tcp_batch;
    tcp_batch->task_list.reserve(max_size);
    tcp_batch->max_size = max_size;
    return Status::OK();
}

Status TcpTransport::freeSubBatch(SubBatchRef &batch) {
    auto tcp_batch = dynamic_cast<TcpSubBatch *>(batch);
    if (!tcp_batch)
        return Status::InvalidArgument("Invalid TCP sub-batch" LOC_MARK);
    delete tcp_batch;
    batch = nullptr;
    return Status::OK();
}

Status TcpTransport::submitTransferTasks(
    SubBatchRef batch, const std::vector<Request> &request_list) {
    auto tcp_batch = dynamic_cast<TcpSubBatch *>(batch);
    if (!tcp_batch)
        return Status::InvalidArgument("Invalid TCP sub-batch" LOC_MARK);
    if (request_list.size() + tcp_batch->task_list.size() > tcp_batch->max_size)
        return Status::TooManyRequests("Exceed batch capacity" LOC_MARK);
    for (auto &request : request_list) {
        tcp_batch->task_list.push_back(TcpTask{});
        auto &task = tcp_batch->task_list[tcp_batch->task_list.size() - 1];
        uint64_t target_addr = request.target_offset;
        task.target_addr = target_addr;
        task.request = request;
        task.status_word = TransferStatusEnum::PENDING;
        startTransfer(&task);
    }
    return Status::OK();
}

Status TcpTransport::getTransferStatus(SubBatchRef batch, int task_id,
                                       TransferStatus &status) {
    auto tcp_batch = dynamic_cast<TcpSubBatch *>(batch);
    if (task_id < 0 || task_id >= (int)tcp_batch->task_list.size()) {
        return Status::InvalidArgument("Invalid task id" LOC_MARK);
    }
    auto &task = tcp_batch->task_list[task_id];
    status = TransferStatus{task.status_word, task.transferred_bytes};
    return Status::OK();
}

void TcpTransport::queryOutstandingTasks(SubBatchRef batch,
                                         std::vector<int> &task_id_list) {
    auto tcp_batch = dynamic_cast<TcpSubBatch *>(batch);
    if (!tcp_batch) return;
    for (int task_id = 0; task_id < (int)tcp_batch->task_list.size();
         ++task_id) {
        auto &task = tcp_batch->task_list[task_id];
        if (task.status_word != TransferStatusEnum::COMPLETED ||
            task.status_word != TransferStatusEnum::FAILED) {
            task_id_list.push_back(task_id);
        }
    }
}

Status TcpTransport::registerLocalMemory(
    const std::vector<BufferEntry> &buffer_list) {
    auto &manager = metadata_->segmentManager();
    auto segment = manager.getLocal();
    auto &current_buffer_list =
        std::get<MemorySegmentDesc>(segment->detail).buffers;
    for (auto &buffer : buffer_list) {
        BufferDesc desc;
        desc.type = MemBufferType::TCP;
        desc.addr = (uint64_t)buffer.addr;
        desc.length = buffer.length;
        desc.location = buffer.location;
        current_buffer_list.push_back(desc);
    }
    return manager.synchronizeLocal();
}

Status TcpTransport::unregisterLocalMemory(
    const std::vector<BufferEntry> &buffer_list) {
    auto &manager = metadata_->segmentManager();
    auto segment = manager.getLocal();
    auto &current_buffer_list =
        std::get<MemorySegmentDesc>(segment->detail).buffers;
    for (auto &buffer : buffer_list) {
        for (auto it = current_buffer_list.begin();
             it != current_buffer_list.end(); ++it) {
            if (it->addr == (uint64_t)buffer.addr &&
                it->length == buffer.length && it->type == MemBufferType::TCP) {
                it = current_buffer_list.erase(it);
            }
        }
    }
    return manager.synchronizeLocal();
}

Status TcpTransport::setupLocalSegment() { return Status::OK(); }

void TcpTransport::startTransfer(TcpTask *task) {
    RpcClient client;
    std::string rpc_server_addr;
    auto status =
        findRemoteSegment(task->request.target_offset, task->request.length,
                          task->request.target_id, rpc_server_addr);
    if (!status.ok()) {
        task->status_word = TransferStatusEnum::FAILED;
        return;
    }
    if (task->request.opcode == Request::WRITE) {
        status = client.sendData(rpc_server_addr, task->request.target_offset,
                                 task->request.source, task->request.length);
    } else {
        status = client.recvData(rpc_server_addr, task->request.target_offset,
                                 task->request.source, task->request.length);
    }
    if (!status.ok()) {
        task->status_word = TransferStatusEnum::FAILED;
        return;
    }
    task->transferred_bytes = task->request.length;
    task->status_word = TransferStatusEnum::COMPLETED;
}

Status TcpTransport::findRemoteSegment(uint64_t dest_addr, uint64_t length,
                                       uint64_t target_id,
                                       std::string &rpc_server_addr) {
    SegmentDescRef desc;
    auto status = metadata_->segmentManager().getRemote(desc, target_id);
    if (!status.ok()) return status;
    int index = 0;
    auto &detail = std::get<MemorySegmentDesc>(desc->detail);
    for (auto &entry : detail.buffers) {
        if (!entry.shared_handle.empty() && entry.addr <= dest_addr &&
            dest_addr + length <= entry.addr + entry.length) {
            rpc_server_addr = detail.rpc_server_addr;
            return Status::OK();
        }
        index++;
    }
    return Status::InvalidArgument(
        "Requested address is not in registered buffer" LOC_MARK);
}

}  // namespace v1
}  // namespace mooncake
