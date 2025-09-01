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

#include "v1/common/status.h"
#include "v1/memory/slab.h"
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
    installed_ = true;
    metadata_->setNotifyCallback([&](const Notification &message) -> int {
        RWSpinlock::WriteGuard guard(notify_lock_);
        notify_list_.push_back(message);
        return 0;
    });
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
    auto tcp_batch = Slab<TcpSubBatch>::Get().allocate();
    if (!tcp_batch)
        return Status::InternalError("Unable to allocate TCP sub-batch");
    batch = tcp_batch;
    tcp_batch->task_list.reserve(max_size);
    tcp_batch->max_size = max_size;
    return Status::OK();
}

Status TcpTransport::freeSubBatch(SubBatchRef &batch) {
    auto tcp_batch = dynamic_cast<TcpSubBatch *>(batch);
    if (!tcp_batch)
        return Status::InvalidArgument("Invalid TCP sub-batch" LOC_MARK);
    Slab<TcpSubBatch>::Get().deallocate(tcp_batch);
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

Status TcpTransport::addMemoryBuffer(BufferDesc &desc,
                                     const MemoryOptions &options) {
    return Status::OK();
}

Status TcpTransport::removeMemoryBuffer(BufferDesc &desc) {
    return Status::OK();
}

void TcpTransport::startTransfer(TcpTask *task) {
    std::string rpc_server_addr;
    auto status =
        findRemoteSegment(task->request.target_offset, task->request.length,
                          task->request.target_id, rpc_server_addr);
    if (!status.ok()) {
        task->status_word = TransferStatusEnum::FAILED;
        return;
    }
    if (task->request.opcode == Request::WRITE) {
        status =
            RpcClient::sendData(rpc_server_addr, task->request.target_offset,
                                task->request.source, task->request.length);
    } else {
        status =
            RpcClient::recvData(rpc_server_addr, task->request.target_offset,
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
    SegmentDesc *desc = nullptr;
    auto status = metadata_->segmentManager().getRemoteCached(desc, target_id);
    if (!status.ok()) return status;
    auto buffer = getBufferDesc(desc, dest_addr, length);
    if (!buffer)
        return Status::InvalidArgument(
            "Requested address is not in registered buffer" LOC_MARK);
    auto &detail = std::get<MemorySegmentDesc>(desc->detail);
    rpc_server_addr = detail.rpc_server_addr;
    return Status::OK();
}

Status TcpTransport::sendNotification(SegmentID target_id,
                                      const Notification &message) {
    std::string rpc_server_addr;
    SegmentDesc *desc = nullptr;
    auto status = metadata_->segmentManager().getRemoteCached(desc, target_id);
    if (!status.ok()) return status;
    if (desc->type != SegmentType::Memory)
        return Status::InvalidArgument("Not memory-kind segment" LOC_MARK);
    auto &detail = std::get<MemorySegmentDesc>(desc->detail);
    rpc_server_addr = detail.rpc_server_addr;
    return RpcClient::notify(rpc_server_addr, message);
}

Status TcpTransport::receiveNotification(
    std::vector<Notification> &notify_list) {
    RWSpinlock::ReadGuard guard(notify_lock_);
    notify_list.clear();
    notify_list.swap(notify_list_);
    return Status::OK();
}

}  // namespace v1
}  // namespace mooncake
