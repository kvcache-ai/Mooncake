// Copyright 2026 KVCache.AI
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

#include "tent/transport/mlu/mlu_transport.h"

#include <glog/logging.h>

#include <algorithm>

#include "tent/common/status.h"
#include "tent/runtime/platform.h"
#include "tent/runtime/slab.h"

namespace mooncake {
namespace tent {

Status MluTransport::install(std::string & /*local_segment_name*/,
                             std::shared_ptr<ControlService> /*metadata*/,
                             std::shared_ptr<Topology> /*local_topology*/,
                             std::shared_ptr<Config> /*conf*/) {
    if (installed_) {
        return Status::InvalidArgument(
            "MLU transport has been installed" LOC_MARK);
    }
    platform_ = dynamic_cast<MluPlatform *>(&Platform::getLoader());
    if (!platform_) {
        return Status::InvalidArgument(
            "MLU transport requires the MluPlatform to be active" LOC_MARK);
    }
    installed_ = true;
    caps.gpu_to_dram = true;
    caps.dram_to_gpu = true;
    return Status::OK();
}

Status MluTransport::uninstall() {
    platform_ = nullptr;
    installed_ = false;
    return Status::OK();
}

Status MluTransport::allocateSubBatch(SubBatchRef &batch, size_t max_size) {
    auto mlu_batch = Slab<MluSubBatch>::Get().allocate();
    if (!mlu_batch)
        return Status::InternalError(
            "Unable to allocate MLU sub-batch" LOC_MARK);
    batch = mlu_batch;
    mlu_batch->task_list.reserve(max_size);
    mlu_batch->max_size = max_size;
    return Status::OK();
}

Status MluTransport::freeSubBatch(SubBatchRef &batch) {
    auto mlu_batch = dynamic_cast<MluSubBatch *>(batch);
    if (!mlu_batch)
        return Status::InvalidArgument("Invalid MLU sub-batch" LOC_MARK);
    Slab<MluSubBatch>::Get().deallocate(mlu_batch);
    batch = nullptr;
    return Status::OK();
}

Status MluTransport::submitTransferTasks(
    SubBatchRef batch, const std::vector<Request> &request_list) {
    if (!installed_)
        return Status::InvalidArgument("MLU transport is not installed");
    auto mlu_batch = dynamic_cast<MluSubBatch *>(batch);
    if (!mlu_batch)
        return Status::InvalidArgument("Invalid MLU sub-batch" LOC_MARK);
    if (request_list.size() > mlu_batch->max_size - mlu_batch->task_list.size())
        return Status::TooManyRequests("Exceed batch capacity" LOC_MARK);
    for (auto &request : request_list) {
        auto &task = mlu_batch->task_list.emplace_back();
        task.request = request;
        task.status_word = TransferStatusEnum::PENDING;
        task.transferred_bytes = 0;
        startTransfer(&task, mlu_batch);
    }
    return Status::OK();
}

void MluTransport::startTransfer(MluTask *task, MluSubBatch *batch) {
    if (task->request.target_id != LOCAL_SEGMENT_ID ||
        (task->request.opcode != Request::READ &&
         task->request.opcode != Request::WRITE)) {
        LOG(ERROR) << "MluTransport: unexpected non-local target "
                   << task->request.target_id
                   << "; MLU only supports local staging copies";
        task->status_word = TransferStatusEnum::FAILED;
        task->transferred_bytes = 0;
        batch->notifyProgress();
        return;
    }

    void *staging = reinterpret_cast<void *>(task->request.target_offset);

    const bool source_is_device =
        platform_->getMemoryType(task->request.source) == MTYPE_MLU;
    const bool staging_is_device =
        platform_->getMemoryType(staging) == MTYPE_MLU;
    if (source_is_device == staging_is_device) {
        LOG(ERROR) << "MluTransport: a staging copy must have exactly one MLU "
                      "device side, but source="
                   << task->request.source << " (device=" << source_is_device
                   << ") and target=" << staging
                   << " (device=" << staging_is_device
                   << "). This indicates a routing bug or an unclassified "
                      "external device pointer.";
        task->status_word = TransferStatusEnum::FAILED;
        task->transferred_bytes = 0;
        batch->notifyProgress();
        return;
    }

    Status status;
    if (task->request.opcode == Request::READ)
        status = platform_->copy(task->request.source, staging,
                                 task->request.length);
    else
        status = platform_->copy(staging, task->request.source,
                                 task->request.length);

    if (status.ok()) {
        task->transferred_bytes = task->request.length;
        task->status_word = TransferStatusEnum::COMPLETED;
    } else {
        LOG(WARNING) << "MluTransport: staging copy failed: "
                     << status.ToString();
        task->status_word = TransferStatusEnum::FAILED;
    }
    batch->notifyProgress();
}

Status MluTransport::getTransferStatus(SubBatchRef batch, int task_id,
                                       TransferStatus &status) {
    auto mlu_batch = dynamic_cast<MluSubBatch *>(batch);
    if (!mlu_batch)
        return Status::InvalidArgument("Invalid MLU sub-batch" LOC_MARK);
    if (task_id < 0 || task_id >= (int)mlu_batch->task_list.size()) {
        return Status::InvalidArgument("Invalid task id" LOC_MARK);
    }
    auto &task = mlu_batch->task_list[task_id];
    status =
        TransferStatus{task.status_word.load(), task.transferred_bytes.load()};
    return Status::OK();
}

Status MluTransport::addMemoryBuffer(BufferDesc &desc,
                                     const MemoryOptions & /*options*/) {
    LocationParser location(desc.location);
    if (location.type() != "mlu" && location.type() != "cpu" &&
        location.type() != kWildcardLocation) {
        return Status::OK();
    }
    if (std::find(desc.transports.begin(), desc.transports.end(),
                  TransportType::MLU) == desc.transports.end()) {
        desc.transports.push_back(TransportType::MLU);
    }
    return Status::OK();
}

Status MluTransport::removeMemoryBuffer(BufferDesc &desc) {
    desc.transports.erase(
        std::remove(desc.transports.begin(), desc.transports.end(),
                    TransportType::MLU),
        desc.transports.end());
    return Status::OK();
}

}  // namespace tent
}  // namespace mooncake
