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

#include "tent/transport/tpu/tpu_transport.h"

#include <glog/logging.h>

#include "tent/common/status.h"
#include "tent/platform/tpu_pjrt_shim.h"
#include "tent/runtime/platform.h"
#include "tent/runtime/slab.h"

namespace mooncake {
namespace tent {

TpuTransport::TpuTransport() : installed_(false) {}

TpuTransport::~TpuTransport() { uninstall(); }

Status TpuTransport::install(std::string &local_segment_name,
                             std::shared_ptr<ControlService> metadata,
                             std::shared_ptr<Topology> local_topology,
                             std::shared_ptr<Config> conf) {
    if (installed_) {
        return Status::InvalidArgument(
            "TPU transport has been installed" LOC_MARK);
    }
    metadata_ = metadata;
    local_segment_name_ = local_segment_name;
    local_topology_ = local_topology;
    conf_ = conf;
    installed_ = true;
    // TPU HBM is not NIC-addressable: only the device<->host staging hop is
    // supported here. gpu_to_gpu stays false so the engine always stages
    // cross-node traffic through host DRAM.
    caps.gpu_to_dram = true;
    caps.dram_to_gpu = true;
    return Status::OK();
}

Status TpuTransport::uninstall() {
    if (installed_) {
        metadata_.reset();
        installed_ = false;
    }
    return Status::OK();
}

Status TpuTransport::allocateSubBatch(SubBatchRef &batch, size_t max_size) {
    auto tpu_batch = Slab<TpuSubBatch>::Get().allocate();
    if (!tpu_batch)
        return Status::InternalError(
            "Unable to allocate TPU sub-batch" LOC_MARK);
    batch = tpu_batch;
    tpu_batch->task_list.reserve(max_size);
    tpu_batch->max_size = max_size;
    return Status::OK();
}

Status TpuTransport::freeSubBatch(SubBatchRef &batch) {
    auto tpu_batch = dynamic_cast<TpuSubBatch *>(batch);
    if (!tpu_batch)
        return Status::InvalidArgument("Invalid TPU sub-batch" LOC_MARK);
    Slab<TpuSubBatch>::Get().deallocate(tpu_batch);
    batch = nullptr;
    return Status::OK();
}

Status TpuTransport::submitTransferTasks(
    SubBatchRef batch, const std::vector<Request> &request_list) {
    auto tpu_batch = dynamic_cast<TpuSubBatch *>(batch);
    if (!tpu_batch)
        return Status::InvalidArgument("Invalid TPU sub-batch" LOC_MARK);
    if (request_list.size() + tpu_batch->task_list.size() > tpu_batch->max_size)
        return Status::TooManyRequests("Exceed batch capacity" LOC_MARK);
    for (auto &request : request_list) {
        tpu_batch->task_list.push_back(TpuTask{});
        auto &task = tpu_batch->task_list[tpu_batch->task_list.size() - 1];
        task.request = request;
        task.status_word = TransferStatusEnum::PENDING;
        task.transferred_bytes = 0;
        startTransfer(&task, tpu_batch);
    }
    return Status::OK();
}

void TpuTransport::startTransfer(TpuTask *task, TpuSubBatch *batch) {
    // TpuTransport only handles the local device<->host staging hop, so the
    // target is always the local staging buffer (LOCAL_SEGMENT_ID). Anything
    // else indicates a routing bug: TPU HBM cannot be a remote transfer peer.
    if (task->request.target_id != LOCAL_SEGMENT_ID) {
        LOG(ERROR) << "TpuTransport: unexpected non-local target "
                   << task->request.target_id
                   << "; TPU only supports local staging copies";
        task->status_word = TransferStatusEnum::FAILED;
        task->transferred_bytes = 0;
        batch->notifyProgress();
        return;
    }

    void *staging = reinterpret_cast<void *>(task->request.target_offset);

    // Exactly one side of a staging hop is TPU HBM: the local stage copies
    // HBM<->host staging buffer, and a delegated remote stage copies the peer's
    // host staging buffer<->its HBM (so `staging` is the device side there).
    // Verify that here instead of relying on Platform::copy to classify: if the
    // adapter fails to recognise a device pointer -- e.g. it only matches base
    // addresses and ProxyManager handed us `base + chunk_offset` -- then
    // Platform::copy sees two host pointers and silently memcpy()s from a token
    // that is not the buffer's data, corrupting the transfer. Fail loudly.
    auto &shim = TpuPjrtShim::instance();
    const bool source_is_device = shim.isDevicePtr(task->request.source);
    const bool staging_is_device = shim.isDevicePtr(staging);
    if (source_is_device == staging_is_device) {
        LOG(ERROR)
            << "TpuTransport: a staging copy must have exactly one TPU "
               "device side, but source="
            << task->request.source << " (device=" << source_is_device
            << ") and target=" << staging << " (device=" << staging_is_device
            << "). Either the PJRT adapter is unavailable, or it does not "
               "resolve interior pointers (see tpu_pjrt_abi.h).";
        task->status_word = TransferStatusEnum::FAILED;
        task->transferred_bytes = 0;
        batch->notifyProgress();
        return;
    }

    Status status;
    if (task->request.opcode == Request::READ)
        // host staging buffer -> device (H2D)
        status = Platform::getLoader().copy(task->request.source, staging,
                                            task->request.length);
    else
        // device -> host staging buffer (D2H)
        status = Platform::getLoader().copy(staging, task->request.source,
                                            task->request.length);

    if (status.ok()) {
        task->transferred_bytes = task->request.length;
        task->status_word = TransferStatusEnum::COMPLETED;
    } else {
        LOG(WARNING) << "TpuTransport: staging copy failed: "
                     << status.ToString();
        task->status_word = TransferStatusEnum::FAILED;
    }
    batch->notifyProgress();
}

Status TpuTransport::getTransferStatus(SubBatchRef batch, int task_id,
                                       TransferStatus &status) {
    auto tpu_batch = dynamic_cast<TpuSubBatch *>(batch);
    if (!tpu_batch)
        return Status::InvalidArgument("Invalid TPU sub-batch" LOC_MARK);
    if (task_id < 0 || task_id >= (int)tpu_batch->task_list.size()) {
        return Status::InvalidArgument("Invalid task id" LOC_MARK);
    }
    auto &task = tpu_batch->task_list[task_id];
    status = TransferStatus{task.status_word, task.transferred_bytes};
    return Status::OK();
}

Status TpuTransport::addMemoryBuffer(BufferDesc &desc,
                                     const MemoryOptions &options) {
    LocationParser location(desc.location);
    // Tag both TPU device buffers and host staging buffers so the staging
    // policy can route the local HBM<->host hop through this transport. Routing
    // keys on the target (host) buffer's transports, so the host buffer must
    // carry TransportType::TPU as well (mirrors NVLinkTransport).
    if (location.type() != "tpu" && location.type() != "cpu" &&
        location.type() != kWildcardLocation) {
        return Status::OK();  // Not our buffer; leave it untagged.
    }
    desc.transports.push_back(TransportType::TPU);
    return Status::OK();
}

Status TpuTransport::removeMemoryBuffer(BufferDesc &desc) {
    return Status::OK();
}

}  // namespace tent
}  // namespace mooncake
