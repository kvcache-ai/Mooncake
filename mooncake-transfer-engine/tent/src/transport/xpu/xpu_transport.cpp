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

#include "tent/transport/xpu/xpu_transport.h"

#include <glog/logging.h>

#include <algorithm>

#include "tent/common/status.h"
#include "tent/runtime/platform.h"
#include "tent/runtime/slab.h"

namespace mooncake {
namespace tent {

XpuTransport::XpuTransport() : installed_(false), platform_(nullptr) {}

XpuTransport::~XpuTransport() { uninstall(); }

Status XpuTransport::install(std::string &local_segment_name,
                             std::shared_ptr<ControlService> metadata,
                             std::shared_ptr<Topology> local_topology,
                             std::shared_ptr<Config> conf) {
    if (installed_) {
        return Status::InvalidArgument(
            "XPU transport has been installed" LOC_MARK);
    }
    // Bind to the concrete XpuPlatform (as NVLinkTransport binds to
    // CudaPlatform); every device operation is driven through it.
    platform_ = dynamic_cast<XpuPlatform *>(&Platform::getLoader());
    if (!platform_) {
        return Status::InvalidArgument(
            "XPU transport requires the XpuPlatform to be active" LOC_MARK);
    }
    metadata_ = metadata;
    local_segment_name_ = local_segment_name;
    local_topology_ = local_topology;
    conf_ = conf;
    installed_ = true;
    // XPU VRAM is not NIC-addressable in the MVP: only the device<->host
    // staging hop is supported here. gpu_to_gpu stays false so the engine
    // always stages cross-node traffic through host DRAM (direct VRAM RDMA and
    // PCIe P2P land in later PRs, as NVLinkTransport already exposes them).
    caps.gpu_to_dram = true;
    caps.dram_to_gpu = true;
    return Status::OK();
}

Status XpuTransport::uninstall() {
    if (installed_) {
        metadata_.reset();
        platform_ = nullptr;
        installed_ = false;
    }
    return Status::OK();
}

Status XpuTransport::allocateSubBatch(SubBatchRef &batch, size_t max_size) {
    auto xpu_batch = Slab<XpuSubBatch>::Get().allocate();
    if (!xpu_batch)
        return Status::InternalError(
            "Unable to allocate XPU sub-batch" LOC_MARK);
    batch = xpu_batch;
    xpu_batch->task_list.reserve(max_size);
    xpu_batch->max_size = max_size;
    return Status::OK();
}

Status XpuTransport::freeSubBatch(SubBatchRef &batch) {
    auto xpu_batch = dynamic_cast<XpuSubBatch *>(batch);
    if (!xpu_batch)
        return Status::InvalidArgument("Invalid XPU sub-batch" LOC_MARK);
    Slab<XpuSubBatch>::Get().deallocate(xpu_batch);
    batch = nullptr;
    return Status::OK();
}

Status XpuTransport::submitTransferTasks(
    SubBatchRef batch, const std::vector<Request> &request_list) {
    auto xpu_batch = dynamic_cast<XpuSubBatch *>(batch);
    if (!xpu_batch)
        return Status::InvalidArgument("Invalid XPU sub-batch" LOC_MARK);
    if (request_list.size() + xpu_batch->task_list.size() > xpu_batch->max_size)
        return Status::TooManyRequests("Exceed batch capacity" LOC_MARK);
    for (auto &request : request_list) {
        // emplace_back default-constructs in place: XpuTask holds std::atomic
        // members and is therefore non-movable, and the reserved capacity in
        // allocateSubBatch guarantees no reallocation.
        auto &task = xpu_batch->task_list.emplace_back();
        task.request = request;
        task.status_word = TransferStatusEnum::PENDING;
        task.transferred_bytes = 0;
        startTransfer(&task, xpu_batch);
    }
    return Status::OK();
}

void XpuTransport::startTransfer(XpuTask *task, XpuSubBatch *batch) {
    // XpuTransport only handles the local device<->host staging hop, so the
    // target is always the local staging buffer (LOCAL_SEGMENT_ID). Anything
    // else indicates a routing bug: XPU VRAM cannot be a remote transfer peer.
    if (task->request.target_id != LOCAL_SEGMENT_ID) {
        LOG(ERROR) << "XpuTransport: unexpected non-local target "
                   << task->request.target_id
                   << "; XPU only supports local staging copies";
        task->status_word = TransferStatusEnum::FAILED;
        task->transferred_bytes = 0;
        batch->notifyProgress();
        return;
    }

    void *staging = reinterpret_cast<void *>(task->request.target_offset);

    // Exactly one side of a staging hop is XPU VRAM: the local stage copies
    // VRAM<->host staging buffer, and a delegated remote stage copies the
    // peer's host staging buffer<->its VRAM (so `staging` is the device side
    // there). Classify through the bound XpuPlatform, which resolves both base
    // and interior USM pointers via its backend registry, so a device pointer
    // handed to us as `base + chunk_offset` is still recognised. If neither (or
    // both) side is device memory the request is malformed; fail loudly rather
    // than let a plain host memcpy corrupt the transfer.
    const bool source_is_device =
        platform_->getMemoryType(task->request.source) == MTYPE_XPU;
    const bool staging_is_device =
        platform_->getMemoryType(staging) == MTYPE_XPU;
    if (source_is_device == staging_is_device) {
        LOG(ERROR) << "XpuTransport: a staging copy must have exactly one XPU "
                      "device side, but source="
                   << task->request.source << " (device=" << source_is_device
                   << ") and target=" << staging
                   << " (device=" << staging_is_device
                   << "). This indicates a routing bug or an unclassified "
                      "external USM pointer.";
        task->status_word = TransferStatusEnum::FAILED;
        task->transferred_bytes = 0;
        batch->notifyProgress();
        return;
    }

    // Which side is the device depends on the stage (see above), so the
    // direction is expressed as dataflow: READ pulls `staging` into `source`,
    // WRITE pushes `source` into `staging`. platform_->copy() derives H2D/D2H
    // from the pointer classification.
    Status status;
    if (task->request.opcode == Request::READ)
        // staging -> source
        status = platform_->copy(task->request.source, staging,
                                 task->request.length);
    else
        // source -> staging
        status = platform_->copy(staging, task->request.source,
                                 task->request.length);

    if (status.ok()) {
        task->transferred_bytes = task->request.length;
        task->status_word = TransferStatusEnum::COMPLETED;
    } else {
        LOG(WARNING) << "XpuTransport: staging copy failed: "
                     << status.ToString();
        task->status_word = TransferStatusEnum::FAILED;
    }
    batch->notifyProgress();
}

Status XpuTransport::getTransferStatus(SubBatchRef batch, int task_id,
                                       TransferStatus &status) {
    auto xpu_batch = dynamic_cast<XpuSubBatch *>(batch);
    if (!xpu_batch)
        return Status::InvalidArgument("Invalid XPU sub-batch" LOC_MARK);
    if (task_id < 0 || task_id >= (int)xpu_batch->task_list.size()) {
        return Status::InvalidArgument("Invalid task id" LOC_MARK);
    }
    auto &task = xpu_batch->task_list[task_id];
    status =
        TransferStatus{task.status_word.load(), task.transferred_bytes.load()};
    return Status::OK();
}

Status XpuTransport::addMemoryBuffer(BufferDesc &desc,
                                     const MemoryOptions &options) {
    LocationParser location(desc.location);
    // Tag both XPU device buffers and host staging buffers so the staging
    // policy can route the local VRAM<->host hop through this transport.
    // Routing keys on the target (host) buffer's transports, so the host buffer
    // must carry TransportType::XPU as well (mirrors NVLinkTransport).
    if (location.type() != "xpu" && location.type() != "cpu" &&
        location.type() != kWildcardLocation) {
        return Status::OK();  // Not our buffer; leave it untagged.
    }
    // Idempotent: a buffer may be re-registered, so avoid duplicate XPU tags.
    if (std::find(desc.transports.begin(), desc.transports.end(),
                  TransportType::XPU) == desc.transports.end()) {
        desc.transports.push_back(TransportType::XPU);
    }
    return Status::OK();
}

Status XpuTransport::removeMemoryBuffer(BufferDesc &desc) {
    // Keep buffer metadata consistent with addMemoryBuffer by dropping the tag.
    desc.transports.erase(
        std::remove(desc.transports.begin(), desc.transports.end(),
                    TransportType::XPU),
        desc.transports.end());
    return Status::OK();
}

}  // namespace tent
}  // namespace mooncake
