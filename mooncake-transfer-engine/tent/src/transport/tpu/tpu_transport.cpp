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

#include <exception>
#include <string>

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
    if (conf) {
        params_.max_concurrent_tasks =
            conf->get("transports/tpu/max_concurrent_tasks",
                      params_.max_concurrent_tasks);
    }
    if (params_.max_concurrent_tasks == 0) {
        return Status::InvalidArgument(
            "transports/tpu/max_concurrent_tasks must be greater than "
            "0" LOC_MARK);
    }
    shutting_down_.store(false, std::memory_order_release);
    thread_pool_ = std::make_unique<ThreadPool>(params_.max_concurrent_tasks);
    installed_ = true;
    // TPU HBM is not NIC-addressable: only the device<->host staging hop is
    // supported here. gpu_to_gpu stays false so the engine always stages
    // cross-node traffic through host DRAM.
    caps.gpu_to_dram = true;
    caps.dram_to_gpu = true;
    LOG(INFO) << "TPU transport installed: max_concurrent_tasks="
              << params_.max_concurrent_tasks;
    return Status::OK();
}

Status TpuTransport::uninstall() {
    if (installed_) {
        quiesce();
        metadata_.reset();
        installed_ = false;
    }
    return Status::OK();
}

Status TpuTransport::quiesce() {
    std::unique_ptr<ThreadPool> thread_pool;
    {
        std::lock_guard<std::mutex> guard(lifecycle_mutex_);
        shutting_down_.store(true, std::memory_order_release);
        thread_pool = std::move(thread_pool_);
    }
    // Join outside the lock; queued copies still run to a terminal status.
    thread_pool.reset();
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
    {
        std::lock_guard<std::mutex> guard(lifecycle_mutex_);
        if (shutting_down_.load(std::memory_order_acquire)) {
            return Status::InternalError(
                "TPU transport is shutting down" LOC_MARK);
        }
        if (request_list.size() + tpu_batch->task_list.size() >
            tpu_batch->max_size)
            return Status::TooManyRequests("Exceed batch capacity" LOC_MARK);

        // Pointers stay valid: task_list was reserved to max_size.
        const size_t start_idx = tpu_batch->task_list.size();
        for (auto &request : request_list) {
            tpu_batch->task_list.emplace_back();
            auto &task = tpu_batch->task_list.back();
            task.request = request;
            task.progress_batch_id = batch->progress_batch_id;
            task.notify_progress = batch->notify_progress;
        }
        for (size_t i = start_idx; i < tpu_batch->task_list.size(); ++i) {
            TpuTask *task = &tpu_batch->task_list[i];
            thread_pool_->enqueue([this, task]() { startTransfer(task); });
        }
    }
    return Status::OK();
}

void TpuTransport::startTransfer(TpuTask *task) {
    // A terminal status lets freeBatch() destroy the task at once, so own the
    // progress callback and touch no task field after publishing.
    auto notify_progress = std::move(task->notify_progress);
    const auto progress_batch_id = task->progress_batch_id;
    Status status;
    try {
        status = copy(task->request);
    } catch (const std::exception &error) {
        // enqueue() keeps an exception in a future that nobody reads.
        status = Status::InternalError(std::string("TPU copy threw: ") +
                                       error.what() + LOC_MARK);
    } catch (...) {
        status = Status::InternalError("TPU copy threw" LOC_MARK);
    }
    if (status.ok()) {
        // Store bytes before status: acquiring COMPLETED then sees them.
        task->transferred_bytes.store(task->request.length,
                                      std::memory_order_release);
        task->status_word.store(TransferStatusEnum::COMPLETED,
                                std::memory_order_release);
    } else {
        task->status_word.store(TransferStatusEnum::FAILED,
                                std::memory_order_release);
    }
    if (notify_progress) notify_progress(progress_batch_id);
}

Status TpuTransport::copy(const Request &request) {
    // TpuTransport only handles the local device<->host staging hop, so the
    // target is always the local staging buffer (LOCAL_SEGMENT_ID). Anything
    // else indicates a routing bug: TPU HBM cannot be a remote transfer peer.
    if (request.target_id != LOCAL_SEGMENT_ID) {
        LOG(ERROR) << "TpuTransport: unexpected non-local target "
                   << request.target_id
                   << "; TPU only supports local staging copies";
        return Status::InvalidArgument(
            "TPU only supports local staging copies" LOC_MARK);
    }

    void *staging = reinterpret_cast<void *>(request.target_offset);

    // Exactly one side of a staging hop is TPU HBM: the local stage copies
    // HBM<->host staging buffer, and a delegated remote stage copies the peer's
    // host staging buffer<->its HBM (so `staging` is the device side there).
    // Verify that here instead of relying on Platform::copy to classify: if the
    // adapter fails to recognise a device pointer -- e.g. it only matches base
    // addresses and ProxyManager handed us `base + chunk_offset` -- then
    // Platform::copy sees two host pointers and silently memcpy()s from a token
    // that is not the buffer's data, corrupting the transfer. Fail loudly.
    auto &shim = TpuPjrtShim::instance();
    const bool source_is_device = shim.isDevicePtr(request.source);
    const bool staging_is_device = shim.isDevicePtr(staging);
    if (source_is_device == staging_is_device) {
        LOG(ERROR)
            << "TpuTransport: a staging copy must have exactly one TPU "
               "device side, but source="
            << request.source << " (device=" << source_is_device
            << ") and target=" << staging << " (device=" << staging_is_device
            << "). Either the PJRT adapter is unavailable, or it does not "
               "resolve interior pointers (see tpu_pjrt_abi.h).";
        return Status::InvalidArgument(
            "TPU staging copy needs exactly one device side" LOC_MARK);
    }

    Status status;
    if (request.opcode == Request::READ)
        // host staging buffer -> device (H2D)
        status =
            Platform::getLoader().copy(request.source, staging, request.length);
    else
        // device -> host staging buffer (D2H)
        status =
            Platform::getLoader().copy(staging, request.source, request.length);

    if (!status.ok()) {
        LOG(WARNING) << "TpuTransport: staging copy failed: "
                     << status.ToString();
    }
    return status;
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
    status.s = task.status_word.load(std::memory_order_acquire);
    status.transferred_bytes =
        task.transferred_bytes.load(std::memory_order_acquire);
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
