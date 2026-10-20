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
#include <cnrt.h>

#include <algorithm>
#include <utility>

#include "tent/common/status.h"
#include "tent/runtime/platform.h"
#include "tent/runtime/slab.h"

namespace mooncake {
namespace tent {
namespace {
Status driverStatus(CNresult result) {
    if (result == CN_SUCCESS) return Status::OK();
    const char *message = nullptr;
    cnGetErrorString(result, &message);
    return Status::InternalError(std::string("CNDRV: ") +
                                 (message ? message : std::to_string(result)));
}

void logError(const Status &status) {
    if (!status.ok()) LOG(ERROR) << status.ToString();
}

class ContextGuard {
   public:
    ContextGuard() : status_(driverStatus(cnCtxGetCurrent(&previous_))) {}
    ~ContextGuard() {
        if (status_.ok()) logError(driverStatus(cnCtxSetCurrent(previous_)));
    }
    Status use(CNcontext context) {
        CHECK_STATUS(status_);
        return driverStatus(cnCtxSetCurrent(context));
    }

   private:
    CNcontext previous_ = nullptr;
    Status status_;
};
}  // namespace

Status MluCopyQueue::synchronize() {
    std::lock_guard<std::mutex> lock(mutex);
    ContextGuard guard;
    CHECK_STATUS(guard.use(context));
    return driverStatus(cnQueueSync(handle));
}

MluCopyQueue::~MluCopyQueue() {
    if (!handle) return;
    logError(synchronize());
    ContextGuard guard;
    auto status = guard.use(context);
    if (status.ok()) status = driverStatus(cnDestroyQueue(handle));
    logError(status);
}

MluTask::MluTask(MluTask &&other) noexcept
    : request(other.request),
      status_word(other.status_word.load()),
      transferred_bytes(other.transferred_bytes.load()),
      queue(std::move(other.queue)),
      completion(std::exchange(other.completion, nullptr)),
      drain_on_error(other.drain_on_error) {}

MluTask::~MluTask() {
    if (!completion) return;
    // Destroying a notifier does not wait for its copy. A caller may free a
    // sub-batch before polling completion, so drain before releasing resources.
    if (status_word.load() == PENDING) logError(queue->synchronize());
    ContextGuard guard;
    auto status = guard.use(queue->context);
    if (status.ok()) status = driverStatus(cnDestroyNotifier(completion));
    logError(status);
}

MluTransport::~MluTransport() { logError(uninstall()); }

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
    CHECK_STATUS(quiesce());
    std::lock_guard<std::mutex> lock(mutex_);
    queues_.clear();
    platform_ = nullptr;
    installed_ = false;
    return Status::OK();
}

Status MluTransport::quiesce() {
    std::lock_guard<std::mutex> lock(mutex_);
    for (auto &[context, queues] : queues_) {
        for (auto &queue : queues) {
            if (queue) CHECK_STATUS(queue->synchronize());
        }
    }
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
    for (auto &task : mlu_batch->task_list) {
        if (task.status_word.load() == PENDING) {
            CHECK_STATUS(task.queue->synchronize());
            task.transferred_bytes =
                task.drain_on_error ? 0 : task.request.length;
            task.status_word = task.drain_on_error ? FAILED : COMPLETED;
        }
    }
    Slab<MluSubBatch>::Get().deallocate(mlu_batch);
    batch = nullptr;
    return Status::OK();
}

Status MluTransport::submitTransferTasks(
    SubBatchRef batch, const std::vector<Request> &request_list) {
    std::lock_guard<std::mutex> lock(mutex_);
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
    ContextGuard caller_context;
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

    const bool to_device = task->request.opcode == Request::READ
                               ? source_is_device
                               : staging_is_device;
    auto status = submitCopy(*task, to_device);
    if (status.ok() && !task->completion) {
        task->transferred_bytes = task->request.length;
        task->status_word = TransferStatusEnum::COMPLETED;
    } else if (!status.ok()) {
        LOG(WARNING) << "MluTransport: staging copy failed: "
                     << status.ToString();
        if (!task->drain_on_error)
            task->status_word = TransferStatusEnum::FAILED;
    }
    batch->notifyProgress();
}

Status MluTransport::submitCopy(MluTask &task, bool to_device) {
    void *src = task.request.source;
    void *dst = reinterpret_cast<void *>(task.request.target_offset);
    if (task.request.opcode == Request::READ) std::swap(src, dst);
    const auto length = task.request.length;
    if (!length) return Status::OK();
    constexpr size_t kInlineH2DLimit = 64 * 1024;
    const bool small_h2d = to_device && length <= kInlineH2DLimit;
    // submitTransferTasks holds mutex_ through the copy, including before
    // the first queue is created. Small-only workloads need no driver queue.
    if (small_h2d && queues_.empty()) return platform_->copy(dst, src, length);
    void *host = to_device ? src : dst;
    void *device = to_device ? dst : src;
    cnrtPointerAttributes_t attributes{};
    auto result = cnrtPointerGetAttributes(&attributes, host);
    if (result != cnrtSuccess)
        return Status::InternalError(cnrtGetErrorStr(result));
    const bool pinned = attributes.type == cnrtMemTypeHost;
    if (!to_device && !pinned) return platform_->copy(dst, src, length);

    CNcontext context = nullptr;
    CHECK_STATUS(driverStatus(cnGetMemAttribute(
        &context, CN_MEM_ATTRIBUTE_CONTEXT, reinterpret_cast<CNaddr>(device))));
    ContextGuard guard;
    CHECK_STATUS(guard.use(context));
    if (to_device && (small_h2d || !pinned)) {
        auto it = queues_.find(context);
        if (it == queues_.end() || !it->second[1])
            return platform_->copy(dst, src, length);
        auto &queue = it->second[1];
        std::lock_guard<std::mutex> lock(queue->mutex);
        CHECK_STATUS(queue->error);
        auto ready = cnQueryQueue(queue->handle);
        if (ready == CN_SUCCESS) return platform_->copy(dst, src, length);
        if (ready != CN_ERROR_NOT_READY) {
            queue->error = driverStatus(ready);
            return queue->error;
        }
        // Busy pinned copies join the same H2D queue below. Pageable copies
        // cannot use that path: drain this direction before copying inline.
        // mutex_ prevents a later submission from passing either path.
        if (!pinned) {
            auto drained = driverStatus(cnQueueSync(queue->handle));
            if (!drained.ok()) {
                queue->error = drained;
                return drained;
            }
            return platform_->copy(dst, src, length);
        }
    }
    CNaddr base = 0;
    cn_uint64_t size = 0;
    auto address = reinterpret_cast<CNaddr>(device);
    CHECK_STATUS(driverStatus(cnMemGetAddressRange(&base, &size, address)));
    if (address < base || address - base > size ||
        length > size - (address - base))
        return Status::InvalidArgument(
            "MLU memory range exceeds its allocation");
    {
        auto &queue = queues_[context][to_device ? 1 : 0];
        if (!queue) {
            auto created = std::make_shared<MluCopyQueue>();
            created->context = context;
            CHECK_STATUS(driverStatus(cnCreateQueue(&created->handle, 0)));
            queue = std::move(created);
        }
        task.queue = queue;
    }
    CHECK_STATUS(driverStatus(
        cnCreateNotifier(&task.completion, CN_NOTIFIER_DISABLE_TIMING)));
    std::lock_guard<std::mutex> lock(task.queue->mutex);
    CHECK_STATUS(task.queue->error);
    auto status = driverStatus(cnMemcpyAsync_V3(reinterpret_cast<CNaddr>(dst),
                                                reinterpret_cast<CNaddr>(src),
                                                length, task.queue->handle));
    if (status.ok())
        status =
            driverStatus(cnPlaceNotifier(task.completion, task.queue->handle));
    if (!status.ok()) {
        // A copy may already be queued when recording its notifier fails.
        // Poll the queue before publishing failure and permitting buffer reuse.
        task.drain_on_error = true;
    }
    return status;
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
    if (task.status_word.load() == PENDING) {
        ContextGuard guard;
        CHECK_STATUS(guard.use(task.queue->context));
        auto result = task.drain_on_error ? cnQueryQueue(task.queue->handle)
                                          : cnQueryNotifier(task.completion);
        if (result == CN_SUCCESS) {
            task.transferred_bytes =
                task.drain_on_error ? 0 : task.request.length;
            task.status_word = task.drain_on_error ? FAILED : COMPLETED;
        } else if (result != CN_ERROR_NOT_READY) {
            LOG(WARNING) << driverStatus(result).ToString();
            task.drain_on_error = true;
            if (task.queue->synchronize().ok()) task.status_word = FAILED;
        }
    }
    status =
        TransferStatus{task.status_word.load(), task.transferred_bytes.load()};
    return Status::OK();
}

Status MluTransport::allocateLocalMemory(void **addr, size_t size,
                                         MemoryOptions &options) {
    if (!installed_)
        return Status::InvalidArgument("MLU transport is not installed");
    if (!addr || !size) return Status::InvalidArgument("Empty allocation");
    if (options.location != kWildcardLocation &&
        LocationParser(options.location).type() != "cpu")
        return platform_->allocate(addr, size, options);
    ContextGuard caller_context;
    *addr = nullptr;
    auto result = cnrtHostMalloc(addr, size);
    return result == cnrtSuccess
               ? Status::OK()
               : Status::InternalError(cnrtGetErrorStr(result));
}

Status MluTransport::freeLocalMemory(void *addr, size_t size) {
    if (!installed_)
        return Status::InvalidArgument("MLU transport is not installed");
    CHECK_STATUS(quiesce());
    ContextGuard caller_context;
    if (platform_->getMemoryType(addr) != MTYPE_CPU)
        return platform_->free(addr, size);
    auto result = cnrtFreeHost(addr);
    return result == cnrtSuccess
               ? Status::OK()
               : Status::InternalError(cnrtGetErrorStr(result));
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
    if (std::find(desc.transports.begin(), desc.transports.end(),
                  TransportType::MLU) == desc.transports.end())
        return Status::OK();
    CHECK_STATUS(quiesce());
    desc.transports.erase(
        std::remove(desc.transports.begin(), desc.transports.end(),
                    TransportType::MLU),
        desc.transports.end());
    return Status::OK();
}

}  // namespace tent
}  // namespace mooncake
