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
    auto status = driverStatus(cnQueueSync(handle));
    if (!status.ok()) error = status;
    return status;
}

MluCopyQueue::~MluCopyQueue() {
    if (!handle) return;
    auto drained = synchronize();
    if (!drained.ok()) {
        logError(drained);
        return;  // Destruction is not a device-side cancellation barrier.
    }
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
    if (status_word.load() == PENDING) {
        auto drained = queue->synchronize();
        if (!drained.ok()) {
            logError(drained);
            return;
        }
    }
    ContextGuard guard;
    auto status = guard.use(queue->context);
    if (status.ok()) status = driverStatus(cnDestroyNotifier(completion));
    logError(status);
}

MluTransport::~MluTransport() { logError(uninstall()); }

Status MluTransport::install(std::string & /*local_segment_name*/,
                             std::shared_ptr<ControlService> metadata,
                             std::shared_ptr<Topology> /*local_topology*/,
                             std::shared_ptr<Config> /*conf*/) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (installed_) {
        return Status::InvalidArgument(
            "MLU transport has been installed" LOC_MARK);
    }
    platform_ = dynamic_cast<MluPlatform *>(&Platform::getLoader());
    if (!platform_) {
        return Status::InvalidArgument(
            "MLU transport requires the MluPlatform to be active" LOC_MARK);
    }
    metadata_ = std::move(metadata);
    installed_ = true;
    caps.gpu_to_dram = true;
    caps.dram_to_gpu = true;
    return Status::OK();
}

Status MluTransport::uninstall() {
    std::lock_guard<std::mutex> lock(mutex_);
    CHECK_STATUS(quiesceLocked());
    queues_.clear();
    metadata_.reset();
    platform_ = nullptr;
    installed_ = false;
    return Status::OK();
}

Status MluTransport::quiesce() {
    std::lock_guard<std::mutex> lock(mutex_);
    return quiesceLocked();
}

Status MluTransport::quiesceLocked() {
    Status first_error;
    for (auto &[context, queues] : queues_) {
        for (auto &queue : queues) {
            if (!queue) continue;
            auto status = queue->synchronize();
            if (!status.ok() && first_error.ok()) first_error = status;
        }
    }
    return first_error;
}

Status MluTransport::drainTask(MluTask &task) {
    if (task.status_word.load() != PENDING) return Status::OK();
    ContextGuard guard;
    CHECK_STATUS(guard.use(task.queue->context));
    if (!task.drain_on_error) {
        auto status = driverStatus(cnWaitNotifier(task.completion));
        if (!status.ok()) task.drain_on_error = true;
    }
    if (task.drain_on_error) CHECK_STATUS(task.queue->synchronize());
    task.transferred_bytes = task.drain_on_error ? 0 : task.request.length;
    task.status_word = task.drain_on_error ? FAILED : COMPLETED;
    return Status::OK();
}

Status MluTransport::drainBuffer(uint64_t addr, size_t size) {
    // mutex_ excludes submission through enqueue and notifier placement.
    // A notifier waits only for this task's queue prefix, not later copies.
    auto overlaps = [addr, size](uint64_t start, size_t length) {
        return size && length &&
               (start >= addr ? start - addr < size : addr - start < length);
    };
    Status first_error;
    for (auto *batch : batches_) {
        for (auto &task : batch->task_list) {
            if (!overlaps(reinterpret_cast<uint64_t>(task.request.source),
                          task.request.length) &&
                !overlaps(task.request.target_offset, task.request.length))
                continue;
            auto status = drainTask(task);
            if (!status.ok() && first_error.ok()) first_error = status;
        }
    }
    return first_error;
}

Status MluTransport::allocateSubBatch(SubBatchRef &batch, size_t max_size) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto mlu_batch = Slab<MluSubBatch>::Get().allocate();
    if (!mlu_batch)
        return Status::InternalError(
            "Unable to allocate MLU sub-batch" LOC_MARK);
    batch = mlu_batch;
    mlu_batch->task_list.reserve(max_size);
    mlu_batch->max_size = max_size;
    batches_.insert(mlu_batch);
    return Status::OK();
}

Status MluTransport::freeSubBatch(SubBatchRef &batch) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto mlu_batch = dynamic_cast<MluSubBatch *>(batch);
    if (!mlu_batch)
        return Status::InvalidArgument("Invalid MLU sub-batch" LOC_MARK);
    Status first_error;
    for (auto &task : mlu_batch->task_list) {
        auto status = drainTask(task);
        if (!status.ok() && first_error.ok()) first_error = status;
    }
    CHECK_STATUS(first_error);
    batches_.erase(mlu_batch);
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
        startTransfer(&task);
    }
    mlu_batch->published_size.store(mlu_batch->task_list.size());
    mlu_batch->notifyProgress();
    return Status::OK();
}

void MluTransport::startTransfer(MluTask *task) {
    ContextGuard caller_context;
    if (task->request.target_id != LOCAL_SEGMENT_ID ||
        (task->request.opcode != Request::READ &&
         task->request.opcode != Request::WRITE)) {
        LOG(ERROR) << "MluTransport: unexpected non-local target "
                   << task->request.target_id
                   << "; MLU only supports local staging copies";
        task->status_word = TransferStatusEnum::FAILED;
        task->transferred_bytes = 0;
        return;
    }

    // A route may have been resolved before unregister removed its snapshot.
    // Recheck it under the same lock that serializes enqueue and buffer drain.
    if (metadata_) {
        const auto local = metadata_->segmentManager().getLocal();
        if (!local->findBuffer(reinterpret_cast<uint64_t>(task->request.source),
                               task->request.length) ||
            !local->findBuffer(task->request.target_offset,
                               task->request.length)) {
            task->status_word = FAILED;
            return;
        }
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
    if (!pinned && queues_.empty()) return platform_->copy(dst, src, length);

    CNcontext context = nullptr;
    CHECK_STATUS(driverStatus(cnGetMemAttribute(
        &context, CN_MEM_ATTRIBUTE_CONTEXT, reinterpret_cast<CNaddr>(device))));
    ContextGuard guard;
    CHECK_STATUS(guard.use(context));
    auto &queue = queues_[context][to_device ? 1 : 0];
    if (queue) {
        auto previous = queue;
        std::lock_guard<std::mutex> lock(previous->mutex);
        if (!previous->error.ok()) {
            auto ready = cnQueryQueue(previous->handle);
            if (ready != CN_SUCCESS)
                return ready == CN_ERROR_NOT_READY ? previous->error
                                                   : driverStatus(ready);
            // Retire the drained queue instead of appending new work to it.
            // Failed tasks may still need to poll it or release their buffers.
            queue.reset();
        }
    }
    if (small_h2d || !pinned) {
        if (!queue) return platform_->copy(dst, src, length);
        std::lock_guard<std::mutex> lock(queue->mutex);
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
    auto result_copy = cnMemcpyAsync_V3(reinterpret_cast<CNaddr>(dst),
                                        reinterpret_cast<CNaddr>(src), length,
                                        task.queue->handle);
    auto status = driverStatus(result_copy);
    if (status.ok()) {
        status =
            driverStatus(cnPlaceNotifier(task.completion, task.queue->handle));
        // The copy was accepted even if its completion marker was rejected.
        task.drain_on_error = !status.ok();
        // Stop adding work behind a copy whose completion cannot be tracked.
        if (!status.ok()) task.queue->error = status;
    } else {
        // Validation/invalid-handle errors reject this submission. A timeout
        // or an unknown driver failure does not establish that no DMA ran.
        switch (result_copy) {
            case CN_ERROR_INVALID_VALUE:
            case CN_ERROR_INVALID_ADDRESS:
            case CN_MEMORY_ERROR_INVALID_ADDRESS:
            case CN_CONTEXT_ERROR_INVALID:
            case CN_QUEUE_ERROR_INVALID:
            case CN_ERROR_NOT_INITIALIZED:
            case CN_ERROR_DEINITIALIZED:
                break;
            default:
                task.drain_on_error = true;
                task.queue->error = status;
        }
    }
    return status;
}

Status MluTransport::getTransferStatus(SubBatchRef batch, int task_id,
                                       TransferStatus &status) {
    status = TransferStatus{PENDING, 0};
    // Submission holds mutex_ until all task fields and notifiers are ready.
    // Polling a submission in progress must neither block nor inspect them.
    std::unique_lock<std::mutex> lock(mutex_, std::try_to_lock);
    if (!lock.owns_lock()) return Status::OK();
    auto mlu_batch = dynamic_cast<MluSubBatch *>(batch);
    if (!mlu_batch)
        return Status::InvalidArgument("Invalid MLU sub-batch" LOC_MARK);
    if (task_id < 0 || task_id >= (int)mlu_batch->max_size) {
        return Status::InvalidArgument("Invalid task id" LOC_MARK);
    }
    if (task_id >= (int)mlu_batch->published_size.load()) return Status::OK();
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
            task.drain_on_error = true;
            // A failed query is not a completion barrier. Stop appending to
            // this queue and let later nonblocking polls establish its drain.
            std::lock_guard<std::mutex> queue_lock(task.queue->mutex);
            task.queue->error = driverStatus(result);
            return task.queue->error;
        }
    }
    status =
        TransferStatus{task.status_word.load(), task.transferred_bytes.load()};
    return Status::OK();
}

Status MluTransport::allocateLocalMemory(void **addr, size_t size,
                                         MemoryOptions &options) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (!installed_)
        return Status::InvalidArgument("MLU transport is not installed");
    if (!addr || !size) return Status::InvalidArgument("Empty allocation");
    if (options.location != kWildcardLocation &&
        LocationParser(options.location).type() != "cpu")
        return platform_->allocate(addr, size, options);
    ContextGuard caller_context;
    *addr = nullptr;
    auto result = cnrtHostMalloc(addr, size);
    if (result != cnrtSuccess)
        return Status::InternalError(cnrtGetErrorStr(result));
    if (options.location != kWildcardLocation && options.location != "cpu") {
        const auto locations = platform_->getLocation(*addr, size);
        const bool local =
            !locations.empty() &&
            std::all_of(locations.begin(), locations.end(),
                        [&](const auto &range) {
                            return range.location == options.location;
                        });
        if (!local) {
            // CNRT offers no host-NUMA allocation parameter. Preserve the
            // requested locality through the normal allocator if its pinned
            // allocation landed elsewhere; copies then use the pageable path.
            auto freed = cnrtFreeHost(*addr);
            if (freed != cnrtSuccess)
                return Status::InternalError(cnrtGetErrorStr(freed));
            *addr = nullptr;
            return platform_->allocate(addr, size, options);
        }
    }
    return Status::OK();
}

Status MluTransport::freeLocalMemory(void *addr, size_t size) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (!installed_)
        return Status::InvalidArgument("MLU transport is not installed");
    CHECK_STATUS(drainBuffer(reinterpret_cast<uint64_t>(addr), size));
    ContextGuard caller_context;
    cnrtPointerAttributes_t attributes{};
    auto queried = cnrtPointerGetAttributes(&attributes, addr);
    if (queried != cnrtSuccess)
        return Status::InternalError(cnrtGetErrorStr(queried));
    if (attributes.type != cnrtMemTypeHost) return platform_->free(addr, size);
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
    std::lock_guard<std::mutex> lock(mutex_);
    if (std::find(desc.transports.begin(), desc.transports.end(),
                  TransportType::MLU) == desc.transports.end())
        return Status::OK();
    CHECK_STATUS(drainBuffer(desc.addr, desc.length));
    desc.transports.erase(
        std::remove(desc.transports.begin(), desc.transports.end(),
                    TransportType::MLU),
        desc.transports.end());
    return Status::OK();
}

}  // namespace tent
}  // namespace mooncake
