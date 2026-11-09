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
#include <time.h>

#include <algorithm>
#include <charconv>
#include <sstream>
#include <tuple>
#include <unordered_set>
#include <utility>

#include "tent/common/status.h"
#include "tent/runtime/platform.h"
#include "tent/runtime/slab.h"

namespace mooncake {
namespace tent {
namespace {
struct IpcAttr {
    uint64_t base = 0;
    uint64_t size = 0;
    uintptr_t handle = 0;
    int device = -1;
    uint64_t generation = 0;
};

std::string formatIpcAttr(const IpcAttr &attr) {
    return std::to_string(attr.base) + ':' + std::to_string(attr.size) + ':' +
           std::to_string(attr.handle) + ':' + std::to_string(attr.device) +
           ':' + std::to_string(attr.generation);
}

bool parseIpcAttr(const std::string &text, IpcAttr &attr) {
    std::array<std::string_view, 5> fields;
    size_t start = 0;
    for (size_t i = 0; i < fields.size(); ++i) {
        const size_t end = text.find(':', start);
        if (i + 1 < fields.size() && end == std::string::npos) return false;
        if (i + 1 == fields.size() && end != std::string::npos) return false;
        fields[i] = std::string_view(text).substr(start, end - start);
        start = end + 1;
    }
    auto number = [](std::string_view field, auto &value) {
        auto [end, ec] =
            std::from_chars(field.data(), field.data() + field.size(), value);
        return ec == std::errc() && end == field.data() + field.size();
    };
    return number(fields[0], attr.base) && number(fields[1], attr.size) &&
           number(fields[2], attr.handle) && number(fields[3], attr.device) &&
           number(fields[4], attr.generation) && attr.size && attr.handle &&
           attr.device >= 0;
}

bool rangeWithin(uint64_t addr, uint64_t length, uint64_t base, uint64_t size) {
    return addr >= base && addr - base <= size &&
           length <= size - (addr - base);
}

uint64_t bootTimeNs() {
    timespec ts{};
    if (clock_gettime(CLOCK_BOOTTIME, &ts) != 0) return 0;
    return static_cast<uint64_t>(ts.tv_sec) * 1000000000ull + ts.tv_nsec;
}

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
      imported_target(std::move(other.imported_target)),
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
                             std::shared_ptr<Config> conf) {
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
    ipc_enabled_ = !(conf && conf->get("transports/mlu/disable_ipc", false));
    if (metadata_) {
        auto local = metadata_->segmentManager().getLocal();
        if (local) machine_id_ = local->machine_id;
    }
    ipc_generation_ = bootTimeNs();
    installed_ = true;
    caps.gpu_to_dram = true;
    caps.dram_to_gpu = true;
    caps.gpu_to_gpu = true;
    return Status::OK();
}

Status MluTransport::uninstall() {
    std::lock_guard<std::mutex> lock(mutex_);
    CHECK_STATUS(quiesceLocked());
    imports_.clear();
    reconciled_snapshots_.clear();
    queues_.clear();
    metadata_.reset();
    platform_ = nullptr;
    installed_ = false;
    machine_id_.clear();
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
    std::vector<std::shared_ptr<void>> mappings;
    std::vector<uint64_t> targets;
    mappings.reserve(request_list.size());
    targets.reserve(request_list.size());
    for (const auto &request : request_list) {
        uint64_t target = request.target_offset;
        std::shared_ptr<void> mapping;
        if (request.target_id != LOCAL_SEGMENT_ID) {
            mapping = relocate(request.target_id, target, request.length);
            if (!mapping)
                return Status::InvalidArgument(
                    "MLU IPC target is not mapped into this process");
        }
        targets.push_back(target);
        mappings.push_back(std::move(mapping));
    }
    for (size_t i = 0; i < request_list.size(); ++i) {
        const auto &request = request_list[i];
        auto &task = mlu_batch->task_list.emplace_back();
        task.request = request;
        task.request.target_offset = targets[i];
        task.imported_target = std::move(mappings[i]);
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
    if ((task->request.target_id != LOCAL_SEGMENT_ID &&
         !task->imported_target) ||
        (task->request.opcode != Request::READ &&
         task->request.opcode != Request::WRITE)) {
        LOG(ERROR) << "MluTransport: inaccessible target "
                   << task->request.target_id
                   << "; MLU requires a local or imported buffer";
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
            (task->request.target_id == LOCAL_SEGMENT_ID &&
             !local->findBuffer(task->request.target_offset,
                                task->request.length))) {
            task->status_word = FAILED;
            return;
        }
    }

    void *staging = reinterpret_cast<void *>(task->request.target_offset);

    const bool source_is_device =
        platform_->getMemoryType(task->request.source) == MTYPE_MLU;
    const bool staging_is_device =
        platform_->getMemoryType(staging) == MTYPE_MLU;
    if (!source_is_device && !staging_is_device) {
        LOG(ERROR) << "MluTransport: a copy must have an MLU "
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

    if (source_is_device && staging_is_device) {
        auto status = submitPeerCopy(*task);
        if (status.ok() && !task->completion) {
            task->transferred_bytes = task->request.length;
            task->status_word = COMPLETED;
        } else if (!status.ok() && !task->drain_on_error) {
            LOG(WARNING) << "MluTransport: peer copy failed: "
                         << status.ToString();
            task->status_word = FAILED;
        }
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

Status MluTransport::submitPeerCopy(MluTask &task) {
    void *src = task.request.source;
    void *dst = reinterpret_cast<void *>(task.request.target_offset);
    if (task.request.opcode == Request::READ) std::swap(src, dst);
    const auto length = task.request.length;
    if (!length) return Status::OK();

    CNcontext src_context = nullptr, dst_context = nullptr;
    int src_device = -1, dst_device = -1;
    for (auto [ptr, context, device] :
         {std::tuple<CNaddr, CNcontext *, int *>{reinterpret_cast<CNaddr>(src),
                                                 &src_context, &src_device},
          {reinterpret_cast<CNaddr>(dst), &dst_context, &dst_device}}) {
        CHECK_STATUS(driverStatus(
            cnGetMemAttribute(context, CN_MEM_ATTRIBUTE_CONTEXT, ptr)));
        CHECK_STATUS(driverStatus(
            cnGetMemAttribute(device, CN_MEM_ATTRIBUTE_DEVICE_ORDINAL, ptr)));
        CNaddr base = 0;
        cn_uint64_t size = 0;
        CHECK_STATUS(driverStatus(cnMemGetAddressRange(&base, &size, ptr)));
        if (ptr < base || ptr - base > size || length > size - (ptr - base))
            return Status::InvalidArgument(
                "MLU peer memory range exceeds its allocation");
    }
    bool accessible = src_context == dst_context;
    if (src_device != dst_device) {
        unsigned int peer_accessible = 0;
        auto result =
            cnrtGetPeerAccessibility(&peer_accessible, src_device, dst_device);
        if (result != cnrtSuccess)
            return Status::InternalError(cnrtGetErrorStr(result));
        accessible = peer_accessible != 0;
    }
    if (!accessible) {
        // Preserve the old host-staging behavior on systems without a direct
        // device link or with two contexts on one device.
        CHECK_STATUS(quiesceLocked());
        void *host = nullptr;
        auto allocated = cnrtHostMalloc(&host, length);
        if (allocated != cnrtSuccess)
            return Status::InternalError(cnrtGetErrorStr(allocated));
        auto copied = platform_->copy(host, src, length);
        if (copied.ok()) copied = platform_->copy(dst, host, length);
        auto freed = cnrtFreeHost(host);
        if (!copied.ok()) return copied;
        if (freed != cnrtSuccess)
            return Status::InternalError(cnrtGetErrorStr(freed));
        return Status::OK();
    }

    ContextGuard guard;
    CHECK_STATUS(guard.use(src_context));
    auto &queue = queues_[src_context][2];
    if (queue) {
        auto previous = queue;
        std::lock_guard<std::mutex> lock(previous->mutex);
        if (!previous->error.ok()) {
            auto ready = cnQueryQueue(previous->handle);
            if (ready != CN_SUCCESS)
                return ready == CN_ERROR_NOT_READY ? previous->error
                                                   : driverStatus(ready);
            queue.reset();
        }
    }
    if (!queue) {
        auto created = std::make_shared<MluCopyQueue>();
        created->context = src_context;
        CHECK_STATUS(driverStatus(cnCreateQueue(&created->handle, 0)));
        queue = std::move(created);
    }
    task.queue = queue;
    CHECK_STATUS(driverStatus(
        cnCreateNotifier(&task.completion, CN_NOTIFIER_DISABLE_TIMING)));
    std::lock_guard<std::mutex> lock(queue->mutex);
    CHECK_STATUS(queue->error);
    auto result =
        src_context == dst_context
            ? cnMemcpyAsync_V3(reinterpret_cast<CNaddr>(dst),
                               reinterpret_cast<CNaddr>(src), length,
                               queue->handle)
            : cnMemcpyPeerAsync(reinterpret_cast<CNaddr>(dst), dst_context,
                                reinterpret_cast<CNaddr>(src), src_context,
                                length, queue->handle);
    auto status = driverStatus(result);
    if (status.ok()) {
        status = driverStatus(cnPlaceNotifier(task.completion, queue->handle));
        task.drain_on_error = !status.ok();
        if (!status.ok()) queue->error = status;
    } else {
        switch (result) {
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
                queue->error = status;
        }
    }
    return status;
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
                                     const MemoryOptions &options) {
    LocationParser location(desc.location);
    if (location.type() != "mlu" && location.type() != "cpu" &&
        location.type() != kWildcardLocation) {
        return Status::OK();
    }
    if (std::find(desc.transports.begin(), desc.transports.end(),
                  TransportType::MLU) == desc.transports.end()) {
        desc.transports.push_back(TransportType::MLU);
    }
    desc.transport_attrs.erase(MLU);
    if (ipc_enabled_ && options.perm == kGlobalReadWrite &&
        location.type() == "mlu" && desc.length) {
        const auto address = static_cast<CNaddr>(desc.addr);
        CNcontext context = nullptr;
        int device = -1;
        CNaddr base = 0;
        cn_uint64_t size = 0;
        auto status = driverStatus(
            cnGetMemAttribute(&context, CN_MEM_ATTRIBUTE_CONTEXT, address));
        if (status.ok())
            status = driverStatus(cnGetMemAttribute(
                &device, CN_MEM_ATTRIBUTE_DEVICE_ORDINAL, address));
        if (status.ok())
            status = driverStatus(cnMemGetAddressRange(&base, &size, address));
        if (status.ok() && !rangeWithin(desc.addr, desc.length, base, size))
            status =
                Status::InvalidArgument("MLU IPC buffer exceeds allocation");
        if (status.ok()) {
            ContextGuard guard;
            status = guard.use(context);
            cnrtIpcMemHandle handle = nullptr;
            if (status.ok()) {
                auto result = cnrtAcquireMemHandle(
                    &handle, reinterpret_cast<void *>(base));
                if (result != cnrtSuccess)
                    status = Status::InternalError(cnrtGetErrorStr(result));
            }
            if (status.ok() && handle) {
                desc.transport_attrs[MLU] = formatIpcAttr(
                    {base, size, reinterpret_cast<uintptr_t>(handle), device,
                     ++ipc_generation_});
            }
        }
        if (!status.ok()) {
            // An unexportable allocation remains usable for local copies and
            // the existing host-staged remote route.
            LOG(INFO) << "MLU buffer " << (void *)desc.addr
                      << " has no IPC handle: " << status.ToString();
            desc.transport_attrs.erase(MLU);
        }
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
    desc.transport_attrs.erase(MLU);
    return Status::OK();
}

bool MluTransport::mayMapRemoteBuffer(SegmentID target_id,
                                      const SegmentDesc &segment,
                                      const BufferDesc &entry) const {
    if (!installed_ || !ipc_enabled_ || target_id == LOCAL_SEGMENT_ID ||
        machine_id_.empty() || segment.machine_id != machine_id_)
        return false;
    auto it = entry.transport_attrs.find(MLU);
    return it != entry.transport_attrs.end() && !it->second.empty();
}

bool MluTransport::prepareRemoteBuffer(SegmentID target_id,
                                       const SegmentDesc &segment,
                                       const BufferDesc &entry,
                                       void *local_source) {
    if (!mayMapRemoteBuffer(target_id, segment, entry)) return false;
    const auto &attr_text = entry.transport_attrs.at(MLU);
    IpcAttr attr;
    if (!parseIpcAttr(attr_text, attr) ||
        !rangeWithin(entry.addr, entry.length, attr.base, attr.size))
        return false;

    cnrtPointerAttributes_t source{};
    auto queried = cnrtPointerGetAttributes(&source, local_source);
    if (queried != cnrtSuccess) return false;
    if (source.type == cnrtMemTypeDevice && source.device != attr.device) {
        unsigned int accessible = 0;
        auto result =
            cnrtGetPeerAccessibility(&accessible, source.device, attr.device);
        if (result != cnrtSuccess || !accessible) return false;
    }

    // Keep removed mappings alive until after releasing mutex_: unmapping may
    // call into the driver and must not stall submission or status polling.
    std::vector<std::shared_ptr<void>> dropped;
    std::lock_guard<std::mutex> lock(mutex_);
    auto &entries = imports_[target_id];
    auto &reconciled = reconciled_snapshots_[target_id];
    mlu_detail::reconcileImports(entries, reconciled, segment, dropped);
    for (const auto &imported : entries)
        if (imported.attr == attr_text) return true;

    ContextGuard guard;
    if (!guard.use(nullptr).ok()) return false;
    auto selected = cnrtSetDevice(attr.device);
    if (selected != cnrtSuccess) return false;
    void *mapped = nullptr;
    auto result = cnrtMapMemHandle(
        &mapped, reinterpret_cast<cnrtIpcMemHandle>(attr.handle), 0);
    if (result != cnrtSuccess) {
        LOG(WARNING) << "MLU IPC map failed for segment " << segment.name
                     << ": " << cnrtGetErrorStr(result);
        return false;
    }
    CNcontext context = nullptr;
    auto context_status = driverStatus(cnGetMemAttribute(
        &context, CN_MEM_ATTRIBUTE_CONTEXT, reinterpret_cast<CNaddr>(mapped)));
    if (!context_status.ok()) {
        cnrtUnMapMemHandle(mapped);
        return false;
    }
    std::shared_ptr<void> mapping(mapped, [context](void *ptr) {
        ContextGuard guard;
        auto status = guard.use(context);
        if (status.ok()) {
            auto result = cnrtUnMapMemHandle(ptr);
            if (result != cnrtSuccess)
                status = Status::InternalError(cnrtGetErrorStr(result));
        }
        logError(status);
    });
    entries.push_back({attr_text, attr.base, attr.size,
                       segment.snapshot_generation, std::move(mapping)});
    // An unversioned snapshot cannot be ordered against a versioned one.
    // Force the next versioned look to check any mapping it just introduced.
    if (!segment.snapshot_generation) reconciled = 0;
    LOG(INFO) << "MLU IPC mapped peer buffer " << (void *)attr.base << " of "
              << segment.name << " at " << mapped << " on mlu:" << attr.device;
    return true;
}

void mlu_detail::reconcileImports(std::vector<mlu_detail::ImportEntry> &entries,
                                  uint64_t &reconciled,
                                  const SegmentDesc &segment,
                                  std::vector<std::shared_ptr<void>> &dropped) {
    // Buffer registration generations do not describe a snapshot: removing
    // the newest buffer makes their maximum go backwards. The publication
    // generation also advances on removals, and orders snapshots across
    // threads. Older snapshots cannot retire mappings imported from newer ones.
    // A legacy descriptor has no publication generation (zero); its age
    // cannot be established, so retain mappings until a versioned snapshot
    // or closeRemoteSegment arrives.
    if (segment.snapshot_generation &&
        reconciled != segment.snapshot_generation) {
        std::unordered_set<std::string_view> published;
        published.reserve(segment.getMemory().buffers.size());
        for (const auto &buffer : segment.getMemory().buffers) {
            auto it = buffer.transport_attrs.find(MLU);
            if (it != buffer.transport_attrs.end())
                published.insert(it->second);
        }
        entries.erase(
            std::remove_if(entries.begin(), entries.end(),
                           [&](const mlu_detail::ImportEntry &imported) {
                               const bool retired =
                                   imported.snapshot_generation <=
                                       segment.snapshot_generation &&
                                   !published.count(imported.attr);
                               if (retired) dropped.push_back(imported.mapping);
                               return retired;
                           }),
            entries.end());
        reconciled = segment.snapshot_generation;
    }
}

std::shared_ptr<void> MluTransport::relocate(SegmentID target_id,
                                             uint64_t &addr, size_t length) {
    auto it = imports_.find(target_id);
    if (it == imports_.end()) return nullptr;
    for (const auto &entry : it->second) {
        if (!rangeWithin(addr, length, entry.base, entry.size)) continue;
        addr =
            addr - entry.base + reinterpret_cast<uint64_t>(entry.mapping.get());
        return entry.mapping;
    }
    return nullptr;
}

void MluTransport::closeRemoteSegment(SegmentID target_id) {
    std::lock_guard<std::mutex> lock(mutex_);
    imports_.erase(target_id);
    reconciled_snapshots_.erase(target_id);
}

}  // namespace tent
}  // namespace mooncake
