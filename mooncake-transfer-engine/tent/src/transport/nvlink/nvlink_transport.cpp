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

#include "tent/transport/nvlink/nvlink_transport.h"

#include <glog/logging.h>

#include <algorithm>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <iomanip>
#include <memory>
#include <unordered_map>

#include "tent/common/status.h"
#include "tent/runtime/slab.h"
#include "tent/runtime/control_plane.h"
#include "tent/common/utils/random.h"
#include "tent/common/utils/string_builder.h"

namespace mooncake {
namespace tent {
namespace {

Status setCudaDeviceForLocation(const LocationParser& location,
                                int& saved_dev) {
    saved_dev = -1;
    CHECK_CUDA(cudaGetDevice(&saved_dev));
    if (location.index() >= 0 && saved_dev != location.index()) {
        CHECK_CUDA(cudaSetDevice(location.index()));
    }
    return Status::OK();
}

Status restoreCudaDeviceForLocation(const LocationParser& location,
                                    int saved_dev) {
    if (saved_dev >= 0 && location.index() >= 0 &&
        saved_dev != location.index()) {
        CHECK_CUDA(cudaSetDevice(saved_dev));
    }
    return Status::OK();
}

/// Thread-local cache of one caller-sync event per device. Events are
/// created lazily and released at thread exit; the destructor restores the
/// device around each destroy because event teardown must run on the
/// event's device on some CUDA-compatible runtimes.
class CallerSyncEventPool {
   public:
    cudaEvent_t getOrCreate(int device_id) {
        auto it = events_.find(device_id);
        if (it != events_.end()) return it->second;
        cudaEvent_t event = nullptr;
        if (cudaEventCreateWithFlags(&event, cudaEventDisableTiming) !=
            cudaSuccess) {
            return nullptr;
        }
        events_[device_id] = event;
        return event;
    }
    ~CallerSyncEventPool() {
        int saved_device = 0;
        cudaGetDevice(&saved_device);
        for (auto& entry : events_) {
            cudaSetDevice(entry.first);
            if (entry.second) cudaEventDestroy(entry.second);
        }
        cudaSetDevice(saved_device);
    }

   private:
    std::unordered_map<int, cudaEvent_t> events_;
};

thread_local CallerSyncEventPool tl_caller_sync_events;

/// Waits for producer work already queued on the caller's per-thread stream
/// so the copy submitted on the transport's internal stream cannot read the
/// source buffer before that work has executed. CPU-blocking
/// cudaEventSynchronize mirrors IntraNodeNvlinkTransport and avoids the
/// expensive cross-device cudaStreamWaitEvent on non-NVIDIA GPUs; it is
/// complementary to (not a replacement for) the
/// cudaMemcpySrcAccessOrderStream attribute applied at copy submission.
Status syncWithCallerStream() {
    int device_id = 0;
    CHECK_CUDA(cudaGetDevice(&device_id));
    cudaEvent_t event = tl_caller_sync_events.getOrCreate(device_id);
    if (!event) {
        return Status::InternalError(
            "unable to create caller-sync event" LOC_MARK);
    }
    CHECK_CUDA(cudaEventRecord(event, cudaStreamPerThread));
    CHECK_CUDA(cudaEventSynchronize(event));
    return Status::OK();
}

}  // namespace

NVLinkTransport::NVLinkTransport() : installed_(false) {}

NVLinkTransport::~NVLinkTransport() { uninstall(); }

Status NVLinkTransport::install(std::string& local_segment_name,
                                std::shared_ptr<ControlService> metadata,
                                std::shared_ptr<Topology> local_topology,
                                std::shared_ptr<Config> conf) {
    if (installed_) {
        return Status::InvalidArgument(
            "NVLink transport has been installed" LOC_MARK);
    }

    platform_ = dynamic_cast<CudaPlatform*>(&Platform::getLoader());
    metadata_ = metadata;
    local_segment_name_ = local_segment_name;
    local_topology_ = local_topology;
    conf_ = conf ? conf : std::make_shared<Config>();
    machine_id_ = metadata->segmentManager().getLocal()->machine_id;
    // Copies below this size run as a blocking cudaMemcpy instead of the
    // async stream + completion-event pipeline (see startTransfer). KiB
    // units; 0 disables the sync fast path.
    async_memcpy_threshold_ =
        conf_->get("transports/nvlink/async_memcpy_threshold", 2048) * 1024;
    host_register_ = conf_->get("transports/nvlink/host_register", false);
    caps.dram_to_gpu = true;
    caps.gpu_to_dram = true;
    caps.gpu_to_gpu = true;
    CHECK_STATUS(setPeerAccess());
    installed_ = true;
    return Status::OK();
}

Status NVLinkTransport::uninstall() {
    if (!installed_) return Status::OK();
    RWSpinlock::WriteGuard guard(relocate_lock_);
    for (auto& peer : relocate_map_)
        for (auto& entry : peer.second) CHECK_STATUS(entry.second->release());
    relocate_map_.clear();
    {
        std::lock_guard<std::mutex> lock(allocate_mutex_);
        registered_memory_.clear();
        allocate_set_.clear();
    }
    metadata_.reset();
    installed_ = false;
    return Status::OK();
}

Status NVLinkTransport::allocateSubBatch(SubBatchRef& batch, size_t max_size) {
    auto shm_batch = Slab<NVLinkSubBatch>::Get().allocate();
    if (!shm_batch)
        return Status::InternalError("Unable to allocate NVLink sub-batch");
    batch = shm_batch;
    shm_batch->task_list.reserve(max_size);
    shm_batch->max_size = max_size;
    // Streams are created lazily in submitTransferTasks where the correct
    // GPU device can be inferred from request source pointers.
    return Status::OK();
}

Status NVLinkTransport::freeSubBatch(SubBatchRef& batch) {
    auto shm_batch = dynamic_cast<NVLinkSubBatch*>(batch);
    if (!shm_batch)
        return Status::InvalidArgument("Invalid NVLink sub-batch" LOC_MARK);
    // Completion events are destroyed by ~NVLinkSubBatch(), which Slab's
    // deallocate() invokes before returning the storage. Streams are
    // pool-managed and not destroyed here.
    Slab<NVLinkSubBatch>::Get().deallocate(shm_batch);
    batch = nullptr;
    return Status::OK();
}

Status NVLinkTransport::submitTransferTasks(
    SubBatchRef batch, const std::vector<Request>& request_list) {
    auto shm_batch = dynamic_cast<NVLinkSubBatch*>(batch);
    if (!shm_batch)
        return Status::InvalidArgument("Invalid NVLink sub-batch" LOC_MARK);

    if (request_list.size() + shm_batch->task_list.size() > shm_batch->max_size)
        return Status::TooManyRequests("Exceed batch capacity" LOC_MARK);

    // Producer work already queued on the caller's per-thread stream must be
    // visible before the batched copy runs on the transport's internal
    // stream; without this the copy races with (and can read the source
    // before) that work. Same contract as IntraNodeNvlinkTransport.
    CHECK_STATUS(syncWithCallerStream());

    // Get local segment for buffer lookup
    auto& segment_manager = metadata_->segmentManager();
    // Owning reference: keeps the snapshot alive while we read through it.
    SegmentDescRef local_segment = segment_manager.getLocal();
    if (!local_segment)
        return Status::InternalError("Local segment not found" LOC_MARK);

    // Determine device for this batch and validate all requests
    int batch_device_id = -1;
    std::vector<NVLinkTask*> new_tasks;
    // No I/O is started until startTransfer() below, so a synchronous
    // failure must also roll back the half-appended task entries: the engine
    // fails such tasks over to another transport, and stale never-started
    // entries must not linger in the sub-batch (failover.md, hazard 2).
    const size_t original_task_count = shm_batch->task_list.size();
    auto rollback_tasks = [&shm_batch, original_task_count]() {
        shm_batch->task_list.resize(original_task_count);
    };

    for (auto& request : request_list) {
        // Find the buffer this source pointer belongs to
        BufferDesc* buf = local_segment->findBuffer(
            reinterpret_cast<uint64_t>(request.source), request.length);
        if (!buf) {
            rollback_tasks();
            return Status::InvalidArgument(
                "Unregistered buffer: source pointer not in any registered "
                "buffer" LOC_MARK);
        }

        // Parse device ID from buffer location (e.g., "cuda:0" -> 0, "cpu" ->
        // -1)
        int device_id = LocationParser(buf->location).index();

        // Capture the first GPU device encountered for stream creation.
        // Mixed-GPU batches use the first GPU's stream and rely on CUDA P2P
        // for cross-device access (same behavior as pre-#2569 code).
        // A future refactor could group requests by device and dispatch to
        // per-device streams, but that requires SubBatch structure changes.
        if (batch_device_id < 0 && device_id >= 0) {
            batch_device_id = device_id;
        }

        // Create and populate task
        shm_batch->task_list.push_back(NVLinkTask{});
        auto& task = shm_batch->task_list[shm_batch->task_list.size() - 1];

        uint64_t target_addr = request.target_offset;
        if (request.target_id != LOCAL_SEGMENT_ID) {
            auto status = relocateSharedMemoryAddress(
                target_addr, request.length, request.target_id);
            if (!status.ok()) {
                rollback_tasks();
                return status;
            }
        }

        task.target_addr = target_addr;
        task.request = request;
        task.status_word = TransferStatusEnum::PENDING;
        new_tasks.push_back(&task);
    }

    // Get or create streams for this batch's device
    if (!shm_batch->async_stream.get()) {
        int stream_device = batch_device_id;
        if (stream_device < 0) {
            // CPU-only batch: use current CUDA device
            cudaGetDevice(&stream_device);
        }
        auto status =
            platform_->getStreamFromPool(shm_batch->sync_stream, stream_device);
        if (status.ok()) {
            status = platform_->getStreamFromPool(shm_batch->async_stream,
                                                  stream_device);
        }
        if (!status.ok()) {
            rollback_tasks();
            return status;
        }
        shm_batch->stream_device_id = stream_device;
    }

    startTransfer(new_tasks, shm_batch);
    return Status::OK();
}

void NVLinkTransport::startTransfer(std::vector<NVLinkTask*>& tasks,
                                    NVLinkSubBatch* batch) {
    if (tasks.empty()) return;

    // A single small copy completes faster with a blocking cudaMemcpy than
    // with the async stream + completion-event pipeline: the sync call
    // returns already-completed and saves the fixed event record/poll
    // notification latency, while the copy itself runs at the same speed.
    // Only single-task batches take this path: batched submissions amortize
    // the per-copy submit cost across tasks, and blocking copies serialize
    // inside the driver under concurrency, so multi-task batches and sizes
    // at/above async_memcpy_threshold_ keep the async pipeline.
    if (async_memcpy_threshold_ > 0 && tasks.size() == 1 &&
        tasks[0]->request.length < async_memcpy_threshold_) {
        NVLinkTask* task = tasks[0];
        void* src = nullptr;
        void* dst = nullptr;
        if (task->request.opcode == Request::READ) {
            dst = task->request.source;
            src = (void*)task->target_addr;
        } else {
            src = task->request.source;
            dst = (void*)task->target_addr;
        }
        // Match the async path's device discipline (fix for #2722): run the
        // copy on the stream's device, then restore the caller's device.
        int saved_device = -1;
        if (batch->stream_device_id >= 0) {
            auto dev_err = cudaGetDevice(&saved_device);
            if (dev_err == cudaSuccess &&
                saved_device != batch->stream_device_id) {
                dev_err = cudaSetDevice(batch->stream_device_id);
            }
            if (dev_err != cudaSuccess) {
                LOG(ERROR) << "NVLinkTransport: cudaGetDevice/cudaSetDevice "
                              "failed for sync copy: "
                           << cudaGetErrorString(dev_err);
                task->status_word = TransferStatusEnum::FAILED;
                return;
            }
        }
        auto err =
            cudaMemcpy(dst, src, task->request.length, cudaMemcpyDefault);
        if (saved_device >= 0 && saved_device != batch->stream_device_id) {
            cudaSetDevice(saved_device);
        }
        if (err == cudaSuccess) {
            task->transferred_bytes = task->request.length;
            task->status_word = TransferStatusEnum::COMPLETED;
        } else {
            LOG(ERROR) << "NVLinkTransport: synchronous cudaMemcpy failed: "
                       << cudaGetErrorString(err);
            task->status_word = TransferStatusEnum::FAILED;
        }
        return;
    }

    std::vector<void*> srcs;
    std::vector<void*> dsts;
    std::vector<size_t> sizes;
    for (auto* task : tasks) {
        void *src = nullptr, *dst = nullptr;
        if (task->request.opcode == Request::READ) {
            dst = task->request.source;      // read into source buffer
            src = (void*)task->target_addr;  // from remote
        } else {
            src = task->request.source;      // write from source buffer
            dst = (void*)task->target_addr;  // to remote
        }
        srcs.push_back(src);
        dsts.push_back(dst);
        sizes.push_back(task->request.length);
    }

    cudaError_t err;

#if CUDART_VERSION >= 13000
    cudaMemcpyAttributes attr{};
    attr.srcAccessOrder = cudaMemcpySrcAccessOrderStream;
    size_t attrs_idx = 0;
    err = cudaMemcpyBatchAsync(const_cast<const void**>(dsts.data()),
                               const_cast<const void**>(srcs.data()),
                               sizes.data(), srcs.size(), &attr, &attrs_idx, 1,
                               batch->async_stream.get());
#elif CUDART_VERSION >= 12080
    cudaMemcpyAttributes attr{};
    attr.srcAccessOrder = cudaMemcpySrcAccessOrderStream;
    size_t attrs_idx = 0;
    size_t fail_idx = tasks.size();
    err = cudaMemcpyBatchAsync(dsts.data(), srcs.data(), sizes.data(),
                               srcs.size(), &attr, &attrs_idx, 1, &fail_idx,
                               batch->async_stream.get());
    if (err != cudaSuccess && err != cudaErrorCallRequiresNewerDriver &&
        fail_idx < tasks.size()) {
        LOG(ERROR) << "NVLinkTransport::startTransfer internal error: "
                   << "cudaMemcpyBatchAsync failed at task index " << fail_idx
                   << " (src=" << srcs[fail_idx] << ", dst=" << dsts[fail_idx]
                   << ", size=" << sizes[fail_idx]
                   << "): " << cudaGetErrorString(err);
        tasks[fail_idx]->status_word = TransferStatusEnum::FAILED;
    }
#else
    err = cudaErrorCallRequiresNewerDriver;
#endif

    if (err == cudaErrorCallRequiresNewerDriver) {
        cudaGetLastError();
        err = cudaSuccess;
        for (size_t i = 0; i < tasks.size(); ++i) {
            auto single_err =
                cudaMemcpyAsync(dsts[i], srcs[i], sizes[i], cudaMemcpyDefault,
                                batch->async_stream.get());
            if (single_err != cudaSuccess) {
                tasks[i]->status_word = TransferStatusEnum::FAILED;
                err = single_err;
            }
        }
    }

    if (err != cudaSuccess) {
        for (auto* task : tasks) {
            if (task->status_word == TransferStatusEnum::PENDING)
                task->status_word = TransferStatusEnum::FAILED;
        }
    }

    // Save and set device to match the stream's device to ensure event
    // creation and recording happen on the correct device (fix for #2722).
    int saved_device = -1;
    if (batch->stream_device_id >= 0) {
        auto err = cudaGetDevice(&saved_device);
        if (err != cudaSuccess) {
            LOG(ERROR) << "NVLinkTransport: cudaGetDevice failed: "
                       << cudaGetErrorString(err);
            for (auto* task : tasks)
                task->status_word = TransferStatusEnum::FAILED;
            return;
        }
        if (saved_device != batch->stream_device_id) {
            err = cudaSetDevice(batch->stream_device_id);
            if (err != cudaSuccess) {
                LOG(ERROR) << "NVLinkTransport: cudaSetDevice failed: "
                           << cudaGetErrorString(err);
                for (auto* task : tasks)
                    task->status_word = TransferStatusEnum::FAILED;
                return;
            }
        }
    }

    cudaEvent_t event;
    auto event_err = cudaEventCreateWithFlags(&event, cudaEventDisableTiming);
    if (event_err != cudaSuccess) {
        LOG(ERROR) << "NVLinkTransport: cudaEventCreateWithFlags failed: "
                   << cudaGetErrorString(event_err);
        // Restore device before returning
        if (saved_device >= 0 && saved_device != batch->stream_device_id) {
            cudaSetDevice(saved_device);
        }
        for (auto* task : tasks) task->status_word = TransferStatusEnum::FAILED;
        return;
    }
    auto record_err = cudaEventRecord(event, batch->async_stream.get());
    if (record_err != cudaSuccess) {
        LOG(ERROR) << "NVLinkTransport: cudaEventRecord failed: "
                   << cudaGetErrorString(record_err);
        cudaEventDestroy(event);
        // Restore device before returning
        if (saved_device >= 0 && saved_device != batch->stream_device_id) {
            cudaSetDevice(saved_device);
        }
        for (auto* task : tasks) task->status_word = TransferStatusEnum::FAILED;
        return;
    }

    // Restore original device
    if (saved_device >= 0 && saved_device != batch->stream_device_id) {
        cudaSetDevice(saved_device);
    }

    batch->completion_events.push_back(event);
    for (auto* task : tasks) task->completion_event = event;
}

Status NVLinkTransport::getTransferStatus(SubBatchRef batch, int task_id,
                                          TransferStatus& status) {
    auto shm_batch = dynamic_cast<NVLinkSubBatch*>(batch);
    if (task_id < 0 || task_id >= (int)shm_batch->task_list.size()) {
        return Status::InvalidArgument("Invalid task id" LOC_MARK);
    }
    auto& task = shm_batch->task_list[task_id];
    if (task.status_word == TransferStatusEnum::PENDING) {
        auto err = cudaEventQuery(task.completion_event);
        if (err == cudaSuccess) {
            task.transferred_bytes = task.request.length;
            task.status_word = TransferStatusEnum::COMPLETED;
        } else if (err != cudaErrorNotReady) {
            task.status_word = TransferStatusEnum::FAILED;
        }
    }
    // Read status AFTER the poll so a just-observed completion/failure is
    // reported on this call rather than one poll cycle late.
    status = TransferStatus{task.status_word, task.transferred_bytes};
    return Status::OK();
}

Status NVLinkTransport::addMemoryBuffer(BufferDesc& desc,
                                        const MemoryOptions& options) {
    LocationParser location(desc.location);
    if (location.type() == "cpu" || location.type() == kWildcardLocation) {
        if (host_register_)
            CHECK_CUDA(cudaHostRegister((void*)desc.addr, desc.length,
                                        cudaHostRegisterDefault));
        desc.transports.push_back(NVLINK);
        return Status::OK();
    }
    if (location.type() != "cuda")
        return Status::InvalidArgument(
            "NVLink requires CPU or CUDA memory" LOC_MARK);
    if (options.type == MNNVL) return Status::OK();
    std::lock_guard<std::mutex> lock(allocate_mutex_);
    Memory memory;
    auto owned = allocate_set_.find((void*)desc.addr);
    if (owned != allocate_set_.end())
        memory = owned->second;
    else {
        int saved_device = -1;
        CHECK_STATUS(setCudaDeviceForLocation(location, saved_device));
        std::unique_ptr<CudaSharedMemory> retained;
        auto retained_status =
            CudaSharedMemory::retain((void*)desc.addr, &retained);
        auto restored = restoreCudaDeviceForLocation(location, saved_device);
        CHECK_STATUS(retained_status);
        CHECK_STATUS(restored);
        memory = std::move(retained);
    }
    auto registered = registered_memory_.find(memory->address());
    if (registered != registered_memory_.end()) memory = registered->second;
    std::string handle;
    CHECK_STATUS(memory->exportHandle(CU_MEM_HANDLE_TYPE_POSIX_FILE_DESCRIPTOR,
                                      &handle));
    desc.addr = memory->address();
    desc.length = memory->size();
    if (memory->isIpc())
        desc.shm_path = std::move(handle);
    else
        desc.transport_attrs[NVLINK] = std::move(handle);
    registered_memory_[desc.addr] = std::move(memory);
    desc.transports.push_back(NVLINK);
    return Status::OK();
}

Status NVLinkTransport::removeMemoryBuffer(BufferDesc& desc) {
    LocationParser location(desc.location);
    if ((location.type() == "cpu" || location.type() == kWildcardLocation) &&
        host_register_)
        CHECK_CUDA(cudaHostUnregister((void*)desc.addr));
    std::lock_guard<std::mutex> lock(allocate_mutex_);
    registered_memory_.erase(desc.addr);
    desc.transport_attrs.erase(NVLINK);
    desc.shm_path.clear();
    return Status::OK();
}

Status NVLinkTransport::allocateLocalMemory(void** addr, size_t size,
                                            MemoryOptions& options) {
    if (!addr || !size)
        return Status::InvalidArgument("Invalid memory allocation" LOC_MARK);
    if (LocationParser(options.location).type() != "cuda") {
        if (options.global_address)
            return Status::InvalidArgument(
                "NVLink GVA requires CUDA memory" LOC_MARK);
        return Platform::getLoader().allocate(addr, size, options);
    }
    int device = LocationParser(options.location).index();
    if (device < 0) CHECK_CUDA(cudaGetDevice(&device));
    CHECK_CUDA(cudaFree(nullptr));
    CUmemAllocationProp properties{};
    properties.type = CU_MEM_ALLOCATION_TYPE_PINNED;
    properties.location.type = CU_MEM_LOCATION_TYPE_DEVICE;
    properties.location.id = device;
    properties.requestedHandleTypes = CU_MEM_HANDLE_TYPE_POSIX_FILE_DESCRIPTOR;
    std::vector<CUmemAccessDesc> access{CudaSharedMemory::deviceAccess(device)};
    std::unique_ptr<CudaSharedMemory> memory;
    auto status = CudaSharedMemory::create(properties, size, &memory);
    if (!status.ok()) {
        if (options.global_address) return status;
        return Platform::getLoader().allocate(addr, size, options);
    }
    CHECK_STATUS(memory->map(access, options.global_address));
    void* result = (void*)memory->address();
    std::lock_guard<std::mutex> lock(allocate_mutex_);
    allocate_set_[result] = std::move(memory);
    *addr = result;
    return Status::OK();
}

Status NVLinkTransport::freeLocalMemory(void* addr, size_t size) {
    std::lock_guard<std::mutex> lock(allocate_mutex_);
    auto owned = allocate_set_.find(addr);
    if (owned == allocate_set_.end())
        return Platform::getLoader().free(addr, size);
    CHECK_STATUS(owned->second->release());
    allocate_set_.erase(owned);
    return Status::OK();
}

Status NVLinkTransport::getOrMapRemoteMemory(uint64_t& address, uint64_t length,
                                             SegmentID peer,
                                             bool fixed_address) {
    if (!installed_ || !address || !length || length > UINT64_MAX - address)
        return Status::InvalidArgument("Invalid shared-memory range" LOC_MARK);
    int device = 0;
    CHECK_CUDA(cudaGetDevice(&device));
    struct LocalCache {
        const NVLinkTransport* owner = nullptr;
        int device = -1;
        HashMap mappings;
    };
    thread_local LocalCache cache;
    if (cache.owner != this || cache.device != device) {
        cache.mappings.clear();
        cache.owner = this;
        cache.device = device;
    }
    auto& local_cache = cache.mappings;
    if (!fixed_address) {
        for (auto& [base, memory] : local_cache[peer]) {
            if (memory->mapped() && address >= base &&
                address - base <= memory->size() &&
                length <= memory->size() - (address - base)) {
                address = memory->address() + address - base;
                return Status::OK();
            }
        }
    }
    RWSpinlock::WriteGuard guard(relocate_lock_);
    SegmentDescRef pin;
    BufferDesc* buffer = nullptr;
    std::string exported;
    bool ipc = false;
    CHECK_STATUS(metadata_->segmentManager().withCachedSegment(
        peer, pin, [&](SegmentDesc* segment) -> Status {
            buffer = segment->findBuffer(address, length);
            if (!buffer)
                return Status::NeedsRefreshCache(
                    "Shared-memory range is not registered" LOC_MARK);
            auto vmm = buffer->transport_attrs.find(NVLINK);
            if (vmm != buffer->transport_attrs.end())
                exported = vmm->second;
            else {
                exported = buffer->shm_path;
                ipc = true;
            }
            if (segment->machine_id != machine_id_ &&
                !segment->machine_id.empty())
                return Status::InvalidArgument(
                    "NVLink requires a same-host peer" LOC_MARK);
            if (exported.empty())
                return Status::NeedsRefreshCache(
                    "Peer has no shared-memory handle" LOC_MARK);
            return Status::OK();
        }));
    auto& mappings = relocate_map_[peer];
    auto existing = mappings.find(buffer->addr);
    CHECK_CUDA(cudaFree(nullptr));
    if (existing == mappings.end()) {
        std::unique_ptr<CudaSharedMemory> memory;
        if (ipc)
            CHECK_STATUS(CudaSharedMemory::importIpcHandle(
                exported, buffer->length, &memory));
        else
            CHECK_STATUS(CudaSharedMemory::importHandle(
                exported, CU_MEM_HANDLE_TYPE_POSIX_FILE_DESCRIPTOR,
                buffer->length, &memory));
        CHECK_STATUS(memory->map({CudaSharedMemory::deviceAccess(device)},
                                 fixed_address ? buffer->addr : 0));
        existing = mappings.emplace(buffer->addr, std::move(memory)).first;
    } else {
        if (fixed_address && existing->second->address() != buffer->addr)
            return Status::InvalidArgument(
                "Unmap the relocated region before mapping its global "
                "address" LOC_MARK);
        CHECK_STATUS(existing->second->setAccess(
            {CudaSharedMemory::deviceAccess(device)}));
    }
    address = existing->second->address() + address - buffer->addr;
    local_cache[peer][buffer->addr] = existing->second;
    return Status::OK();
}

Status NVLinkTransport::relocateSharedMemoryAddress(uint64_t& address,
                                                    uint64_t length,
                                                    uint64_t target_id) {
    return getOrMapRemoteMemory(address, length, target_id, false);
}

Status NVLinkTransport::mapRemoteMemory(SegmentID peer, void* address,
                                        size_t size) {
    if (peer == LOCAL_SEGMENT_ID)
        return Status::InvalidArgument("Expected a remote segment" LOC_MARK);
    uint64_t mapped = (uint64_t)address;
    return getOrMapRemoteMemory(mapped, size, peer, true);
}

Status NVLinkTransport::unmapRemoteMemory(SegmentID peer, void* address) {
    RWSpinlock::WriteGuard guard(relocate_lock_);
    auto found = relocate_map_.find(peer);
    if (found == relocate_map_.end())
        return Status::InvalidArgument(
            "Memory mapping does not exist" LOC_MARK);
    auto region = found->second.find((uint64_t)address);
    if (region == found->second.end())
        return Status::InvalidArgument(
            "Memory mapping does not exist" LOC_MARK);
    CHECK_STATUS(region->second->release());
    found->second.erase(region);
    if (found->second.empty()) relocate_map_.erase(found);
    return Status::OK();
}

Status NVLinkTransport::setPeerAccess() {
    int device_count = 0;
    int cuda_dev = 0;
    CHECK_CUDA(cudaGetDevice(&cuda_dev));
    CHECK_CUDA(cudaGetDeviceCount(&device_count));
    if (device_count < 2) return Status::OK();
    for (int i = 0; i < device_count; ++i) {
        cudaSetDevice(i);
        for (int j = 0; j < device_count; ++j) {
            if (i == j) continue;
            int can_access = 0;
            cudaDeviceCanAccessPeer(&can_access, i, j);
            if (!can_access) {
                continue;
            }
            cudaError_t err = cudaDeviceEnablePeerAccess(j, 0);
            if (err != cudaSuccess) {
                if (err == cudaErrorPeerAccessAlreadyEnabled) {
                    cudaGetLastError();
                } else {
                    cudaSetDevice(cuda_dev);
                    return Status::InternalError(
                        "cudaDeviceEnablePeerAccess failed");
                }
            }
        }
    }
    cudaSetDevice(cuda_dev);
    return Status::OK();
}
}  // namespace tent
}  // namespace mooncake
