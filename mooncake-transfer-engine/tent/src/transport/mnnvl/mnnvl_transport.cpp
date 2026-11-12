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

#include "tent/transport/mnnvl/mnnvl_transport.h"

#include <glog/logging.h>

#include <algorithm>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <iomanip>
#include <memory>
#include <unordered_map>

#include <cuda.h>
#include <cuda_runtime.h>

#include "tent/common/status.h"
#include "tent/runtime/slab.h"
#include "tent/runtime/control_plane.h"
#include "tent/common/utils/string_builder.h"

namespace mooncake {
namespace tent {

static Status buildCUmemAllocationProp(CUmemAllocationHandleType handle_type,
                                       CUmemAllocationProp &prop,
                                       int device_id) {
    prop.type = CU_MEM_ALLOCATION_TYPE_PINNED;
    prop.location.type = CU_MEM_LOCATION_TYPE_DEVICE;
    prop.requestedHandleTypes = handle_type;
    CUdevice device;
    CHECK_CU(cuDeviceGet(&device, device_id));
    prop.location.id = device;
    int flag = 0;
    CHECK_CU(cuDeviceGetAttribute(
        &flag, CU_DEVICE_ATTRIBUTE_GPU_DIRECT_RDMA_WITH_CUDA_VMM_SUPPORTED,
        device));
    if (flag) prop.allocFlags.gpuDirectRDMACapable = 1;
    return Status::OK();
}

// Extended GPU Memory (EGM): host DRAM allocated through the CUDA VMM API
// with a HOST_NUMA location. Such allocations can be exported as fabric
// handles and read/written by GPUs on other nodes of a multi-node NVLink
// domain, so host-resident data (e.g. a CPU-side weight or KV cache) moves
// over NVLink instead of the NIC.
static Status buildEgmAllocationProp(CUmemAllocationHandleType handle_type,
                                     CUmemAllocationProp &prop, int numa_node) {
    prop.type = CU_MEM_ALLOCATION_TYPE_PINNED;
    prop.location.type = CU_MEM_LOCATION_TYPE_HOST_NUMA;
    prop.location.id = numa_node;
    prop.requestedHandleTypes = handle_type;
    return Status::OK();
}

// First CUDA device attached to `numa_node` (-1 when unknown). Copies to or
// from EGM memory should run on a GPU of the owning socket: the path is
// bounded by that socket's C2C link, and a GPU on the other socket adds a
// CPU-to-CPU hop.
static int findDeviceOnNuma(int numa_node) {
    if (numa_node < 0) return -1;
    int device_count = 0;
    if (cudaGetDeviceCount(&device_count) != cudaSuccess) return -1;
    for (int device_id = 0; device_id < device_count; ++device_id) {
        int host_numa = -1;
        if (cudaDeviceGetAttribute(&host_numa, cudaDevAttrHostNumaId,
                                   device_id) == cudaSuccess &&
            host_numa == numa_node)
            return device_id;
    }
    return -1;
}

static bool supportFabricMem() {
    int num_devices = 0;
    cudaError_t err = cudaGetDeviceCount(&num_devices);
    if (err != cudaSuccess || num_devices == 0) {
        LOG(ERROR) << "Unable to find CUDA devices: "
                   << cudaGetErrorString(err);
        return false;
    }

    for (int device_id = 0; device_id < num_devices; ++device_id) {
        int device_support_fabric_mem = 0;
        auto result = cuDeviceGetAttribute(
            &device_support_fabric_mem,
            CU_DEVICE_ATTRIBUTE_HANDLE_TYPE_FABRIC_SUPPORTED, device_id);
        if (result != CUDA_SUCCESS || !device_support_fabric_mem) {
            LOG(ERROR) << "Some CUDA devices does not support FABRIC mode";
            return false;
        }
    }
    return true;
}

namespace {

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
        for (auto &entry : events_) {
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

MnnvlTransport::MnnvlTransport() : installed_(false) {}

MnnvlTransport::~MnnvlTransport() { uninstall(); }

Status MnnvlTransport::install(std::string &local_segment_name,
                               std::shared_ptr<ControlService> metadata,
                               std::shared_ptr<Topology> local_topology,
                               std::shared_ptr<Config> conf) {
    if (installed_) {
        return Status::InvalidArgument(
            "MNNVL transport has been installed " LOC_MARK);
    }

    platform_ = dynamic_cast<CudaPlatform *>(&Platform::getLoader());
    metadata_ = metadata;
    local_segment_name_ = local_segment_name;
    local_topology_ = local_topology;
    conf_ = conf ? conf : std::make_shared<Config>();
    machine_id_ = metadata->segmentManager().getLocal()->machine_id;

    // It seems that cudaMemcpy is much faster than cudaMemcpyAsync
    async_memcpy_threshold_ =
        conf_->get("transports/nvlink/async_memcpy_threshold", 1024) * 1024 *
        128;

    caps.dram_to_gpu = true;
    if (Platform::getLoader().type() != "cpu") caps.gpu_to_gpu = true;

    auto handle_type_str =
        conf_->get("transports/mnnvl/handle_type",
                   conf_->get("transports/nvlink/handle_type", "fabric"));
    if (handle_type_str == "fabric")
        handle_type_ = CU_MEM_HANDLE_TYPE_FABRIC;
    else if (handle_type_str == "posix_fd")
        handle_type_ = CU_MEM_HANDLE_TYPE_POSIX_FILE_DESCRIPTOR;
    else
        return Status::InvalidArgument("Unknown MNNVL handle type" LOC_MARK);
    if (handle_type_ == CU_MEM_HANDLE_TYPE_FABRIC && !supportFabricMem())
        return Status::NotImplemented(
            "CUDA Fabric handle type not supported" LOC_MARK);
    host_register_ = conf_->get("transports/nvlink/host_register", true);
    egm_enabled_ = conf_->get("transports/mnnvl/egm", false);
    if (egm_enabled_) {
        // Host buffers allocated through allocateLocalMemory() (or any
        // HOST_NUMA VMM allocation registered by the caller) are exported
        // like device memory, so peers can address them directly.
        caps.dram_to_dram = true;
        caps.gpu_to_dram = true;
        LOG(INFO) << "MnnvlTransport: EGM host memory over NVLink enabled";
    }
    CHECK_STATUS(setPeerAccess());
    installed_ = true;
    return Status::OK();
}

Status MnnvlTransport::uninstall() {
    if (!installed_) return Status::OK();
    RWSpinlock::WriteGuard guard(relocate_lock_);
    for (auto &peer : relocate_map_)
        for (auto &entry : peer.second) CHECK_STATUS(entry.second->release());
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

Status MnnvlTransport::allocateSubBatch(SubBatchRef &batch, size_t max_size) {
    auto mnnvl_batch = Slab<MnnvlSubBatch>::Get().allocate();
    if (!mnnvl_batch)
        return Status::InternalError("Unable to allocate MNNVL sub-batch");
    batch = mnnvl_batch;
    mnnvl_batch->task_list.reserve(max_size);
    mnnvl_batch->max_size = max_size;
    // Streams are created lazily in submitTransferTasks where the correct
    // GPU device can be inferred from request source pointers.
    return Status::OK();
}

Status MnnvlTransport::freeSubBatch(SubBatchRef &batch) {
    auto mnnvl_batch = dynamic_cast<MnnvlSubBatch *>(batch);
    if (!mnnvl_batch)
        return Status::InvalidArgument("Invalid MNNVL sub-batch" LOC_MARK);
    // Completion events are destroyed by ~MnnvlSubBatch(), which Slab's
    // deallocate() invokes before returning the storage. Streams are
    // pool-managed and not destroyed here.
    Slab<MnnvlSubBatch>::Get().deallocate(mnnvl_batch);
    batch = nullptr;
    return Status::OK();
}

Status MnnvlTransport::submitTransferTasks(
    SubBatchRef batch, const std::vector<Request> &request_list) {
    auto mnnvl_batch = dynamic_cast<MnnvlSubBatch *>(batch);
    if (!mnnvl_batch)
        return Status::InvalidArgument("Invalid NVLink sub-batch" LOC_MARK);

    if (request_list.size() + mnnvl_batch->task_list.size() >
        mnnvl_batch->max_size)
        return Status::TooManyRequests("Exceed batch capacity" LOC_MARK);

    // Producer work already queued on the caller's per-thread stream must be
    // visible before the batched copy runs on the transport's internal
    // stream; without this the copy races with (and can read the source
    // before) that work. Same contract as IntraNodeNvlinkTransport.
    CHECK_STATUS(syncWithCallerStream());

    // Get local segment for buffer lookup
    auto &segment_manager = metadata_->segmentManager();
    // Owning reference: keeps the snapshot alive while we read through it.
    SegmentDescRef local_segment = segment_manager.getLocal();
    if (!local_segment)
        return Status::InternalError("Local segment not found" LOC_MARK);

    // Determine device for this batch and validate all requests
    int batch_device_id = -1;
    std::vector<MnnvlTask *> new_tasks;
    // No I/O is started until startTransfer() below, so a synchronous
    // failure must also roll back the half-appended task entries: the engine
    // fails such tasks over to another transport, and stale never-started
    // entries must not linger in the sub-batch (failover.md, hazard 2).
    const size_t original_task_count = mnnvl_batch->task_list.size();
    auto rollback_tasks = [&mnnvl_batch, original_task_count]() {
        mnnvl_batch->task_list.resize(original_task_count);
    };

    for (auto &request : request_list) {
        // Find the buffer this source pointer belongs to
        BufferDesc *buf = local_segment->findBuffer(
            reinterpret_cast<uint64_t>(request.source), request.length);
        if (!buf) {
            rollback_tasks();
            return Status::InvalidArgument(
                "Unregistered buffer: source pointer not in any registered "
                "buffer" LOC_MARK);
        }

        // Parse device ID from buffer location (e.g., "cuda:0" -> 0, "cpu" ->
        // -1)
        LocationParser location(buf->location);
        int device_id = location.type() == "cuda"
                            ? location.index()
                            : findDeviceOnNuma(location.index());

        // Capture the first GPU device encountered for stream creation.
        // Mixed-GPU batches use the first GPU's stream and rely on CUDA P2P
        // for cross-device access (same behavior as pre-#2569 code).
        // A future refactor could group requests by device and dispatch to
        // per-device streams, but that requires SubBatch structure changes.
        if (batch_device_id < 0 && device_id >= 0) {
            batch_device_id = device_id;
        }

        // Create and populate task
        mnnvl_batch->task_list.push_back(MnnvlTask{});
        auto &task = mnnvl_batch->task_list[mnnvl_batch->task_list.size() - 1];

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
    if (!mnnvl_batch->async_stream.get()) {
        int stream_device = batch_device_id;
        if (stream_device < 0) {
            // CPU-only batch: use current CUDA device
            cudaGetDevice(&stream_device);
        }
        auto status = platform_->getStreamFromPool(mnnvl_batch->sync_stream,
                                                   stream_device);
        if (status.ok()) {
            status = platform_->getStreamFromPool(mnnvl_batch->async_stream,
                                                  stream_device);
        }
        if (!status.ok()) {
            rollback_tasks();
            return status;
        }
        mnnvl_batch->stream_device_id = stream_device;
    }

    startTransfer(new_tasks, mnnvl_batch);
    return Status::OK();
}

void MnnvlTransport::startTransfer(std::vector<MnnvlTask *> &tasks,
                                   MnnvlSubBatch *batch) {
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
        MnnvlTask *task = tasks[0];
        void *src = nullptr;
        void *dst = nullptr;
        if (task->request.opcode == Request::READ) {
            dst = task->request.source;
            src = (void *)task->target_addr;
        } else {
            src = task->request.source;
            dst = (void *)task->target_addr;
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
                LOG(ERROR) << "MnnvlTransport: cudaGetDevice/cudaSetDevice "
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
            LOG(ERROR) << "MnnvlTransport: synchronous cudaMemcpy failed: "
                       << cudaGetErrorString(err);
            task->status_word = TransferStatusEnum::FAILED;
        }
        return;
    }

    std::vector<void *> srcs;
    std::vector<void *> dsts;
    std::vector<size_t> sizes;
    for (auto *task : tasks) {
        void *src = nullptr, *dst = nullptr;
        if (task->request.opcode == Request::READ) {
            dst = task->request.source;       // read into source buffer
            src = (void *)task->target_addr;  // from remote
        } else {
            src = task->request.source;       // write from source buffer
            dst = (void *)task->target_addr;  // to remote
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
    err = cudaMemcpyBatchAsync(const_cast<const void **>(dsts.data()),
                               const_cast<const void **>(srcs.data()),
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
        LOG(ERROR) << "MnnvlTransport::startTransfer internal error: "
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
        for (auto *task : tasks) {
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
            LOG(ERROR) << "MnnvlTransport: cudaGetDevice failed: "
                       << cudaGetErrorString(err);
            for (auto *task : tasks)
                task->status_word = TransferStatusEnum::FAILED;
            return;
        }
        if (saved_device != batch->stream_device_id) {
            err = cudaSetDevice(batch->stream_device_id);
            if (err != cudaSuccess) {
                LOG(ERROR) << "MnnvlTransport: cudaSetDevice failed: "
                           << cudaGetErrorString(err);
                for (auto *task : tasks)
                    task->status_word = TransferStatusEnum::FAILED;
                return;
            }
        }
    }

    cudaEvent_t event;
    auto event_err = cudaEventCreateWithFlags(&event, cudaEventDisableTiming);
    if (event_err != cudaSuccess) {
        LOG(ERROR) << "MnnvlTransport: cudaEventCreateWithFlags failed: "
                   << cudaGetErrorString(event_err);
        // Restore device before returning
        if (saved_device >= 0 && saved_device != batch->stream_device_id) {
            cudaSetDevice(saved_device);
        }
        for (auto *task : tasks) task->status_word = TransferStatusEnum::FAILED;
        return;
    }
    auto record_err = cudaEventRecord(event, batch->async_stream.get());
    if (record_err != cudaSuccess) {
        LOG(ERROR) << "MnnvlTransport: cudaEventRecord failed: "
                   << cudaGetErrorString(record_err);
        cudaEventDestroy(event);
        // Restore device before returning
        if (saved_device >= 0 && saved_device != batch->stream_device_id) {
            cudaSetDevice(saved_device);
        }
        for (auto *task : tasks) task->status_word = TransferStatusEnum::FAILED;
        return;
    }

    // Restore original device
    if (saved_device >= 0 && saved_device != batch->stream_device_id) {
        cudaSetDevice(saved_device);
    }

    batch->completion_events.push_back(event);
    for (auto *task : tasks) task->completion_event = event;
}

Status MnnvlTransport::getTransferStatus(SubBatchRef batch, int task_id,
                                         TransferStatus &status) {
    auto mnnvl_batch = dynamic_cast<MnnvlSubBatch *>(batch);
    if (task_id < 0 || task_id >= (int)mnnvl_batch->task_list.size()) {
        return Status::InvalidArgument("Invalid task id" LOC_MARK);
    }
    auto &task = mnnvl_batch->task_list[task_id];
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

Status MnnvlTransport::addMemoryBuffer(BufferDesc &desc,
                                       const MemoryOptions &options) {
    LocationParser location(desc.location);
    const bool host =
        location.type() == "cpu" || location.type() == kWildcardLocation;
    if (!host && location.type() != "cuda")
        return Status::InvalidArgument(
            "MNNVL requires CPU or CUDA memory" LOC_MARK);
    std::lock_guard<std::mutex> lock(allocate_mutex_);
    Memory memory;
    auto owned = allocate_set_.find((void *)desc.addr);
    if (owned != allocate_set_.end())
        memory = owned->second;
    else if (!host || egm_enabled_) {
        std::unique_ptr<CudaSharedMemory> retained;
        auto status = CudaSharedMemory::retain((void *)desc.addr, &retained);
        if (status.ok())
            memory = std::move(retained);
        else if (!host)
            return status;
    }
    if (!memory || memory->isIpc()) {
        if (host && host_register_)
            CHECK_CUDA(cudaHostRegister((void *)desc.addr, desc.length,
                                        cudaHostRegisterDefault));
        return Status::OK();
    }
    auto registered = registered_memory_.find(memory->address());
    if (registered != registered_memory_.end()) memory = registered->second;
    CHECK_STATUS(memory->exportHandle(handle_type_, &desc.mnnvl_handle));
    desc.addr = memory->address();
    desc.length = memory->size();
    registered_memory_[desc.addr] = std::move(memory);
    desc.transports.push_back(MNNVL);
    return Status::OK();
}

Status MnnvlTransport::removeMemoryBuffer(BufferDesc &desc) {
    if (desc.mnnvl_handle.empty() &&
        LocationParser(desc.location).type() == "cpu" && host_register_)
        CHECK_CUDA(cudaHostUnregister((void *)desc.addr));
    std::lock_guard<std::mutex> lock(allocate_mutex_);
    registered_memory_.erase(desc.addr);
    desc.mnnvl_handle.clear();
    return Status::OK();
}

Status MnnvlTransport::allocateLocalMemory(void **addr, size_t size,
                                           MemoryOptions &options) {
    if (!addr || !size)
        return Status::InvalidArgument("Invalid memory allocation" LOC_MARK);
    LocationParser location(options.location);
    const bool host = location.type() != "cuda";
    if (host && !egm_enabled_) {
        if (options.global_address)
            return Status::NotImplemented(
                "MNNVL host GVA requires EGM" LOC_MARK);
        return Platform::getLoader().allocate(addr, size, options);
    }
    CHECK_CUDA(cudaFree(nullptr));
    CUmemAllocationProp properties{};
    std::vector<CUmemAccessDesc> access;
    CHECK_STATUS(CudaSharedMemory::allDeviceAccess(&access));
    if (host) {
        int numa = location.index() < 0 ? 0 : location.index();
        CHECK_STATUS(buildEgmAllocationProp(handle_type_, properties, numa));
        CUmemAccessDesc cpu{};
        cpu.location.type = CU_MEM_LOCATION_TYPE_HOST_NUMA;
        cpu.location.id = numa;
        cpu.flags = CU_MEM_ACCESS_FLAGS_PROT_READWRITE;
        access.push_back(cpu);
    } else {
        int device = location.index();
        if (device < 0) CHECK_CUDA(cudaGetDevice(&device));
        CHECK_STATUS(
            buildCUmemAllocationProp(handle_type_, properties, device));
    }
    std::unique_ptr<CudaSharedMemory> memory;
    auto status = CudaSharedMemory::create(properties, size, &memory);
    if (!status.ok()) {
        if (options.global_address) return status;
        return Platform::getLoader().allocate(addr, size, options);
    }
    CHECK_STATUS(memory->map(access, options.global_address));
    void *result = (void *)memory->address();
    std::lock_guard<std::mutex> lock(allocate_mutex_);
    allocate_set_[result] = std::move(memory);
    *addr = result;
    return Status::OK();
}

Status MnnvlTransport::freeLocalMemory(void *addr, size_t size) {
    std::lock_guard<std::mutex> lock(allocate_mutex_);
    auto owned = allocate_set_.find(addr);
    if (owned == allocate_set_.end())
        return Platform::getLoader().free(addr, size);
    CHECK_STATUS(owned->second->release());
    allocate_set_.erase(owned);
    return Status::OK();
}

Status MnnvlTransport::getOrMapRemoteMemory(uint64_t &address, uint64_t length,
                                            SegmentID peer,
                                            bool fixed_address) {
    if (!installed_ || !address || !length || length > UINT64_MAX - address)
        return Status::InvalidArgument("Invalid shared-memory range" LOC_MARK);
    int device = 0;
    CHECK_CUDA(cudaGetDevice(&device));
    struct LocalCache {
        const MnnvlTransport *owner = nullptr;
        int device = -1;
        HashMap mappings;
    };
    thread_local LocalCache cache;
    if (cache.owner != this || cache.device != device) {
        cache.mappings.clear();
        cache.owner = this;
        cache.device = device;
    }
    auto &local_cache = cache.mappings;
    if (!fixed_address) {
        for (auto &[base, memory] : local_cache[peer]) {
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
    BufferDesc *buffer = nullptr;
    std::string exported;
    CHECK_STATUS(metadata_->segmentManager().withCachedSegment(
        peer, pin, [&](SegmentDesc *segment) -> Status {
            buffer = segment->findBuffer(address, length);
            if (!buffer)
                return Status::NeedsRefreshCache(
                    "Shared-memory range is not registered" LOC_MARK);
            exported = buffer->mnnvl_handle;
            if (handle_type_ == CU_MEM_HANDLE_TYPE_POSIX_FILE_DESCRIPTOR &&
                segment->machine_id != machine_id_ &&
                !segment->machine_id.empty())
                return Status::InvalidArgument(
                    "POSIX memory handles require a same-host peer" LOC_MARK);
            if (exported.empty())
                return Status::NeedsRefreshCache(
                    "Peer has no shared-memory handle" LOC_MARK);
            return Status::OK();
        }));
    auto &mappings = relocate_map_[peer];
    auto existing = mappings.find(buffer->addr);
    CHECK_CUDA(cudaFree(nullptr));
    if (existing == mappings.end()) {
        std::unique_ptr<CudaSharedMemory> memory;
        CHECK_STATUS(CudaSharedMemory::importHandle(exported, handle_type_,
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

Status MnnvlTransport::relocateSharedMemoryAddress(uint64_t &address,
                                                   uint64_t length,
                                                   uint64_t target_id) {
    return getOrMapRemoteMemory(address, length, target_id, false);
}

Status MnnvlTransport::mapRemoteMemory(SegmentID peer, void *address,
                                       size_t size) {
    if (peer == LOCAL_SEGMENT_ID)
        return Status::InvalidArgument("Expected a remote segment" LOC_MARK);
    uint64_t mapped = (uint64_t)address;
    return getOrMapRemoteMemory(mapped, size, peer, true);
}

Status MnnvlTransport::unmapRemoteMemory(SegmentID peer, void *address) {
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

Status MnnvlTransport::setPeerAccess() {
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
