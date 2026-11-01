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

#include <sys/syscall.h>
#include <unistd.h>
// Older glibc (e.g. RHEL8 in manylinux2_28) does not define the pidfd
// syscall numbers in its kernel headers. The numbers below come from the
// arch-independent asm-generic/unistd.h table.
#ifndef SYS_pidfd_open
#define SYS_pidfd_open 434 /* since Linux 5.3 */
#endif
#ifndef SYS_pidfd_getfd
#define SYS_pidfd_getfd 438 /* since Linux 5.6 */
#endif

#include <cuda.h>
#include <cuda_runtime.h>

#include "tent/common/status.h"
#include "tent/runtime/slab.h"
#include "tent/runtime/control_plane.h"
#include "tent/common/utils/string_builder.h"
#include "tent/metrics/tent_metrics.h"

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

static Status roundGranularity(CUmemAllocationProp &prop, size_t granularity,
                               size_t &size) {
    CHECK_CU(cuMemGetAllocationGranularity(&granularity, &prop,
                                           CU_MEM_ALLOC_GRANULARITY_MINIMUM));
    size = (size + granularity - 1) & ~(granularity - 1);
    if (size == 0) size = granularity;
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

// True when `handle` is a host-NUMA (EGM) allocation exportable with
// `handle_type`; used to tell EGM buffers apart from plain host memory that
// only supports cudaHostRegister.
static bool isEgmAllocation(CUmemGenericAllocationHandle handle,
                            CUmemAllocationHandleType handle_type) {
    CUmemAllocationProp prop = {};
    if (cuMemGetAllocationPropertiesFromHandle(&prop, handle) != CUDA_SUCCESS)
        return false;
    return prop.location.type == CU_MEM_LOCATION_TYPE_HOST_NUMA &&
           (prop.requestedHandleTypes & handle_type);
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

struct RetainedAllocation {
    CUmemGenericAllocationHandle handle;
    ~RetainedAllocation() { (void)cuMemRelease(handle); }
};

// CUDA calls below must restore the submitting/polling thread's device on
// errors.
class MnnvlDeviceGuard {
   public:
    explicit MnnvlDeviceGuard(int device) {
        error_ = cudaGetDevice(&saved_);
        if (error_ == cudaSuccess && device >= 0 && saved_ != device)
            error_ = cudaSetDevice(device);
    }
    ~MnnvlDeviceGuard() {
        if (saved_ >= 0) (void)cudaSetDevice(saved_);
    }
    cudaError_t error() const { return error_; }

   private:
    int saved_{-1};
    cudaError_t error_{cudaSuccess};
};

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
/// source buffer before that work has executed. The default CPU wait mirrors
/// IntraNodeNvlinkTransport; gpu_wait enqueues the dependency on the internal
/// stream. Neither mode replaces external fencing for application streams.
Status syncWithCallerStream(cudaStream_t stream, int stream_device,
                            bool gpu_wait) {
    int device_id = 0;
    CHECK_CUDA(cudaGetDevice(&device_id));
    cudaEvent_t event = tl_caller_sync_events.getOrCreate(device_id);
    if (!event) {
        return Status::InternalError(
            "unable to create caller-sync event" LOC_MARK);
    }
    CHECK_CUDA(cudaEventRecord(event, cudaStreamPerThread));
    if (gpu_wait) {
        MnnvlDeviceGuard device(stream_device);
        CHECK_CUDA(device.error());
        // The wait captures this record; a later record on the thread-local
        // event does not change work already waiting on this stream.
        CHECK_CUDA(cudaStreamWaitEvent(stream, event, 0));
    } else {
        CHECK_CUDA(cudaEventSynchronize(event));
    }
    return Status::OK();
}

}  // namespace

MnnvlCompletionEventPool::~MnnvlCompletionEventPool() {
    for (auto &[device, events] : idle_) {
        MnnvlDeviceGuard guard(device);
        for (auto event : events) (void)cudaEventDestroy(event);
    }
}

Status MnnvlCompletionEventPool::acquire(int device, cudaEvent_t &event) {
    MnnvlDeviceGuard guard(device);
    CHECK_CUDA(guard.error());
    {
        std::lock_guard<std::mutex> lock(mutex_);
        auto &events = idle_[device];
        if (!events.empty()) {
            event = events.back();
            events.pop_back();
            TentMetrics::instance().recordMnnvlEvent(
                TentMetrics::MnnvlEvent::PoolHit);
            VLOG(1) << "MnnvlTransport: completion event pool hit device="
                    << device;
            return Status::OK();
        }
    }
    CHECK_CUDA(cudaEventCreateWithFlags(&event, cudaEventDisableTiming));
    TentMetrics::instance().recordMnnvlEvent(TentMetrics::MnnvlEvent::PoolMiss);
    VLOG(1) << "MnnvlTransport: completion event pool miss device=" << device;
    return Status::OK();
}

void MnnvlCompletionEventPool::release(int device, cudaEvent_t event,
                                       bool completed) {
    if (!event) return;
    MnnvlDeviceGuard guard(device);
    if (guard.error() == cudaSuccess && completed && capacity_ != 0) {
        std::lock_guard<std::mutex> lock(mutex_);
        auto &events = idle_[device];
        if (events.size() < capacity_) {
            events.push_back(event);
            return;
        }
    }
    (void)cudaEventDestroy(event);
}

MnnvlCompletion::~MnnvlCompletion() {
    if (!event) return;
    MnnvlDeviceGuard guard(device);
    // Only completed, error-free records can enter the idle pool. Pending
    // events and events that failed recording are never recycled.
    const bool completed =
        guard.error() == cudaSuccess && recorded &&
        (result == cudaSuccess || result == cudaErrorNotReady) &&
        cudaEventQuery(event) == cudaSuccess;
    pool->release(device, event, completed);
}

MnnvlSubBatch::~MnnvlSubBatch() {
    if (!async_stream.get()) return;
    MnnvlDeviceGuard guard(stream_device_id);
    if (guard.error() == cudaSuccess &&
        cudaStreamQuery(async_stream.get()) != cudaSuccess) {
        auto error = cudaStreamSynchronize(async_stream.get());
        if (error != cudaSuccess)
            LOG(ERROR) << "MNNVL drain failed: " << cudaGetErrorString(error);
    }
    // Completion groups destruct before the stream handles return to their
    // pool.
    completion_groups.clear();
}

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

    if (!conf) conf = std::make_shared<Config>();
    CHECK_STATUS(parseMnnvlConfig(*conf, options_));
    completion_pool_ = std::make_shared<MnnvlCompletionEventPool>(
        options_.completion_event_pool ? options_.completion_event_pool_capacity
                                       : 0);
    if (!supportFabricMem()) {
        return Status::InvalidArgument(
            "CUDA Fabric handle type not supported " LOC_MARK);
    }

    platform_ = dynamic_cast<CudaPlatform *>(&Platform::getLoader());
    if (!platform_)
        return Status::InvalidArgument("MNNVL requires the CUDA platform");
    metadata_ = metadata;
    local_segment_name_ = local_segment_name;
    local_topology_ = local_topology;
    conf_ = conf;
    machine_id_ = metadata->segmentManager().getLocal()->machine_id;

    // It seems that cudaMemcpy is much faster than cudaMemcpyAsync
    async_memcpy_threshold_ =
        conf_->get("transports/nvlink/async_memcpy_threshold", 1024) * 1024 *
        128;

    caps.dram_to_gpu = true;
    if (Platform::getLoader().type() != "cpu") caps.gpu_to_gpu = true;

    supported_ = true;
    auto handle_type_str =
        conf_->get("transports/nvlink/handle_type", "fabric");
    if (handle_type_str == "fabric")
        handle_type_ = CU_MEM_HANDLE_TYPE_FABRIC;
    else
        handle_type_ = CU_MEM_HANDLE_TYPE_POSIX_FILE_DESCRIPTOR;
    host_register_ = conf_->get("transports/nvlink/host_register", true);
    egm_enabled_ = options_.egm_enabled;
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
#if CUDART_VERSION >= 12080
    const char *copy_api = "cudaMemcpyBatchAsync";
#else
    const char *copy_api = "cudaMemcpyAsync";
#endif
    LOG(INFO) << "MnnvlTransport: installed segment=" << local_segment_name_
              << " handle_type="
              << (handle_type_ == CU_MEM_HANDLE_TYPE_FABRIC ? "fabric"
                                                            : "posix_fd")
              << " require_fabric=" << options_.require_fabric
              << " completion_event_pool=" << options_.completion_event_pool
              << " pool_capacity="
              << (options_.completion_event_pool
                      ? options_.completion_event_pool_capacity
                      : 0)
              << " sync_mode=" << (options_.gpu_wait ? "gpu_wait" : "cpu_block")
              << " egm=" << egm_enabled_ << " copy_api=" << copy_api
              << " cuda_runtime=" << CUDART_VERSION;
    return Status::OK();
}

Status MnnvlTransport::uninstall() {
    if (installed_) {
        metadata_.reset();
        // TODO close all opened entries
        relocate_map_.clear();
        installed_ = false;
    }
    return Status::OK();
}

Status MnnvlTransport::allocateSubBatch(SubBatchRef &batch, size_t max_size) {
    auto mnnvl_batch = Slab<MnnvlSubBatch>::Get().allocate();
    if (!mnnvl_batch)
        return Status::InternalError("Unable to allocate MNNVL sub-batch");
    batch = mnnvl_batch;
    mnnvl_batch->task_list.reserve(max_size);
    mnnvl_batch->max_size = max_size;
    mnnvl_batch->shared_polling = options_.completion_event_pool;
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
        return Status::InvalidArgument("Invalid MNNVL sub-batch" LOC_MARK);

    std::lock_guard<std::mutex> lock(mnnvl_batch->mutex);
    if (request_list.size() >
        mnnvl_batch->max_size - mnnvl_batch->task_list.size())
        return Status::TooManyRequests("Exceed batch capacity" LOC_MARK);
    if (request_list.empty()) return Status::OK();
    if (!metadata_ || !platform_ || !completion_pool_)
        return Status::InvalidArgument("MNNVL is not installed" LOC_MARK);

    // Get local segment for buffer lookup
    auto &segment_manager = metadata_->segmentManager();
    // Owning reference: keeps the snapshot alive while we read through it.
    SegmentDescRef local_segment = segment_manager.getLocal();
    if (!local_segment)
        return Status::InternalError("Local segment not found" LOC_MARK);

    // Determine device for this batch and validate all requests
    int batch_device_id = -1;
    std::vector<MnnvlTask> validated;
    validated.reserve(request_list.size());

    for (const auto &request : request_list) {
        if (request.opcode != Request::READ && request.opcode != Request::WRITE)
            return Status::InvalidArgument("Invalid MNNVL operation" LOC_MARK);
        // Find the buffer this source pointer belongs to
        BufferDesc *buf = local_segment->findBuffer(
            reinterpret_cast<uint64_t>(request.source), request.length);
        if (!buf) {
            return Status::InvalidArgument(
                "Unregistered buffer: source pointer not in any registered "
                "buffer" LOC_MARK);
        }

        // Device for the copy stream: the buffer's GPU for "cuda:N"; for host
        // buffers ("cpu:N") a GPU attached to NUMA node N, so EGM traffic
        // stays on the owning socket's C2C link (-1 if unknown).
        LocationParser buf_location(buf->location);
        int device_id = buf_location.type() == "cuda"
                            ? buf_location.index()
                            : findDeviceOnNuma(buf_location.index());

        // Capture the first GPU device encountered for stream creation.
        // Mixed-GPU batches use the first GPU's stream and rely on CUDA P2P
        // for cross-device access (same behavior as pre-#2569 code).
        // A future refactor could group requests by device and dispatch to
        // per-device streams, but that requires SubBatch structure changes.
        if (batch_device_id < 0 && device_id >= 0) {
            batch_device_id = device_id;
        }

        // Validate the whole submission before changing existing SubBatch work.
        MnnvlTask task;
        uint64_t target_addr = request.target_offset;
        if (request.target_id == LOCAL_SEGMENT_ID) {
            if (!local_segment->findBuffer(target_addr, request.length))
                return Status::InvalidArgument(
                    "Unregistered MNNVL local target" LOC_MARK);
        } else {
            auto status = relocateSharedMemoryAddress(
                target_addr, request.length, request.target_id);
            if (!status.ok()) return status;
        }

        task.target_addr = target_addr;
        task.request = request;
        task.status_word = TransferStatusEnum::PENDING;
        validated.push_back(std::move(task));
    }

    int caller_device = -1;
    CHECK_CUDA(cudaGetDevice(&caller_device));
    int stream_device =
        mnnvl_batch->async_stream.get()
            ? mnnvl_batch->stream_device_id
            : (batch_device_id >= 0 ? batch_device_id : caller_device);
    CUDAStreamHandle new_stream;
    if (!mnnvl_batch->async_stream.get()) {
        MnnvlDeviceGuard guard(stream_device);
        CHECK_CUDA(guard.error());
        CHECK_STATUS(platform_->getStreamFromPool(new_stream, stream_device));
    }
    cudaStream_t stream = mnnvl_batch->async_stream.get()
                              ? mnnvl_batch->async_stream.get()
                              : new_stream.get();
    auto completion = std::make_unique<MnnvlCompletion>();
    completion->pool = completion_pool_;
    completion->device = stream_device;
    CHECK_STATUS(completion_pool_->acquire(stream_device, completion->event));

    // Reserve all bookkeeping before enqueueing any copy. Record on the
    // original caller device; syncWithCallerStream restores it after a
    // cross-device wait.
    std::vector<MnnvlTask *> new_tasks;
    new_tasks.reserve(validated.size());
    std::vector<void *> srcs, dsts;
    std::vector<size_t> sizes;
    srcs.reserve(validated.size());
    dsts.reserve(validated.size());
    sizes.reserve(validated.size());
    for (const auto &task : validated) {
        void *target = reinterpret_cast<void *>(task.target_addr);
        const bool read = task.request.opcode == Request::READ;
        srcs.push_back(read ? target : task.request.source);
        dsts.push_back(read ? task.request.source : target);
        sizes.push_back(task.request.length);
    }
    mnnvl_batch->completion_groups.reserve(
        mnnvl_batch->completion_groups.size() + 1);
    CHECK_STATUS(
        syncWithCallerStream(stream, stream_device, options_.gpu_wait));
    MnnvlDeviceGuard guard(stream_device);
    CHECK_CUDA(guard.error());

    if (new_stream.get()) {
        mnnvl_batch->async_stream = std::move(new_stream);
        mnnvl_batch->stream_device_id = stream_device;
        VLOG(1) << "MnnvlTransport: copy stream ready batch=" << mnnvl_batch
                << " device=" << stream_device;
    }
    const size_t group = mnnvl_batch->completion_groups.size();
    for (auto &task : validated) {
        task.completion_group = group;
        mnnvl_batch->task_list.push_back(std::move(task));
        new_tasks.push_back(&mnnvl_batch->task_list.back());
    }
    mnnvl_batch->completion_groups.push_back(std::move(completion));
    // Only aggregate diagnostic fields when verbose logging is enabled.
    if (VLOG_IS_ON(1)) {
        uint64_t bytes = 0;
        size_t reads = 0, local = 0;
        for (const auto &request : request_list) {
            bytes += request.length;
            reads += request.opcode == Request::READ;
            local += request.target_id == LOCAL_SEGMENT_ID;
        }
        VLOG(1) << "MnnvlTransport: submit batch=" << mnnvl_batch
                << " group=" << group << " device=" << stream_device
                << " tasks=" << request_list.size() << " bytes=" << bytes
                << " reads=" << reads
                << " writes=" << request_list.size() - reads
                << " local=" << local
                << " remote=" << request_list.size() - local << " handle_type="
                << (handle_type_ == CU_MEM_HANDLE_TYPE_FABRIC ? "fabric"
                                                              : "posix_fd")
                << " sync_mode="
                << (options_.gpu_wait ? "gpu_wait" : "cpu_block");
    }
    startTransfer(new_tasks, srcs, dsts, sizes, mnnvl_batch,
                  *mnnvl_batch->completion_groups.back());
    return Status::OK();
}

void MnnvlTransport::startTransfer(std::vector<MnnvlTask *> &tasks,
                                   std::vector<void *> &srcs,
                                   std::vector<void *> &dsts,
                                   std::vector<size_t> &sizes,
                                   MnnvlSubBatch *batch,
                                   MnnvlCompletion &completion) {
    if (tasks.empty()) return;
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
    if (err != cudaSuccess && fail_idx < tasks.size()) {
        LOG(ERROR) << "MnnvlTransport::startTransfer internal error: "
                   << "cudaMemcpyBatchAsync failed at task index " << fail_idx
                   << " (src=" << srcs[fail_idx] << ", dst=" << dsts[fail_idx]
                   << ", size=" << sizes[fail_idx]
                   << "): " << cudaGetErrorString(err);
    }
#else
    err = cudaSuccess;
    for (size_t i = 0; i < tasks.size(); ++i) {
        auto single_err =
            cudaMemcpyAsync(dsts[i], srcs[i], sizes[i], cudaMemcpyDefault,
                            batch->async_stream.get());
        if (single_err != cudaSuccess) {
            err = single_err;
            break;
        }
    }
#endif

    if (err == cudaSuccess) {
        err = cudaEventRecord(completion.event, batch->async_stream.get());
        completion.recorded = err == cudaSuccess;
    }
    if (err != cudaSuccess) {
        // Batch memcpy may have accepted a prefix. Drain before terminal
        // failure allows runtime teardown or recovery to reuse these buffers.
        // Submission remains successful: returning an error here would invite
        // redispatch.
        const auto drain = cudaStreamSynchronize(batch->async_stream.get());
        LOG(ERROR) << "MNNVL copy/completion failed: "
                   << cudaGetErrorString(err)
                   << "; drain: " << cudaGetErrorString(drain)
                   << "; batch=" << batch
                   << " group=" << tasks.front()->completion_group
                   << " device=" << completion.device;
        completion.result = err;
        for (auto *task : tasks) task->status_word = TransferStatusEnum::FAILED;
    }
}

Status MnnvlTransport::getTransferStatus(SubBatchRef batch, int task_id,
                                         TransferStatus &status) {
    auto mnnvl_batch = dynamic_cast<MnnvlSubBatch *>(batch);
    if (!mnnvl_batch)
        return Status::InvalidArgument("Invalid MNNVL sub-batch" LOC_MARK);
    std::lock_guard<std::mutex> lock(mnnvl_batch->mutex);
    if (task_id < 0 || task_id >= (int)mnnvl_batch->task_list.size()) {
        return Status::InvalidArgument("Invalid task id" LOC_MARK);
    }
    auto &task = mnnvl_batch->task_list[task_id];
    if (task.status_word == TransferStatusEnum::PENDING) {
        auto &completion =
            *mnnvl_batch->completion_groups[task.completion_group];
        if (completion.result == cudaErrorNotReady &&
            (!mnnvl_batch->shared_polling || completion.last_polled_task < 0 ||
             task_id <= completion.last_polled_task)) {
            MnnvlDeviceGuard guard(completion.device);
            TentMetrics::instance().recordMnnvlEvent(
                TentMetrics::MnnvlEvent::CompletionQuery);
            // Once copies are accepted, return terminal task status even if the
            // CUDA context fails; a poll error would invite redispatch.
            completion.result = guard.error() == cudaSuccess
                                    ? cudaEventQuery(completion.event)
                                    : guard.error();
            if (completion.result != cudaSuccess &&
                completion.result != cudaErrorNotReady) {
                const auto drain =
                    cudaStreamSynchronize(mnnvl_batch->async_stream.get());
                LOG(ERROR) << "MNNVL completion query failed: "
                           << cudaGetErrorString(completion.result)
                           << "; drain: " << cudaGetErrorString(drain);
            }
            // The cached terminal result makes this one log per submission,
            // not one per task or repeated status poll.
            if (completion.result != cudaErrorNotReady) {
                VLOG(1) << "MnnvlTransport: complete batch=" << mnnvl_batch
                        << " group=" << task.completion_group
                        << " device=" << completion.device << " status="
                        << (completion.result == cudaSuccess ? "COMPLETED"
                                                             : "FAILED")
                        << " result=" << cudaGetErrorString(completion.result);
            }
        }
        completion.last_polled_task = task_id;
        if (completion.result == cudaSuccess) {
            task.transferred_bytes = task.request.length;
            task.status_word = TransferStatusEnum::COMPLETED;
        } else if (completion.result != cudaErrorNotReady) {
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
    if (!desc.length || desc.addr + desc.length < desc.addr)
        return Status::InvalidArgument("Invalid MNNVL registration range");
    LocationParser location(desc.location);
    bool is_host =
        location.type() == "cpu" || location.type() == kWildcardLocation;
    if (!is_host && location.type() != "cuda")
        return Status::InvalidArgument(
            "Unrecognized location - neither cpu or cuda: " + location.type());

    auto pin_host = [&]() -> Status {
        if (!host_register_) return Status::OK();
        std::lock_guard<std::mutex> lock(registration_mutex_);
        if (pinned_host_buffers_.count(desc.addr))
            return Status::InvalidArgument("MNNVL host buffer already pinned");
        // Allocate bookkeeping before pinning; a failed attempt gets no token.
        auto owner = std::make_shared<const uint8_t>(0);
        auto entry =
            pinned_host_buffers_.emplace(desc.addr, HostPin{desc.length, owner})
                .first;
        auto error = cudaHostRegister((void *)desc.addr, desc.length,
                                      cudaHostRegisterDefault);
        if (error != cudaSuccess) {
            pinned_host_buffers_.erase(entry);
            return Status::InternalError(std::string("cudaHostRegister: ") +
                                         cudaGetErrorString(error));
        }
        desc.mnnvl_host_pin = std::move(owner);
        return Status::OK();
    };
    if (options.perm == kLocalReadWrite) {
        if (!is_host) return Status::OK();
        CUmemGenericAllocationHandle local_handle;
        if (cuMemRetainAllocationHandle(&local_handle, (void *)desc.addr) ==
            CUDA_SUCCESS) {
            RetainedAllocation retained{local_handle};
            if (isEgmAllocation(local_handle, handle_type_))
                return Status::OK();
        }
        return pin_host();
    }
    CUmemGenericAllocationHandle handle;
    auto result = cuMemRetainAllocationHandle(&handle, (void *)desc.addr);
    if (is_host) {
        bool egm = egm_enabled_ && result == CUDA_SUCCESS &&
                   isEgmAllocation(handle, handle_type_);
        if (result == CUDA_SUCCESS && !egm) cuMemRelease(handle);
        if (!egm) {
            if (options_.require_fabric && egm_enabled_) {
                TentMetrics::instance().recordMnnvlEvent(
                    TentMetrics::MnnvlEvent::StrictFailure);
                return Status::InvalidArgument(
                    "Strict MNNVL requires an exportable EGM allocation");
            }
            // Plain host memory: usable as the local side of a transfer
            // (dram_to_gpu) but not addressable by peers.
            return pin_host();
        }
    } else if (result != CUDA_SUCCESS) {
        if (options_.require_fabric) {
            TentMetrics::instance().recordMnnvlEvent(
                TentMetrics::MnnvlEvent::StrictFailure);
            return Status::InvalidArgument(
                "Strict MNNVL requires fabric-exportable GPU memory; use "
                "local-only registration for local endpoints");
        }
        LOG(INFO) << "Memory region " << (void *)desc.addr
                  << "  will not be registered for MNNVL transport.";
        return Status::OK();
    }

    RetainedAllocation retained{handle};
    CUmemAllocationProp prop = {};
    CHECK_CU(cuMemGetAllocationPropertiesFromHandle(&prop, handle));
    if (options_.require_fabric &&
        !(prop.requestedHandleTypes & CU_MEM_HANDLE_TYPE_FABRIC)) {
        TentMetrics::instance().recordMnnvlEvent(
            TentMetrics::MnnvlEvent::StrictFailure);
        return Status::InvalidArgument(
            "Strict MNNVL allocation has no FABRIC handle type");
    }
    void *real_addr = nullptr;
    size_t real_size = 0;
    // cuMemGetAddressRange need not support VMM mappings. Allocations created
    // here have an authoritative mapped extent, including granularity rounding.
    {
        std::lock_guard<std::mutex> lock(allocate_mutex_);
        for (const auto &[base, extent] : allocate_set_) {
            const auto address = reinterpret_cast<uint64_t>(base);
            if (desc.addr >= address && desc.addr - address < extent) {
                real_addr = base;
                real_size = extent;
                break;
            }
        }
    }
    result = real_addr
                 ? CUDA_SUCCESS
                 : cuMemGetAddressRange((CUdeviceptr *)&real_addr, &real_size,
                                        (CUdeviceptr)desc.addr);
    if (result != CUDA_SUCCESS) {
        if (options_.require_fabric)
            return Status::InternalError(
                "Strict MNNVL cannot determine export allocation extent");
        const uint64_t granularity = 2 * 1024 * 1024;
        if (desc.length > UINT64_MAX - (granularity - 1))
            return Status::InvalidArgument("MNNVL export extent overflow");
        real_addr = (void *)desc.addr;
        real_size = (desc.length + granularity - 1) & ~(granularity - 1);
    }
    const auto allocation_base = reinterpret_cast<uint64_t>(real_addr);
    if (desc.addr < allocation_base ||
        desc.addr - allocation_base > real_size ||
        desc.length > real_size - (desc.addr - allocation_base))
        return Status::InvalidArgument("MNNVL registration exceeds allocation");
    const uint64_t allocation_offset = desc.addr - allocation_base;

    if (handle_type_ == CU_MEM_HANDLE_TYPE_POSIX_FILE_DESCRIPTOR) {
        int shared_fd;
        result =
            cuMemExportToShareableHandle(&shared_fd, handle, handle_type_, 0);
        if (result == CUDA_SUCCESS)
            desc.mnnvl_handle =
                std::to_string(getpid()) + "-" + std::to_string(shared_fd);
    } else {
        CUmemFabricHandle export_handle;
        result = cuMemExportToShareableHandle(&export_handle, handle,
                                              handle_type_, 0);
        if (result == CUDA_SUCCESS)
            desc.mnnvl_handle =
                serializeBinaryData(&export_handle, sizeof(CUmemFabricHandle));
    }
    // retained releases the allocation reference on every return path.
    if (result != CUDA_SUCCESS) {
        if (options_.require_fabric)
            TentMetrics::instance().recordMnnvlEvent(
                TentMetrics::MnnvlEvent::StrictFailure);
        return Status::InternalError(
            std::string("cuMemExportToShareableHandle: cuResult ") +
            std::to_string(result) + LOC_MARK);
    }
    // Preserve the logical range for routing, duplicate registration and
    // deregistration. Importers map the allocation separately from that range.
    // A versioned envelope prevents older importers from silently using offset
    // zero for an interior range; whole-allocation handles stay unchanged.
    if (allocation_offset != 0 || desc.length != real_size) {
        desc.mnnvl_handle = json{
            {"version", 1},
            {"handle", desc.mnnvl_handle},
            {"mapped_length", real_size},
            {"offset",
             allocation_offset}}.dump();
    }

    desc.transports.push_back(TransportType::MNNVL);
    VLOG(1) << "MnnvlTransport: exported buffer location=" << desc.location
            << " bytes=" << desc.length << " mapped_bytes=" << real_size
            << " offset=" << allocation_offset << " handle_type="
            << (handle_type_ == CU_MEM_HANDLE_TYPE_FABRIC ? "fabric"
                                                          : "posix_fd");
    return Status::OK();
}

bool MnnvlTransport::tracksLocalBuffer(const BufferDesc &desc) const {
    std::lock_guard<std::mutex> lock(registration_mutex_);
    auto pinned = pinned_host_buffers_.find(desc.addr);
    return pinned != pinned_host_buffers_.end() && desc.mnnvl_host_pin &&
           pinned->second.length == desc.length &&
           pinned->second.owner == desc.mnnvl_host_pin;
}

Status MnnvlTransport::removeMemoryBuffer(BufferDesc &desc) {
    desc.mnnvl_handle.clear();
    std::lock_guard<std::mutex> lock(registration_mutex_);
    auto pinned = pinned_host_buffers_.find(desc.addr);
    if (pinned != pinned_host_buffers_.end() && desc.mnnvl_host_pin &&
        pinned->second.length == desc.length &&
        pinned->second.owner == desc.mnnvl_host_pin) {
        CHECK_CUDA(cudaHostUnregister((void *)desc.addr));
        pinned_host_buffers_.erase(pinned);
    }
    desc.mnnvl_host_pin.reset();
    return Status::OK();
}

Status MnnvlTransport::allocateLocalMemory(void **addr, size_t size,
                                           MemoryOptions &options) {
    LocationParser location(options.location);
    bool is_host = location.type() != "cuda";
    if (is_host && !egm_enabled_) {
        return Platform::getLoader().allocate(addr, size, options);
    }

    CUmemAllocationProp prop = {};
    size_t granularity = 0;
    int numa_node = -1;
    if (is_host) {
        numa_node = location.type() == "cpu" ? location.index() : 0;
        if (numa_node < 0) numa_node = 0;
        CHECK_STATUS(buildEgmAllocationProp(handle_type_, prop, numa_node));
    } else {
        CHECK_STATUS(
            buildCUmemAllocationProp(handle_type_, prop, location.index()));
    }
    CHECK_STATUS(roundGranularity(prop, granularity, size));

    CUmemGenericAllocationHandle handle;
    void *ptr = nullptr;
    auto result = cuMemCreate(&handle, size, &prop, 0);
    if (result != CUDA_SUCCESS) {
        if (options_.require_fabric) {
            TentMetrics::instance().recordMnnvlEvent(
                TentMetrics::MnnvlEvent::StrictFailure);
            return Status::InternalError("Strict MNNVL cuMemCreate failed: " +
                                         std::to_string(result));
        }
        TentMetrics::instance().recordMnnvlEvent(
            TentMetrics::MnnvlEvent::FabricFallback);
        LOG(WARNING) << "Fallback to "
                     << (is_host ? "numa_alloc" : "cudaMalloc")
                     << " because the platform does not support fabric"
                     << (is_host ? " EGM" : "") << " memory: cuResult "
                     << result;
        return Platform::getLoader().allocate(addr, size, options);
    }

    result = cuMemAddressReserve((CUdeviceptr *)&ptr, size, granularity, 0, 0);
    if (result != CUDA_SUCCESS) {
        cuMemRelease(handle);
        return Status::InternalError(
            std::string("cuMemAddressReserve: cuResult ") +
            std::to_string(result) + LOC_MARK);
    }

    result = cuMemMap((CUdeviceptr)ptr, size, 0, handle, 0);
    if (result != CUDA_SUCCESS) {
        cuMemAddressFree((CUdeviceptr)ptr, size);
        cuMemRelease(handle);
        return Status::InternalError(std::string("cuMemMap: cuResult ") +
                                     std::to_string(result) + LOC_MARK);
    }

    int device_count = 0;
    auto cuda_err = cudaGetDeviceCount(&device_count);
    if (cuda_err != cudaSuccess) {
        // Teardown order: unmap, release, then free VA range.
        cuMemUnmap((CUdeviceptr)ptr, size);
        cuMemRelease(handle);
        cuMemAddressFree((CUdeviceptr)ptr, size);
        return Status::InternalError(std::string("cudaGetDeviceCount: ") +
                                     cudaGetErrorString(cuda_err) + LOC_MARK);
    }
    std::vector<CUmemAccessDesc> accessDesc(device_count);
    for (int device_id = 0; device_id < device_count; ++device_id) {
        accessDesc[device_id].location.type = CU_MEM_LOCATION_TYPE_DEVICE;
        accessDesc[device_id].location.id = device_id;
        accessDesc[device_id].flags = CU_MEM_ACCESS_FLAGS_PROT_READWRITE;
    }
    if (is_host) {
        // Host memory is filled and consumed by the CPU as well.
        CUmemAccessDesc host_access = {};
        host_access.location.type = CU_MEM_LOCATION_TYPE_HOST_NUMA;
        host_access.location.id = numa_node;
        host_access.flags = CU_MEM_ACCESS_FLAGS_PROT_READWRITE;
        accessDesc.push_back(host_access);
    }

    result = cuMemSetAccess((CUdeviceptr)ptr, size, accessDesc.data(),
                            accessDesc.size());
    if (result != CUDA_SUCCESS) {
        // Teardown order: unmap, release, then free VA range.
        cuMemUnmap((CUdeviceptr)ptr, size);
        cuMemRelease(handle);
        cuMemAddressFree((CUdeviceptr)ptr, size);
        return Status::InternalError(std::string("cuMemSetAccess: cuResult ") +
                                     std::to_string(result) + LOC_MARK);
    }

    // The mapping holds its own reference to the physical allocation; drop
    // the creator reference so freeLocalMemory's unmap + release actually
    // returns the memory instead of leaking one reference per allocation.
    result = cuMemRelease(handle);
    if (result != CUDA_SUCCESS) {
        LOG(WARNING) << "MnnvlTransport: cuMemRelease after mapping failed: "
                     << result;
    }

    *addr = ptr;
    std::lock_guard<std::mutex> lock(allocate_mutex_);
    allocate_set_[*addr] = size;
    return Status::OK();
}

Status MnnvlTransport::freeLocalMemory(void *addr, size_t size) {
    std::lock_guard<std::mutex> lock(allocate_mutex_);
    auto it = allocate_set_.find(addr);
    if (it == allocate_set_.end()) {
        return Platform::getLoader().free(addr, size);
    }
    // Use the mapped extent recorded at allocation time: callers pass the
    // size they requested, which may be smaller than the granularity-rounded
    // mapping, and cuMemUnmap/cuMemAddressFree need the full range.
    size_t mapped_size = it->second;
    CUmemGenericAllocationHandle handle;
    CHECK_CU(cuMemRetainAllocationHandle(&handle, addr));
    // Teardown order: unmap, release, then free VA range.
    CHECK_CU(cuMemUnmap((CUdeviceptr)addr, mapped_size));
    CHECK_CU(cuMemRelease(handle));
    CHECK_CU(cuMemAddressFree((CUdeviceptr)addr, mapped_size));
    allocate_set_.erase(it);
    return Status::OK();
}

static std::pair<pid_t, int> parsePidFd(const std::string &handle_str) {
    std::stringstream ss(handle_str);
    std::string token;
    std::vector<std::string> parts;

    while (std::getline(ss, token, '-')) parts.push_back(token);
    if (parts.size() != 2) {
        throw std::runtime_error("Invalid mnnvl_handle format: " + handle_str);
    }

    pid_t pid = static_cast<pid_t>(std::stoi(parts[0]));
    int fd = std::stoi(parts[1]);
    return {pid, fd};
}

static int importFdFromProcess(pid_t pid, int remote_fd) {
    int pidfd = syscall(SYS_pidfd_open, pid, 0);
    if (pidfd < 0) {
        perror("pidfd_open failed");
        return -1;
    }

    int newfd = syscall(SYS_pidfd_getfd, pidfd, remote_fd, 0);
    if (newfd < 0) {
        perror("pidfd_getfd failed");
        close(pidfd);
        return -1;
    }

    close(pidfd);
    return newfd;
}

// Legacy handles describe a whole allocation. Only subranges need an envelope.
static Status decodeMnnvlExport(const BufferDesc &buffer,
                                std::string &raw_handle,
                                uint64_t &mapped_length, uint64_t &offset) {
    raw_handle = buffer.mnnvl_handle;
    mapped_length = buffer.length;
    offset = 0;
    if (!raw_handle.empty() && raw_handle.front() == '{') {
        try {
            const auto envelope = json::parse(raw_handle);
            if (!envelope.is_object() || envelope.at("version") != 1 ||
                !envelope.at("handle").is_string() ||
                !envelope.at("mapped_length").is_number_unsigned() ||
                !envelope.at("offset").is_number_unsigned())
                return Status::InvalidArgument("Invalid MNNVL export envelope");
            raw_handle = envelope.at("handle").get<std::string>();
            mapped_length = envelope.at("mapped_length").get<uint64_t>();
            offset = envelope.at("offset").get<uint64_t>();
        } catch (const json::exception &) {
            return Status::InvalidArgument("Invalid MNNVL export envelope");
        }
    }
    if (raw_handle.empty() || !buffer.length || !mapped_length ||
        offset > mapped_length || buffer.length > mapped_length - offset)
        return Status::InvalidArgument("Invalid MNNVL export range");
    return Status::OK();
}

Status MnnvlTransport::relocateSharedMemoryAddress(uint64_t &dest_addr,
                                                   uint64_t length,
                                                   uint64_t target_id) {
    thread_local HashMap tl_relocate_map;
    if (tl_relocate_map.empty()) {
        RWSpinlock::ReadGuard guard(relocate_lock_);
        tl_relocate_map = relocate_map_;
    }

    for (auto &entry : tl_relocate_map[target_id]) {
        if (entry.first <= dest_addr &&
            dest_addr - entry.first <= entry.second.length &&
            length <= entry.second.length - (dest_addr - entry.first)) {
            auto mnnvl_addr = entry.second.mnnvl_addr;
            dest_addr = dest_addr - entry.first + ((uint64_t)mnnvl_addr) +
                        entry.second.mapping_offset;
            return Status::OK();
        }
    }

    RWSpinlock::WriteGuard guard(relocate_lock_);

    BufferDesc *buffer;
    // Owning reference: `buffer` is used after the lambda returns.
    SegmentDescRef pin;
    auto &segment_manager = metadata_->segmentManager();
    CHECK_STATUS(segment_manager.withCachedSegment(
        target_id, pin, [&](SegmentDesc *segment) {
            buffer = segment->findBuffer(dest_addr, length);
            if (!buffer || buffer->mnnvl_handle.empty())
                return Status::NeedsRefreshCache(
                    "Requested address is not in registered buffer" LOC_MARK);
            if (options_.require_fabric &&
                (buffer->permission == kLocalReadWrite ||
                 std::find(buffer->transports.begin(), buffer->transports.end(),
                           MNNVL) == buffer->transports.end()))
                return Status::InvalidArgument(
                    "Peer buffer has no accessible MNNVL fabric export");
            return Status::OK();
        }));

    std::string raw_handle;
    uint64_t mapped_length, allocation_offset;
    CHECK_STATUS(decodeMnnvlExport(*buffer, raw_handle, mapped_length,
                                   allocation_offset));
    if (!relocate_map_[target_id].count(buffer->addr)) {
        CUmemGenericAllocationHandle handle;
        void *mnnvl_addr = nullptr;
        // A non-empty mnnvl_handle (checked above) means the peer exported
        // this buffer, whether it is device memory or EGM host memory.

        // Do NOT switch to buffer->location's device index: that ordinal is
        // the PEER's device index, relative to the peer's
        // CUDA_VISIBLE_DEVICES. In the local process it may be an invalid
        // ordinal, and even when numerically valid it may denote a different
        // physical GPU. The driver calls below are device-agnostic: the VA
        // mapping is process-wide and cuMemSetAccess grants read/write
        // access to ALL local devices.
        if (handle_type_ == CU_MEM_HANDLE_TYPE_POSIX_FILE_DESCRIPTOR) {
            pid_t pid;
            int remote_fd;
            try {
                std::tie(pid, remote_fd) = parsePidFd(raw_handle);
            } catch (const std::exception &) {
                return Status::InvalidArgument(
                    "Invalid peer MNNVL POSIX-FD handle");
            }
            int local_fd = importFdFromProcess(pid, remote_fd);
            if (local_fd < 0)
                return Status::InternalError(
                    "Unable to import fd from remote process");
            // For POSIX_FILE_DESCRIPTOR the driver takes the fd value itself
            // (cast to void*), not a pointer to it; the export side is the
            // one that takes a pointer. Passing &local_fd fails with
            // CUDA_ERROR_UNKNOWN. The imported handle holds its own
            // reference, so the fd is closed right away either way.
            auto import_result = cuMemImportFromShareableHandle(
                &handle, (void *)(uintptr_t)local_fd, handle_type_);
            close(local_fd);
            if (import_result != CUDA_SUCCESS) {
                return Status::InternalError(
                    std::string("cuMemImportFromShareableHandle: cuResult ") +
                    std::to_string(import_result) + LOC_MARK);
            }
        } else {
            if (raw_handle.size() != 2 * sizeof(CUmemFabricHandle))
                return Status::InvalidArgument(
                    "Invalid peer MNNVL FABRIC handle length");
            std::vector<unsigned char> output_buffer;
            try {
                deserializeBinaryData(raw_handle, output_buffer);
            } catch (const std::invalid_argument &) {
                return Status::InvalidArgument(
                    "Invalid peer MNNVL FABRIC handle encoding");
            }
            if (output_buffer.size() != sizeof(CUmemFabricHandle)) {
                return Status::InternalError(
                    "Received MNNVL handle length incorrect");
            }
            CUmemFabricHandle export_handle;
            memcpy(&export_handle, output_buffer.data(), sizeof(export_handle));
            CHECK_CU(cuMemImportFromShareableHandle(&handle, &export_handle,
                                                    handle_type_));
        }
        CHECK_CU(cuMemAddressReserve((CUdeviceptr *)&mnnvl_addr, mapped_length,
                                     0, 0, 0));
        CHECK_CU(
            cuMemMap((CUdeviceptr)mnnvl_addr, mapped_length, 0, handle, 0));
        int device_count;
        cudaGetDeviceCount(&device_count);
        CUmemAccessDesc accessDesc[device_count];
        for (int device_id = 0; device_id < device_count; ++device_id) {
            accessDesc[device_id].location.type = CU_MEM_LOCATION_TYPE_DEVICE;
            accessDesc[device_id].location.id = device_id;
            accessDesc[device_id].flags = CU_MEM_ACCESS_FLAGS_PROT_READWRITE;
        }
        CHECK_CU(cuMemSetAccess((CUdeviceptr)mnnvl_addr, mapped_length,
                                accessDesc, device_count));
        OpenedMnnvlEntry mnnvl_entry;
        mnnvl_entry.mnnvl_addr = mnnvl_addr;
        mnnvl_entry.length = buffer->length;
        mnnvl_entry.mapped_length = mapped_length;
        mnnvl_entry.mapping_offset = allocation_offset;
        int cuda_dev = 0;
        CHECK_CUDA(cudaGetDevice(&cuda_dev));
        mnnvl_entry.cuda_id = cuda_dev;
        relocate_map_[target_id][buffer->addr] = mnnvl_entry;
        VLOG(1) << "MnnvlTransport: imported peer buffer target_id="
                << target_id << " location=" << buffer->location
                << " bytes=" << buffer->length
                << " mapped_bytes=" << mapped_length
                << " offset=" << allocation_offset << " handle_type="
                << (handle_type_ == CU_MEM_HANDLE_TYPE_FABRIC ? "fabric"
                                                              : "posix_fd");
    }

    const auto &entry = relocate_map_[target_id][buffer->addr];
    dest_addr = dest_addr - buffer->addr + ((uint64_t)entry.mnnvl_addr) +
                entry.mapping_offset;
    tl_relocate_map = relocate_map_;
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
