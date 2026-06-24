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

#include "transport/intranode_nvlink_transport/intranode_nvlink_transport.h"

#include <bits/stdint-uintn.h>
#include "cuda_alike.h"
#include <glog/logging.h>

#include <algorithm>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <iomanip>
#include <memory>
#include <vector>

#include "common.h"
#include "common/serialization.h"
#include "config.h"
#include "transfer_engine.h"
#include "transfer_metadata.h"
#include "transport/transport.h"

static bool checkCudaErrorReturn(cudaError_t result, const char *message) {
    if (result != cudaSuccess) {
        LOG(ERROR) << message << " (Error code: " << result << " - "
                   << cudaGetErrorString(result) << ")" << std::endl;
        return false;
    }
    return true;
}

namespace mooncake {

namespace {

/// Per-device CUDA stream pool (thread-local).
struct CudaStreamEntry {
    cudaStream_t stream;
    int device_id;
};

class PerDeviceStreamPool {
   public:
    CudaStreamEntry getOrCreate(int device_id) {
        auto it = pool_.find(device_id);
        if (it != pool_.end()) return it->second;
        int saved_device = 0;
        cudaGetDevice(&saved_device);
        if (cudaSetDevice(device_id) != cudaSuccess) {
            LOG(ERROR) << "IntraNodeNvlinkTransport: cudaSetDevice("
                       << device_id << ") failed";
            return {nullptr, -1};
        }
        CudaStreamEntry entry;
        entry.device_id = device_id;
        cudaError_t err = cudaStreamCreateWithFlags(&entry.stream,
                                                    cudaStreamNonBlocking);
        if (err != cudaSuccess)
            LOG(FATAL) << "Failed to create NVLink CUDA stream on device "
                       << device_id << ": " << cudaGetErrorString(err);
        cudaSetDevice(saved_device);
        cudaDeviceProp prop;
        std::string pci = "unknown";
        if (cudaGetDeviceProperties(&prop, device_id) == cudaSuccess) {
            pci = std::string(prop.name) + " PCI " +
                  std::to_string(prop.pciBusID) + ":" +
                  std::to_string(prop.pciDeviceID);
        }
        const char* visible = getenv("CUDA_VISIBLE_DEVICES");
        LOG(INFO) << "IntraNodeNvlinkTransport: NVLink CUDA stream created on "
                  << "device " << device_id << " [physical: " << pci
                  << "] CUDA_VISIBLE_DEVICES="
                  << (visible ? visible : "(not set)")
                  << " pid=" << getpid();
        pool_[device_id] = entry;
        return entry;
    }
    ~PerDeviceStreamPool() {
        int saved_device = 0;
        cudaGetDevice(&saved_device);
        for (auto &kv : pool_) {
            cudaSetDevice(kv.first);
            if (kv.second.stream) cudaStreamDestroy(kv.second.stream);
        }
        cudaSetDevice(saved_device);
    }
   private:
    std::unordered_map<int, CudaStreamEntry> pool_;
};

static thread_local PerDeviceStreamPool tl_device_stream_pool;

static thread_local cudaEvent_t tl_caller_sync_event = nullptr;
static thread_local int tl_caller_sync_event_device = -1;

static cudaEvent_t getCallerSyncEvent() {
    int current_device = 0;
    cudaGetDevice(&current_device);
    if (tl_caller_sync_event_device != current_device) {
        if (tl_caller_sync_event) cudaEventDestroy(tl_caller_sync_event);
        cudaError_t err = cudaEventCreateWithFlags(&tl_caller_sync_event,
                                                    cudaEventDisableTiming);
        if (err != cudaSuccess)
            LOG(FATAL) << "Failed to create NVLink sync event on device "
                       << current_device << ": " << cudaGetErrorString(err);
        tl_caller_sync_event_device = current_device;
    }
    return tl_caller_sync_event;
}

static int getDeviceForPointer(const void *ptr) {
    cudaPointerAttributes attr;
    if (cudaPointerGetAttributes(&attr, ptr) != cudaSuccess) {
        cudaGetLastError();
        return -1;
    }
    return (attr.type == cudaMemoryTypeDevice) ? attr.device : -1;
}

static CudaStreamEntry getStreamForRequest(const void *source) {
    int device_id = getDeviceForPointer(source);
    if (device_id < 0) {
        cudaGetDevice(&device_id);
        if (device_id < 0) device_id = 0;
    }
    return tl_device_stream_pool.getOrCreate(device_id);
}

}  // anonymous namespace

using Slice = Transport::Slice;

/// Submit batched memcpy operations using cudaMemcpyBatchAsync when available
/// (CUDA 12.8+), falling back to per-slice cudaMemcpyAsync otherwise.
/// Uses cudaMemcpySrcAccessOrderStream to ensure source data visibility for
/// P2P copies. This attribute is REQUIRED — without it, the GPU does not
/// insert the necessary memory barriers for P2P access, causing segfaults.
/// The caller must also establish GPU-level stream synchronization
/// (via cudaEventRecord + cudaStreamWaitEvent) before calling this function.
/// Individual slice errors are tracked so that slices whose memcpy failed
/// are marked as FAILED while successfully submitted ones are POSTED.
static void submitBatchMemcpy(const std::vector<Slice *> &slices,
                              const std::vector<void *> &srcs,
                              const std::vector<void *> &dsts,
                              const std::vector<size_t> &sizes,
                              cudaStream_t stream) {
    if (slices.empty()) return;

    const size_t count = slices.size();
    cudaError_t err = cudaSuccess;

    // Log the active memcpy path once per process lifetime
    static const bool logged_once = [] {
#if CUDART_VERSION >= 13000
        LOG(INFO) << "IntraNodeNvlinkTransport: using cudaMemcpyBatchAsync "
                  << "(CUDA >= 13.0 path)";
#elif CUDART_VERSION >= 12080
        LOG(INFO) << "IntraNodeNvlinkTransport: using cudaMemcpyBatchAsync "
                  << "(CUDA >= 12.8 path)";
#else
        LOG(INFO)
            << "IntraNodeNvlinkTransport: using per-slice cudaMemcpyAsync "
            << "(CUDA < 12.8 fallback path)";
#endif
        return true;
    }();
    (void)logged_once;

#if CUDART_VERSION >= 12080
    // srcAccessOrderStream is REQUIRED for P2P copies — without it, the GPU
    // does not insert necessary memory barriers for cross-device access,
    // resulting in segmentation faults. The caller also establishes a
    // GPU-level dependency via cudaEventRecord(cudaStreamPerThread) +
    // cudaStreamWaitEvent(nvlink_stream) to ensure source data is coherent
    // before the memcpy starts; these two mechanisms are complementary.
    cudaMemcpyAttributes attr{};
    attr.srcAccessOrder = cudaMemcpySrcAccessOrderStream;
    size_t attrs_idx = 0;
    // cudaMemcpyBatchAsync in CUDA 12.8 takes non-const size_t* for sizes
    std::vector<size_t> mutable_sizes(sizes);
    size_t fail_idx = count;
#endif

#if CUDART_VERSION >= 13000
    err = cudaMemcpyBatchAsync(const_cast<const void **>(dsts.data()),
                               const_cast<const void **>(srcs.data()),
                               mutable_sizes.data(), static_cast<size_t>(count),
                               &attr, &attrs_idx, 1, stream);
    if (err != cudaSuccess) {
        LOG(ERROR) << "IntraNodeNvlinkTransport: cudaMemcpyBatchAsync "
                   << "failed: " << cudaGetErrorString(err);
        // CUDA >= 13.0 does not return fail_idx; conservatively mark all
        // as FAILED since we cannot determine which copies succeeded.
        for (size_t i = 0; i < count; ++i) {
            if (slices[i]->status == Slice::PENDING) {
                slices[i]->markFailed();
            }
        }
    } else {
        for (size_t i = 0; i < count; ++i) {
            slices[i]->status = Slice::POSTED;
            slices[i]->local.cuda_stream = (void *)stream;
        }
    }
#elif CUDART_VERSION >= 12080
    err = cudaMemcpyBatchAsync(const_cast<void **>(dsts.data()),
                               const_cast<void **>(srcs.data()),
                               mutable_sizes.data(), static_cast<size_t>(count),
                               &attr, &attrs_idx, 1, &fail_idx, stream);
    if (err != cudaSuccess) {
        if (fail_idx < count) {
            LOG(ERROR) << "IntraNodeNvlinkTransport: cudaMemcpyBatchAsync "
                       << "failed at index " << fail_idx
                       << " (src=" << srcs[fail_idx]
                       << ", dst=" << dsts[fail_idx]
                       << ", size=" << sizes[fail_idx]
                       << "): " << cudaGetErrorString(err);
        } else {
            LOG(ERROR) << "IntraNodeNvlinkTransport: cudaMemcpyBatchAsync "
                       << "failed: " << cudaGetErrorString(err);
        }
        // Copies [0, fail_idx) were submitted successfully → POSTED.
        // Copy [fail_idx] failed → FAILED.
        // Copies (fail_idx, count) were never submitted → FAILED.
        for (size_t i = 0; i < fail_idx; ++i) {
            slices[i]->status = Slice::POSTED;
            slices[i]->local.cuda_stream = (void *)stream;
        }
        for (size_t i = fail_idx; i < count; ++i) {
            if (slices[i]->status == Slice::PENDING) {
                slices[i]->markFailed();
            }
        }
    } else {
        for (size_t i = 0; i < count; ++i) {
            slices[i]->status = Slice::POSTED;
            slices[i]->local.cuda_stream = (void *)stream;
        }
    }
#else
    // Fallback for CUDA < 12.8: submit each memcpy individually
    for (size_t i = 0; i < count; ++i) {
        auto single_err = cudaMemcpyAsync(dsts[i], srcs[i], sizes[i],
                                          cudaMemcpyDefault, stream);
        if (single_err != cudaSuccess) {
            LOG(ERROR) << "IntraNodeNvlinkTransport: cudaMemcpyAsync failed at "
                       << "index " << i << ": "
                       << cudaGetErrorString(single_err);
            slices[i]->markFailed();
            continue;
        }
        slices[i]->status = Slice::POSTED;
        slices[i]->local.cuda_stream = (void *)stream;
    }
    return;  // Slice states already set above
#endif
}

static int getNumDevices() {
    static int cached_num_devices = -1;
    if (cached_num_devices == -1) {
        if (!checkCudaErrorReturn(
                cudaGetDeviceCount(&cached_num_devices),
                "IntraNodeNvlinkTransport: cudaGetDeviceCount failed")) {
            return 0;
        }
    }
    return cached_num_devices;
}

static bool enableP2PAccess(int src_device_id, int dst_device_id) {
    int canAccessPeer = 0;
    if (!checkCudaErrorReturn(
            cudaDeviceCanAccessPeer(&canAccessPeer, src_device_id,
                                    dst_device_id),
            "IntraNodeNvlinkTransport: failed to query peer access")) {
        return false;
    }

    if (!canAccessPeer) {
        LOG(ERROR) << "IntraNodeNvlinkTransport: device " << src_device_id
                   << " cannot p2p access device " << dst_device_id;
        return false;
    }

    // enable src->dst p2p access
    if (!checkCudaErrorReturn(
            cudaSetDevice(src_device_id),
            "IntraNodeNvlinkTransport: failed to set device")) {
        return false;
    }
    cudaError_t result = cudaDeviceEnablePeerAccess(dst_device_id, 0);

    if (result != cudaSuccess && result != cudaErrorPeerAccessAlreadyEnabled) {
        LOG(ERROR) << "IntraNodeNvlinkTransport: failed to enable p2p access "
                      "(Error code: "
                   << result << " - " << cudaGetErrorString(result) << ")"
                   << std::endl;

        return false;
    }

    // enable dst->src p2p access
    if (!checkCudaErrorReturn(
            cudaSetDevice(dst_device_id),
            "IntraNodeNvlinkTransport: failed to set device")) {
        return false;
    }
    result = cudaDeviceEnablePeerAccess(src_device_id, 0);

    if (result != cudaSuccess && result != cudaErrorPeerAccessAlreadyEnabled) {
        LOG(ERROR) << "IntraNodeNvlinkTransport: failed to enable p2p access "
                      "(Error code: "
                   << result << " - " << cudaGetErrorString(result) << ")"
                   << std::endl;

        return false;
    }

    return true;
}
IntraNodeNvlinkTransport::IntraNodeNvlinkTransport() {}

// IntraNodeNvlinkTransport::IntraNodeNvlinkTransport() :
// use_fabric_mem_(supportFabricMem()) {}
//     int num_devices = getNumDevices();
//     if (globalConfig().trace) {
//         LOG(INFO) << "IntraNodeNvlinkTransport: use_fabric_mem_:" <<
//         use_fabric_mem_
//                   << ", num_devices: " << num_devices;
//     }

//     for (int src_device_id = 0; src_device_id < num_devices; ++src_device_id)
//     {
//         for (int dst_device_id = src_device_id + 1; dst_device_id <
//         num_devices;
//              ++dst_device_id) {
//             if (enableP2PAccess(src_device_id, dst_device_id)) {
//                 if (globalConfig().trace) {
//                     LOG(INFO)
//                         << "IntraNodeNvlinkTransport: enabled p2p access
//                         between device "
//                         << src_device_id << " and " << dst_device_id;
//                 }
//             } else {
//                 LOG(ERROR) << "IntraNodeNvlinkTransport: failed to enable p2p
//                 access "
//                               "between device "
//                            << src_device_id << " and " << dst_device_id;
//             }
//         }
//     }
// }

IntraNodeNvlinkTransport::~IntraNodeNvlinkTransport() {
    for (auto &entry : remap_entries_) {
        cudaIpcCloseMemHandle(entry.second.shm_addr);
    }
    remap_entries_.clear();
}

int IntraNodeNvlinkTransport::install(
    std::string &local_server_name, std::shared_ptr<TransferMetadata> metadata,
    std::shared_ptr<Topology> topology) {
    metadata_ = metadata;
    local_server_name_ = local_server_name;

    auto desc = std::make_shared<SegmentDesc>();
    if (!desc) return ERR_MEMORY;
    desc->name = local_server_name_;
    desc->protocol = "nvlink_intra";
    metadata_->addLocalSegment(LOCAL_SEGMENT_ID, local_server_name_,
                               std::move(desc));
    return 0;
}

Status IntraNodeNvlinkTransport::submitTransfer(
    BatchID batch_id, const std::vector<TransferRequest> &entries) {
    auto &batch_desc = *((BatchDesc *)(batch_id));
    if (batch_desc.task_list.size() + entries.size() > batch_desc.batch_size) {
        LOG(ERROR) << "IntraNodeNvlinkTransport: Exceed the limitation of "
                      "current batch's "
                      "capacity";
        return Status::InvalidArgument(
            "IntraNodeNvlinkTransport: Exceed the limitation of capacity, "
            "batch id: " +
            std::to_string(batch_id));
    }

    size_t task_id = batch_desc.task_list.size();
    batch_desc.task_list.resize(task_id + entries.size());

    // Get per-device transfer stream for the source buffer's device.
    CudaStreamEntry stream_entry =
        getStreamForRequest(entries.empty() ? nullptr : entries[0].source);
    cudaStream_t stream = stream_entry.stream;
    if (!stream)
        return Status::Context("Failed to create NVLink CUDA stream");
    // Synchronize with caller's GPU work via cudaEventSynchronize (CPU-blocking)
    // to avoid expensive cross-device cudaStreamWaitEvent on non-NVIDIA GPUs.
    cudaEvent_t sync_event = getCallerSyncEvent();
    cudaError_t sync_err =
        cudaEventRecord(sync_event, cudaStreamPerThread);
    if (sync_err != cudaSuccess) {
        LOG(ERROR) << "IntraNodeNvlinkTransport: cudaEventRecord failed: "
                   << cudaGetErrorString(sync_err);
        return Status::Context("cudaEventRecord failed: " +
                               std::string(cudaGetErrorString(sync_err)));
    }
    sync_err = cudaEventSynchronize(sync_event);
    if (sync_err != cudaSuccess) {
        LOG(ERROR) << "IntraNodeNvlinkTransport: cudaEventSynchronize failed: "
                   << cudaGetErrorString(sync_err);
        return Status::Context("cudaEventSynchronize failed: " +
                               std::string(cudaGetErrorString(sync_err)));
    }

    // Phase 1: Prepare slices and collect memcpy parameters
    std::vector<void *> dsts, srcs;
    std::vector<size_t> sizes;
    std::vector<Slice *> slices;

    for (auto &request : entries) {
        TransferTask &task = batch_desc.task_list[task_id];
        ++task_id;
        uint64_t dest_addr = request.target_offset;
        if (request.target_id != LOCAL_SEGMENT_ID) {
            int rc = relocateSharedMemoryAddress(dest_addr, request.length,
                                                 request.target_id);
            if (rc) return Status::Memory("device memory not registered");
        }
        task.total_bytes = request.length;
        Slice *slice = getSliceCache().allocate();
        slice->source_addr = (char *)request.source;
        slice->local.dest_addr = (char *)dest_addr;
        slice->length = request.length;
        slice->opcode = request.opcode;
        slice->task = &task;
        slice->target_id = request.target_id;
        slice->status = Slice::PENDING;
        slice->ts = getCurrentTimeInNano();
        task.slice_list.push_back(slice);
        __sync_fetch_and_add(&task.slice_count, 1);

        void *src = (request.opcode == TransferRequest::READ)
                        ? (void *)slice->local.dest_addr
                        : (void *)slice->source_addr;
        void *dst = (request.opcode == TransferRequest::READ)
                        ? slice->source_addr
                        : (void *)slice->local.dest_addr;
        srcs.push_back(src);
        dsts.push_back(dst);
        sizes.push_back(slice->length);
        slices.push_back(slice);
    }

    // Phase 2: Submit all memcpy operations
    submitBatchMemcpy(slices, srcs, dsts, sizes, stream);

    return Status::OK();
}

Status IntraNodeNvlinkTransport::getTransferStatus(BatchID batch_id,
                                                   size_t task_id,
                                                   TransferStatus &status) {
    auto &batch_desc = *((BatchDesc *)(batch_id));
    const size_t task_count = batch_desc.task_list.size();
    if (task_id >= task_count) {
        return Status::InvalidArgument(
            "IntraNodeNvlinkTransport::getTransportStatus invalid argument, "
            "batch id: " +
            std::to_string(batch_id));
    }
    auto &task = batch_desc.task_list[task_id];
    // Poll POSTED slices for async completion via cudaStreamQuery.
    std::unordered_map<cudaStream_t, cudaError_t> stream_status_cache;
    for (auto *slice : task.slice_list) {
        if (slice && slice->status == Slice::POSTED) {
            cudaStream_t stream = (cudaStream_t)slice->local.cuda_stream;
            auto it = stream_status_cache.find(stream);
            cudaError_t cuda_err;
            if (it == stream_status_cache.end()) {
                cuda_err = cudaStreamQuery(stream);
                stream_status_cache[stream] = cuda_err;
            } else {
                cuda_err = it->second;
            }
            if (cuda_err == cudaSuccess) {
                slice->markSuccess();
            } else if (cuda_err != cudaErrorNotReady) {
                slice->markFailed();
            }
        }
    }
    status.transferred_bytes = task.transferred_bytes;
    uint64_t success_slice_count = task.success_slice_count;
    uint64_t failed_slice_count = task.failed_slice_count;
    if (success_slice_count + failed_slice_count == task.slice_count) {
        if (failed_slice_count) {
            status.s = TransferStatusEnum::FAILED;
        } else {
            status.s = TransferStatusEnum::COMPLETED;
        }
        task.is_finished = true;
    } else {
        status.s = TransferStatusEnum::WAITING;
    }
    return Status::OK();
}

Status IntraNodeNvlinkTransport::submitTransferTask(
    const std::vector<TransferTask *> &task_list) {
    // Get per-device transfer stream. See submitTransfer() for rationale.
    CudaStreamEntry stream_entry = getStreamForRequest(
        task_list.empty() ? nullptr : task_list[0]->request->source);
    cudaStream_t stream = stream_entry.stream;
    if (!stream)
        return Status::Context("Failed to create NVLink CUDA stream");
    cudaEvent_t sync_event = getCallerSyncEvent();
    cudaError_t sync_err =
        cudaEventRecord(sync_event, cudaStreamPerThread);
    if (sync_err != cudaSuccess) {
        LOG(ERROR) << "IntraNodeNvlinkTransport: cudaEventRecord failed: "
                   << cudaGetErrorString(sync_err);
        return Status::Context("cudaEventRecord failed: " +
                               std::string(cudaGetErrorString(sync_err)));
    }
    sync_err = cudaEventSynchronize(sync_event);
    if (sync_err != cudaSuccess) {
        LOG(ERROR) << "IntraNodeNvlinkTransport: cudaEventSynchronize failed: "
                   << cudaGetErrorString(sync_err);
        return Status::Context("cudaEventSynchronize failed: " +
                               std::string(cudaGetErrorString(sync_err)));
    }

    // Phase 1: Prepare slices and collect memcpy parameters
    std::vector<void *> dsts, srcs;
    std::vector<size_t> sizes;
    std::vector<Slice *> slices;

    for (size_t index = 0; index < task_list.size(); ++index) {
        assert(task_list[index]);
        auto &task = *task_list[index];
        assert(task.request);
        auto &request = *task.request;
        uint64_t dest_addr = request.target_offset;
        if (request.target_id != LOCAL_SEGMENT_ID) {
            int rc = relocateSharedMemoryAddress(dest_addr, request.length,
                                                 request.target_id);
            if (rc) return Status::Memory("device memory not registered");
        }
        task.total_bytes = request.length;
        Slice *slice = getSliceCache().allocate();
        slice->source_addr = (char *)request.source;
        slice->local.dest_addr = (char *)dest_addr;
        slice->length = request.length;
        slice->opcode = request.opcode;
        slice->task = &task;
        slice->target_id = request.target_id;
        slice->status = Slice::PENDING;
        slice->ts = getCurrentTimeInNano();
        task.slice_list.push_back(slice);
        __sync_fetch_and_add(&task.slice_count, 1);

        void *src = (request.opcode == TransferRequest::READ)
                        ? (void *)slice->local.dest_addr
                        : (void *)slice->source_addr;
        void *dst = (request.opcode == TransferRequest::READ)
                        ? slice->source_addr
                        : (void *)slice->local.dest_addr;
        srcs.push_back(src);
        dsts.push_back(dst);
        sizes.push_back(slice->length);
        slices.push_back(slice);
    }

    // Phase 2: Submit all memcpy operations
    submitBatchMemcpy(slices, srcs, dsts, sizes, stream);

    return Status::OK();
}

int IntraNodeNvlinkTransport::registerLocalMemory(void *addr, size_t length,
                                                  const std::string &location,
                                                  bool remote_accessible,
                                                  bool update_metadata) {
    std::lock_guard<std::mutex> lock(register_mutex_);
    if (globalConfig().trace) {
        LOG(INFO) << "register memory: addr " << addr << ", length " << length;
    }
    cudaPointerAttributes attr;
    cudaError_t err = cudaPointerGetAttributes(&attr, addr);
    if (err != cudaSuccess) {
        LOG(ERROR)
            << "IntraNodeNvlinkTransport: cudaPointerGetAttributes failed";
        return -1;
    }

    if (attr.type != cudaMemoryTypeDevice) {
        LOG(ERROR) << "Unsupported memory type, " << addr << " " << attr.type;
        return -1;
    }

    // Resolve the true cudaMalloc base address. Framework caching allocators
    // (PyTorch, etc.) sub-allocate tensors within larger cudaMalloc segments.
    // cudaIpcGetMemHandle always returns a handle for the entire segment, so
    // we must register at segment granularity for correct IPC relocation.
    CUdeviceptr base_ptr = 0;
    size_t alloc_size = 0;
    CUresult cu_err =
        cuMemGetAddressRange(&base_ptr, &alloc_size, (CUdeviceptr)addr);
    if (cu_err != CUDA_SUCCESS) {
        LOG(ERROR) << "IntraNodeNvlinkTransport: cuMemGetAddressRange failed "
                   << "for addr " << addr << " (error " << cu_err << ")";
        return -1;
    }

    // Skip if this cudaMalloc block is already registered
    if (registered_base_addrs_.count((uint64_t)base_ptr)) {
        return 0;
    }

    cudaIpcMemHandle_t handle;
    err = cudaIpcGetMemHandle(&handle, (void *)base_ptr);
    if (err != cudaSuccess) {
        LOG(ERROR) << "IntraNodeNvlinkTransport: cudaIpcGetMemHandle failed";
        return -1;
    }

    (void)remote_accessible;
    BufferDesc desc;
    desc.addr = (uint64_t)base_ptr;
    desc.length = alloc_size;
    desc.name = location;
    desc.shm_name = serializeBinaryData(&handle, sizeof(cudaIpcMemHandle_t));
    int rc = metadata_->addLocalMemoryBuffer(desc, true);
    if (rc == 0) {
        registered_base_addrs_.insert((uint64_t)base_ptr);
    }
    return rc;
}

int IntraNodeNvlinkTransport::unregisterLocalMemory(void *addr,
                                                    bool update_metadata) {
    CUdeviceptr base_ptr = 0;
    size_t alloc_size = 0;
    CUresult cu_err =
        cuMemGetAddressRange(&base_ptr, &alloc_size, (CUdeviceptr)addr);

    void *key_ptr = addr;
    if (cu_err == CUDA_SUCCESS) {
        key_ptr = (void *)base_ptr;
    } else {
        LOG(WARNING)
            << "IntraNodeNvlinkTransport: cuMemGetAddressRange failed for "
            << "addr " << addr << " during unregister (error " << cu_err
            << "). Memory may already be freed, using provided address.";
    }

    {
        std::lock_guard<std::mutex> lock(register_mutex_);
        registered_base_addrs_.erase((uint64_t)key_ptr);
    }
    return metadata_->removeLocalMemoryBuffer(key_ptr, update_metadata);
}

int IntraNodeNvlinkTransport::relocateSharedMemoryAddress(uint64_t &dest_addr,
                                                          uint64_t length,
                                                          uint64_t target_id) {
    auto desc = metadata_->getSegmentDescByID(target_id);
    int index = 0;
    for (auto &entry : desc->buffers) {
        if (!entry.shm_name.empty() && entry.addr <= dest_addr &&
            dest_addr + length <= entry.addr + entry.length) {
            remap_lock_.lockShared();
            if (remap_entries_.count(std::make_pair(target_id, entry.addr))) {
                auto shm_addr =
                    remap_entries_[std::make_pair(target_id, entry.addr)]
                        .shm_addr;
                remap_lock_.unlockShared();
                dest_addr = dest_addr - entry.addr + ((uint64_t)shm_addr);
                return 0;
            }
            remap_lock_.unlockShared();
            RWSpinlock::WriteGuard lock_guard(remap_lock_);
            if (!remap_entries_.count(std::make_pair(target_id, entry.addr))) {
                std::vector<unsigned char> output_buffer;
                deserializeBinaryData(entry.shm_name, output_buffer);
                if (output_buffer.size() == sizeof(cudaIpcMemHandle_t)) {
                    cudaIpcMemHandle_t handle;
                    memcpy(&handle, output_buffer.data(), sizeof(handle));
                    void *shm_addr = nullptr;
                    cudaError_t err = cudaIpcOpenMemHandle(
                        &shm_addr, handle, cudaIpcMemLazyEnablePeerAccess);
                    if (err != cudaSuccess) {
                        LOG(ERROR) << "IntraNodeNvlinkTransport: "
                                      "cudaIpcOpenMemHandle failed: "
                                   << cudaGetErrorString(err);
                        return -1;
                    }
                    OpenedShmEntry shm_entry;
                    shm_entry.shm_addr = shm_addr;
                    shm_entry.length = entry.length;
                    remap_entries_[std::make_pair(target_id, entry.addr)] =
                        shm_entry;
                } else {
                    LOG(ERROR) << "Mismatched NVLink data transfer method";
                    return -1;
                }
            }
            auto shm_addr =
                remap_entries_[std::make_pair(target_id, entry.addr)].shm_addr;
            dest_addr = dest_addr - entry.addr + ((uint64_t)shm_addr);
            return 0;
        }
        index++;
    }
    LOG(ERROR) << "Requested address " << (void *)dest_addr << " to "
               << (void *)(dest_addr + length) << " not found!";
    return ERR_INVALID_ARGUMENT;
}

int IntraNodeNvlinkTransport::registerLocalMemoryBatch(
    const std::vector<Transport::BufferEntry> &buffer_list,
    const std::string &location) {
    for (auto &buffer : buffer_list)
        registerLocalMemory(buffer.addr, buffer.length, location, true, false);
    return metadata_->updateLocalSegmentDesc();
}

int IntraNodeNvlinkTransport::unregisterLocalMemoryBatch(
    const std::vector<void *> &addr_list) {
    for (auto &addr : addr_list) unregisterLocalMemory(addr, false);
    return metadata_->updateLocalSegmentDesc();
}

void *IntraNodeNvlinkTransport::allocatePinnedLocalMemory(size_t size) {
    void *ptr = nullptr;
    cudaError_t res = cudaMalloc(&ptr, size);
    if (res == cudaSuccess) {
        LOG(INFO) << "IntraNodeNvlinkTransport: Falling back to cudaMalloc for "
                  << size << " bytes (memory will NOT be exportable)";
        return ptr;
    } else {
        LOG(ERROR)
            << "IntraNodeNvlinkTransport: cudaMalloc failed during fallback: "
            << cudaGetErrorString(res);
        return nullptr;
    }
}

void IntraNodeNvlinkTransport::freePinnedLocalMemory(void *ptr) {
    cudaFree(ptr);
    return;
}

}  // namespace mooncake
