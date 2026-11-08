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
#include <unistd.h>

#include <algorithm>
#include <cassert>
#include <cctype>
#include <cerrno>
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
        cudaError_t err =
            cudaStreamCreateWithFlags(&entry.stream, cudaStreamNonBlocking);
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
        const char *visible = getenv("CUDA_VISIBLE_DEVICES");
        LOG(INFO) << "IntraNodeNvlinkTransport: NVLink CUDA stream created on "
                  << "device " << device_id << " [physical: " << pci
                  << "] CUDA_VISIBLE_DEVICES="
                  << (visible ? visible : "(not set)") << " pid=" << getpid();
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

/// Per-device event pool (thread-local). Caches one event per device to
/// avoid repeated create/destroy on device switches, and ensures proper
/// cleanup when the thread exits.
class PerDeviceEventPool {
   public:
    cudaEvent_t getOrCreate(int device_id) {
        auto it = pool_.find(device_id);
        if (it != pool_.end()) return it->second;
        int saved_device = 0;
        cudaGetDevice(&saved_device);
        if (cudaSetDevice(device_id) != cudaSuccess) {
            LOG(ERROR) << "IntraNodeNvlinkTransport: cudaSetDevice("
                       << device_id << ") failed when creating event";
            return nullptr;
        }
        cudaEvent_t event = nullptr;
        cudaError_t err =
            cudaEventCreateWithFlags(&event, cudaEventDisableTiming);
        if (err != cudaSuccess)
            LOG(FATAL) << "Failed to create NVLink sync event on device "
                       << device_id << ": " << cudaGetErrorString(err);
        cudaSetDevice(saved_device);
        pool_[device_id] = event;
        return event;
    }
    ~PerDeviceEventPool() {
        int saved_device = 0;
        cudaGetDevice(&saved_device);
        for (auto &kv : pool_) {
            cudaSetDevice(kv.first);
            if (kv.second) cudaEventDestroy(kv.second);
        }
        cudaSetDevice(saved_device);
    }

   private:
    std::unordered_map<int, cudaEvent_t> pool_;
};

static thread_local PerDeviceEventPool tl_device_event_pool;

static cudaEvent_t getCallerSyncEvent() {
    int current_device = 0;
    cudaGetDevice(&current_device);
    return tl_device_event_pool.getOrCreate(current_device);
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

// MC_NVLINK_VMM=0 turns off sharing of virtual-memory (cuMemCreate) buffers.
static bool vmmEnabled() {
    static const bool enabled = [] {
        const char *value = getenv("MC_NVLINK_VMM");
        if (!value) return true;
        std::string v(value);
        std::transform(v.begin(), v.end(), v.begin(), ::tolower);
        return !(v == "0" || v == "false" || v == "off" || v == "no");
    }();
    return enabled;
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
    // The fd server thread calls back into this object; stop it first.
    vmm_fd_server_.stop();
    for (auto &entry : remap_entries_) {
        releaseOpenedEntry(entry.second);
    }
    remap_entries_.clear();
#ifdef USE_CUDA
    for (auto &kv : vmm_exports_) {
        for (uint64_t handle : kv.second.handles) {
            cuMemRelease((CUmemGenericAllocationHandle)handle);
        }
    }
#endif
    vmm_exports_.clear();
    vmm_ids_by_base_.clear();
}

void IntraNodeNvlinkTransport::releaseOpenedEntry(OpenedShmEntry &entry) {
    if (!entry.vmm) {
        cudaIpcCloseMemHandle(entry.shm_addr);
        return;
    }
#ifdef USE_CUDA
    CUdeviceptr va = (CUdeviceptr)entry.shm_addr;
    for (const auto &chunk : entry.vmm_chunks) {
        cuMemUnmap(va + chunk.offset, chunk.size);
    }
    cuMemAddressFree(va, entry.length);
    for (uint64_t handle : entry.vmm_handles) {
        cuMemRelease((CUmemGenericAllocationHandle)handle);
    }
#endif
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
    if (!stream) return Status::Context("Failed to create NVLink CUDA stream");
    // Synchronize with caller's GPU work via cudaEventSynchronize
    // (CPU-blocking) to avoid expensive cross-device cudaStreamWaitEvent on
    // non-NVIDIA GPUs.
    cudaEvent_t sync_event = getCallerSyncEvent();
    cudaError_t sync_err = cudaEventRecord(sync_event, cudaStreamPerThread);
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
    uint64_t success_slice_count =
        __atomic_load_n(&task.success_slice_count, __ATOMIC_ACQUIRE);
    uint64_t failed_slice_count =
        __atomic_load_n(&task.failed_slice_count, __ATOMIC_ACQUIRE);
    // Completion counters publish the preceding byte updates.
    status.transferred_bytes =
        __atomic_load_n(&task.transferred_bytes, __ATOMIC_RELAXED);
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
    if (!stream) return Status::Context("Failed to create NVLink CUDA stream");
    cudaEvent_t sync_event = getCallerSyncEvent();
    cudaError_t sync_err = cudaEventRecord(sync_event, cudaStreamPerThread);
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

#ifdef USE_CUDA
    // Virtual-memory (cuMemCreate) allocations have no cudaIpc handle. This
    // includes PyTorch with PYTORCH_CUDA_ALLOC_CONF=expandable_segments:True.
    // cuMemRetainAllocationHandle only succeeds for them.
    CUmemGenericAllocationHandle probe_handle;
    if (cuMemRetainAllocationHandle(&probe_handle, addr) == CUDA_SUCCESS) {
        cuMemRelease(probe_handle);
        if (vmmEnabled()) {
            return registerVmmMemory(addr, length, attr.device, location);
        }
        LOG(ERROR) << "IntraNodeNvlinkTransport: buffer " << addr
                   << " is a virtual-memory (cuMemCreate) allocation, and "
                      "MC_NVLINK_VMM=0 turns off sharing such buffers. Unset "
                      "MC_NVLINK_VMM, or register memory allocated with "
                      "cudaMalloc/cuMemAlloc.";
        return -1;
    }
#endif

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
        LOG(ERROR) << "IntraNodeNvlinkTransport: cudaIpcGetMemHandle failed: "
                   << cudaGetErrorString(err);
        // Virtual-memory allocations (cuMemCreate) have no legacy IPC handle.
        // cuMemRetainAllocationHandle only succeeds for those, so use it to
        // tell the user why registration failed.
        CUmemGenericAllocationHandle vmm_handle;
        if (cuMemRetainAllocationHandle(&vmm_handle, (void *)base_ptr) ==
            CUDA_SUCCESS) {
            cuMemRelease(vmm_handle);
            LOG(ERROR)
                << "IntraNodeNvlinkTransport: buffer " << addr
                << " is a virtual-memory (cuMemCreate) allocation, which has "
                   "no cudaIpc handle. This is the case for PyTorch with "
                   "PYTORCH_CUDA_ALLOC_CONF=expandable_segments:True. Register "
                   "a buffer allocated with cudaMalloc/cuMemAlloc instead, or "
                   "disable expandable_segments for it.";
        }
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

// Registers [addr, addr + length) of VMM memory. For VMM memory,
// cuMemGetAddressRange returns the extent of the one physical allocation
// mapped at an address, so walk the range chunk by chunk. The published
// buffer covers the chunks completely; importers map them the same way.
int IntraNodeNvlinkTransport::registerVmmMemory(void *addr, size_t length,
                                                int device,
                                                const std::string &location) {
#ifdef USE_CUDA
    const uint64_t end = (uint64_t)addr + length;
    VmmExport vmm_export;
    vmm_export.base = 0;
    vmm_export.device = device;
    auto release_handles = [](VmmExport &e) {
        for (uint64_t handle : e.handles) {
            cuMemRelease((CUmemGenericAllocationHandle)handle);
        }
        e.handles.clear();
    };

    uint64_t cursor = (uint64_t)addr;
    while (cursor < end) {
        CUdeviceptr base = 0;
        size_t size = 0;
        CUresult res = cuMemGetAddressRange(&base, &size, (CUdeviceptr)cursor);
        if (res != CUDA_SUCCESS) {
            LOG(ERROR) << "IntraNodeNvlinkTransport: VMM buffer " << addr
                       << " (length " << length << ") is not fully mapped: "
                       << "nothing is mapped at " << (void *)cursor
                       << " (error " << res
                       << "). Register only memory that "
                          "is mapped and stays mapped.";
            release_handles(vmm_export);
            return -1;
        }
        CUmemGenericAllocationHandle handle;
        res = cuMemRetainAllocationHandle(&handle, (void *)base);
        if (res != CUDA_SUCCESS) {
            LOG(ERROR) << "IntraNodeNvlinkTransport: buffer " << addr
                       << " mixes virtual-memory and other allocations at "
                       << (void *)base;
            release_handles(vmm_export);
            return -1;
        }
        vmm_export.handles.push_back((uint64_t)handle);
        CUmemAllocationProp prop = {};
        res = cuMemGetAllocationPropertiesFromHandle(&prop, handle);
        if (res != CUDA_SUCCESS) {
            LOG(ERROR) << "IntraNodeNvlinkTransport: "
                          "cuMemGetAllocationPropertiesFromHandle failed for "
                       << (void *)base << " (error " << res << ")";
            release_handles(vmm_export);
            return -1;
        }
        if (!(prop.requestedHandleTypes &
              CU_MEM_HANDLE_TYPE_POSIX_FILE_DESCRIPTOR)) {
            if (prop.requestedHandleTypes & CU_MEM_HANDLE_TYPE_FABRIC) {
                // PyTorch picks FABRIC when the device has fabric access
                // (e.g. IMEX is configured).
                LOG(ERROR) << "IntraNodeNvlinkTransport: virtual-memory buffer "
                           << addr
                           << " only has a FABRIC handle; nvlink_intra shares "
                              "virtual memory through POSIX file descriptors. "
                              "For PyTorch, add "
                              "expandable_segments_handle_type:posix_fd to "
                              "PYTORCH_CUDA_ALLOC_CONF.";
            } else {
                LOG(ERROR) << "IntraNodeNvlinkTransport: virtual-memory buffer "
                           << addr
                           << " cannot be shared: it was created without "
                              "CU_MEM_HANDLE_TYPE_POSIX_FILE_DESCRIPTOR in "
                              "requestedHandleTypes. For PyTorch with "
                              "expandable_segments:True, set "
                              "TORCH_CUDA_EXPANDABLE_SEGMENTS_IPC=1 before "
                              "CUDA is initialized, or disable "
                              "expandable_segments.";
            }
            release_handles(vmm_export);
            return -1;
        }
        if (vmm_export.chunks.empty()) vmm_export.base = base;
        vmm_export.chunks.push_back({base - vmm_export.base, size});
        cursor = base + size;
    }
    vmm_export.length = cursor - vmm_export.base;

    std::lock_guard<std::mutex> lock(vmm_mutex_);
    auto existing = vmm_ids_by_base_.find(vmm_export.base);
    if (existing != vmm_ids_by_base_.end()) {
        auto &old = vmm_exports_[existing->second];
        if (old.length >= vmm_export.length) {
            release_handles(vmm_export);
            return 0;
        }
        // The new range extends a registered one with the same first chunk:
        // replace it, since buffers are keyed by their base address.
        release_handles(old);
        vmm_exports_.erase(existing->second);
        vmm_ids_by_base_.erase(existing);
        metadata_->removeLocalMemoryBuffer((void *)vmm_export.base, false);
    }

    if (!vmm_fd_server_.running()) {
        const char *dir = getenv("MC_NVLINK_FD_DIR");
        int rc = vmm_fd_server_.start(
            dir ? dir : "", [this](uint64_t buffer_id, std::vector<int> &fds) {
                return exportVmmFds(buffer_id, fds);
            });
        if (rc != 0) {
            release_handles(vmm_export);
            return -1;
        }
    }

    intranode_vmm::VmmBufferInfo info;
    info.buffer_id = next_vmm_buffer_id_++;
    info.endpoint = vmm_fd_server_.endpoint();
    info.chunks = vmm_export.chunks;
    std::vector<unsigned char> payload =
        intranode_vmm::encodeVmmBufferInfo(info);
    if (payload.empty()) {
        LOG(ERROR) << "IntraNodeNvlinkTransport: cannot encode VMM buffer "
                   << addr;
        release_handles(vmm_export);
        return -1;
    }

    BufferDesc desc;
    desc.addr = vmm_export.base;
    desc.length = vmm_export.length;
    desc.name = location;
    desc.shm_name = serializeBinaryData(payload.data(), payload.size());
    const uint64_t base = vmm_export.base;
    const size_t num_chunks = vmm_export.chunks.size();
    vmm_exports_[info.buffer_id] = std::move(vmm_export);
    vmm_ids_by_base_[base] = info.buffer_id;
    int rc = metadata_->addLocalMemoryBuffer(desc, true);
    if (rc != 0) {
        release_handles(vmm_exports_[info.buffer_id]);
        vmm_exports_.erase(info.buffer_id);
        vmm_ids_by_base_.erase(base);
        return rc;
    }
    if (globalConfig().trace) {
        LOG(INFO) << "IntraNodeNvlinkTransport: registered VMM buffer "
                  << (void *)base << " length " << desc.length << " in "
                  << num_chunks << " chunk(s)";
    }
    return 0;
#else
    (void)addr;
    (void)length;
    (void)device;
    (void)location;
    LOG(ERROR) << "IntraNodeNvlinkTransport: virtual-memory buffers need a "
                  "CUDA build";
    return -1;
#endif
}

bool IntraNodeNvlinkTransport::unregisterVmmMemory(uint64_t base,
                                                   bool update_metadata,
                                                   int &rc) {
    {
        std::lock_guard<std::mutex> lock(vmm_mutex_);
        auto it = vmm_ids_by_base_.find(base);
        if (it == vmm_ids_by_base_.end()) return false;
#ifdef USE_CUDA
        for (uint64_t handle : vmm_exports_[it->second].handles) {
            cuMemRelease((CUmemGenericAllocationHandle)handle);
        }
#endif
        vmm_exports_.erase(it->second);
        vmm_ids_by_base_.erase(it);
    }
    rc = metadata_->removeLocalMemoryBuffer((void *)base, update_metadata);
    return true;
}

// Called on the fd server thread. Exports a new fd per chunk on each request
// so that registered buffers do not hold file descriptors.
int IntraNodeNvlinkTransport::exportVmmFds(uint64_t buffer_id,
                                           std::vector<int> &fds) {
#ifdef USE_CUDA
    std::lock_guard<std::mutex> lock(vmm_mutex_);
    auto it = vmm_exports_.find(buffer_id);
    if (it == vmm_exports_.end()) return -ENOENT;
    cudaSetDevice(it->second.device);
    for (uint64_t handle : it->second.handles) {
        int fd = -1;
        CUresult res = cuMemExportToShareableHandle(
            &fd, (CUmemGenericAllocationHandle)handle,
            CU_MEM_HANDLE_TYPE_POSIX_FILE_DESCRIPTOR, 0);
        if (res != CUDA_SUCCESS) {
            LOG(ERROR) << "IntraNodeNvlinkTransport: "
                          "cuMemExportToShareableHandle failed for VMM buffer "
                       << buffer_id << " (error " << res << ")";
            for (int opened : fds) close(opened);
            fds.clear();
            return -EIO;
        }
        fds.push_back(fd);
    }
    return 0;
#else
    (void)buffer_id;
    (void)fds;
    return -ENOTSUP;
#endif
}

// Maps a peer's VMM buffer into this process: fetch one fd per chunk,
// import them, map them back to back into a new reservation and grant
// access to every visible device.
int IntraNodeNvlinkTransport::openVmmBuffer(
    const intranode_vmm::VmmBufferInfo &info, uint64_t length,
    OpenedShmEntry &shm_entry) {
#ifdef USE_CUDA
    const auto &last = info.chunks.back();
    if (last.offset + last.size != length) {
        LOG(ERROR) << "IntraNodeNvlinkTransport: VMM buffer " << info.buffer_id
                   << " chunks do not match its length " << length;
        return -1;
    }
    std::vector<int> fds;
    if (intranode_vmm::fetchVmmFds(info.endpoint, info.buffer_id,
                                   info.chunks.size(), fds) != 0) {
        return -1;
    }
    std::vector<uint64_t> handles;
    CUresult res = CUDA_SUCCESS;
    for (int &fd : fds) {
        if (res == CUDA_SUCCESS) {
            CUmemGenericAllocationHandle handle;
            res = cuMemImportFromShareableHandle(
                &handle, (void *)(uintptr_t)fd,
                CU_MEM_HANDLE_TYPE_POSIX_FILE_DESCRIPTOR);
            if (res == CUDA_SUCCESS) handles.push_back((uint64_t)handle);
        }
        close(fd);
    }
    OpenedShmEntry opened;
    opened.shm_addr = nullptr;
    opened.length = length;
    opened.vmm = true;
    opened.vmm_handles = std::move(handles);
    if (res != CUDA_SUCCESS) {
        LOG(ERROR) << "IntraNodeNvlinkTransport: "
                      "cuMemImportFromShareableHandle failed (error "
                   << res << ")";
        for (uint64_t handle : opened.vmm_handles) {
            cuMemRelease((CUmemGenericAllocationHandle)handle);
        }
        return -1;
    }

    CUdeviceptr va = 0;
    res = cuMemAddressReserve(&va, length, 0, 0, 0);
    if (res != CUDA_SUCCESS) {
        LOG(ERROR) << "IntraNodeNvlinkTransport: cuMemAddressReserve of "
                   << length << " bytes failed (error " << res << ")";
        for (uint64_t handle : opened.vmm_handles) {
            cuMemRelease((CUmemGenericAllocationHandle)handle);
        }
        return -1;
    }
    opened.shm_addr = (void *)va;
    for (size_t i = 0; i < info.chunks.size(); ++i) {
        const auto &chunk = info.chunks[i];
        res = cuMemMap(va + chunk.offset, chunk.size, 0,
                       (CUmemGenericAllocationHandle)opened.vmm_handles[i], 0);
        if (res != CUDA_SUCCESS) {
            LOG(ERROR) << "IntraNodeNvlinkTransport: cuMemMap of VMM chunk "
                       << i << " failed (error " << res << ")";
            break;
        }
        opened.vmm_chunks.push_back(chunk);
    }
    if (res == CUDA_SUCCESS) {
        int device_count = 0;
        cudaGetDeviceCount(&device_count);
        std::vector<CUmemAccessDesc> access(device_count);
        for (int i = 0; i < device_count; ++i) {
            access[i].location.type = CU_MEM_LOCATION_TYPE_DEVICE;
            access[i].location.id = i;
            access[i].flags = CU_MEM_ACCESS_FLAGS_PROT_READWRITE;
        }
        res = cuMemSetAccess(va, length, access.data(), device_count);
        if (res != CUDA_SUCCESS) {
            // Some visible device may lack peer access to the owner; the
            // copies only need the current one.
            int current = 0;
            cudaGetDevice(&current);
            CUmemAccessDesc one = {};
            one.location.type = CU_MEM_LOCATION_TYPE_DEVICE;
            one.location.id = current;
            one.flags = CU_MEM_ACCESS_FLAGS_PROT_READWRITE;
            res = cuMemSetAccess(va, length, &one, 1);
            if (res != CUDA_SUCCESS) {
                LOG(ERROR) << "IntraNodeNvlinkTransport: cuMemSetAccess failed "
                              "(error "
                           << res << ")";
            }
        }
    }
    if (res != CUDA_SUCCESS) {
        releaseOpenedEntry(opened);
        return -1;
    }
    shm_entry = std::move(opened);
    return 0;
#else
    (void)info;
    (void)length;
    (void)shm_entry;
    LOG(ERROR) << "IntraNodeNvlinkTransport: virtual-memory buffers need a "
                  "CUDA build";
    return -1;
#endif
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

    int vmm_rc = 0;
    if (unregisterVmmMemory((uint64_t)key_ptr, update_metadata, vmm_rc)) {
        return vmm_rc;
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
                        // cudaErrorDeviceUninitialized ("invalid device
                        // context") is what CUDA returns when the handle
                        // looks like it was exported by this very process:
                        // the exporter's PID equals ours. Peers in separate
                        // PID namespaces (e.g. two containers) can get the
                        // same PID.
                        if (err == cudaErrorDeviceUninitialized) {
                            LOG(ERROR)
                                << "IntraNodeNvlinkTransport: the peer segment "
                                << target_id
                                << " may run in a different PID namespace "
                                   "with the same PID as this process (CUDA "
                                   "IPC handles carry the exporter's PID). "
                                   "Run the peers in a shared PID namespace, "
                                   "e.g. docker run --pid=host, or hostPID / "
                                   "shareProcessNamespace in Kubernetes.";
                        }
                        return -1;
                    }
                    OpenedShmEntry shm_entry;
                    shm_entry.shm_addr = shm_addr;
                    shm_entry.length = entry.length;
                    remap_entries_[std::make_pair(target_id, entry.addr)] =
                        shm_entry;
                } else {
                    intranode_vmm::VmmBufferInfo info;
                    if (!intranode_vmm::decodeVmmBufferInfo(output_buffer,
                                                            info)) {
                        LOG(ERROR) << "Mismatched NVLink data transfer method";
                        return -1;
                    }
                    OpenedShmEntry shm_entry;
                    if (openVmmBuffer(info, entry.length, shm_entry) != 0) {
                        return -1;
                    }
                    remap_entries_[std::make_pair(target_id, entry.addr)] =
                        std::move(shm_entry);
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
    for (auto &buffer : buffer_list) {
        int ret = registerLocalMemory(buffer.addr, buffer.length, location,
                                      true, false);
        if (ret) return ret;
    }
    return metadata_->updateLocalSegmentDesc();
}

int IntraNodeNvlinkTransport::unregisterLocalMemoryBatch(
    const std::vector<void *> &addr_list) {
    int first_error = 0;
    for (auto &addr : addr_list) {
        int ret = unregisterLocalMemory(addr, false);
        if (ret && !first_error) first_error = ret;
    }
    int metadata_ret = metadata_->updateLocalSegmentDesc();
    return first_error ? first_error : metadata_ret;
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
