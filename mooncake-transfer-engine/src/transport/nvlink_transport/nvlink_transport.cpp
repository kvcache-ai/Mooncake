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

#include "transport/nvlink_transport/nvlink_transport.h"

#include "cuda_alike.h"
#include <glog/logging.h>

#include <cassert>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <memory>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include <unistd.h>

#include "common.h"
#include "transfer_metadata.h"

namespace mooncake {

namespace {

/// Per-device CUDA stream pool (thread-local).
/// Each thread maintains a map of device_id → stream.
/// The source buffer's device is queried via cudaPointerGetAttributes,
/// and the stream for THAT device is used for DMA copy submission.
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
            LOG(ERROR) << "NvlinkTransport: cudaSetDevice(" << device_id
                       << ") failed when creating stream";
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
        const char* visible = getenv("CUDA_VISIBLE_DEVICES");
        LOG(INFO) << "NvlinkTransport: NVLink CUDA stream created on device "
                  << device_id << " [physical: " << pci
                  << "] CUDA_VISIBLE_DEVICES="
                  << (visible ? visible : "(not set)") << " pid=" << getpid();
        pool_[device_id] = entry;
        return entry;
    }
    ~PerDeviceStreamPool() {
        int saved_device = 0;
        cudaGetDevice(&saved_device);
        for (auto& kv : pool_) {
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
            LOG(ERROR) << "NvlinkTransport: cudaSetDevice(" << device_id
                       << ") failed when creating event";
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
        for (auto& kv : pool_) {
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

static int getDeviceForPointer(const void* ptr) {
    cudaPointerAttributes attr;
    if (cudaPointerGetAttributes(&attr, ptr) != cudaSuccess) {
        cudaGetLastError();
        return -1;
    }
    return (attr.type == cudaMemoryTypeDevice) ? attr.device : -1;
}

static CudaStreamEntry getStreamForRequest(const void* source) {
    int device_id = getDeviceForPointer(source);
    if (device_id < 0) {
        cudaGetDevice(&device_id);
        if (device_id < 0) device_id = 0;
    }
    return tl_device_stream_pool.getOrCreate(device_id);
}

}  // anonymous namespace

using Slice = Transport::Slice;

void NvlinkTransport::markSubmissionFailed(TransferTask& task) {
    // Both NVLink submission paths call this only before allocating a real
    // Slice. Use the existing slice counters as a terminal failure sentinel so
    // the task remains queryable and freeable without changing the shared
    // TransferTask state model.
    uint64_t expected_slice_count = 0;
    if (!__atomic_compare_exchange_n(&task.slice_count, &expected_slice_count,
                                     1, false, __ATOMIC_ACQ_REL,
                                     __ATOMIC_ACQUIRE)) {
        return;
    }
    __atomic_store_n(&task.failed_slice_count, 1, __ATOMIC_RELEASE);
    __atomic_store_n(&task.is_finished, true, __ATOMIC_RELEASE);
    if (task.batch_id != 0) {
        auto& batch_desc = toBatchDesc(task.batch_id);
        batch_desc.has_failure.store(true, std::memory_order_release);
    }
}

/// Submit batched memcpy operations using cudaMemcpyBatchAsync when available
/// (CUDA 12.8+), falling back to per-slice cudaMemcpyAsync otherwise.
/// Uses cudaMemcpySrcAccessOrderStream to ensure source data visibility for
/// P2P copies. This attribute is REQUIRED — without it, the GPU does not
/// insert the necessary memory barriers for P2P access, causing segfaults.
/// The caller must also establish GPU-level stream synchronization
/// (via cudaEventRecord + cudaStreamWaitEvent) before calling this function.
/// Individual slice errors are tracked so that slices whose memcpy failed
/// are marked as FAILED while successfully submitted ones are POSTED.
static void submitBatchMemcpy(const std::vector<Slice*>& slices,
                              const std::vector<void*>& srcs,
                              const std::vector<void*>& dsts,
                              const std::vector<size_t>& sizes,
                              cudaStream_t stream) {
    if (slices.empty()) return;

    const size_t count = slices.size();
    cudaError_t err = cudaSuccess;

    // Log the active memcpy path once per process lifetime
    static const bool logged_once = [] {
#if CUDART_VERSION >= 13000
        LOG(INFO) << "NvlinkTransport: using cudaMemcpyBatchAsync "
                  << "(CUDA >= 13.0 path)";
#elif CUDART_VERSION >= 12080
        LOG(INFO) << "NvlinkTransport: using cudaMemcpyBatchAsync "
                  << "(CUDA >= 12.8 path)";
#else
        LOG(INFO) << "NvlinkTransport: using per-slice cudaMemcpyAsync "
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
    err = cudaMemcpyBatchAsync(const_cast<const void**>(dsts.data()),
                               const_cast<const void**>(srcs.data()),
                               mutable_sizes.data(), static_cast<size_t>(count),
                               &attr, &attrs_idx, 1, stream);
    if (err != cudaSuccess) {
        LOG(ERROR) << "NvlinkTransport: cudaMemcpyBatchAsync "
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
            slices[i]->local.cuda_stream = (void*)stream;
        }
    }
#elif CUDART_VERSION >= 12080
    err = cudaMemcpyBatchAsync(const_cast<void**>(dsts.data()),
                               const_cast<void**>(srcs.data()),
                               mutable_sizes.data(), static_cast<size_t>(count),
                               &attr, &attrs_idx, 1, &fail_idx, stream);
    if (err != cudaSuccess) {
        if (fail_idx < count) {
            LOG(ERROR) << "NvlinkTransport: cudaMemcpyBatchAsync "
                       << "failed at index " << fail_idx
                       << " (src=" << srcs[fail_idx]
                       << ", dst=" << dsts[fail_idx]
                       << ", size=" << sizes[fail_idx]
                       << "): " << cudaGetErrorString(err);
        } else {
            LOG(ERROR) << "NvlinkTransport: cudaMemcpyBatchAsync "
                       << "failed: " << cudaGetErrorString(err);
        }
        // Copies [0, fail_idx) were submitted successfully → POSTED.
        // Copy [fail_idx] failed → FAILED.
        // Copies (fail_idx, count) were never submitted → FAILED.
        for (size_t i = 0; i < fail_idx; ++i) {
            slices[i]->status = Slice::POSTED;
            slices[i]->local.cuda_stream = (void*)stream;
        }
        for (size_t i = fail_idx; i < count; ++i) {
            if (slices[i]->status == Slice::PENDING) {
                slices[i]->markFailed();
            }
        }
    } else {
        for (size_t i = 0; i < count; ++i) {
            slices[i]->status = Slice::POSTED;
            slices[i]->local.cuda_stream = (void*)stream;
        }
    }
#else
    // Fallback for CUDA < 12.8: submit each memcpy individually
    for (size_t i = 0; i < count; ++i) {
        auto single_err = cudaMemcpyAsync(dsts[i], srcs[i], sizes[i],
                                          cudaMemcpyDefault, stream);
        if (single_err != cudaSuccess) {
            LOG(ERROR) << "NvlinkTransport: cudaMemcpyAsync failed at "
                       << "index " << i << ": "
                       << cudaGetErrorString(single_err);
            slices[i]->markFailed();
            continue;
        }
        slices[i]->status = Slice::POSTED;
        slices[i]->local.cuda_stream = (void*)stream;
    }
    return;  // Slice states already set above
#endif
}

int NvlinkTransport::install(std::string& local_server_name,
                             std::shared_ptr<TransferMetadata> metadata,
                             std::shared_ptr<Topology> topology) {
    metadata_ = metadata;
    local_server_name_ = local_server_name;

    auto desc = std::make_shared<SegmentDesc>();
    if (!desc) return ERR_MEMORY;
    desc->name = local_server_name_;
    desc->protocol = "nvlink";
    metadata_->addLocalSegment(LOCAL_SEGMENT_ID, local_server_name_,
                               std::move(desc));
    return 0;
}

Status NvlinkTransport::submitTransfer(
    BatchID batch_id, const std::vector<TransferRequest>& entries) {
    auto& batch_desc = *((BatchDesc*)(batch_id));
    if (batch_desc.task_list.size() + entries.size() > batch_desc.batch_size) {
        LOG(ERROR)
            << "NvlinkTransport: Exceed the limitation of current batch's "
               "capacity";
        return Status::InvalidArgument(
            "NvlinkTransport: Exceed the limitation of capacity, batch id: " +
            std::to_string(batch_id));
    }

    const size_t first_task_id = batch_desc.task_list.size();
    batch_desc.task_list.resize(first_task_id + entries.size());
    std::vector<TransferTask*> tasks;
    tasks.reserve(entries.size());
    for (size_t index = 0; index < entries.size(); ++index) {
        auto& task = batch_desc.task_list[first_task_id + index];
        task.batch_id = batch_id;
        task.transport_ = this;
#ifdef USE_ASCEND_HETEROGENEOUS
        task.request = const_cast<TransferRequest*>(&entries[index]);
#else
        task.request = &entries[index];
#endif
        task.total_bytes = entries[index].length;
        tasks.push_back(&task);
    }

    auto fail_submission = [&](Status status) {
        for (auto* task : tasks) {
            markSubmissionFailed(*task);
        }
        return status;
    };

    // Resolve every remote range before allocating any Slice. A relocation
    // failure therefore has no partially staged Slice ownership to unwind.
    std::vector<uint64_t> resolved_addresses;
    resolved_addresses.reserve(entries.size());
    for (const auto& request : entries) {
        uint64_t dest_addr = request.target_offset;
        if (request.target_id != LOCAL_SEGMENT_ID) {
            int rc = relocateSharedMemoryAddress(dest_addr, request.length,
                                                 request.target_id);
            if (rc != 0) {
                return fail_submission(
                    Status::Memory("NVLink remote address relocation failed"));
            }
        }
        resolved_addresses.push_back(dest_addr);
    }

    // Get per-device transfer stream for the source buffer's device.
    CudaStreamEntry stream_entry =
        getStreamForRequest(entries.empty() ? nullptr : entries[0].source);
    cudaStream_t stream = stream_entry.stream;
    if (!stream)
        return fail_submission(
            Status::Context("Failed to create NVLink CUDA stream"));

    // Synchronize with the caller's GPU work (e.g., PyTorch gather operations)
    // that produced the source data. We use cudaEventSynchronize (CPU-blocking)
    // instead of cudaStreamWaitEvent to avoid expensive cross-device event
    // operations on non-NVIDIA GPUs. The CPU blocking is acceptable because
    // the transfer depends on the caller's work anyway — they cannot overlap.
    cudaEvent_t sync_event = getCallerSyncEvent();
    cudaError_t sync_err = cudaEventRecord(sync_event, cudaStreamPerThread);
    if (sync_err != cudaSuccess) {
        LOG(ERROR) << "NvlinkTransport: cudaEventRecord failed: "
                   << cudaGetErrorString(sync_err);
        return fail_submission(
            Status::Context("cudaEventRecord failed: " +
                            std::string(cudaGetErrorString(sync_err))));
    }
    sync_err = cudaEventSynchronize(sync_event);
    if (sync_err != cudaSuccess) {
        LOG(ERROR) << "NvlinkTransport: cudaEventSynchronize failed: "
                   << cudaGetErrorString(sync_err);
        return fail_submission(
            Status::Context("cudaEventSynchronize failed: " +
                            std::string(cudaGetErrorString(sync_err))));
    }

    // Phase 1: Prepare slices and collect memcpy parameters
    std::vector<void*> dsts, srcs;
    std::vector<size_t> sizes;
    std::vector<Slice*> slices;

    for (size_t index = 0; index < entries.size(); ++index) {
        const auto& request = entries[index];
        TransferTask& task = *tasks[index];
        const uint64_t dest_addr = resolved_addresses[index];
        Slice* slice = getSliceCache().allocate();
        slice->source_addr = (char*)request.source;
        slice->local.dest_addr = (char*)dest_addr;
        slice->length = request.length;
        slice->opcode = request.opcode;
        slice->task = &task;
        slice->target_id = request.target_id;
        slice->status = Slice::PENDING;
        slice->ts = getCurrentTimeInNano();
        task.slice_list.push_back(slice);
        __sync_fetch_and_add(&task.slice_count, 1);

        void* src = (request.opcode == TransferRequest::READ)
                        ? (void*)slice->local.dest_addr
                        : (void*)slice->source_addr;
        void* dst = (request.opcode == TransferRequest::READ)
                        ? slice->source_addr
                        : (void*)slice->local.dest_addr;
        srcs.push_back(src);
        dsts.push_back(dst);
        sizes.push_back(slice->length);
        slices.push_back(slice);
    }

    // Phase 2: Submit all memcpy operations
    submitBatchMemcpy(slices, srcs, dsts, sizes, stream);
    return Status::OK();
}

Status NvlinkTransport::getTransferStatus(BatchID batch_id, size_t task_id,
                                          TransferStatus& status) {
    auto& batch_desc = *((BatchDesc*)(batch_id));
    const size_t task_count = batch_desc.task_list.size();
    if (task_id >= task_count) {
        return Status::InvalidArgument(
            "NvlinkTransport::getTransportStatus invalid argument, batch id: " +
            std::to_string(batch_id));
    }
    auto& task = batch_desc.task_list[task_id];
    // Poll POSTED slices for async completion via cudaStreamQuery.
    // Cache the query result per stream to avoid redundant driver calls.
    // With SGLang-side torch.cuda.device(gpu_id), the calling thread's
    // active device matches the stream's device, so no device switching
    // is needed.
    std::unordered_map<cudaStream_t, cudaError_t> stream_status_cache;
    for (auto* slice : task.slice_list) {
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

Status NvlinkTransport::submitTransferTask(
    const std::vector<TransferTask*>& task_list) {
    auto fail_submission = [&](Status status) {
        for (auto* task : task_list) {
            if (task == nullptr) continue;
            markSubmissionFailed(*task);
        }
        return status;
    };

    std::vector<uint64_t> resolved_addresses;
    resolved_addresses.reserve(task_list.size());
    for (auto* task : task_list) {
        if (task == nullptr || task->request == nullptr) {
            return fail_submission(
                Status::InvalidArgument("NVLink transfer task is incomplete"));
        }
        auto& request = *task->request;
        task->total_bytes = request.length;
        uint64_t dest_addr = request.target_offset;
        if (request.target_id != LOCAL_SEGMENT_ID) {
            int rc = relocateSharedMemoryAddress(dest_addr, request.length,
                                                 request.target_id);
            if (rc != 0) {
                return fail_submission(
                    Status::Memory("NVLink remote address relocation failed"));
            }
        }
        resolved_addresses.push_back(dest_addr);
    }

    // Get per-device transfer stream. See submitTransfer() for rationale.
    CudaStreamEntry stream_entry = getStreamForRequest(
        task_list.empty() ? nullptr : task_list[0]->request->source);
    cudaStream_t stream = stream_entry.stream;
    if (!stream)
        return fail_submission(
            Status::Context("Failed to create NVLink CUDA stream"));
    // Synchronize with caller's GPU work via cudaEventSynchronize.
    cudaEvent_t sync_event = getCallerSyncEvent();
    cudaError_t sync_err = cudaEventRecord(sync_event, cudaStreamPerThread);
    if (sync_err != cudaSuccess) {
        LOG(ERROR) << "NvlinkTransport: cudaEventRecord failed: "
                   << cudaGetErrorString(sync_err);
        return fail_submission(
            Status::Context("cudaEventRecord failed: " +
                            std::string(cudaGetErrorString(sync_err))));
    }
    sync_err = cudaEventSynchronize(sync_event);
    if (sync_err != cudaSuccess) {
        LOG(ERROR) << "NvlinkTransport: cudaEventSynchronize failed: "
                   << cudaGetErrorString(sync_err);
        return fail_submission(
            Status::Context("cudaEventSynchronize failed: " +
                            std::string(cudaGetErrorString(sync_err))));
    }

    // Phase 1: Prepare slices and collect memcpy parameters
    std::vector<void*> dsts, srcs;
    std::vector<size_t> sizes;
    std::vector<Slice*> slices;

    for (size_t index = 0; index < task_list.size(); ++index) {
        assert(task_list[index]);
        auto& task = *task_list[index];
        assert(task.request);
        auto& request = *task.request;
        const uint64_t dest_addr = resolved_addresses[index];
        Slice* slice = getSliceCache().allocate();
        slice->source_addr = (char*)request.source;
        slice->local.dest_addr = (char*)dest_addr;
        slice->length = request.length;
        slice->opcode = request.opcode;
        slice->task = &task;
        slice->target_id = request.target_id;
        slice->status = Slice::PENDING;
        slice->ts = getCurrentTimeInNano();
        task.slice_list.push_back(slice);
        __sync_fetch_and_add(&task.slice_count, 1);

        void* src = (request.opcode == TransferRequest::READ)
                        ? (void*)slice->local.dest_addr
                        : (void*)slice->source_addr;
        void* dst = (request.opcode == TransferRequest::READ)
                        ? slice->source_addr
                        : (void*)slice->local.dest_addr;
        srcs.push_back(src);
        dsts.push_back(dst);
        sizes.push_back(slice->length);
        slices.push_back(slice);
    }

    // Phase 2: Submit all memcpy operations
    submitBatchMemcpy(slices, srcs, dsts, sizes, stream);
    return Status::OK();
}

}  // namespace mooncake
