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

#include <bits/stdint-uintn.h>
#include "cuda_alike.h"
#include <glog/logging.h>

#include <algorithm>
#include <cassert>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <iomanip>
#include <memory>
#include <thread>

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
static int getNumDevices() {
    static int cached_num_devices = -1;
    if (cached_num_devices == -1) {
        if (!checkCudaErrorReturn(
                cudaGetDeviceCount(&cached_num_devices),
                "NvlinkTransport: cudaGetDeviceCount failed")) {
            return 0;
        }
    }
    return cached_num_devices;
}

static bool supportFabricMem() {
    if (getenv("MC_USE_NVLINK_IPC")) return false;

    int num_devices = 0;
    cudaError_t err = cudaGetDeviceCount(&num_devices);
    if (err != cudaSuccess) {
        LOG(ERROR) << "NvlinkTransport: cudaGetDeviceCount failed: "
                   << cudaGetErrorString(err);
        return false;
    }
    if (num_devices == 0) {
        LOG(ERROR) << "NvlinkTransport: no device found";
        return false;
    }

#ifdef USE_CUDA
    for (int device_id = 0; device_id < num_devices; ++device_id) {
        int device_support_fabric_mem = 0;
        cuDeviceGetAttribute(&device_support_fabric_mem,
                             CU_DEVICE_ATTRIBUTE_HANDLE_TYPE_FABRIC_SUPPORTED,
                             device_id);
        if (!device_support_fabric_mem) {
            return false;
        }
    }
#endif
    return true;
}

static bool enableP2PAccess(int src_device_id, int dst_device_id) {
    int canAccessPeer = 0;
    if (!checkCudaErrorReturn(cudaDeviceCanAccessPeer(
                                  &canAccessPeer, src_device_id, dst_device_id),
                              "NvlinkTransport: failed to query peer access")) {
        return false;
    }

    if (!canAccessPeer) {
        LOG(ERROR) << "NvlinkTransport: device " << src_device_id
                   << " cannot p2p access device " << dst_device_id;
        return false;
    }

    // enable src->dst p2p access
    if (!checkCudaErrorReturn(cudaSetDevice(src_device_id),
                              "NvlinkTransport: failed to set device")) {
        return false;
    }
    cudaError_t result = cudaDeviceEnablePeerAccess(dst_device_id, 0);

    if (result != cudaSuccess && result != cudaErrorPeerAccessAlreadyEnabled) {
        LOG(ERROR)
            << "NvlinkTransport: failed to enable p2p access (Error code: "
            << result << " - " << cudaGetErrorString(result) << ")"
            << std::endl;

        return false;
    }

    // enable dst->src p2p access
    if (!checkCudaErrorReturn(cudaSetDevice(dst_device_id),
                              "NvlinkTransport: failed to set device")) {
        return false;
    }
    result = cudaDeviceEnablePeerAccess(src_device_id, 0);

    if (result != cudaSuccess && result != cudaErrorPeerAccessAlreadyEnabled) {
        LOG(ERROR)
            << "NvlinkTransport: failed to enable p2p access (Error code: "
            << result << " - " << cudaGetErrorString(result) << ")"
            << std::endl;

        return false;
    }

    return true;
}

NvlinkTransport::NvlinkTransport() : use_fabric_mem_(supportFabricMem()) {
    // TTL/lease eviction config. Reading env here keeps the reclaim path
    // self-contained in the transport (no external control plane). TTL only
    // needs to be well under the peer's restart time; default 120s, <=0
    // disables.
    const char *ttl = std::getenv("MOONCAKE_FABRIC_IMPORT_TTL");
    if (ttl && *ttl)
        import_ttl_s_ = std::atof(ttl);
    else
        import_ttl_s_ = 120.0;
    const char *iv = std::getenv("MOONCAKE_FABRIC_EVICT_INTERVAL");
    if (iv && *iv) {
        double v = std::atof(iv);
        // Ignore 0 / negative / garbage so a mistyped env can't turn the scan
        // cadence into a busy-spin; keep the default 30s instead.
        if (v > 0) evict_interval_s_ = v;
    }
    running_.store(true);
    if (use_fabric_mem_ && import_ttl_s_ > 0) {
        evict_thread_ = std::thread(&NvlinkTransport::evictLoop, this);
        LOG(INFO) << "NvlinkTransport: idle-import evictor on (ttl "
                  << import_ttl_s_ << "s, scan " << evict_interval_s_ << "s)";
    } else if (use_fabric_mem_) {
        // Surface the disabled case explicitly: a mistyped TTL (atof -> 0)
        // silently turns reclaim off, which is hard to diagnose otherwise.
        LOG(INFO) << "NvlinkTransport: idle-import evictor OFF (ttl "
                  << import_ttl_s_
                  << " <= 0); imported fabric segments held until process exit";
    }
}
//     int num_devices = getNumDevices();
//     if (globalConfig().trace) {
//         LOG(INFO) << "NvlinkTransport: use_fabric_mem_:" << use_fabric_mem_
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
//                         << "NvlinkTransport: enabled p2p access between
//                         device "
//                         << src_device_id << " and " << dst_device_id;
//                 }
//             } else {
//                 LOG(ERROR) << "NvlinkTransport: failed to enable p2p access "
//                               "between device "
//                            << src_device_id << " and " << dst_device_id;
//             }
//         }
//     }
// }

bool NvlinkTransport::releaseImportedEntry(const OpenedShmEntry &entry) {
    if (use_fabric_mem_) {
        // Explicit teardown via the stored import handle. Deliberately NOT
        // freePinnedLocalMemory: its cuMemRetainAllocationHandle does not work
        // on an imported allocation and early-returns, leaving the mapping (and
        // the exporter's pinned pages) in place. Order: unmap, free the VA,
        // then release the import handle so the last importer reference is
        // dropped.
        bool ok = true;
        CUresult r = cuMemUnmap((CUdeviceptr)entry.shm_addr, entry.length);
        if (r != CUDA_SUCCESS) {
            LOG(ERROR) << "NvlinkTransport: cuMemUnmap failed during release: "
                       << r;
            ok = false;
        }
        r = cuMemAddressFree((CUdeviceptr)entry.shm_addr, entry.length);
        if (r != CUDA_SUCCESS) {
            LOG(ERROR)
                << "NvlinkTransport: cuMemAddressFree failed during release: "
                << r;
            ok = false;
        }
        r = cuMemRelease(entry.import_handle);
        if (r != CUDA_SUCCESS) {
            LOG(ERROR) << "NvlinkTransport: cuMemRelease(import_handle) failed "
                          "during release: "
                       << r;
            ok = false;
        }
        return ok;
    }
    cudaError_t e = cudaIpcCloseMemHandle(entry.shm_addr);
    if (e != cudaSuccess) {
        LOG(ERROR)
            << "NvlinkTransport: cudaIpcCloseMemHandle failed during release: "
            << cudaGetErrorString(e);
        return false;
    }
    return true;
}

NvlinkTransport::~NvlinkTransport() {
    // Set running_ under evict_cv_mutex_ so the store is ordered against the
    // evictor's wait predicate: otherwise a notify fired between the evictor's
    // while-check and its wait_for is lost and join() blocks a full
    // evict_interval_s_ before the timeout wakes it.
    {
        std::lock_guard<std::mutex> lk(evict_cv_mutex_);
        running_.store(false);
    }
    evict_cv_.notify_all();
    if (evict_thread_.joinable()) evict_thread_.join();
    // cuMem* are context-bound and the destructing thread may not share the
    // import context (same hazard the evictor handles); make it current first,
    // else release fails with CUDA_ERROR_INVALID_CONTEXT and leaks the pages.
    // Save and restore the caller's context: a destructor running on a user
    // thread must not leave that thread pointed at the import context, or the
    // thread's later CUDA calls land on the wrong context.
    CUcontext ctx = worker_ctx_.load();
    CUcontext prev_ctx = nullptr;
    if (ctx) {
        cuCtxGetCurrent(&prev_ctx);
        cuCtxSetCurrent(ctx);
    }
    for (auto &entry : remap_entries_) {
        releaseImportedEntry(entry.second);
    }
    remap_entries_.clear();
    if (ctx) cuCtxSetCurrent(prev_ctx);
}

static inline int64_t steadyNowNs() {
    return std::chrono::duration_cast<std::chrono::nanoseconds>(
               std::chrono::steady_clock::now().time_since_epoch())
        .count();
}

int NvlinkTransport::evictIdleSegments(double ttl_s) {
    if (ttl_s <= 0) return 0;
    const int64_t now_ns = steadyNowNs();
    const int64_t ttl_ns = static_cast<int64_t>(ttl_s * 1e9);
    std::vector<OpenedShmEntry> to_release;
    {
        // Under the write lock we only DECIDE (idle check) and DETACH (erase
        // from remap_entries_); the blocking cuMem* teardown runs AFTER the
        // lock is dropped. Holding a spinlock across blocking driver calls
        // would make any thread contending for it busy-spin and burn a core.
        // Safety without an in-lock unmap rests on two facts: (1) an entry is
        // "idle" only if it had no write for > ttl_s, and relocate stamps
        // last_active_ns BEFORE its synchronous copy, so any in-flight write
        // looks fresh and is skipped here; (2) erasing the entry under the lock
        // stops a concurrent relocate from re-using or re-importing it before
        // we release. The lease lives in the entry, so it is reclaimed with it
        // -- no separate map to scan.
        RWSpinlock::WriteGuard lock_guard(remap_lock_);
        for (auto it = remap_entries_.begin(); it != remap_entries_.end();) {
            int64_t la =
                it->second.last_active_ns.load(std::memory_order_relaxed);
            // la == 0 -> imported but not yet stamped; treat as active.
            bool idle = (la != 0) && (now_ns - la) > ttl_ns;
            if (idle) {
                to_release.push_back(it->second);
                it = remap_entries_.erase(it);
            } else {
                ++it;
            }
        }
    }
    // Release outside the lock. The evictor thread already made worker_ctx_
    // current (cuMem* are context-bound) before calling us.
    int released = 0;
    for (const auto &entry : to_release) {
        if (releaseImportedEntry(entry)) ++released;
    }
    return released;
}

void NvlinkTransport::evictLoop() {
    while (running_.load()) {
        {
            std::unique_lock<std::mutex> lk(evict_cv_mutex_);
            evict_cv_.wait_for(lk,
                               std::chrono::duration<double>(evict_interval_s_),
                               [this] { return !running_.load(); });
        }
        if (!running_.load()) break;
        CUcontext ctx = worker_ctx_.load();
        if (!ctx) continue;  // nothing imported yet -> nothing to reclaim
        // cuMem* are context-bound; a fresh std::thread has no current context.
        CUresult cr = cuCtxSetCurrent(ctx);
        if (cr != CUDA_SUCCESS) {
            LOG(ERROR) << "NvlinkTransport: evictor cuCtxSetCurrent failed: "
                       << cr;
            continue;
        }
        int n = evictIdleSegments(import_ttl_s_);
        if (n > 0)
            LOG(INFO) << "NvlinkTransport: evictIdleSegments released " << n
                      << " idle imported segment(s) (idle > " << import_ttl_s_
                      << "s)";
    }
}

int NvlinkTransport::unregisterRemoteSegment(SegmentID target_id) {
    // Per-target version of the destructor. The caller is responsible for
    // draining in-flight transfers to target_id (the mapping VA may already be
    // captured by an in-flight RDMA write).
    //
    // Collect-then-release, like evictIdleSegments: under the write lock we
    // only detach the matching entries from remap_entries_, then run the
    // blocking cuMem* teardown OUTSIDE the lock so a thread contending for the
    // spinlock never busy-spins across blocking driver calls. Erasing under the
    // lock blocks a concurrent relocate from re-using / re-importing these
    // mappings; the drain contract covers writes already in flight. released
    // counts only entries whose backing pages were ACTUALLY reclaimed (all CUDA
    // release calls succeeded), so it is the real reclaim count, not the number
    // of cache entries erased.
    std::vector<OpenedShmEntry> to_release;
    {
        RWSpinlock::WriteGuard lock_guard(remap_lock_);
        for (auto it = remap_entries_.begin(); it != remap_entries_.end();) {
            if (it->first.first == target_id) {
                to_release.push_back(it->second);
                it = remap_entries_.erase(it);
            } else {
                ++it;
            }
        }
    }
    if (to_release.empty()) return 0;
    // cuMem* are context-bound. Unlike the evictor, this runs on the caller's
    // (manual-knob) thread, which may not have the import context current; make
    // it current for the teardown and restore the caller's context after (same
    // save/set/restore as the destructor).
    CUcontext ctx = worker_ctx_.load();
    CUcontext prev_ctx = nullptr;
    if (ctx) {
        cuCtxGetCurrent(&prev_ctx);
        cuCtxSetCurrent(ctx);
    }
    int released = 0;
    for (const auto &entry : to_release) {
        if (releaseImportedEntry(entry)) ++released;
    }
    if (ctx) cuCtxSetCurrent(prev_ctx);
    return released;
}

int NvlinkTransport::install(std::string &local_server_name,
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
    BatchID batch_id, const std::vector<TransferRequest> &entries) {
    auto &batch_desc = *((BatchDesc *)(batch_id));
    if (batch_desc.task_list.size() + entries.size() > batch_desc.batch_size) {
        LOG(ERROR)
            << "NvlinkTransport: Exceed the limitation of current batch's "
               "capacity";
        return Status::InvalidArgument(
            "NvlinkTransport: Exceed the limitation of capacity, batch id: " +
            std::to_string(batch_id));
    }

    size_t task_id = batch_desc.task_list.size();
    batch_desc.task_list.resize(task_id + entries.size());

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
        __sync_fetch_and_add(&task.slice_count, 1);
        cudaError_t err;
        if (slice->opcode == TransferRequest::READ)
            err = cudaMemcpy(slice->source_addr, (void *)slice->local.dest_addr,
                             slice->length, cudaMemcpyDefault);
        else
            err = cudaMemcpy((void *)slice->local.dest_addr, slice->source_addr,
                             slice->length, cudaMemcpyDefault);
        if (err != cudaSuccess)
            slice->markFailed();
        else
            slice->markSuccess();
    }

    return Status::OK();
}

Status NvlinkTransport::getTransferStatus(BatchID batch_id, size_t task_id,
                                          TransferStatus &status) {
    auto &batch_desc = *((BatchDesc *)(batch_id));
    const size_t task_count = batch_desc.task_list.size();
    if (task_id >= task_count) {
        return Status::InvalidArgument(
            "NvlinkTransport::getTransportStatus invalid argument, batch id: " +
            std::to_string(batch_id));
    }
    auto &task = batch_desc.task_list[task_id];
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
    const std::vector<TransferTask *> &task_list) {
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
        task.slice_list.push_back(slice);
        __sync_fetch_and_add(&task.slice_count, 1);
        cudaError_t err;
        if (slice->opcode == TransferRequest::READ)
            err = cudaMemcpy(slice->source_addr, (void *)slice->local.dest_addr,
                             slice->length, cudaMemcpyDefault);
        else
            err = cudaMemcpy((void *)slice->local.dest_addr, slice->source_addr,
                             slice->length, cudaMemcpyDefault);
        if (err != cudaSuccess)
            slice->markFailed();
        else
            slice->markSuccess();
    }
    return Status::OK();
}

int NvlinkTransport::registerLocalMemory(void *addr, size_t length,
                                         const std::string &location,
                                         bool remote_accessible,
                                         bool update_metadata) {
    std::lock_guard<std::mutex> lock(register_mutex_);
    if (globalConfig().trace) {
        LOG(INFO) << "register memory: addr " << addr << ", length " << length;
    }
    if (!use_fabric_mem_) {
        cudaPointerAttributes attr;
        cudaError_t err = cudaPointerGetAttributes(&attr, addr);
        if (err != cudaSuccess) {
            LOG(ERROR) << "NvlinkTransport: cudaPointerGetAttributes failed";
            return -1;
        }

        if (attr.type != cudaMemoryTypeDevice) {
            LOG(ERROR) << "Unsupported memory type, " << addr << " "
                       << attr.type;
            return -1;
        }

        cudaIpcMemHandle_t handle;
        err = cudaIpcGetMemHandle(&handle, addr);
        if (err != cudaSuccess) {
            LOG(ERROR) << "NvlinkTransport: cudaIpcGetMemHandle failed";
            return -1;
        }

        (void)remote_accessible;
        BufferDesc desc;
        desc.addr = (uint64_t)addr;
        desc.length = length;
        desc.name = location;
        desc.shm_name =
            serializeBinaryData(&handle, sizeof(cudaIpcMemHandle_t));
        return metadata_->addLocalMemoryBuffer(desc, true);
    } else {
        CUmemGenericAllocationHandle handle;
        auto result = cuMemRetainAllocationHandle(&handle, addr);
        if (result != CUDA_SUCCESS) {
            LOG(WARNING) << "Memory region " << addr
                         << " is not allocated by cuMemCreate, "
                         << "but it can be used as local buffer";
            return 0;
        }

        // Find whole physical page for memory registration
        void *real_addr;
        size_t real_size;
        result = cuMemGetAddressRange((CUdeviceptr *)&real_addr, &real_size,
                                      (CUdeviceptr)addr);
        if (result != CUDA_SUCCESS) {
            LOG(WARNING) << "NvlinkTransport: cuMemGetAddressRange failed: "
                         << result;
            const uint64_t granularity = 2 * 1024 * 1024;
            real_addr = addr;
            real_size = (length + granularity - 1) & ~(granularity - 1);
        }

        CUmemFabricHandle export_handle;
        result = cuMemExportToShareableHandle(&export_handle, handle,
                                              CU_MEM_HANDLE_TYPE_FABRIC, 0);
        if (result != CUDA_SUCCESS) {
            LOG(ERROR)
                << "NvlinkTransport: cuMemExportToShareableHandle failed: "
                << result;
            return -1;
        }

        (void)remote_accessible;
        BufferDesc desc;
        desc.addr = (uint64_t)real_addr;  // (uint64_t)addr;
        desc.length = real_size;          // length;
        desc.name = location;
        desc.shm_name =
            serializeBinaryData(&export_handle, sizeof(CUmemFabricHandle));
        return metadata_->addLocalMemoryBuffer(desc, true);
    }
}

int NvlinkTransport::unregisterLocalMemory(void *addr, bool update_metadata) {
    return metadata_->removeLocalMemoryBuffer(addr, update_metadata);
}

int NvlinkTransport::relocateSharedMemoryAddress(uint64_t &dest_addr,
                                                 uint64_t length,
                                                 uint64_t target_id) {
    auto desc = metadata_->getSegmentDescByID(target_id);
    int index = 0;
    for (auto &entry : desc->buffers) {
        if (!entry.shm_name.empty() && entry.addr <= dest_addr &&
            dest_addr + length <= entry.addr + entry.length) {
            remap_lock_.lockShared();
            auto cache_it =
                remap_entries_.find(std::make_pair(target_id, entry.addr));
            if (cache_it != remap_entries_.end()) {
                // Refresh the lease before releasing remap_lock_ and before the
                // caller's synchronous copy. A relaxed atomic store under the
                // shared lock (the evictor reads it under the write lock); a
                // target is evicted only after ttl_s with no write, and ttl_s
                // is far larger than any single transfer, so an in-flight write
                // always looks fresh here.
                cache_it->second.last_active_ns.store(
                    steadyNowNs(), std::memory_order_relaxed);
                auto shm_addr = cache_it->second.shm_addr;
                remap_lock_.unlockShared();
                dest_addr = dest_addr - entry.addr + ((uint64_t)shm_addr);
                return 0;
            }
            remap_lock_.unlockShared();
            RWSpinlock::WriteGuard lock_guard(remap_lock_);
            if (!remap_entries_.count(std::make_pair(target_id, entry.addr))) {
                std::vector<unsigned char> output_buffer;
                deserializeBinaryData(entry.shm_name, output_buffer);
                if (output_buffer.size() == sizeof(cudaIpcMemHandle_t) &&
                    !use_fabric_mem_) {
                    cudaIpcMemHandle_t handle;
                    memcpy(&handle, output_buffer.data(), sizeof(handle));
                    void *shm_addr = nullptr;
                    cudaError_t err = cudaIpcOpenMemHandle(
                        &shm_addr, handle, cudaIpcMemLazyEnablePeerAccess);
                    if (err != cudaSuccess) {
                        LOG(ERROR)
                            << "NvlinkTransport: cudaIpcOpenMemHandle failed: "
                            << cudaGetErrorString(err);
                        return -1;
                    }
                    OpenedShmEntry shm_entry;
                    shm_entry.shm_addr = shm_addr;
                    shm_entry.length = entry.length;
                    remap_entries_[std::make_pair(target_id, entry.addr)] =
                        shm_entry;
                } else if (output_buffer.size() == sizeof(CUmemFabricHandle) &&
                           use_fabric_mem_) {
                    CUmemFabricHandle export_handle;
                    memcpy(&export_handle, output_buffer.data(),
                           sizeof(export_handle));
                    void *shm_addr = nullptr;
                    CUmemGenericAllocationHandle handle;
                    auto result = cuMemImportFromShareableHandle(
                        &handle, &export_handle, CU_MEM_HANDLE_TYPE_FABRIC);
                    if (result != CUDA_SUCCESS) {
                        LOG(ERROR) << "NvlinkTransport: "
                                      "cuMemImportFromShareableHandle failed: "
                                   << result;
                        return -1;
                    }
                    result = cuMemAddressReserve((CUdeviceptr *)&shm_addr,
                                                 entry.length, 0, 0, 0);
                    if (result != CUDA_SUCCESS) {
                        LOG(ERROR)
                            << "NvlinkTransport: cuMemAddressReserve failed: "
                            << result;
                        return -1;
                    }
                    result = cuMemMap((CUdeviceptr)shm_addr, entry.length, 0,
                                      handle, 0);
                    if (result != CUDA_SUCCESS) {
                        LOG(ERROR)
                            << "NvlinkTransport: cuMemMap failed: " << result;
                        return -1;
                    }

                    int device_count;
                    cudaGetDeviceCount(&device_count);
                    CUmemAccessDesc accessDesc[device_count];
                    for (int device_id = 0; device_id < device_count;
                         ++device_id) {
                        accessDesc[device_id].location.type =
                            CU_MEM_LOCATION_TYPE_DEVICE;
                        accessDesc[device_id].location.id = device_id;
                        accessDesc[device_id].flags =
                            CU_MEM_ACCESS_FLAGS_PROT_READWRITE;
                    }
                    result = cuMemSetAccess((CUdeviceptr)shm_addr, entry.length,
                                            accessDesc, device_count);
                    if (result != CUDA_SUCCESS) {
                        LOG(ERROR) << "NvlinkTransport: cuMemSetAccess failed: "
                                   << result;
                        return -1;
                    }
                    // Keep the imported handle alive for the mapping's lifetime
                    // -- releaseImportedEntry drops it with cuMemRelease at
                    // teardown. Releasing it here and relying on
                    // cuMemRetainAllocationHandle (freePinnedLocalMemory) to
                    // recover it later does NOT work on imported allocations:
                    // the retain fails, unmap is skipped, and the exporter's
                    // pages stay pinned. The handle ref does not affect writes.
                    // Capture the context the mappings live in (valid here --
                    // we just cuMemMap'd); the evictor/dtor make it current
                    // before cuMemUnmap/cuMemRelease.
                    if (worker_ctx_.load() == nullptr) {
                        CUcontext cur = nullptr;
                        if (cuCtxGetCurrent(&cur) == CUDA_SUCCESS && cur)
                            worker_ctx_.store(cur);
                    }
                    OpenedShmEntry shm_entry;
                    shm_entry.shm_addr = shm_addr;
                    shm_entry.length = entry.length;
                    shm_entry.import_handle = handle;
                    remap_entries_[std::make_pair(target_id, entry.addr)] =
                        shm_entry;
                } else {
                    LOG(ERROR) << "Mismatched NVLink data transfer method";
                    return -1;
                }
            }
            auto &mapped =
                remap_entries_[std::make_pair(target_id, entry.addr)];
            // Stamp the lease on the import path too (held under the write
            // lock).
            mapped.last_active_ns.store(steadyNowNs(),
                                        std::memory_order_relaxed);
            auto shm_addr = mapped.shm_addr;
            dest_addr = dest_addr - entry.addr + ((uint64_t)shm_addr);
            return 0;
        }
        index++;
    }
    LOG(ERROR) << "Requested address " << (void *)dest_addr << " to "
               << (void *)(dest_addr + length) << " not found!";
    return ERR_INVALID_ARGUMENT;
}

int NvlinkTransport::registerLocalMemoryBatch(
    const std::vector<Transport::BufferEntry> &buffer_list,
    const std::string &location) {
    for (auto &buffer : buffer_list)
        registerLocalMemory(buffer.addr, buffer.length, location, true, false);
    return metadata_->updateLocalSegmentDesc();
}

int NvlinkTransport::unregisterLocalMemoryBatch(
    const std::vector<void *> &addr_list) {
    for (auto &addr : addr_list) unregisterLocalMemory(addr, false);
    return metadata_->updateLocalSegmentDesc();
}

void *NvlinkTransport::allocatePinnedLocalMemory(size_t size) {
    if (!supportFabricMem()) {
        void *ptr = nullptr;
        cudaMalloc(&ptr, size);
        return ptr;
    }
    size_t granularity = 0;
    CUdevice currentDev;
    CUmemAllocationProp prop = {};
    CUmemGenericAllocationHandle handle;
    void *ptr = nullptr;
    int cudaDev;
    int flag = 0;
    cudaError_t err = cudaGetDevice(&cudaDev);
    if (err != cudaSuccess) {
        LOG(ERROR) << "NvlinkTransport: cudaGetDevice failed: "
                   << cudaGetErrorString(err);
        return nullptr;
    }
    CUresult result = cuDeviceGet(&currentDev, cudaDev);
    if (result != CUDA_SUCCESS) {
        LOG(ERROR) << "NvlinkTransport: cuDeviceGet failed: " << result;
        return nullptr;
    }
    prop.type = CU_MEM_ALLOCATION_TYPE_PINNED;
    prop.location.type = CU_MEM_LOCATION_TYPE_DEVICE;
    prop.requestedHandleTypes = CU_MEM_HANDLE_TYPE_FABRIC;
    prop.location.id = currentDev;
    result = cuDeviceGetAttribute(
        &flag, CU_DEVICE_ATTRIBUTE_GPU_DIRECT_RDMA_WITH_CUDA_VMM_SUPPORTED,
        currentDev);
    if (result != CUDA_SUCCESS) {
        LOG(ERROR) << "NvlinkTransport: cuDeviceGetAttribute failed: "
                   << result;
        return nullptr;
    }
    if (flag) prop.allocFlags.gpuDirectRDMACapable = 1;
    result = cuMemGetAllocationGranularity(&granularity, &prop,
                                           CU_MEM_ALLOC_GRANULARITY_MINIMUM);
    if (result != CUDA_SUCCESS) {
        LOG(ERROR) << "NvlinkTransport: cuMemGetAllocationGranularity failed: "
                   << result;
        return nullptr;
    }
    // fix size
    size = (size + granularity - 1) & ~(granularity - 1);
    if (size == 0) size = granularity;
    result = cuMemCreate(&handle, size, &prop, 0);
    if (result != CUDA_SUCCESS) {
        LOG(ERROR) << "NvlinkTransport: cuMemCreate failed: " << result;
        return nullptr;
    }
    result = cuMemAddressReserve((CUdeviceptr *)&ptr, size, granularity, 0, 0);
    if (result != CUDA_SUCCESS) {
        LOG(ERROR) << "NvlinkTransport: cuMemAddressReserve failed: " << result;
        cuMemRelease(handle);
        return nullptr;
    }
    result = cuMemMap((CUdeviceptr)ptr, size, 0, handle, 0);
    if (result != CUDA_SUCCESS) {
        LOG(ERROR) << "NvlinkTransport: cuMemMap failed: " << result;
        cuMemAddressFree((CUdeviceptr)ptr, size);
        cuMemRelease(handle);
        return nullptr;
    }
    int device_count;
    cudaGetDeviceCount(&device_count);
    CUmemAccessDesc accessDesc[device_count];
    for (int idx = 0; idx < device_count; ++idx) {
        accessDesc[idx].location.type = CU_MEM_LOCATION_TYPE_DEVICE;
        accessDesc[idx].location.id = idx;
        accessDesc[idx].flags = CU_MEM_ACCESS_FLAGS_PROT_READWRITE;
    }
    result = cuMemSetAccess((CUdeviceptr)ptr, size, accessDesc, device_count);
    if (result != CUDA_SUCCESS) {
        LOG(ERROR) << "NvlinkTransport: cuMemSetAccess failed: " << result;
        cuMemUnmap((CUdeviceptr)ptr, size);
        cuMemAddressFree((CUdeviceptr)ptr, size);
        cuMemRelease(handle);
        return nullptr;
    }
    return ptr;
}

void NvlinkTransport::freePinnedLocalMemory(void *ptr) {
    if (!supportFabricMem()) {
        cudaFree(ptr);
        return;
    }
    CUmemGenericAllocationHandle handle;
    size_t size = 0;
    auto result = cuMemRetainAllocationHandle(&handle, ptr);
    if (result != CUDA_SUCCESS) {
        LOG(ERROR) << "NvlinkTransport: cuMemRetainAllocationHandle failed: "
                   << result;
        return;
    }
    result = cuMemGetAddressRange(NULL, &size, (CUdeviceptr)ptr);
    if (result == CUDA_SUCCESS) {
        cuMemUnmap((CUdeviceptr)ptr, size);
        cuMemAddressFree((CUdeviceptr)ptr, size);
    }
    cuMemRelease(handle);
}
}  // namespace mooncake
