// Copyright 2024 KVCache.AI
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

#include "transport/rdma_transport/pxn/pxn_runtime_manager.h"

#include <cuda_runtime.h>
#include <glog/logging.h>

#include <cassert>
#include <chrono>

#include "common.h"
#include "config.h"
#include "memory_location.h"
#include "transport/rdma_transport/pxn/pxn_topology_manager.h"
#include "transport/rdma_transport/pxn/pxn_context_resource.h"
#include "transport/rdma_transport/pxn/pxn_object_cache.h"
#include "transport/rdma_transport/pxn/pxn_memcpy_backend.h"
#include "transport/rdma_transport/rdma_context.h"
#include "transport/rdma_transport/rdma_transport.h"
#include "transport/rdma_transport/worker_pool.h"

namespace mooncake {
namespace pxn {

// reclaim copy worker type slice, just release PxnCopyWorker struct
// not delete shell Slice(batch release)
static void PxnReclaimCwFromShell(void *shell_raw) {
    Transport::Slice *shell = static_cast<Transport::Slice *>(shell_raw);
    if (shell == nullptr || !shell->pxn_info.is_copy_worker) return;
    PxnCopyWorker *cw =
        static_cast<PxnCopyWorker *>(shell->pxn_info.pxn_copy_worker);
    if (cw != nullptr) {
        PxnGetCopyWorkerCache().PxnDeallocate(cw);
        shell->pxn_info.pxn_copy_worker = nullptr;
    }
}

PxnWorkerPoolState::~PxnWorkerPoolState() {
    for (int shard_idx = 0; shard_idx < pxn_worker_thread_count; ++shard_idx) {
        for (auto *shell_raw : pxn_slice_queue[shard_idx]) {
            PxnReclaimCwFromShell(shell_raw);
        }
        pxn_slice_queue[shard_idx].clear();
    }
    for (auto &disp_ptr : pxn_dispatchers) {
        if (!disp_ptr) continue;
        for (auto *shell_raw : disp_ptr->pxn_pending_cws) {
            PxnReclaimCwFromShell(shell_raw);
        }
        disp_ptr->pxn_pending_cws.clear();
    }

    if (pxn_dispatchers.empty()) {
        return;
    }

    int prev_device = -1;
    cudaGetDevice(&prev_device);
    if (pxn_proxy_gpu_id >= 0) cudaSetDevice(pxn_proxy_gpu_id);

    for (auto &disp_ptr : pxn_dispatchers) {
        if (!disp_ptr) continue;
        if (disp_ptr->pxn_stream) cudaStreamDestroy(disp_ptr->pxn_stream);
        if (disp_ptr->pxn_slot_pool) {
            for (auto &slot : disp_ptr->pxn_slot_pool->pxn_slots) {
                if (slot.pxn_send_event) cudaEventDestroy(slot.pxn_send_event);
            }
        }
    }

    if (prev_device >= 0) cudaSetDevice(prev_device);
}

// construct PxnRuntimeManager and memcpy backend
// (CUDA>=12.8 BatchAsync，or peer-async fallback)
PxnRuntimeManager::PxnRuntimeManager(const std::vector<std::string> &hca_list,
                                     bool IbRelaxOrderEn)
    : pxn_ib_relaxed_ordering_(IbRelaxOrderEn),
      pxn_backend_(PxnCreateMemcpyBackend()) {
    int rc = PxnTopologyManager::PxnCreateAndBuild(pxn_topo_mgr_ref_, hca_list);
    if (rc != 0) {
        LOG(WARNING) << "PxnRuntimeManager: PxnCreateAndBuild failed rc=" << rc
                     << ", PXN topology disabled";
    }
}

// src_gpu: cuda device physics id, local_device_id:pxn_hca_list_ group
// subscript return pxn_cuda_list_ group subscript
int PxnRuntimeManager::PxnLookupProxyGpu(int src_gpu,
                                         int local_device_id) const {
    if (pxn_topo_mgr_ref_ == nullptr || !pxn_topo_mgr_ref_->PxnIsBuilt())
        return -1;
    if (src_gpu < 0 || local_device_id < 0) return -1;
    int src_idx = pxn_topo_mgr_ref_->PxnCudaDeviceToIndex(src_gpu);
    if (src_idx < 0) return -1;
    return pxn_topo_mgr_ref_->PxnLookupProxyGpu(src_idx, local_device_id);
}

// ============================================================
// PxnInitWorkerPoolState
// ------------------------------------------------------------
// Called from WorkerPool ctor. Slot pool memory comes from the RdmaContext
// (allocated once in RdmaContext::construct). Here we only:
//   1) probe the context resource
//   2) select the memcpy backend for the proxy GPU
//   3) slice the context buffer into per-worker views
//   4) create per-worker CUDA stream + slot events
// ============================================================
// init worker pool resource
int PxnRuntimeManager::PxnInitWorkerPoolState(
    PxnWorkerPoolState *state, RdmaContext *context, int worker_thread_count,
    bool (*can_post_fn)(int thread_id)) {
    if (state == nullptr || context == nullptr) return -1;
    if (pxn_topo_mgr_ref_ == nullptr || !pxn_topo_mgr_ref_->PxnIsBuilt())
        return 0;

    // one nic->one rdma context->one rdma pxn context resource->one worker pool
    // object
    auto *ctx_res = context->getPxnResource();
    if (ctx_res == nullptr || ctx_res->pxn_dev_pool == nullptr ||
        ctx_res->pxn_mr == nullptr) {
        VLOG(1) << "PXN: RdmaContext '" << context->deviceName()
                << "' has no PXN resource; PXN disabled for this WorkerPool";
        return 0;
    }
    // int rdma pxn context resource, record worker thread cnt
    if (worker_thread_count != ctx_res->pxn_worker_thread_count) {
        LOG(WARNING) << "PXN: worker_thread_count mismatch (WorkerPool="
                     << worker_thread_count
                     << ", context=" << ctx_res->pxn_worker_thread_count
                     << "); using context value";
        worker_thread_count = ctx_res->pxn_worker_thread_count;
    }

    state->pxn_worker_thread_count = worker_thread_count;
    state->pxn_local_device_id =
        pxn_topo_mgr_ref_->PxnHcaNameToIndex(context->deviceName());
    state->pxn_proxy_gpu_id = ctx_res->pxn_proxy_gpu_id;

    // Shard queues + submit-side scratch sized to worker_thread_count.
    state->pxn_slice_queue =
        std::vector<std::vector<void *>>(worker_thread_count);
    state->pxn_slice_queue_count =
        std::vector<std::atomic<uint64_t>>(worker_thread_count);
    state->pxn_slice_queue_lock = std::vector<TicketLock>(worker_thread_count);
    for (int i = 0; i < worker_thread_count; ++i) {
        state->pxn_slice_queue_count[i].store(0, std::memory_order_relaxed);
    }

    // thread-granular independent resources
    state->pxn_dispatchers.resize(worker_thread_count);
    for (int idx = 0; idx < worker_thread_count; ++idx) {
        bool need_dispatcher =
            (can_post_fn == nullptr) ? true : can_post_fn(idx);
        LOG(INFO) << "PXN: thread " << idx
                  << " need_dispatcher=" << need_dispatcher << " can_post_fn="
                  << (can_post_fn != nullptr ? "set" : "null");
        if (need_dispatcher) {
            state->pxn_dispatchers[idx].reset(new PxnMemcpyDispatcherRuntime());
        }
    }

    // switch to proxy gpu
    int prev_device = -1;
    cudaGetDevice(&prev_device);
    if (cudaSetDevice(ctx_res->pxn_proxy_gpu_id) != cudaSuccess) {
        LOG(ERROR) << "PXN: cudaSetDevice(" << ctx_res->pxn_proxy_gpu_id
                   << ") failed";
        return -1;
    }

    const size_t slot_size = ctx_res->pxn_slot_size;
    const int slots_per_worker = ctx_res->pxn_slot_count_per_worker;
    char *dev_pool_base = static_cast<char *>(ctx_res->pxn_dev_pool);

    pxn_slot_size_cache_.store(slot_size, std::memory_order_relaxed);

    int dispatcher_count = 0;
    for (int worker_idx = 0; worker_idx < worker_thread_count; ++worker_idx) {
        bool need_dispatcher =
            (can_post_fn == nullptr) ? true : can_post_fn(worker_idx);
        if (!need_dispatcher) continue;
        auto &disp_rt = *state->pxn_dispatchers[worker_idx];

        // dispatcher owns the CUDA stream
        if (cudaStreamCreateWithFlags(&disp_rt.pxn_stream,
                                      cudaStreamNonBlocking) != cudaSuccess) {
            LOG(ERROR) << "PXN: cudaStreamCreate (dispatcher) failed thread "
                       << worker_idx;
            if (prev_device >= 0) cudaSetDevice(prev_device);
            return -1;
        }

        // dispatcher owns the slot pool
        auto pool = std::unique_ptr<PxnSlotPool>(new PxnSlotPool());
        pool->pxn_slot_size = slot_size;
        pool->pxn_slot_count = slots_per_worker;
        pool->pxn_pool_index = worker_idx;
        pool->pxn_proxy_gpu_id = ctx_res->pxn_proxy_gpu_id;
        pool->pxn_dev_pool =
            dev_pool_base + static_cast<size_t>(worker_idx) *
                                static_cast<size_t>(slots_per_worker) *
                                slot_size;
        pool->pxn_mr = ctx_res->pxn_mr;
        pool->pxn_slots = std::vector<PxnSlotEntry>(slots_per_worker);
        for (int slot_idx = 0; slot_idx < slots_per_worker; ++slot_idx) {
            cudaEventCreateWithFlags(&pool->pxn_slots[slot_idx].pxn_send_event,
                                     cudaEventDisableTiming);
        }
        disp_rt.pxn_slot_pool = std::move(pool);
        disp_rt.pxn_disp_idx = dispatcher_count;
        dispatcher_count++;
    }
    state->pxn_dispatcher_count = dispatcher_count;
    if (prev_device >= 0) cudaSetDevice(prev_device);

    LOG(INFO) << "PXN: initialized WorkerPool state nic="
              << context->deviceName()
              << " proxy_gpu=" << ctx_res->pxn_proxy_gpu_id
              << " threads=" << worker_thread_count << " slot=" << slot_size
              << "x" << slots_per_worker;
    return 0;
}

// use BufferDesc select device
int PxnRuntimeManager::PxnSelectDeviceForBuffer(
    TransferMetadata::SegmentDesc *desc,
    const TransferMetadata::BufferDesc &buffer, int retry_count) {
    std::string location = buffer.name;
    int device_id = desc->topology.selectDevice(location, retry_count);
    if (device_id >= 0) return device_id;

    device_id = desc->topology.selectDevice(kWildcardLocation, retry_count);
    return device_id;
}

// seal current cw -> push to pxn_pending_shells, clear current
static inline void PxnSealCurrentCw(PxnPackingContext &ctx) {
    ctx.pxn_pending_shells[ctx.pxn_current_context].push_back(
        ctx.pxn_current_shell);
    auto *cw = static_cast<PxnCopyWorker *>(
        ctx.pxn_current_shell->pxn_info.pxn_copy_worker);
    ctx.pxn_fin_cw_rdma_cnt += cw->pxn_rdma_workers_cnt;
    ctx.pxn_current_cw = nullptr;
    ctx.pxn_current_shell = nullptr;
    ctx.pxn_current_task = nullptr;
}

// seg-level: seal if pack_key changed (called once per seg, before slice loop)
static inline void PxnSealIfKeyChanged(PxnPackingContext &ctx,
                                       const PxnDataSeg &seg) {
    PxnPackKey pack_key;
    pack_key.pxn_context = nullptr;
    pack_key.pxn_src_gpu = seg.src_gpu;
    pack_key.pxn_proxy_gpu = seg.proxy_gpu;

    if (ctx.pxn_current_cw != nullptr &&
        !ctx.pxn_current_key.PxnEqual(pack_key)) {
        PxnSealCurrentCw(ctx);
    }
}

// slice-level: seal if slot full + ensure cw exists
void PxnRuntimeManager::PxnEnsureSlotSpace(
    PxnPackingContext &ctx, const PxnDataSeg &seg,
    const std::shared_ptr<RdmaContext> &context, Transport::TransferTask *task,
    size_t slice_length, size_t slot_size, int peer_device_id) {
    if (ctx.pxn_current_cw != nullptr &&
        ctx.pxn_current_cw->pxn_cumulative_size + slice_length > slot_size) {
        PxnSealCurrentCw(ctx);
    }

    if (ctx.pxn_current_cw == nullptr) {
        PxnPackKey pack_key;
        pack_key.pxn_context = nullptr;
        pack_key.pxn_src_gpu = seg.src_gpu;
        pack_key.pxn_proxy_gpu = seg.proxy_gpu;

        auto *cw = PxnGetCopyWorkerCache().PxnAllocate();
        cw->pxn_src_gpu_id = seg.src_gpu;
        cw->pxn_proxy_gpu_id = seg.proxy_gpu;
        cw->peer_device_id = peer_device_id;

        // First task association: refcount = 1
        cw->pxn_task_refcount.fetch_add(1, std::memory_order_relaxed);
        task->pxn_copy_worker_list.push_back(cw);

        Transport::Slice *shell = Transport::getSliceCache().allocate();
        shell->pxn_info.is_copy_worker = true;
        shell->pxn_info.pxn_copy_worker = cw;
        shell->task = task;
        shell->target_id = task->request->target_id;
        shell->status = Transport::Slice::PENDING;
        task->slice_list.push_back(shell);

        ctx.pxn_current_cw = cw;
        ctx.pxn_current_shell = shell;
        ctx.pxn_current_context = context;
        ctx.pxn_current_key = pack_key;
        ctx.pxn_current_slot_size = slot_size;
        ctx.pxn_current_task = task;
    } else {
        // Same CW is being reused across tasks: keep ownership balanced by
        // pushing the CW into every task's pxn_copy_worker_list and
        // bumping refcount, so it will only be freed when the last owning
        // task destructs.
        if (ctx.pxn_current_task != task) {
            ctx.pxn_current_cw->pxn_task_refcount.fetch_add(
                1, std::memory_order_relaxed);
            task->pxn_copy_worker_list.push_back(ctx.pxn_current_cw);
            ctx.pxn_current_task = task;
        }
    }
}

// attach rdma worker and seg range to copy worker
static inline void PxnAttachSliceToCw(PxnPackingContext &ctx,
                                      Transport::Slice *slice) {
    auto *cw = ctx.pxn_current_cw;

    uint64_t slice_src = reinterpret_cast<uint64_t>(slice->source_addr);
    size_t slice_len = slice->length;
    size_t offset_in_slot = cw->pxn_cumulative_size;

    cw->pxn_rdma_workers.push_back(slice);
    cw->pxn_offsets_in_slot.push_back(offset_in_slot);
    cw->pxn_rdma_workers_cnt++;
    cw->pxn_cumulative_size += slice_len;
    cw->pxn_refcount.fetch_add(1, std::memory_order_relaxed);
    slice->pxn_info.pxn_copy_worker = ctx.pxn_current_shell;

    if (!cw->pxn_seg_ranges.empty()) {
        auto *last = cw->pxn_seg_ranges.back();
        if (last->pxn_src_end == slice_src) {
            last->pxn_src_end = slice_src + slice_len;
            return;
        }
    }
    auto *range = PxnGetSegRangeCache().PxnAllocate();
    range->pxn_src_start = slice_src;
    range->pxn_src_end = slice_src + slice_len;
    range->pxn_offset_in_slot = offset_in_slot;
    cw->pxn_seg_ranges.push_back(range);
}

// flush copy worker
static uint64_t PxnFlushSealedCopyWorkers(PxnPackingContext &ctx, bool force) {
    uint64_t submitted_rdma = 0;
    // finish current copy worker
    if (force && ctx.pxn_current_shell != nullptr) {
        PxnSealCurrentCw(ctx);
    }

    for (auto &entry : ctx.pxn_pending_shells) {
        // all finished copy worker
        entry.first->submitPostSend(entry.second);
    }
    ctx.pxn_pending_shells.clear();

    // submitPostSend dealt rdma worker count
    submitted_rdma = ctx.pxn_fin_cw_rdma_cnt;
    ctx.pxn_fin_cw_rdma_cnt = 0;

    return submitted_rdma;
}

// according remote nic, find local same rail nic
static int PxnFindLocalRailNic(
    const std::string &peer_nic_name, const PxnTopologyManager &topo_mgr,
    const std::vector<std::shared_ptr<RdmaContext>> &context_list) {
    int local_device_id = topo_mgr.PxnHcaNameToIndex(peer_nic_name);
    if (local_device_id < 0 ||
        static_cast<size_t>(local_device_id) >= context_list.size()) {
        return -1;
    }
    if (!context_list[local_device_id] ||
        !context_list[local_device_id]->active()) {
        return -1;
    }
    return local_device_id;
}

// Check if a peer rail is available (not paused due to errors)
// Returns true if available, false if paused
bool PxnRuntimeManager::PxnIsRailAvailable(
    PxnPackingContext &ctx, const std::string &peer_nic_path,
    int local_device_id,
    const std::vector<std::shared_ptr<RdmaContext>> &context_list) {
    auto &cached =
        ctx.pxn_rail_available_cache[peer_nic_path + "@" +
                                     std::to_string(local_device_id)];
    if (cached != 0) return cached == 1;

    if (local_device_id < 0 ||
        static_cast<size_t>(local_device_id) >= context_list.size()) {
        cached = -1;
        return false;
    }
    auto &context = context_list[local_device_id];
    if (!context || !context->active()) {
        cached = -1;
        return false;
    }
    auto worker_pool = context->getWorkerPool();
    if (!worker_pool) {
        cached = -1;
        return false;
    }
    // Access WorkerPool::isRailAvailable (allowed via friend declaration)
    bool available = worker_pool->isRailAvailable(peer_nic_path);
    cached = available ? 1 : -1;
    return available;
}

static inline void PxnPushBypassRequest(
    std::vector<Transport::TransferRequest> &bypass_requests,
    const Transport::TransferRequest &basereq, uint64_t dst_start,
    uint64_t dst_end) {
    bypass_requests.push_back(basereq);
    auto &req = bypass_requests.back();

    uint64_t src_base = reinterpret_cast<uint64_t>(basereq.source);
    req.source = reinterpret_cast<void *>(src_base +
                                          (dst_start - basereq.target_offset));
    req.target_offset = dst_start;
    req.length = dst_end - dst_start;
}

// Based on the remote buffer boundary, split the remote kvcache memory.
static inline const TransferMetadata::BufferDesc *PxnFindRemoteBufBoundary(
    const TransferMetadata::SegmentDesc *peer_desc, uint64_t remaining_start,
    uint64_t remaining_end, uint64_t &seg_end) {
    seg_end = remaining_end;
    for (const auto &buf : peer_desc->buffers) {
        uint64_t buf_end = buf.addr + buf.length;
        if (remaining_start >= buf.addr && remaining_start < buf_end) {
            seg_end = std::min(remaining_end, buf_end);
            return &buf;
        }
    }
    uint64_t next_buf_start = remaining_end;
    for (const auto &buf : peer_desc->buffers) {
        if (buf.addr > remaining_start && buf.addr < next_buf_start) {
            next_buf_start = buf.addr;
        }
    }
    seg_end = std::min(remaining_end, next_buf_start);
    return nullptr;
}

// Based on the local buffer boundary, segment the local data block.
static inline const TransferMetadata::BufferDesc *PxnFindLocalBufBoundary(
    const TransferMetadata::SegmentDesc *local_desc, uint64_t src_start,
    uint64_t src_end, uint64_t &seg_end) {
    seg_end = src_end;
    for (const auto &buf : local_desc->buffers) {
        uint64_t buf_end = buf.addr + buf.length;
        if (src_start >= buf.addr && src_start < buf_end) {
            seg_end = std::min(src_end, buf_end);
            return &buf;
        }
    }
    uint64_t next_buf_start = src_end;
    for (const auto &buf : local_desc->buffers) {
        if (buf.addr > src_start && buf.addr < next_buf_start) {
            next_buf_start = buf.addr;
        }
    }
    seg_end = std::min(src_end, next_buf_start);
    return nullptr;
}

// task -> copy worker list, each copy worker with rdma worker list
int PxnRuntimeManager::PxnPlanAndPack(
    const std::vector<Transport::TransferTask *> &task_list, size_t task_index,
    size_t request_index,
    const std::vector<std::shared_ptr<RdmaContext>> &context_list,
    TransferMetadata::SegmentDesc *local_desc, size_t submit_watermark,
    PxnPackingContext &ctx) {
    uint64_t seg_slice_count = 0;
    int peer_device_id;
    auto &bypass_requests = ctx.pxn_bypass_requests;
    bypass_requests.clear();
    Transport::TransferTask *task = task_list[task_index];
    auto &request = task->request[request_index];
    if (request.length == 0 || context_list.empty()) {
        return 0;
    }

    if (request.advise_retry_cnt > 0 || ctx.cfg->without_pxn ||
        request.opcode == Transport::TransferRequest::READ) {
        bypass_requests.push_back(request);
        return 0;
    }

    uint64_t src_start_addr = reinterpret_cast<uint64_t>(request.source);
    auto &meta_source_ctx = context_list.front();
    auto &peer_desc = ctx.pxn_segment_desc_cache[request.target_id];
    if (!peer_desc) {
        peer_desc = meta_source_ctx->engine().meta()->getSegmentDescByID(
            request.target_id);
    }
    if (!peer_desc || !local_desc) {
        bypass_requests.push_back(request);
        return 0;
    }

    const size_t kBlockSize = ctx.cfg->slice_size;
    const size_t kFragmentSize = ctx.cfg->fragment_limit;
    const int kMaxRetryCount = ctx.cfg->retry_cnt;
    size_t slot_size = pxn_slot_size_cache_.load(std::memory_order_relaxed);
    if (slot_size == 0) slot_size = kPxnDefaultSlotSize;

    uint64_t remaining_start = request.target_offset;
    uint64_t remaining_end = request.target_offset + request.length;
    while (remaining_start < remaining_end) {
        // remote buffer boundary
        uint64_t remote_seg_end;
        const TransferMetadata::BufferDesc *segRemoteBuf =
            PxnFindRemoteBufBoundary(peer_desc.get(), remaining_start,
                                     remaining_end, remote_seg_end);
        // cannot find remote buffer, or remote buffer not gpu memory
        if (!segRemoteBuf || !segRemoteBuf->isGpuMem) {
            PxnPushBypassRequest(bypass_requests, request, remaining_start,
                                 remote_seg_end);
            remaining_start = remote_seg_end;
            continue;
        }

        peer_device_id =
            PxnSelectDeviceForBuffer(peer_desc.get(), *segRemoteBuf);
        if (peer_device_id < 0 ||
            static_cast<size_t>(peer_device_id) >= peer_desc->devices.size()) {
            PxnPushBypassRequest(bypass_requests, request, remaining_start,
                                 remote_seg_end);
            remaining_start = remote_seg_end;
            continue;
        }

        const std::string &peer_nic_name =
            peer_desc->devices[peer_device_id].name;
        std::string cached_peer_nic_path =
            MakeNicPath(peer_desc->name, peer_nic_name);

        int local_device_id =
            PxnFindLocalRailNic(peer_nic_name, *PxnTopoMgrRef(), context_list);
        if (local_device_id < 0) {
            PxnPushBypassRequest(bypass_requests, request, remaining_start,
                                 remote_seg_end);
            remaining_start = remote_seg_end;
            continue;
        }

        // Check if the rail is available (not paused due to errors)
        if (!PxnIsRailAvailable(ctx, cached_peer_nic_path, local_device_id,
                                context_list)) {
            PxnPushBypassRequest(bypass_requests, request, remaining_start,
                                 remote_seg_end);
            remaining_start = remote_seg_end;
            continue;
        }

        uint64_t local_cursor = remaining_start;
        while (local_cursor < remote_seg_end) {
            PxnDataSeg seg;
            uint64_t local_src_end;
            seg.src_addr =
                src_start_addr + (local_cursor - request.target_offset);
            const TransferMetadata::BufferDesc *segLocalBuf =
                PxnFindLocalBufBoundary(
                    local_desc, seg.src_addr,
                    seg.src_addr + (remote_seg_end - local_cursor),
                    local_src_end);

            seg.length = local_src_end - seg.src_addr;
            seg.dst_addr = local_cursor;
            seg.is_pxn = false;

            do {
                if (segLocalBuf == nullptr) break;
                if (!segLocalBuf->isGpuMem) break;
                int src_gpu = static_cast<int>(segLocalBuf->gpuid);
                int proxy_gpu = PxnLookupProxyGpu(src_gpu, local_device_id);
                if (proxy_gpu < 0) break;
                seg.is_pxn = true;
                seg.src_gpu = src_gpu;
                seg.proxy_gpu = proxy_gpu;
                seg.local_device_id = local_device_id;
                seg.remote_rkey = segRemoteBuf->rkey[peer_device_id];
                seg.peer_nic_path = cached_peer_nic_path;
            } while (false);

            if (!seg.is_pxn) {
                PxnPushBypassRequest(bypass_requests, request, local_cursor,
                                     (local_cursor + seg.length));
                local_cursor = seg.dst_addr + seg.length;
                continue;
            }

            auto &context = context_list[seg.local_device_id];
            uint32_t local_lkey = segLocalBuf->lkey[seg.local_device_id];
            PxnSealIfKeyChanged(ctx, seg);
            for (uint64_t off = 0; off < seg.length; off += kBlockSize) {
                bool merge_final =
                    seg.length - off <= kBlockSize + kFragmentSize;
                size_t this_len = merge_final ? seg.length - off : kBlockSize;
                Transport::Slice *slice = Transport::getSliceCache().allocate();
                if (!slice->from_cache) ctx.pxn_nr_slices++;
                slice->source_addr =
                    reinterpret_cast<char *>(seg.src_addr) + off;
                slice->length = this_len;
                slice->opcode = request.opcode;
                slice->rdma.dest_addr = seg.dst_addr + off;
                slice->rdma.dest_rkey = seg.remote_rkey;
                slice->rdma.source_lkey = local_lkey;
                slice->rdma.retry_cnt = request.advise_retry_cnt;
                slice->rdma.max_retry_cnt = kMaxRetryCount;
                slice->peer_nic_path = seg.peer_nic_path;
                slice->task = task;
                slice->target_id = request.target_id;
                slice->status = Transport::Slice::PENDING;
                slice->ts = 0;
                slice->pxn_info.is_copy_worker = false;
                slice->pxn_info.pxn_fallback_source_addr = slice->source_addr;
                slice->pxn_info.pxn_fallback_source_lkey = local_lkey;
                task->slice_list.push_back(slice);

                PxnEnsureSlotSpace(ctx, seg, context, task, this_len, slot_size,
                                   peer_device_id);
                PxnAttachSliceToCw(ctx, slice);

                task->total_bytes += this_len;
                ++seg_slice_count;

                if (ctx.pxn_nr_slices >= submit_watermark) {
                    __sync_fetch_and_add(&task->slice_count, seg_slice_count);
                    seg_slice_count = 0;
                    ctx.pxn_nr_slices -=
                        PxnFlushSealedCopyWorkers(ctx, /*force=*/false);
                }

                if (merge_final) break;
            }
            local_cursor = seg.dst_addr + seg.length;
        }

        remaining_start = remote_seg_end;
    }

    __sync_fetch_and_add(&task->slice_count, seg_slice_count);

    // Seal any CW that is still open for this task so that it does not leak
    // into the next task's packing. A CW must not be shared across tasks via
    // ctx.pxn_current_cw, because task-A's slices may complete and
    // deallocate the CW before task-B starts touching it, leaving
    // ctx.pxn_current_cw as a dangling pointer.
    // if (ctx.pxn_current_shell != nullptr) {
    //    PxnSealCurrentCw(ctx);
    //}

    // flush left copy worker only after last task
    if (task_index + 1 == task_list.size() &&
        request_index + 1 == task->request_count) {
        PxnFlushSealedCopyWorkers(ctx, /*force=*/true);
        ctx.pxn_nr_slices = 0;
    }

    return 0;
}

void PxnRuntimeManager::PxnFailPendingSlices(PxnPackingContext &ctx) {
    auto fail_cw_shell = [](Transport::Slice *shell) {
        if (shell == nullptr) return;
        auto *cw =
            static_cast<PxnCopyWorker *>(shell->pxn_info.pxn_copy_worker);
        if (cw != nullptr) {
            for (auto *rdma_raw : cw->pxn_rdma_workers) {
                auto *rdma_slice = static_cast<Transport::Slice *>(rdma_raw);
                if (rdma_slice != nullptr &&
                    rdma_slice->status == Transport::Slice::PENDING) {
                    rdma_slice->markFailed();
                }
            }
        }
        if (shell->status == Transport::Slice::PENDING) {
            __sync_fetch_and_add(&shell->task->slice_count, 1);
            shell->markFailed();
        }
    };

    for (auto &entry : ctx.pxn_pending_shells) {
        for (auto *shell : entry.second) fail_cw_shell(shell);
    }
    ctx.pxn_pending_shells.clear();
    fail_cw_shell(ctx.pxn_current_shell);
    ctx.pxn_current_shell = nullptr;
    ctx.pxn_current_cw = nullptr;
    ctx.pxn_current_task = nullptr;
}

// WorkerPool::submitPostSend call, route copy worker to worker pool thread
int PxnRuntimeManager::PxnRouteSlicesToShards(
    PxnWorkerPoolState *state, const std::vector<Transport::Slice *> &slices,
    uint64_t &submitted_slice_count) {
    if (state == nullptr) return ERR_INVALID_ARGUMENT;
    if (state->pxn_worker_thread_count <= 0) {
        LOG(ERROR) << "PXN: PxnRouteSlicesToShards state not initialized "
                      "(worker_count="
                   << state->pxn_worker_thread_count << ")";
        return ERR_INVALID_ARGUMENT;
    }

    thread_local std::vector<std::vector<void *>> per_shard;
    if (static_cast<int>(per_shard.size()) < state->pxn_worker_thread_count) {
        per_shard.resize(state->pxn_worker_thread_count);
    }
    for (int i = 0; i < state->pxn_worker_thread_count; ++i) {
        per_shard[i].clear();  // keep capacity, drop stale entries
    }

    for (auto *shell : slices) {
        if (shell == nullptr || !shell->pxn_info.is_copy_worker) {
            LOG(ERROR) << "PXN: PxnRouteSlicesToShards received a non-shell "
                          "slice (slice="
                       << shell << "); dropping";
            continue;
        }
        auto *cw =
            static_cast<PxnCopyWorker *>(shell->pxn_info.pxn_copy_worker);
        if (cw == nullptr) {
            LOG(ERROR) << "PXN: CopyWorker shell without PxnCopyWorker "
                          "(shell="
                       << shell << "); dropping";
            continue;
        }

        size_t shard_id = (static_cast<size_t>(shell->target_id) * 10007 +
                           static_cast<size_t>(cw->peer_device_id) +
                           static_cast<size_t>(cw->pxn_src_gpu_id)) %
                          static_cast<size_t>(state->pxn_worker_thread_count);

        per_shard[shard_id].push_back(shell);
        submitted_slice_count += cw->pxn_rdma_workers_cnt;
    }

    // Transfer the per-shard buckets into state->pxn_slice_queue.
    for (int shard_idx = 0; shard_idx < state->pxn_worker_thread_count;
         ++shard_idx) {
        if (per_shard[shard_idx].empty()) continue;
        state->pxn_slice_queue_lock[shard_idx].lock();
        for (auto *shell_p : per_shard[shard_idx]) {
            state->pxn_slice_queue[shard_idx].push_back(shell_p);
        }
        state->pxn_slice_queue_count[shard_idx].fetch_add(
            per_shard[shard_idx].size(), std::memory_order_relaxed);
        state->pxn_slice_queue_lock[shard_idx].unlock();
    }
    return 0;
}

void PxnRuntimeManager::PxnAssembleMemcpy(
    Transport::Slice *shell, PxnSlotPool *pool,
    std::vector<Transport::Slice *> *batch, std::vector<void *> *dsts,
    std::vector<const void *> *srcs, std::vector<size_t> *sizes) {
    auto *cw = static_cast<PxnCopyWorker *>(shell->pxn_info.pxn_copy_worker);
    if (cw == nullptr) return;
    int slot_idx = cw->pxn_slot_idx;
    char *slot_base = static_cast<char *>(pool->pxn_dev_pool) +
                      static_cast<size_t>(slot_idx) * pool->pxn_slot_size;
    for (auto *range : cw->pxn_seg_ranges) {
        dsts->push_back(slot_base + range->pxn_offset_in_slot);
        srcs->push_back(reinterpret_cast<const void *>(range->pxn_src_start));
        sizes->push_back(range->pxn_src_end - range->pxn_src_start);
    }
    batch->push_back(shell);
}

// pxn_send_threads[thread_id].pxn_per_thread_workers copy worker,
// insert slot cascade queue
void PxnRuntimeManager::PxnSubmitPendingWorkers(std::vector<void *> &queue,
                                                PxnSlotPool &pool) {
    if (queue.empty()) return;
    for (auto *shell_raw : queue) {
        auto *shell = static_cast<Transport::Slice *>(shell_raw);
        if (shell == nullptr || !shell->pxn_info.is_copy_worker) {
            continue;
        }
        auto *cw =
            static_cast<PxnCopyWorker *>(shell->pxn_info.pxn_copy_worker);
        if (cw == nullptr) continue;

        uint8_t st = cw->pxn_state.load(std::memory_order_acquire);
        if (st != PxnCopyWorker::kPending) {
            LOG(ERROR) << "PXN: PxnSubmitPendingWorkers picked cw " << cw
                       << " shell=" << shell
                       << " with unexpected state=" << (int)st
                       << " (expected kPending); skipping";
            continue;
        }

        int slot_idx =
            static_cast<int>(pool.pxn_global_step % pool.pxn_slot_count);
        auto &slot = pool.pxn_slots[slot_idx];
        cw->pxn_slot_idx = slot_idx;
        cw->pxn_slot_pool = &pool;
        pool.pxn_global_step++;

        if (slot.pxn_cascade_tail == nullptr) {
            if (slot.pxn_cascade_head != nullptr) {
                LOG(ERROR) << "PXN: attaching to slot " << slot_idx
                           << " with cascade_tail=nullptr but cascade_head="
                           << slot.pxn_cascade_head
                           << " (invariant violated); forcing head reset. cw="
                           << cw;
            }
            slot.pxn_cascade_head = shell;
            slot.pxn_cascade_tail = shell;
            pool.pxn_active_slot_count++;
            cw->pxn_state.store(PxnCopyWorker::kWaitingSlot,
                                std::memory_order_release);
        } else {
            auto *tail_shell =
                static_cast<Transport::Slice *>(slot.pxn_cascade_tail);
            auto *tail_cw = static_cast<PxnCopyWorker *>(
                tail_shell->pxn_info.pxn_copy_worker);
            tail_cw->pxn_cascade_next = shell;
            slot.pxn_cascade_tail = shell;
            cw->pxn_state.store(PxnCopyWorker::kWaitingCascade,
                                std::memory_order_release);
        }
    }
    queue.clear();
}

// pxn cqe rdma worker deal
int PxnRuntimeManager::PxnDemoteSlice(void *slice_raw) {
    auto *slice = reinterpret_cast<Transport::Slice *>(slice_raw);
    if (slice->pxn_info.pxn_fallback_source_addr == nullptr) {
        LOG(ERROR) << "PXN: PxnDemoteSlice called on slice " << slice
                   << " without pxn_fallback_source_addr; original address "
                      "cannot be restored";
        return ERR_INVALID_ARGUMENT;
    }
    slice->source_addr = slice->pxn_info.pxn_fallback_source_addr;
    slice->rdma.source_lkey = slice->pxn_info.pxn_fallback_source_lkey;
    // Sever the PXN linkage so this slice, once re-dispatched onto the
    // normal path by retry_slice_queue, is no longer treated as PXN.
    slice->pxn_info.pxn_copy_worker = nullptr;
    return 0;
}

static int PxnDispatcherProcessAllSlots(
    PxnMemcpyDispatcherRuntime &disp_rt, bool &should_flush,
    std::unordered_map<std::string, std::vector<Transport::Slice *>>
        *collective_slice_queue) {
    auto &pool = *disp_rt.pxn_slot_pool;
    auto &batch = disp_rt.pxn_reuse_batch;
    auto &dsts = disp_rt.pxn_reuse_dsts;
    auto &srcs = disp_rt.pxn_reuse_srcs;
    auto &sizes = disp_rt.pxn_reuse_sizes;
    auto &events = disp_rt.pxn_reuse_events;

    bool pool_event_added = false;
    bool contains_cascade_tail = false;
    const size_t batch_threshold = static_cast<size_t>(pool.pxn_slot_count) / 2;

    for (int slot_idx = 0; slot_idx < pool.pxn_slot_count; ++slot_idx) {
        auto &slot = pool.pxn_slots[slot_idx];
        if (slot.pxn_in_pending_batch) {
            continue;
        }
        auto *head_shell =
            static_cast<Transport::Slice *>(slot.pxn_cascade_head);
        if (head_shell == nullptr) {
            continue;
        }
        auto *head =
            static_cast<PxnCopyWorker *>(head_shell->pxn_info.pxn_copy_worker);
        if (head == nullptr) {
            continue;
        }

        uint8_t head_state = head->pxn_state.load(std::memory_order_acquire);

        if (head_state == PxnCopyWorker::kMemcpyIssued) {
            if (head->pxn_event_ptr == nullptr ||
                cudaEventQuery(*head->pxn_event_ptr) != cudaSuccess) {
                continue;
            }
            head->pxn_state.store(PxnCopyWorker::kMemcpyDone,
                                  std::memory_order_release);
            for (size_t slice_idx = 0;
                 slice_idx < head->pxn_rdma_workers.size(); ++slice_idx) {
                auto *slice = reinterpret_cast<Transport::Slice *>(
                    head->pxn_rdma_workers[slice_idx]);
                slice->source_addr = static_cast<char *>(pool.pxn_dev_pool) +
                                     static_cast<size_t>(head->pxn_slot_idx) *
                                         pool.pxn_slot_size +
                                     head->pxn_offsets_in_slot[slice_idx];
                slice->rdma.source_lkey = pool.pxn_mr->lkey;
                (*collective_slice_queue)[slice->peer_nic_path].push_back(
                    slice);
            }
            // cascade_head point to next
            void *next_shell = head->pxn_cascade_next;
            slot.pxn_cascade_head = next_shell;
            if (next_shell == nullptr) {
                slot.pxn_cascade_tail = nullptr;
                pool.pxn_active_slot_count--;
            }
            continue;
        }

        if (head_state == PxnCopyWorker::kWaitingSlot ||
            head_state == PxnCopyWorker::kReadyToCopy) {
            if (head_state == PxnCopyWorker::kWaitingSlot) {
                uint8_t expected = 0;
                if (!slot.pxn_used.compare_exchange_strong(
                        expected, 1, std::memory_order_acq_rel)) {
                    continue;
                }
            }
            int this_slot_idx = head->pxn_slot_idx;
            char *slot_base =
                static_cast<char *>(pool.pxn_dev_pool) +
                static_cast<size_t>(this_slot_idx) * pool.pxn_slot_size;
            for (auto *range : head->pxn_seg_ranges) {
                dsts.push_back(slot_base + range->pxn_offset_in_slot);
                srcs.push_back(
                    reinterpret_cast<const void *>(range->pxn_src_start));
                sizes.push_back(range->pxn_src_end - range->pxn_src_start);
            }
            batch.push_back(head_shell);
            slot.pxn_in_pending_batch = true;

            if (!pool_event_added) {
                events.push_back(
                    pool.pxn_slots[head->pxn_slot_idx].pxn_send_event);
                pool_event_added = true;
            } else {
                events.back() =
                    pool.pxn_slots[head->pxn_slot_idx].pxn_send_event;
            }

            // CW is cascade tail, need flush
            if (head->pxn_cascade_next == nullptr) {
                contains_cascade_tail = true;
            }
        }
    }

    should_flush =
        contains_cascade_tail || batch.size() >= batch_threshold ||
        (!batch.empty() &&
         batch.size() >= static_cast<size_t>(pool.pxn_active_slot_count));
    return 0;
}

void PxnRuntimeManager::PxnRunDispatcherOnce(
    PxnWorkerPoolState *state, int thread_id,
    std::unordered_map<std::string, std::vector<Transport::Slice *>>
        *collective_slice_queue) {
    if (state == nullptr) return;
    if (!state->pxn_dispatchers[thread_id]) return;

    auto &disp_rt = *state->pxn_dispatchers[thread_id];
    auto &pool = *disp_rt.pxn_slot_pool;

    thread_local int t_pxn_device = -1;
    // drain shard queues into per-dispatcher pending list
    int disp_idx = disp_rt.pxn_disp_idx;
    int dispatcher_count = state->pxn_dispatcher_count;
    for (int shard_id = disp_idx; shard_id < state->pxn_worker_thread_count;
         shard_id += dispatcher_count) {
        if (state->pxn_slice_queue_count[shard_id].load(
                std::memory_order_relaxed) == 0)
            continue;
        state->pxn_slice_queue_lock[shard_id].lock();
        for (auto *shell_raw : state->pxn_slice_queue[shard_id]) {
            disp_rt.pxn_pending_cws.push_back(shell_raw);
        }
        state->pxn_slice_queue[shard_id].clear();
        state->pxn_slice_queue_count[shard_id].store(0,
                                                     std::memory_order_relaxed);
        state->pxn_slice_queue_lock[shard_id].unlock();
    }

    if (!disp_rt.pxn_pending_cws.empty()) {
        if (t_pxn_device != pool.pxn_proxy_gpu_id) {
            cudaSetDevice(pool.pxn_proxy_gpu_id);
            t_pxn_device = pool.pxn_proxy_gpu_id;
        }
        PxnSubmitPendingWorkers(disp_rt.pxn_pending_cws, pool);
    }

    bool should_flush = false;
    auto &batch = disp_rt.pxn_reuse_batch;
    PxnDispatcherProcessAllSlots(disp_rt, should_flush, collective_slice_queue);

    if (should_flush && !batch.empty()) {
        auto &dsts = disp_rt.pxn_reuse_dsts;
        auto &srcs = disp_rt.pxn_reuse_srcs;
        auto &sizes = disp_rt.pxn_reuse_sizes;
        auto &events = disp_rt.pxn_reuse_events;
        cudaError_t err = PxnBackend()->PxnSubmitBatchMultiEvent(
            dsts, srcs, sizes, disp_rt.pxn_stream, events);
        if (err != cudaSuccess) {
            LOG(ERROR) << "PXN dispatcher: PxnSubmitBatchMultiEvent failed: "
                       << cudaGetErrorString(err);
        }
        cudaEvent_t *shared_event =
            &pool.pxn_slots[static_cast<PxnCopyWorker *>(
                                batch.back()->pxn_info.pxn_copy_worker)
                                ->pxn_slot_idx]
                 .pxn_send_event;
        for (auto *shell : batch) {
            auto *cw =
                static_cast<PxnCopyWorker *>(shell->pxn_info.pxn_copy_worker);
            if (cw == nullptr) continue;
            cw->pxn_event_ptr = shared_event;
            cw->pxn_state.store(PxnCopyWorker::kMemcpyIssued,
                                std::memory_order_release);
            // clear slot batch mark
            pool.pxn_slots[cw->pxn_slot_idx].pxn_in_pending_batch = false;
        }
        batch.clear();
        dsts.clear();
        srcs.clear();
        sizes.clear();
        events.clear();
    }
}

}  // namespace pxn
}  // namespace mooncake
