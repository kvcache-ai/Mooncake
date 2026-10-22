// Copyright 2024 KVCache.AI
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

#ifndef MOONCAKE_PXN_RUNTIME_MANAGER_H_
#define MOONCAKE_PXN_RUNTIME_MANAGER_H_

#include <memory>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>
#include <atomic>
#include <thread>

#include "transport/rdma_transport/pxn/pxn_topology_manager.h"
#include "config.h"
#include "transport/rdma_transport/pxn/pxn_memcpy_backend.h"
#include "transport/rdma_transport/pxn/pxn_types.h"
#include "transport/rdma_transport/pxn/pxn_worker_pool_state.h"
#include "transport/transport.h"

namespace mooncake {

class RdmaContext;
class WorkerPool;

namespace pxn {

struct PxnDataSeg {
    // local src_gpu addr
    uint64_t src_addr = 0;
    // remote gpu addr
    uint64_t dst_addr = 0;
    size_t   length = 0;

    bool     is_pxn = false;

    // is_pxn=true fill these
    int      src_gpu = -1;
    int      proxy_gpu = -1;
    // same rail local nic group index
    int      local_device_id = -1;
    uint32_t remote_rkey = 0;
    std::string peer_nic_path;
};

// use to judge new copy worker
struct PxnPackKey {
    RdmaContext *pxn_context = nullptr;
    int pxn_src_gpu = -1;
    int pxn_proxy_gpu = -1;

    bool PxnEqual(const PxnPackKey &other) const {
        return pxn_context == other.pxn_context &&
               pxn_src_gpu == other.pxn_src_gpu &&
               pxn_proxy_gpu == other.pxn_proxy_gpu;
    }
};

constexpr size_t kPxnBypassRequestsReserve = 256;

struct PxnPackingContext {
    PxnCopyWorker *pxn_current_cw = nullptr;
    Transport::Slice *pxn_current_shell = nullptr;
    // Track which TransferTask the current CW is currently associated with.
    // If a later slice comes from a different task while the CW is still
    // open, we push the CW into that task's pxn_copy_worker_list and bump
    // pxn_task_refcount so the CW is only actually deallocated after all
    // tasks that reference it have been destructed.
    Transport::TransferTask *pxn_current_task = nullptr;
    std::shared_ptr<RdmaContext> pxn_current_context;
    PxnPackKey pxn_current_key;
    PxnWorkerPoolState *pxn_current_state = nullptr;
    size_t pxn_current_slot_size = 0;
    size_t pxn_fin_cw_rdma_cnt = 0;
    // context + copy worker
    std::unordered_map<std::shared_ptr<RdmaContext>,
                       std::vector<Transport::Slice *>> pxn_pending_shells;
    // PXN rdma worker cumulative count
    uint64_t pxn_nr_slices = 0;

    const GlobalConfig *cfg = nullptr;

    std::vector<Transport::TransferRequest> pxn_bypass_requests;


    std::unordered_map<Transport::SegmentID,
                       std::shared_ptr<TransferMetadata::SegmentDesc>>
        pxn_segment_desc_cache;


    std::unordered_map<std::string, int8_t> pxn_rail_available_cache;

    explicit PxnPackingContext(const GlobalConfig *config)
        : cfg(config) {
        pxn_bypass_requests.reserve(kPxnBypassRequestsReserve);
    }
};

class PxnRuntimeManager {
   public:
    // Builds a PXN topology from `hca_list` and constructs the runtime
    // manager. If topology build fails, the runtime manager still
    // constructs successfully but with a null topology reference;
    // downstream PXN operations will safely no-op via null checks.
    explicit PxnRuntimeManager(const std::vector<std::string>& hca_list,
                               bool IbRelaxOrderEn);
    ~PxnRuntimeManager() = default;
    PxnRuntimeManager(const PxnRuntimeManager &) = delete;
    PxnRuntimeManager &operator=(const PxnRuntimeManager &) = delete;

    // Access the topology manager captured at construction. Callers on
    // the hot path (PxnAllocContextResource, PxnInitWorkerPoolState)
    PxnTopologyManager *PxnTopoMgrRef() const { return pxn_topo_mgr_ref_.get(); }

    // get the memcpy backend (BatchAsync or PeerAsync) used by this manager.
    PxnMemcpyBackend *PxnBackend() const { return pxn_backend_.get(); }

    int PxnLookupProxyGpu(int src_gpu, int local_device_id) const;

    // given a specific buffer, select the compatible device_id.
    int PxnSelectDeviceForBuffer(
        TransferMetadata::SegmentDesc *desc,
        const TransferMetadata::BufferDesc &buffer,
        int retry_count = 0);

    // init worker pool pxn resource
    int PxnInitWorkerPoolState(PxnWorkerPoolState *state,
                               RdmaContext *context,
                               int worker_thread_count,
                               bool (*can_post_fn)(int thread_id));

    // Check if a peer rail is available (not paused due to errors)
    bool PxnIsRailAvailable(
        PxnPackingContext &ctx,
        const std::string &peer_nic_path,
        int local_device_id,
        const std::vector<std::shared_ptr<RdmaContext>> &context_list);

    // Builds the per-request bypass list into ctx.pxn_bypass_requests and
    // packs copy-worker shells for cross-rail WRITEs. Callers iterate over
    // ctx.pxn_bypass_requests after this returns.
    //
    // Return value: 0 on success, non-zero on failure.
    int PxnPlanAndPack(
        const std::vector<Transport::TransferTask *> &task_list,
        size_t task_index,
        size_t request_index,
        const std::vector<std::shared_ptr<RdmaContext>> &context_list,
        TransferMetadata::SegmentDesc *local_desc,
        size_t submit_watermark,
        PxnPackingContext &ctx);

    void PxnFailPendingSlices(PxnPackingContext &ctx);

    // route copy worker to worker pool thread
    int PxnRouteSlicesToShards(PxnWorkerPoolState *state,
                               const std::vector<Transport::Slice *> &slices,
                               uint64_t &submitted_slice_count);

    void PxnRunDispatcherOnce(
        PxnWorkerPoolState *state, int thread_id,
        std::unordered_map<std::string, std::vector<Transport::Slice *>>
            *collective_slice_queue);

    int PxnDemoteSlice(void *slice_raw);

    bool PxnIbRelaxedOrderingEnabled() const { return pxn_ib_relaxed_ordering_; }

    void PxnEnsureSlotSpace(
        PxnPackingContext &ctx,
        const PxnDataSeg &seg,
        const std::shared_ptr<RdmaContext> &context,
        Transport::TransferTask *task,
        size_t slice_length,
        size_t slot_size,
        int peer_device_id);

   private:
    // Shared ownership of the PxnTopologyManager. Uses shared_ptr (not a
    // raw back-reference) so that a rebuild in Topology::discover which
    // replaces pxn_topo_mgr_owned_ won't leave us with a dangling
    // pointer.
    std::unique_ptr<PxnTopologyManager> pxn_topo_mgr_ref_;

    bool pxn_ib_relaxed_ordering_ = false;

    // Per-manager memcpy backend. See PxnBackend() accessor above.
    std::unique_ptr<PxnMemcpyBackend> pxn_backend_;

    // cache slot_size
    std::atomic<size_t> pxn_slot_size_cache_{0};

    // Internal helpers used by the hot-path entry points.
    void PxnSubmitPendingWorkers(std::vector<void *> &queue, PxnSlotPool &pool);

    // Assemble one CopyWorker's memcpy parameters (dsts/srcs/sizes) and
    // push it into `batch`.
    void PxnAssembleMemcpy(Transport::Slice *shell,
                           PxnSlotPool *pool,
                           std::vector<Transport::Slice *> *batch,
                           std::vector<void *> *dsts,
                           std::vector<const void *> *srcs,
                           std::vector<size_t> *sizes);
};

}  // namespace pxn
}  // namespace mooncake

#endif  // MOONCAKE_PXN_RUNTIME_MANAGER_H_
