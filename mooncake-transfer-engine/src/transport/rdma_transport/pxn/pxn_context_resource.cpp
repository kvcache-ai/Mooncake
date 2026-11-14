// Copyright 2024 KVCache.AI
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

#include "transport/rdma_transport/pxn/pxn_context_resource.h"
#include "transport/rdma_transport/pxn/pxn_runtime_manager.h"

#include <cuda_runtime.h>
#include <glog/logging.h>
#include <infiniband/verbs.h>

#include "transport/rdma_transport/pxn/pxn_topology_manager.h"

namespace mooncake {
namespace pxn {

void PxnAllocContextResource(ibv_pd* pd, const std::string& nic_name,
                             int worker_thread_count,
                             PxnTopologyManager* topo_mgr,
                             std::unique_ptr<PxnContextResource>* out,
                             size_t slot_size, int slot_count_per_worker,
                             bool ib_relaxed_ordering) {
    if (out == nullptr) return;
    out->reset();

    if (pd == nullptr || worker_thread_count <= 0 || slot_size == 0 ||
        slot_count_per_worker <= 0) {
        return;
    }

    // pxn topology has built
    if (topo_mgr == nullptr || !topo_mgr->PxnIsBuilt()) {
        return;
    }

    // nic name -> pxn_hca_list_ group subscript
    int nic_id = topo_mgr->PxnHcaNameToIndex(nic_name);
    if (nic_id < 0) {
        VLOG(1) << "PXN: NIC '" << nic_name
                << "' not in topology; context resource skipped";
        return;
    }

    // proxy cuda device physics id
    int proxy_gpu = topo_mgr->PxnLookupNicAffinityGpu(nic_id);
    if (proxy_gpu < 0) {
        VLOG(1) << "PXN: no affinity GPU for NIC " << nic_name
                << "; context resource skipped";
        return;
    }

    // proxy cuda device physics id/slot size/slot cnt/worker thread count
    auto res = std::unique_ptr<PxnContextResource>(new PxnContextResource());
    res->pxn_proxy_gpu_id = proxy_gpu;
    res->pxn_slot_size = slot_size;
    res->pxn_slot_count_per_worker = slot_count_per_worker;
    res->pxn_worker_thread_count = worker_thread_count;

    // switch proxy cuda device ctx
    int prev_device = -1;
    cudaGetDevice(&prev_device);
    if (cudaSetDevice(proxy_gpu) != cudaSuccess) {
        LOG(WARNING) << "PXN: cudaSetDevice(" << proxy_gpu
                     << ") failed for NIC " << nic_name;
        return;
    }

    // Enable peer access between proxy GPU and all other GPUs for NVLink D2D
    int device_count = 0;
    cudaGetDeviceCount(&device_count);
    for (int gpu = 0; gpu < device_count; ++gpu) {
        if (gpu == proxy_gpu) continue;
        int can_access = 0;
        cudaDeviceCanAccessPeer(&can_access, proxy_gpu, gpu);
        if (can_access) {
            cudaError_t pe = cudaDeviceEnablePeerAccess(gpu, 0);
            if (pe != cudaSuccess && pe != cudaErrorPeerAccessAlreadyEnabled) {
                LOG(WARNING) << "PXN: cudaDeviceEnablePeerAccess(" << gpu
                             << ") from proxy_gpu " << proxy_gpu
                             << " failed: " << cudaGetErrorString(pe);
            }
        }
    }

    // proxy cuda device,cudaMalloc whole memory
    size_t total = res->PxnTotalBytes();
    if (cudaMalloc(&res->pxn_dev_pool, total) != cudaSuccess) {
        LOG(WARNING) << "PXN: cudaMalloc " << total << " bytes on GPU "
                     << proxy_gpu << " failed for NIC " << nic_name;
        if (prev_device >= 0) cudaSetDevice(prev_device);
        return;
    }

    const int PxnBaseAccessRights = IBV_ACCESS_LOCAL_WRITE |
                                    IBV_ACCESS_REMOTE_WRITE |
                                    IBV_ACCESS_REMOTE_READ;
    int PxnAccessRights = PxnBaseAccessRights;
    if (ib_relaxed_ordering) {
        PxnAccessRights |= IBV_ACCESS_RELAXED_ORDERING;
    }
    res->pxn_mr = ibv_reg_mr(pd, res->pxn_dev_pool, total, PxnAccessRights);
    if (res->pxn_mr == nullptr) {
        LOG(WARNING) << "PXN: ibv_reg_mr failed for NIC " << nic_name
                     << " (GDR probably not enabled between NIC and GPU "
                     << proxy_gpu << ")";
        cudaFree(res->pxn_dev_pool);
        res->pxn_dev_pool = nullptr;
        if (prev_device >= 0) cudaSetDevice(prev_device);
        return;
    }
    if (prev_device >= 0) cudaSetDevice(prev_device);

    LOG(INFO) << "PXN: allocated context resource for NIC " << nic_name
              << " proxy_gpu=" << proxy_gpu << " total=" << total << " bytes"
              << " (" << slot_size << " x " << slot_count_per_worker << " x "
              << worker_thread_count << ")";
    *out = std::move(res);
}

// release rdma pxn context resource
void PxnFreeContextResource(std::unique_ptr<PxnContextResource>* out) {
    if (out == nullptr || *out == nullptr) return;
    auto& res = **out;
    int prev_device = -1;
    cudaGetDevice(&prev_device);
    if (res.pxn_proxy_gpu_id >= 0) cudaSetDevice(res.pxn_proxy_gpu_id);
    if (res.pxn_mr) ibv_dereg_mr(res.pxn_mr);
    if (res.pxn_dev_pool) cudaFree(res.pxn_dev_pool);
    if (prev_device >= 0) cudaSetDevice(prev_device);
    out->reset();
}

}  // namespace pxn
}  // namespace mooncake
