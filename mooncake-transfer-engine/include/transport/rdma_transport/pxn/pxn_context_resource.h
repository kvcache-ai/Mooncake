// Copyright 2024 KVCache.AI
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

#ifndef MOONCAKE_PXN_CONTEXT_RESOURCE_H_
#define MOONCAKE_PXN_CONTEXT_RESOURCE_H_

#include <cuda_runtime.h>
#include <infiniband/verbs.h>

#include <cstddef>
#include <memory>
#include <string>
#include "transport/rdma_transport/pxn/pxn_types.h"

namespace mooncake {
namespace pxn {

class PxnTopologyManager;

// ============================================================
// Per-RdmaContext (per-NIC) PXN resource:
//   - one large cudaMalloc'd buffer on the proxy GPU
//   - one ibv_mr registering that buffer on the NIC's PD
//   - the affinity proxy GPU id
//   - slot geometry (size, total count)
// ============================================================
struct PxnContextResource {
    // proxy gpu physics cuda id
    int pxn_proxy_gpu_id = -1;
    // cudaMalloc whole cuda memory
    void* pxn_dev_pool = nullptr;
    // proxy gpu memory mr
    ibv_mr* pxn_mr = nullptr;

    size_t pxn_slot_size = 0;
    int pxn_slot_count_per_worker = 0;
    int pxn_worker_thread_count = 0;

    // current proxy gpu need calloc memory size
    size_t PxnTotalBytes() const {
        return pxn_slot_size * static_cast<size_t>(pxn_slot_count_per_worker) *
               static_cast<size_t>(pxn_worker_thread_count);
    }

    PxnContextResource() = default;
    PxnContextResource(const PxnContextResource&) = delete;
    PxnContextResource& operator=(const PxnContextResource&) = delete;
};

// ============================================================
// Allocation / release rdma pxn context resource
// ------------------------------------------------------------
// RdmaContext::construct() and ::deconstruct() call
// nic pxn failure scene:
//   - topo_mgr null or not build
//   - nic not in topology / have no affinity GPU
//   - cudaMalloc or ibv_reg_mr failed
// ============================================================
void PxnAllocContextResource(ibv_pd* pd, const std::string& nic_name,
                             int worker_thread_count,
                             PxnTopologyManager* topo_mgr,
                             std::unique_ptr<PxnContextResource>* out,
                             size_t slot_size = kPxnDefaultSlotSize,
                             int slot_count_per_worker = kPxnDefaultSlotCount,
                             bool ib_relaxed_ordering = false);

void PxnFreeContextResource(std::unique_ptr<PxnContextResource>* out);

}  // namespace pxn
}  // namespace mooncake

#endif  // MOONCAKE_PXN_CONTEXT_RESOURCE_H_
