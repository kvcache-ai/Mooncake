// Copyright 2024 KVCache.AI
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

#ifndef MOONCAKE_PXN_WORKER_POOL_STATE_H_
#define MOONCAKE_PXN_WORKER_POOL_STATE_H_

#include <atomic>
#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "common.h"  // TicketLock
#include "transport/rdma_transport/pxn/pxn_types.h"
#include "transport/transport.h"

namespace mooncake {
namespace pxn {

struct PxnMemcpyDispatcherRuntime {
    // exclusive slot pool + stream + all CUDA resource
    std::unique_ptr<PxnSlotPool> pxn_slot_pool;
    cudaStream_t pxn_stream = nullptr;

    // sequential index among enabled dispatchers, used for shard sharding
    int pxn_disp_idx = 0;

    // cw pending queue
    std::vector<void *> pxn_pending_cws;

    std::vector<Transport::Slice *> pxn_reuse_batch;
    std::vector<void *> pxn_reuse_dsts;
    std::vector<const void *> pxn_reuse_srcs;
    std::vector<size_t> pxn_reuse_sizes;
    std::vector<cudaEvent_t> pxn_reuse_events;
};

struct PxnWorkerPoolState {
    // Shard queues, sized to WorkerPool::worker_count_ in
    // PxnInitWorkerPoolState.
    std::vector<std::vector<void *>> pxn_slice_queue;
    std::vector<std::atomic<uint64_t>> pxn_slice_queue_count;
    std::vector<TicketLock> pxn_slice_queue_lock;

    // ===== Metadata =====
    int pxn_worker_thread_count = 0;
    int pxn_dispatcher_count = 0;
    int pxn_local_device_id = -1;
    int pxn_proxy_gpu_id = -1;

    // ===== Per-thread runtime =====
    std::vector<std::unique_ptr<PxnMemcpyDispatcherRuntime>> pxn_dispatchers;

    int pxn_numa_socket_id = -1;

    explicit PxnWorkerPoolState(int numa_socket_id = -1)
        : pxn_numa_socket_id(numa_socket_id) {}

    ~PxnWorkerPoolState();

    PxnWorkerPoolState(const PxnWorkerPoolState &) = delete;
    PxnWorkerPoolState &operator=(const PxnWorkerPoolState &) = delete;
};

}  // namespace pxn
}  // namespace mooncake

#endif  // MOONCAKE_PXN_WORKER_POOL_STATE_H_
