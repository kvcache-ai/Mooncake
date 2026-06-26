// IBGDA device context and operations.
//
// Wraps mlx5gda_qp_devctx and issues RDMA writes/atomics via
// device-side WQE construction.
//
// Some platforms cannot safely write mlx5 UAR/MMIO from device code.  Those
// platforms use the proxy-doorbell backend: device code publishes IBGDA work
// requests into a host-mapped ring, and a host proxy rings DBR/UAR/BF.
#pragma once

#include <cstdint>
#include "transport/device/device_ops.cuh"

#if !defined(MOONCAKE_EP_USE_MUSA) && !defined(MOONCAKE_EP_USE_MACA)
#include <cuda/atomic>
#endif
#include <transport/device/ibgda/mlx5gda.h>

// mlx5 32-bit atomic-add WQE segment (not in mlx5gda.h; defined here for use
// in mc_ibgda_write_rdma_atomic_add_wqe).
struct mlx5_wqe_atomic_add_32_seg {
    __be32 add_data;
    __be32 field_boundary;
    __be64 compare;
};

namespace mooncake {
namespace device {

// ---------------------------------------------------------------------------
// IbgdaContext
// ---------------------------------------------------------------------------

struct IbgdaContext {
    mlx5gda_qp_devctx* qp_devctxs;   // device ptr: [num_qps]
    const uint64_t* raddrs;          // device ptr: [num_ranks] remote GDR base
    const uint32_t* rkeys;           // device ptr: [num_ranks] remote rkey
    const void* local_atomic_base;   // local scratch base for atomic responses
    const void* remote_atomic_base;  // symmetric remote signal base
};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

__device__ __forceinline__ mlx5gda_qp_devctx* mc_ibgda_channel(
    const IbgdaContext& ctx, int channel, int dst_rank, int qps_per_rank) {
    int qp_idx = dst_rank * qps_per_rank + (channel % qps_per_rank);
    return ctx.qp_devctxs + qp_idx;
}

__device__ __forceinline__ void mc_ibgda_lock(mlx5gda_qp_devctx* qp) {
#if defined(MOONCAKE_EP_USE_MUSA) || defined(MOONCAKE_EP_USE_MACA)
    uint32_t old;
    do {
        old = atomicCAS(&qp->mutex, 0u, 1u);
    } while (old != 0);
#else
    cuda::atomic_ref<uint32_t, cuda::thread_scope_system> lock(qp->mutex);
    while (lock.exchange(1u, cuda::memory_order_acquire) != 0u);
#endif
}

__device__ __forceinline__ void mc_ibgda_unlock(mlx5gda_qp_devctx* qp) {
#if defined(MOONCAKE_EP_USE_MUSA) || defined(MOONCAKE_EP_USE_MACA)
    mc_st_release_u32(&qp->mutex, 0u);
#else
    cuda::atomic_ref<uint32_t, cuda::thread_scope_system> lock(qp->mutex);
    lock.store(0u, cuda::memory_order_release);
#endif
}

__device__ __forceinline__ bool mc_ibgda_proxy_enabled(
    const mlx5gda_qp_devctx* qp) {
    return qp->doorbell_backend == MLX5GDA_DOORBELL_PROXY &&
           qp->proxy_ring != nullptr;
}

__device__ __forceinline__ void mc_ibgda_poll_cq(mlx5gda_qp_devctx* qp,
                                                 uint16_t expect) {
    uint16_t wq_tail = qp->wq_tail;
    while (static_cast<int16_t>(wq_tail - expect) <= 0) {
        uint16_t cq_be =
            *reinterpret_cast<volatile uint16_t*>(&qp->cq->wqe_counter);
        uint8_t opcode = qp->cq->op_own >> 4;
        if (opcode == 0xD)
            printf("[EP IBGDA] Requester error: syndrome=0x%llx\n",
                   static_cast<unsigned long long>(qp->cq->timestamp >> 56));
        if (!(opcode == 0x0 || opcode == 0xF)) {
            printf("[EP IBGDA] Unexpected CQE opcode=0x%x, trapping\n", opcode);
            __trap();
        }
        wq_tail = mc_bswap16(cq_be) + 1;
    }
    if (wq_tail != qp->wq_tail) qp->wq_tail = wq_tail;
}

__device__ __forceinline__ bool mc_ibgda_proxy_publish_doorbell(
    mlx5gda_qp_devctx* qp, uint32_t first_wq_head, uint32_t wqe_count) {
    mlx5gda_proxy_ring* ring = qp->proxy_ring;
    uint64_t tail = ring->producer_tail;
    uint64_t completion =
        *const_cast<volatile uint64_t*>(&ring->completion_head);
    uint64_t capacity = static_cast<uint64_t>(ring->size_mask + 1);
    uint32_t spins = 0;
    while (tail - completion >= capacity) {
        if (++spins > 100000000u) {
            ring->error_head = tail;
            return false;
        }
        __threadfence_system();
        completion = *const_cast<volatile uint64_t*>(&ring->completion_head);
    }

    auto* slot = &ring->slots[tail & ring->size_mask];
    slot->state = MLX5GDA_PROXY_SLOT_FREE;
    slot->wqe_count = wqe_count;
    slot->wq_head = first_wq_head + wqe_count;
    slot->seq = tail;
    __threadfence_system();
    mc_st_release_u32(&slot->state, MLX5GDA_PROXY_SLOT_POSTED);
    mc_st_release_u64(&ring->producer_tail, tail + 1);
    return true;
}

__device__ __forceinline__ void mc_ibgda_post_send_db(mlx5gda_qp_devctx* qp) {
    uint32_t num_posted = static_cast<uint32_t>(qp->wq_head);
    if (mc_ibgda_proxy_enabled(qp)) {
        if (!mc_ibgda_proxy_publish_doorbell(qp, num_posted - 1, 1)) {
            printf("[EP IBGDA] proxy doorbell publish failed\n");
            __trap();
        }
        return;
    }

    // DBR write — always done (NIC polls doorbell record in GPU memory)
    mc_st_release_u32(reinterpret_cast<uint32_t*>(&qp->dbr->send_counter),
                      mc_bswap32(num_posted));
    // BF (Blue Flame) doorbell — only for direct IBGDA when the BF register is
    // mapped into GPU VA. Proxy-doorbell platforms publish a ring ticket above
    // and leave bf null in the device context.
    if (qp->bf != nullptr) {
        auto* last_wqe = qp->wq + ((num_posted - 1) & qp->wqeid_mask);
        mc_st_release_u64(reinterpret_cast<uint64_t*>(qp->bf + qp->bf_offset),
                          *reinterpret_cast<uint64_t*>(last_wqe));
        qp->bf_offset ^= MLX5GDA_BF_SIZE;
    }
}

// Issue an RDMA WRITE WQE.  laddr/raddr are device VAs; keys are big-endian.
__device__ __forceinline__ void mc_ibgda_write_rdma_write_wqe(
    mlx5gda_qp_devctx* qp, uint64_t laddr, __be32 lkey, uint64_t raddr,
    __be32 rkey, uint32_t bytes) {
    auto* wqe = reinterpret_cast<mlx5gda_rdma_write_wqe*>(
        qp->wq + (qp->wq_head & qp->wqeid_mask));

    wqe->ctrl = {};
    wqe->ctrl.qpn_ds = mc_bswap32((qp->qpn << 8) | 3);
    wqe->ctrl.fm_ce_se = MLX5_WQE_CTRL_CQ_UPDATE;
    wqe->ctrl.opmod_idx_opcode = mc_bswap32(
        (static_cast<uint32_t>(qp->wq_head) << 8) | MLX5_OPCODE_RDMA_WRITE);

    wqe->raddr.raddr = mc_bswap64(raddr);
    wqe->raddr.rkey = rkey;
    wqe->raddr.reserved = 0;

    wqe->data.byte_count = mc_bswap32(bytes);
    wqe->data.lkey = lkey;
    wqe->data.addr = mc_bswap64(laddr);

    ++qp->wq_head;
}

// Issue an RDMA ATOMIC MASKED FETCH-AND-ADD WQE (32-bit add_data).
// This matches the original CUDA IBGDA EP kernel. A regular 64-bit
// MLX5_OPCODE_ATOMIC_FA does not implement the 32-bit signal-buffer add used
// by dispatch/combine.
__device__ __forceinline__ void mc_ibgda_write_rdma_atomic_add_wqe(
    mlx5gda_qp_devctx* qp, int32_t value, uint64_t laddr, __be32 lkey,
    uint64_t raddr, __be32 rkey) {
    auto* wqe = reinterpret_cast<mlx5gda_rdma_atomic_wqe*>(
        qp->wq + (qp->wq_head & qp->wqeid_mask));

    wqe->ctrl = {};
    wqe->ctrl.qpn_ds = mc_bswap32((qp->qpn << 8) | 4);
    wqe->ctrl.fm_ce_se = MLX5_WQE_CTRL_CQ_UPDATE;
    wqe->ctrl.opmod_idx_opcode =
        mc_bswap32(MLX5_OPCODE_ATOMIC_MASKED_FA |
                   (static_cast<uint32_t>(qp->wq_head) << 8) | 0x08000000);

    wqe->raddr.raddr = mc_bswap64(raddr);
    wqe->raddr.rkey = rkey;
    wqe->raddr.reserved = 0;

    // atomic_seg: add_data (32-bit, big-endian), field_boundary=0, compare=0
    auto* atomic_seg =
        reinterpret_cast<mlx5_wqe_atomic_add_32_seg*>(&wqe->atomic);
    atomic_seg->add_data = mc_bswap32(static_cast<uint32_t>(value));
    atomic_seg->field_boundary = 0;
    atomic_seg->compare = 0;

    wqe->data.byte_count = mc_bswap32(static_cast<uint32_t>(4));
    wqe->data.lkey = lkey;
    wqe->data.addr = mc_bswap64(laddr);

    ++qp->wq_head;
}

// ---------------------------------------------------------------------------
// High-level IBGDA operations
// ---------------------------------------------------------------------------

// RDMA WRITE: send `nbytes` from `send_ptr` to `recv_ptr` on `dst_rank`.
// Must be called by lane 0 only.
__device__ __forceinline__ void mc_ibgda_put(const IbgdaContext& ctx,
                                             int channel, int dst_rank,
                                             int src_rank, int qps_per_rank,
                                             const void* send_ptr,
                                             uint64_t recv_raddr,
                                             uint32_t nbytes) {
    auto* qp = mc_ibgda_channel(ctx, channel, dst_rank, qps_per_rank);
    mc_ibgda_lock(qp);
    mc_ibgda_write_rdma_write_wqe(qp, reinterpret_cast<uint64_t>(send_ptr),
                                  mc_bswap32(ctx.rkeys[src_rank]), recv_raddr,
                                  mc_bswap32(ctx.rkeys[dst_rank]), nbytes);
    mc_ibgda_post_send_db(qp);
    mc_ibgda_unlock(qp);
}

// RDMA ATOMIC ADD: add `value` to the 32-bit word at `recv_raddr` on
// `dst_rank`. Must be called by lane 0 only.
__device__ __forceinline__ void mc_ibgda_red_add(
    const IbgdaContext& ctx, int channel, int dst_rank, int src_rank,
    int qps_per_rank,
    uint64_t laddr,       // local scratch VA for the atomic result
    uint64_t recv_raddr,  // remote VA of the signal word
    int32_t value) {
    auto* qp = mc_ibgda_channel(ctx, channel, dst_rank, qps_per_rank);
    mc_ibgda_lock(qp);
    mc_ibgda_write_rdma_atomic_add_wqe(
        qp, value, laddr, mc_bswap32(ctx.rkeys[src_rank]), recv_raddr,
        mc_bswap32(ctx.rkeys[dst_rank]));
    mc_ibgda_post_send_db(qp);
    mc_ibgda_unlock(qp);
}

}  // namespace device
}  // namespace mooncake
