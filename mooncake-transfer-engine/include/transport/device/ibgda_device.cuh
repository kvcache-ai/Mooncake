// IBGDA device context and operations.
//
// Wraps mlx5gda_qp_devctx and issues RDMA writes/atomics via
// device-side WQE construction.
//
// On MUSA: BF (Blue Flame) doorbell is not available (musaHostRegisterIoMemory
// doesn't support MMIO), so qp->bf is NULL and the kernel uses DBR-only mode.
// All other IBGDA logic (WQE construction, CQ polling, DBR write) is shared.
#pragma once

#include <cstdint>
#include "transport/device/device_ops.cuh"

#ifdef MOONCAKE_EP_USE_MACA

namespace mooncake {
namespace device {

struct IbgdaContext {
    void* qp_devctxs;
    const uint64_t* raddrs;
    const uint32_t* rkeys;
    const void* local_atomic_base;
    const void* remote_atomic_base;
};

__device__ __forceinline__ void mc_ibgda_put(const IbgdaContext&, int, int, int,
                                             int, const void*, uint64_t,
                                             uint32_t) {}

__device__ __forceinline__ void mc_ibgda_put_defer_db(const IbgdaContext&, int,
                                                      int, int, int,
                                                      const void*, uint64_t,
                                                      uint32_t) {}

__device__ __forceinline__ void mc_ibgda_red_add(const IbgdaContext&, int, int,
                                                 int, int, uint64_t, uint64_t,
                                                 int32_t) {}

}  // namespace device
}  // namespace mooncake

#else  // !MOONCAKE_EP_USE_MACA

#ifndef MOONCAKE_EP_USE_MUSA
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
#ifdef MOONCAKE_EP_USE_MUSA
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
#ifdef MOONCAKE_EP_USE_MUSA
    mc_st_release_u32(&qp->mutex, 0u);
#else
    cuda::atomic_ref<uint32_t, cuda::thread_scope_system> lock(qp->mutex);
    lock.store(0u, cuda::memory_order_release);
#endif
}

__device__ __forceinline__ void mc_ibgda_poll_cq(mlx5gda_qp_devctx* qp,
                                                 uint16_t expect) {
    uint16_t wq_tail = qp->wq_tail;
    while (static_cast<int16_t>(wq_tail - expect) <= 0) {
        uint16_t cq_be =
            *reinterpret_cast<volatile uint16_t*>(&qp->cq->wqe_counter);
        uint8_t opcode = qp->cq->op_own >> 4;
        if (opcode == 0xD)
            printf("[EP IBGDA] Requester error: syndrome=0x%lx\n",
                   qp->cq->timestamp >> 56);
        if (!(opcode == 0x0 || opcode == 0xF)) {
            printf("[EP IBGDA] Unexpected CQE opcode=0x%x, trapping\n", opcode);
            __trap();
        }
        wq_tail = mc_bswap16(cq_be) + 1;
    }
    if (wq_tail != qp->wq_tail) qp->wq_tail = wq_tail;
}

__device__ __forceinline__ uint32_t
mc_ibgda_reserve_wqe(mlx5gda_qp_devctx* qp) {
    return atomicAdd(&qp->wq_head_atomic, 1u);
}

__device__ __forceinline__ void mc_ibgda_mark_wqe_ready(mlx5gda_qp_devctx* qp,
                                                        uint32_t slot) {
    mc_st_release_u32(qp->wq_ready + (slot & qp->wqeid_mask), slot + 1);
}

__device__ __forceinline__ bool mc_ibgda_wqe_ready(mlx5gda_qp_devctx* qp,
                                                   uint32_t slot) {
    auto* ready_ptr =
        reinterpret_cast<const int*>(qp->wq_ready + (slot & qp->wqeid_mask));
    return static_cast<uint32_t>(mc_ld_acquire(ready_ptr)) == slot + 1;
}

__device__ __forceinline__ void mc_ibgda_post_send_db(mlx5gda_qp_devctx* qp,
                                                      uint32_t num_posted) {
    if (num_posted == 0) return;
    mc_st_release_u32(reinterpret_cast<uint32_t*>(&qp->dbr->send_counter),
                      mc_bswap32(num_posted));
    if (qp->bf != nullptr) {
        auto* last_wqe = qp->wq + ((num_posted - 1) & qp->wqeid_mask);
        mc_st_release_u64(reinterpret_cast<uint64_t*>(qp->bf + qp->bf_offset),
                          *reinterpret_cast<uint64_t*>(last_wqe));
        qp->bf_offset ^= MLX5GDA_BF_SIZE;
    }
}

__device__ __forceinline__ void mc_ibgda_flush_ready_wqes(
    mlx5gda_qp_devctx* qp) {
    mc_ibgda_lock(qp);
    uint32_t head = qp->db_head;
    const uint32_t reserved = qp->wq_head_atomic;
    while (head < reserved && mc_ibgda_wqe_ready(qp, head)) ++head;
    if (head != qp->db_head) {
        qp->db_head = head;
        qp->wq_head = static_cast<uint16_t>(head);
        mc_ibgda_post_send_db(qp, head);
    }
    mc_ibgda_unlock(qp);
}

// Issue an RDMA WRITE WQE.  laddr/raddr are device VAs; keys are big-endian.
__device__ __forceinline__ void mc_ibgda_write_rdma_write_wqe(
    mlx5gda_qp_devctx* qp, uint32_t slot, uint64_t laddr, __be32 lkey,
    uint64_t raddr, __be32 rkey, uint32_t bytes) {
    auto* wqe = reinterpret_cast<mlx5gda_rdma_write_wqe*>(
        qp->wq + (slot & qp->wqeid_mask));
    const uint32_t wqe_counter = slot & 0xffffu;

    wqe->ctrl = {};
    wqe->ctrl.qpn_ds = mc_bswap32((qp->qpn << 8) | 3);
    // Debug: data WQEs request CQEs sparsely to estimate CQE overhead.
    wqe->ctrl.fm_ce_se = ((slot & 0x3f) == 0x3f) ? MLX5_WQE_CTRL_CQ_UPDATE : 0;
    wqe->ctrl.opmod_idx_opcode =
        mc_bswap32((wqe_counter << 8) | MLX5_OPCODE_RDMA_WRITE);

    wqe->raddr.raddr = mc_bswap64(raddr);
    wqe->raddr.rkey = rkey;
    wqe->raddr.reserved = 0;

    wqe->data.byte_count = mc_bswap32(bytes);
    wqe->data.lkey = lkey;
    wqe->data.addr = mc_bswap64(laddr);
}

// Issue an RDMA ATOMIC MASKED FETCH-AND-ADD WQE (32-bit add_data).
// This matches the original CUDA IBGDA EP kernel. A regular 64-bit
// MLX5_OPCODE_ATOMIC_FA does not implement the 32-bit signal-buffer add used
// by dispatch/combine.
__device__ __forceinline__ void mc_ibgda_write_rdma_atomic_add_wqe(
    mlx5gda_qp_devctx* qp, uint32_t slot, int32_t value, uint64_t laddr,
    __be32 lkey, uint64_t raddr, __be32 rkey) {
    auto* wqe = reinterpret_cast<mlx5gda_rdma_atomic_wqe*>(
        qp->wq + (slot & qp->wqeid_mask));
    const uint32_t wqe_counter = slot & 0xffffu;

    wqe->ctrl = {};
    wqe->ctrl.qpn_ds = mc_bswap32((qp->qpn << 8) | 4);
    wqe->ctrl.fm_ce_se = MLX5_WQE_CTRL_CQ_UPDATE;
    wqe->ctrl.opmod_idx_opcode = mc_bswap32(MLX5_OPCODE_ATOMIC_MASKED_FA |
                                            (wqe_counter << 8) | 0x08000000);

    wqe->raddr.raddr = mc_bswap64(raddr);
    wqe->raddr.rkey = rkey;
    wqe->raddr.reserved = 0;

    auto* atomic_seg =
        reinterpret_cast<mlx5_wqe_atomic_add_32_seg*>(&wqe->atomic);
    atomic_seg->add_data = mc_bswap32(static_cast<uint32_t>(value));
    atomic_seg->field_boundary = 0;
    atomic_seg->compare = 0;

    wqe->data.byte_count = mc_bswap32(static_cast<uint32_t>(4));
    wqe->data.lkey = lkey;
    wqe->data.addr = mc_bswap64(laddr);
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
    uint32_t slot = mc_ibgda_reserve_wqe(qp);
    mc_ibgda_write_rdma_write_wqe(qp, slot,
                                  reinterpret_cast<uint64_t>(send_ptr),
                                  mc_bswap32(ctx.rkeys[src_rank]), recv_raddr,
                                  mc_bswap32(ctx.rkeys[dst_rank]), nbytes);
    mc_ibgda_mark_wqe_ready(qp, slot);
    mc_ibgda_flush_ready_wqes(qp);
}

__device__ __forceinline__ void mc_ibgda_put_defer_db(
    const IbgdaContext& ctx, int channel, int dst_rank, int src_rank,
    int qps_per_rank, const void* send_ptr, uint64_t recv_raddr,
    uint32_t nbytes) {
    auto* qp = mc_ibgda_channel(ctx, channel, dst_rank, qps_per_rank);
    uint32_t slot = mc_ibgda_reserve_wqe(qp);
    mc_ibgda_write_rdma_write_wqe(qp, slot,
                                  reinterpret_cast<uint64_t>(send_ptr),
                                  mc_bswap32(ctx.rkeys[src_rank]), recv_raddr,
                                  mc_bswap32(ctx.rkeys[dst_rank]), nbytes);
    mc_ibgda_mark_wqe_ready(qp, slot);
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
    uint32_t slot = mc_ibgda_reserve_wqe(qp);
    mc_ibgda_write_rdma_atomic_add_wqe(
        qp, slot, value, laddr, mc_bswap32(ctx.rkeys[src_rank]), recv_raddr,
        mc_bswap32(ctx.rkeys[dst_rank]));
    mc_ibgda_mark_wqe_ready(qp, slot);
    mc_ibgda_flush_ready_wqes(qp);
}

}  // namespace device
}  // namespace mooncake

#endif  // MOONCAKE_EP_USE_MACA
