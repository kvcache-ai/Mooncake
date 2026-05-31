// IBGDA device context and operations.
//
// On CUDA: wraps mlx5gda_qp_devctx and issues RDMA writes/atomics via
//          device-side WQE construction (same logic as the original kernel).
// On MUSA: the struct is empty and all functions are no-ops — MTLink P2P
//          handles all inter-rank communication.
//
// The kernel includes this header unconditionally; the #ifdef is here, not
// in the kernel.
#pragma once

#include <cstdint>
#include "transport/device/device_ops.cuh"

#ifndef MOONCAKE_EP_USE_MUSA
#include <mooncake_ibgda/mlx5gda.h>

// mlx5 32-bit atomic-add WQE segment (not in mlx5gda.h; defined here for use
// in mc_ibgda_write_rdma_atomic_add_wqe).
struct mlx5_wqe_atomic_add_32_seg {
    __be32 add_data;
    __be32 field_boundary;
    __be64 compare;
};
#endif

namespace mooncake {
namespace device {

// ---------------------------------------------------------------------------
// IbgdaContext
// ---------------------------------------------------------------------------

struct IbgdaContext {
#ifndef MOONCAKE_EP_USE_MUSA
    mlx5gda_qp_devctx* qp_devctxs;  // device ptr: [num_qps]
    const uint64_t* raddrs;         // device ptr: [num_ranks] remote GDR base
    const uint32_t* rkeys;          // device ptr: [num_ranks] remote rkey
#endif
};

// ---------------------------------------------------------------------------
// Helpers (CUDA only)
// ---------------------------------------------------------------------------

#ifndef MOONCAKE_EP_USE_MUSA

__device__ __forceinline__ mlx5gda_qp_devctx* mc_ibgda_channel(
    const IbgdaContext& ctx, int channel, int dst_rank, int qps_per_rank) {
    int qp_idx = dst_rank * qps_per_rank + (channel % qps_per_rank);
    return ctx.qp_devctxs + qp_idx;
}

__device__ __forceinline__ void mc_ibgda_lock(mlx5gda_qp_devctx* qp) {
    uint32_t old;
    do {
        old = atomicCAS(&qp->mutex, 0u, 1u);
    } while (old != 0);
}

__device__ __forceinline__ void mc_ibgda_unlock(mlx5gda_qp_devctx* qp) {
    mc_st_release_u32(&qp->mutex, 0u);
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
        if (!(opcode == 0x0 || opcode == 0xF)) asm("trap;");
        wq_tail = mc_bswap16(cq_be) + 1;
    }
    if (wq_tail != qp->wq_tail) qp->wq_tail = wq_tail;
}

__device__ __forceinline__ void mc_ibgda_post_send_db(mlx5gda_qp_devctx* qp) {
    uint32_t num_posted = static_cast<uint32_t>(qp->wq_head);
    mc_st_release_u32(reinterpret_cast<uint32_t*>(&qp->dbr->send_counter),
                      mc_bswap32(num_posted));
    auto* last_wqe = qp->wq + ((num_posted - 1) & qp->wqeid_mask);
    mc_st_release_u64(reinterpret_cast<uint64_t*>(qp->bf + qp->bf_offset),
                      *reinterpret_cast<uint64_t*>(last_wqe));
    qp->bf_offset ^= MLX5GDA_BF_SIZE;
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

// Issue an RDMA ATOMIC FETCH-AND-ADD WQE (32-bit add_data in big-endian).
__device__ __forceinline__ void mc_ibgda_write_rdma_atomic_add_wqe(
    mlx5gda_qp_devctx* qp, int32_t value, uint64_t laddr, __be32 lkey,
    uint64_t raddr, __be32 rkey) {
    auto* wqe = reinterpret_cast<mlx5gda_rdma_atomic_wqe*>(
        qp->wq + (qp->wq_head & qp->wqeid_mask));

    wqe->ctrl = {};
    wqe->ctrl.qpn_ds = mc_bswap32((qp->qpn << 8) | 4);
    wqe->ctrl.fm_ce_se = MLX5_WQE_CTRL_CQ_UPDATE;
    wqe->ctrl.opmod_idx_opcode = mc_bswap32(
        (static_cast<uint32_t>(qp->wq_head) << 8) | MLX5_OPCODE_ATOMIC_FA);

    wqe->raddr.raddr = mc_bswap64(raddr);
    wqe->raddr.rkey = rkey;
    wqe->raddr.reserved = 0;

    // atomic_seg: add_data (32-bit, big-endian), field_boundary=0, compare=0
    auto* atomic_seg =
        reinterpret_cast<mlx5_wqe_atomic_add_32_seg*>(&wqe->atomic);
    atomic_seg->add_data = mc_bswap32(static_cast<uint32_t>(value));
    atomic_seg->field_boundary = 0;
    atomic_seg->compare = 0;

    wqe->data.byte_count = mc_bswap32(8);  // atomic always 8 bytes
    wqe->data.lkey = lkey;
    wqe->data.addr = mc_bswap64(laddr);

    ++qp->wq_head;
}

#endif  // !MOONCAKE_EP_USE_MUSA

// ---------------------------------------------------------------------------
// High-level IBGDA operations (no-ops on MUSA)
// ---------------------------------------------------------------------------

// RDMA WRITE: send `nbytes` from `send_ptr` to `recv_ptr` on `dst_rank`.
// Must be called by lane 0 only; caller holds the QP lock.
__device__ __forceinline__ void mc_ibgda_put(const IbgdaContext& ctx,
                                             int channel, int dst_rank,
                                             int src_rank, int qps_per_rank,
                                             const void* send_ptr,
                                             uint64_t recv_raddr,
                                             uint32_t nbytes) {
#ifndef MOONCAKE_EP_USE_MUSA
    auto* qp = mc_ibgda_channel(ctx, channel, dst_rank, qps_per_rank);
    mc_ibgda_lock(qp);
    mc_ibgda_poll_cq(qp, qp->wq_head);
    mc_ibgda_write_rdma_write_wqe(qp, reinterpret_cast<uint64_t>(send_ptr),
                                  mc_bswap32(ctx.rkeys[src_rank]), recv_raddr,
                                  mc_bswap32(ctx.rkeys[dst_rank]), nbytes);
    mc_ibgda_post_send_db(qp);
    mc_ibgda_unlock(qp);
#endif
}

// RDMA ATOMIC ADD: add `value` to the 32-bit word at `recv_raddr` on
// `dst_rank`. Must be called by lane 0 only; caller holds the QP lock.
__device__ __forceinline__ void mc_ibgda_red_add(
    const IbgdaContext& ctx, int channel, int dst_rank, int src_rank,
    int qps_per_rank,
    uint64_t laddr,       // local scratch VA for the atomic result
    uint64_t recv_raddr,  // remote VA of the signal word
    int32_t value) {
#ifndef MOONCAKE_EP_USE_MUSA
    auto* qp = mc_ibgda_channel(ctx, channel, dst_rank, qps_per_rank);
    mc_ibgda_lock(qp);
    mc_ibgda_poll_cq(qp, qp->wq_head);
    mc_ibgda_write_rdma_atomic_add_wqe(qp, value, laddr,
                                       mc_bswap32(ctx.rkeys[src_rank]),
                                       recv_raddr,
                                       mc_bswap32(ctx.rkeys[dst_rank]));
    mc_ibgda_post_send_db(qp);
    mc_ibgda_unlock(qp);
#endif
}

}  // namespace device
}  // namespace mooncake
