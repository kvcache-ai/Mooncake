#pragma once

#include <tent/device/ir/device_ops.cuh>
#include <tent/device/network/ibgda/mlx5_wqe.cuh>

namespace mooncake::tent::device::ibgda {

struct IbGdaCtx {
    DeviceOps* dops;
    IbGdaQpDevCtx* channels;
    uint32_t* rkey_per_rank;
    uint64_t* raddr_per_rank;
    void* local_base;
    void* local_atomic_base;
    void* remote_atomic_base;
    uint32_t local_lkey;
    int rank;
    int num_channels_per_rank;
    int num_ranks;
};

static __device__ __forceinline__ IbGdaQpDevCtx* channel(IbGdaCtx* ctx, int ch,
                                                         int rank) {
    return ctx->channels + rank * ctx->num_channels_per_rank +
           (ch % ctx->num_channels_per_rank);
}

static __device__ __forceinline__ uint64_t remote_addr(IbGdaCtx* ctx,
                                                       const void* local_ptr,
                                                       int rank) {
    return ctx->raddr_per_rank[rank] +
           (reinterpret_cast<const char*>(local_ptr) -
            reinterpret_cast<const char*>(ctx->local_base));
}

static __device__ __forceinline__ uint64_t local_atomic_addr(IbGdaCtx* ctx,
                                                             const void* sym) {
    auto offset = reinterpret_cast<const char*>(sym) -
                  reinterpret_cast<const char*>(ctx->remote_atomic_base);
    auto local_ptr =
        reinterpret_cast<const char*>(ctx->local_atomic_base) + offset;
    return ctx->raddr_per_rank[ctx->rank] +
           (local_ptr - reinterpret_cast<const char*>(ctx->local_base));
}

// Channel lock/unlock — go through DeviceOps for portability.
// lock_channel: spin until mutex is 0, then CAS 0→1 with acquire semantics.
// unlock_channel: store 0 with release semantics.
static __device__ __forceinline__ void lock_channel(IbGdaCtx* ctx,
                                                    IbGdaQpDevCtx* qp) {
    uint32_t old;
    do {
        old = ctx->dops->atomic_cas_acquire(&qp->mutex, 0, 1);
    } while (old != 0);
}

static __device__ __forceinline__ void unlock_channel(IbGdaCtx* ctx,
                                                      IbGdaQpDevCtx* qp) {
    ctx->dops->store_release_32(&qp->mutex, 0);
}

// CQ counter polling — go through DeviceOps for portability.
// Note: DeviceOps load_acquire_32 reads a 32-bit value, but the CQ counter
// is a 16-bit big-endian field. We read the containing 32-bit word and
// extract the lower 16 bits (the wqe_counter is the first field in the CQE).
static __device__ __forceinline__ uint16_t load_cq_counter(IbGdaCtx* ctx,
                                                           IbGdaQpDevCtx* qp) {
    // The wqe_counter is a __be16 at the start of the CQE.
    // Read 32 bits via DeviceOps and extract the lower 16 bits.
    uint32_t raw = ctx->dops->load_acquire_32(&qp->cq->wqe_counter);
    return static_cast<uint16_t>(raw & 0xFFFF);
}

static __device__ __forceinline__ void poll_cq(IbGdaCtx* ctx, IbGdaQpDevCtx* qp,
                                               uint16_t expected) {
    uint16_t wq_tail = qp->wq_tail;
    while (static_cast<int16_t>(wq_tail - expected) <= 0) {
        uint16_t cq_wqe_counter_be = load_cq_counter(ctx, qp);
        uint8_t opcode = qp->cq->op_own >> 4;
        if (opcode == 0xD) {
            printf("Requester_Error: syndrome = 0x%lx\n",
                   qp->cq->timestamp >> 56);
        }
        if (!(opcode == 0x0 || opcode == 0xF)) asm("trap;");
        wq_tail = bswap16(cq_wqe_counter_be) + 1;
    }
    if (wq_tail != qp->wq_tail) qp->wq_tail = wq_tail;
}

static __device__ __forceinline__ void post_send_db(IbGdaCtx* ctx,
                                                    IbGdaQpDevCtx* qp) {
    uint32_t num_posted_wqe = static_cast<uint32_t>(qp->wq_head);
    uint32_t send_counter = bswap32(static_cast<uint32_t>(qp->wq_head));
    ctx->dops->mmio_write32(&qp->dbr->send_counter, send_counter);
    auto* last_wqe = qp->wq + ((num_posted_wqe - 1) & qp->wqeid_mask);
    ctx->dops->mmio_write64(qp->bf + qp->bf_offset,
                            *reinterpret_cast<uint64_t*>(last_wqe));
    qp->bf_offset ^= kBlueFlameSize;
}

static __device__ __forceinline__ void write_rdma_write_wqe(
    IbGdaQpDevCtx* qp, uint64_t laddr, __be32 lkey, uint64_t raddr, __be32 rkey,
    uint32_t bytes) {
    auto* wqe = reinterpret_cast<IbGdaRdmaWriteWqe*>(
        qp->wq + (qp->wq_head & qp->wqeid_mask));

    auto& ctrl_seg = wqe->ctrl;
    auto& raddr_seg = wqe->raddr;
    auto& data_seg = wqe->data;

    ctrl_seg = {};
    ctrl_seg.qpn_ds = bswap32((qp->qpn << 8) | 3);
    ctrl_seg.fm_ce_se = MLX5_WQE_CTRL_CQ_UPDATE;
    ctrl_seg.opmod_idx_opcode = bswap32(
        (static_cast<uint32_t>(qp->wq_head) << 8) | MLX5_OPCODE_RDMA_WRITE);

    raddr_seg.raddr = bswap64(raddr);
    raddr_seg.rkey = rkey;
    raddr_seg.reserved = 0;

    data_seg.byte_count = bswap32(bytes);
    data_seg.lkey = lkey;
    data_seg.addr = bswap64(laddr);

    ++qp->wq_head;
}

static __device__ __forceinline__ void write_rdma_atomic_add_wqe(
    IbGdaQpDevCtx* qp, int32_t value, uint64_t laddr, __be32 lkey,
    uint64_t raddr, __be32 rkey) {
    auto* wqe = reinterpret_cast<IbGdaRdmaAtomicWqe*>(
        qp->wq + (qp->wq_head & qp->wqeid_mask));

    auto& ctrl_seg = wqe->ctrl;
    auto& raddr_seg = wqe->raddr;
    auto& data_seg = wqe->data;

    ctrl_seg = {};
    ctrl_seg.qpn_ds = bswap32((qp->qpn << 8) | 4);
    ctrl_seg.fm_ce_se = MLX5_WQE_CTRL_CQ_UPDATE;
    ctrl_seg.opmod_idx_opcode =
        bswap32(MLX5_OPCODE_ATOMIC_MASKED_FA |
                (static_cast<uint32_t>(qp->wq_head) << 8) | 0x08000000);

    raddr_seg.raddr = bswap64(raddr);
    raddr_seg.rkey = rkey;
    raddr_seg.reserved = 0;

    auto* atomic_add =
        reinterpret_cast<mlx5_wqe_atomic_add_32_seg*>(&wqe->atomic);
    atomic_add->add_data = bswap32(static_cast<uint32_t>(value));
    atomic_add->field_boundary = 0;
    atomic_add->compare = 0;

    data_seg.byte_count = bswap32(static_cast<uint32_t>(4));
    data_seg.lkey = lkey;
    data_seg.addr = bswap64(laddr);

    ++qp->wq_head;
}

static __device__ __forceinline__ void ibgda_put(void* opaque, int ch,
                                                 void* recv, const void* send,
                                                 size_t n, int dst) {
    auto* ctx = reinterpret_cast<IbGdaCtx*>(opaque);
    auto* qp = channel(ctx, ch, dst);
    lock_channel(ctx, qp);
    write_rdma_write_wqe(qp, reinterpret_cast<uint64_t>(send),
                         bswap32(ctx->local_lkey), remote_addr(ctx, recv, dst),
                         bswap32(ctx->rkey_per_rank[dst]),
                         static_cast<uint32_t>(n));
    post_send_db(ctx, qp);
    unlock_channel(ctx, qp);
}

static __device__ __forceinline__ void ibgda_get(void*, int, const void*, void*,
                                                 size_t, int) {
    asm("trap;");
}

static __device__ __forceinline__ void ibgda_red_add(void* opaque, int ch,
                                                     void* sym, uint64_t val,
                                                     int dst) {
    auto* ctx = reinterpret_cast<IbGdaCtx*>(opaque);
    auto* qp = channel(ctx, ch, dst);
    lock_channel(ctx, qp);
    write_rdma_atomic_add_wqe(
        qp, static_cast<int32_t>(val), local_atomic_addr(ctx, sym),
        bswap32(ctx->local_lkey), remote_addr(ctx, sym, dst),
        bswap32(ctx->rkey_per_rank[dst]));
    post_send_db(ctx, qp);
    unlock_channel(ctx, qp);
}

static __device__ __forceinline__ void ibgda_signal(void* opaque, int ch,
                                                    int dst, uint64_t action) {
    auto* ctx = reinterpret_cast<IbGdaCtx*>(opaque);
    auto* sym = reinterpret_cast<void*>(ctx->remote_atomic_base);
    ibgda_red_add(ctx, ch, sym, action, dst);
}

static __device__ __forceinline__ void ibgda_wait_signal(void* opaque, void* sig,
                                                         uint64_t expected) {
    auto* ctx = reinterpret_cast<IbGdaCtx*>(opaque);
    auto expected32 = static_cast<uint32_t>(expected);
    ctx->dops->spin_wait_eq(sig, expected32);
}

static __device__ __forceinline__ void ibgda_flush(void*, int, int) {}

static __device__ __forceinline__ void ibgda_wait(void* opaque, int ch,
                                                  uint16_t expected) {
    auto* ctx = reinterpret_cast<IbGdaCtx*>(opaque);
    auto* qp = channel(ctx, ch, ctx->rank);
    lock_channel(ctx, qp);
    poll_cq(ctx, qp, expected);
    unlock_channel(ctx, qp);
}

static __device__ __forceinline__ void ibgda_barrier(void*, int) {
    asm("trap;");
}

static __device__ __forceinline__ void ibgda_put_value(void* opaque, int ch,
                                                       void* sym, uint64_t val,
                                                       int dst) {
    ibgda_red_add(opaque, ch, sym, val, dst);
}

}  // namespace mooncake::tent::device::ibgda
