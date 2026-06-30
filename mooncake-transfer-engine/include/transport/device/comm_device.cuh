// Communication device API — top-level context and routing.
//
// CommCtx bundles all transport state the kernel needs.  The kernel
// constructs one from the raw pointers passed in and calls mc_route_put /
// mc_signal / mc_red_add instead of touching transport internals directly.
#pragma once

#include "transport/device/device_ops.cuh"
#include "transport/device/p2p_device.cuh"
#include "transport/device/ibgda_device.cuh"

namespace mooncake {
namespace device {

// ---------------------------------------------------------------------------
// CommCtx
// ---------------------------------------------------------------------------

struct CommCtx {
    P2PContext p2p;
    IbgdaContext ibgda;
    int rank;
};

// Construct CommCtx from the raw kernel arguments.
// raddrs/rkeys/qp_devctxs may be nullptr on MUSA (ignored).
__device__ __forceinline__ CommCtx make_comm_ctx(
    void* gdr_buffer, const int32_t* nvlink_available,
    void* const* ipc_peer_ptrs, void* raddrs, void* rkeys, void* qp_devctxs,
    const void* rdma_send_signal_buffer, const void* rdma_recv_signal_buffer,
    int rank, int num_ranks, int num_qps) {
    CommCtx ctx;
    ctx.rank = rank;

    ctx.p2p.available = nvlink_available;
    ctx.p2p.peer_ptrs = ipc_peer_ptrs;
    ctx.p2p.local_base = gdr_buffer;

#ifdef MOONCAKE_EP_USE_MACA
    ctx.ibgda.qp_devctxs = qp_devctxs;
#else
    ctx.ibgda.qp_devctxs = reinterpret_cast<mlx5gda_qp_devctx*>(qp_devctxs);
#endif
    ctx.ibgda.raddrs = reinterpret_cast<const uint64_t*>(raddrs);
    ctx.ibgda.rkeys = reinterpret_cast<const uint32_t*>(rkeys);
    ctx.ibgda.local_atomic_base = rdma_send_signal_buffer;
    ctx.ibgda.remote_atomic_base = rdma_recv_signal_buffer;

    return ctx;
}

// ---------------------------------------------------------------------------
// Routing helpers
// ---------------------------------------------------------------------------

__device__ __forceinline__ bool mc_comm_p2p_available(const CommCtx& ctx,
                                                      int dst_rank) {
    return mc_p2p_available(ctx.p2p, dst_rank);
}

// Translate a local GDR pointer to the peer's mapped VA.
__device__ __forceinline__ void* mc_comm_peer_ptr(const CommCtx& ctx,
                                                  int dst_rank,
                                                  const void* local_ptr) {
    return mc_p2p_peer_ptr(ctx.p2p, dst_rank, local_ptr);
}

// ---------------------------------------------------------------------------
// mc_route_put
//
// Returns the destination pointer for a warp-cooperative copy:
//   - local rank:  recv_ptr itself (caller does UNROLLED_WARP_COPY)
//   - P2P rank:    peer-mapped recv_ptr (caller does UNROLLED_WARP_COPY)
//   - IBGDA rank:  nullptr (caller must stage data then call mc_rdma_put)
// ---------------------------------------------------------------------------
__device__ __forceinline__ void* mc_route_put(const CommCtx& ctx, int dst_rank,
                                              void* recv_ptr) {
    if (dst_rank == ctx.rank) return recv_ptr;
    if (mc_comm_p2p_available(ctx, dst_rank))
        return mc_comm_peer_ptr(ctx, dst_rank, recv_ptr);
    return nullptr;  // IBGDA path
}

// Issue an IBGDA RDMA WRITE.  Call only when mc_route_put returned nullptr.
// lane_id: only lane 0 issues the WQE.
__device__ __forceinline__ void mc_rdma_put(
    const CommCtx& ctx, int channel, int dst_rank, int qps_per_rank,
    const void* send_ptr,
    void* recv_ptr,  // local VA of the recv slot (for raddr computation)
    uint32_t nbytes, int lane_id, bool debug = false,
    bool poll_completion = false) {
    if (lane_id == 0) {
        uint64_t offset = reinterpret_cast<const char*>(recv_ptr) -
                          reinterpret_cast<const char*>(ctx.p2p.local_base);
        uint64_t recv_raddr =
            ctx.ibgda.raddrs[dst_rank] + offset;
        if (debug) {
            printf("MOONCAKE_PG_DEVICE_API_RDMA_PUT_ADDR rank=%d peer=%d "
                   "local_base=%p recv_ptr=%p offset=0x%llx "
                   "peer_raddr_base=0x%llx recv_raddr=0x%llx send_ptr=%p "
                   "bytes=%u\n",
                   ctx.rank, dst_rank, ctx.p2p.local_base, recv_ptr,
                   static_cast<unsigned long long>(offset),
                   static_cast<unsigned long long>(ctx.ibgda.raddrs[dst_rank]),
                   static_cast<unsigned long long>(recv_raddr), send_ptr,
                   nbytes);
        }
        mc_ibgda_put(ctx.ibgda, channel, dst_rank, ctx.rank, qps_per_rank,
                     send_ptr, recv_raddr, nbytes, debug, poll_completion);
    }
}

// ---------------------------------------------------------------------------
// mc_signal / mc_red_add
//
// Route a signal (store) or reduction (atomic add) to dst_rank.
// sig_ptr is a local VA within the GDR buffer.
// ---------------------------------------------------------------------------

__device__ __forceinline__ void mc_signal(const CommCtx& ctx, int dst_rank,
                                          int channel, int qps_per_rank,
                                          int* sig_ptr, int32_t val,
                                          bool debug = false,
                                          bool poll_completion = false) {
    if (dst_rank == ctx.rank) {
        mc_st_release(sig_ptr, val);
        return;
    }
    if (mc_comm_p2p_available(ctx, dst_rank)) {
        mc_p2p_signal(ctx.p2p, dst_rank, sig_ptr, val);
    } else {
        uint64_t signal_offset =
            reinterpret_cast<const char*>(sig_ptr) -
            reinterpret_cast<const char*>(ctx.p2p.local_base);
        uint64_t atomic_result_offset =
            (reinterpret_cast<const char*>(sig_ptr) -
             reinterpret_cast<const char*>(ctx.ibgda.remote_atomic_base)) +
            (reinterpret_cast<const char*>(ctx.ibgda.local_atomic_base) -
             reinterpret_cast<const char*>(ctx.p2p.local_base));
        uint64_t recv_raddr =
            ctx.ibgda.raddrs[dst_rank] + signal_offset;
        uint64_t laddr =
            ctx.ibgda.raddrs[ctx.rank] + atomic_result_offset;
        if (debug) {
            printf("MOONCAKE_PG_DEVICE_API_SIGNAL_ADDR rank=%d peer=%d "
                   "sig_ptr=%p local_base=%p remote_sig_base=%p "
                   "local_atomic_base=%p signal_offset=0x%llx "
                   "atomic_result_offset=0x%llx self_raddr_base=0x%llx "
                   "peer_raddr_base=0x%llx laddr=0x%llx recv_raddr=0x%llx "
                   "val=%d\n",
                   ctx.rank, dst_rank, sig_ptr, ctx.p2p.local_base,
                   ctx.ibgda.remote_atomic_base, ctx.ibgda.local_atomic_base,
                   static_cast<unsigned long long>(signal_offset),
                   static_cast<unsigned long long>(atomic_result_offset),
                   static_cast<unsigned long long>(ctx.ibgda.raddrs[ctx.rank]),
                   static_cast<unsigned long long>(ctx.ibgda.raddrs[dst_rank]),
                   static_cast<unsigned long long>(laddr),
                   static_cast<unsigned long long>(recv_raddr), val);
        }
        mc_ibgda_red_add(ctx.ibgda, channel, dst_rank, ctx.rank, qps_per_rank,
                         laddr, recv_raddr, val, debug, poll_completion);
    }
}

// Store-style signal for protocols where the signal word is an epoch/sequence,
// not an accumulated counter.  P2P uses a release store, and IBGDA uses a
// 4-byte RDMA WRITE from the local signal word to the peer's same offset.
__device__ __forceinline__ void mc_signal_write(
    const CommCtx& ctx, int dst_rank, int channel, int qps_per_rank,
    int* sig_ptr, int32_t val, bool debug = false,
    bool poll_completion = false) {
    mc_st_release(sig_ptr, val);
    if (dst_rank == ctx.rank) return;
    if (mc_comm_p2p_available(ctx, dst_rank)) {
        mc_p2p_signal(ctx.p2p, dst_rank, sig_ptr, val);
    } else {
        mc_rdma_put(ctx, channel, dst_rank, qps_per_rank, sig_ptr, sig_ptr,
                    sizeof(int32_t), 0, debug, poll_completion);
    }
}

__device__ __forceinline__ void mc_red_add(const CommCtx& ctx, int dst_rank,
                                           int channel, int qps_per_rank,
                                           int* sig_ptr, int32_t val) {
    mc_signal(ctx, dst_rank, channel, qps_per_rank, sig_ptr, val);
}

}  // namespace device
}  // namespace mooncake
