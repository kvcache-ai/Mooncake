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
    IbgdaContext ibgda;  // empty struct on MUSA — zero overhead
    int rank;
};

// Construct CommCtx from the raw kernel arguments.
// raddrs/rkeys/qp_devctxs may be nullptr on MUSA (ignored).
__device__ __forceinline__ CommCtx
make_comm_ctx(void* gdr_buffer, const int32_t* nvlink_available,
              void* const* ipc_peer_ptrs, void* raddrs, void* rkeys,
              void* qp_devctxs, int rank, int num_ranks, int num_qps) {
    CommCtx ctx;
    ctx.rank = rank;

    ctx.p2p.available = nvlink_available;
    ctx.p2p.peer_ptrs = ipc_peer_ptrs;
    ctx.p2p.local_base = gdr_buffer;

#ifndef MOONCAKE_EP_USE_MUSA
    ctx.ibgda.qp_devctxs = reinterpret_cast<mlx5gda_qp_devctx*>(qp_devctxs);
    ctx.ibgda.raddrs = reinterpret_cast<const uint64_t*>(raddrs);
    ctx.ibgda.rkeys = reinterpret_cast<const uint32_t*>(rkeys);
#endif

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
__device__ __forceinline__ void* mc_route_put(const CommCtx& ctx,
                                              int dst_rank, void* recv_ptr) {
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
    uint32_t nbytes, int lane_id) {
#ifndef MOONCAKE_EP_USE_MUSA
    if (lane_id == 0) {
        uint64_t recv_raddr =
            ctx.ibgda.raddrs[dst_rank] +
            (reinterpret_cast<const char*>(recv_ptr) -
             reinterpret_cast<const char*>(ctx.p2p.local_base));
        mc_ibgda_put(ctx.ibgda, channel, dst_rank, ctx.rank, qps_per_rank,
                     send_ptr, recv_raddr, nbytes);
    }
#endif
}

// ---------------------------------------------------------------------------
// mc_signal / mc_red_add
//
// Route a signal (store) or reduction (atomic add) to dst_rank.
// sig_ptr is a local VA within the GDR buffer.
// ---------------------------------------------------------------------------

__device__ __forceinline__ void mc_signal(const CommCtx& ctx, int dst_rank,
                                          int channel, int qps_per_rank,
                                          int* sig_ptr, int32_t val) {
    if (dst_rank == ctx.rank) {
        mc_st_release(sig_ptr, val);
        return;
    }
    if (mc_comm_p2p_available(ctx, dst_rank)) {
        mc_p2p_signal(ctx.p2p, dst_rank, sig_ptr, val);
    } else {
#ifndef MOONCAKE_EP_USE_MUSA
        uint64_t recv_raddr =
            ctx.ibgda.raddrs[dst_rank] +
            (reinterpret_cast<const char*>(sig_ptr) -
             reinterpret_cast<const char*>(ctx.p2p.local_base));
        uint64_t laddr = ctx.ibgda.raddrs[ctx.rank] +
                         (reinterpret_cast<const char*>(sig_ptr) -
                          reinterpret_cast<const char*>(ctx.p2p.local_base));
        mc_ibgda_red_add(ctx.ibgda, channel, dst_rank, ctx.rank, qps_per_rank,
                         laddr, recv_raddr, val);
#endif
    }
}

__device__ __forceinline__ void mc_red_add(const CommCtx& ctx, int dst_rank,
                                           int channel, int qps_per_rank,
                                           int* sig_ptr, int32_t val) {
    mc_signal(ctx, dst_rank, channel, qps_per_rank, sig_ptr, val);
}

}  // namespace device
}  // namespace mooncake
