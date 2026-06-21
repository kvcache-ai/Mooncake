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

    ctx.ibgda.qp_devctxs = reinterpret_cast<mlx5gda_qp_devctx*>(qp_devctxs);
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
    uint32_t nbytes, int lane_id) {
    if (lane_id == 0) {
        uint64_t recv_raddr =
            ctx.ibgda.raddrs[dst_rank] +
            (reinterpret_cast<const char*>(recv_ptr) -
             reinterpret_cast<const char*>(ctx.p2p.local_base));
        mc_ibgda_put(ctx.ibgda, channel, dst_rank, ctx.rank, qps_per_rank,
                     send_ptr, recv_raddr, nbytes);
    }
}

// ---------------------------------------------------------------------------
// mc_signal / mc_red_add
//
// Route a signal (store) or reduction (atomic add) to dst_rank.
// sig_ptr is a local VA within the GDR buffer.
// ---------------------------------------------------------------------------

__device__ __forceinline__ uint64_t mc_signal_raddr(const CommCtx& ctx,
                                                    int dst_rank,
                                                    int* sig_ptr) {
    return ctx.ibgda.raddrs[dst_rank] +
           (reinterpret_cast<const char*>(sig_ptr) -
            reinterpret_cast<const char*>(ctx.p2p.local_base));
}

__device__ __forceinline__ uint64_t mc_signal_atomic_laddr(const CommCtx& ctx,
                                                           int* sig_ptr) {
    return ctx.ibgda.raddrs[ctx.rank] +
           (reinterpret_cast<const char*>(sig_ptr) -
            reinterpret_cast<const char*>(ctx.ibgda.remote_atomic_base)) +
           (reinterpret_cast<const char*>(ctx.ibgda.local_atomic_base) -
            reinterpret_cast<const char*>(ctx.p2p.local_base));
}

__device__ __forceinline__ int* mc_signal_write_source(const CommCtx& ctx,
                                                       int* sig_ptr) {
    return reinterpret_cast<int*>(
        reinterpret_cast<char*>(
            const_cast<void*>(ctx.ibgda.local_atomic_base)) +
        (reinterpret_cast<const char*>(sig_ptr) -
         reinterpret_cast<const char*>(ctx.ibgda.remote_atomic_base)));
}

// Signal via the historical route: local/P2P signals are release stores, while
// IBGDA signals use RDMA atomic add.
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
        uint64_t recv_raddr = mc_signal_raddr(ctx, dst_rank, sig_ptr);
        uint64_t laddr = mc_signal_atomic_laddr(ctx, sig_ptr);
        mc_ibgda_red_add(ctx.ibgda, channel, dst_rank, ctx.rank, qps_per_rank,
                         laddr, recv_raddr, val);
    }
}

// Signal via RDMA write. Use this only when the remote signal word is known to
// be single-writer for the current protocol phase. The optional completion wait
// protects the local source word from being reused before the NIC has DMA-read
// it, which is important for protocols that double-buffer their signal memory.
__device__ __forceinline__ void mc_signal_write_from(
    const CommCtx& ctx, int dst_rank, int channel, int qps_per_rank,
    int* local_sig_ptr, int* sig_ptr, int32_t val,
    bool wait_completion = true) {
    if (dst_rank == ctx.rank) {
        mc_st_release(sig_ptr, val);
        return;
    }
    if (mc_comm_p2p_available(ctx, dst_rank)) {
        mc_p2p_signal(ctx.p2p, dst_rank, sig_ptr, val);
    } else {
        uint64_t recv_raddr = mc_signal_raddr(ctx, dst_rank, sig_ptr);
        mc_st_release(local_sig_ptr, val);
        mc_fence();
        mc_ibgda_put(ctx.ibgda, channel, dst_rank, ctx.rank, qps_per_rank,
                     local_sig_ptr, recv_raddr, sizeof(int32_t),
                     wait_completion);
    }
}

__device__ __forceinline__ void mc_signal_write(const CommCtx& ctx,
                                                int dst_rank, int channel,
                                                int qps_per_rank, int* sig_ptr,
                                                int32_t val,
                                                bool wait_completion = true) {
    mc_signal_write_from(ctx, dst_rank, channel, qps_per_rank,
                         mc_signal_write_source(ctx, sig_ptr), sig_ptr, val,
                         wait_completion);
}

__device__ __forceinline__ void mc_red_add(const CommCtx& ctx, int dst_rank,
                                           int channel, int qps_per_rank,
                                           int* sig_ptr, int32_t val) {
    mc_signal(ctx, dst_rank, channel, qps_per_rank, sig_ptr, val);
}

}  // namespace device
}  // namespace mooncake
