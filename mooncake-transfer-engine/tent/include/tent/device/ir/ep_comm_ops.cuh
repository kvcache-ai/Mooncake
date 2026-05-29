// Copyright 2026 KVCache.AI
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

// EpCommOps — the "top IR" of the m+n decomposition.
//
// This header defines EpCommCtx, which bundles all transport state needed by
// EP kernels, and provides __forceinline__ routing functions that dispatch
// to the right backend (P2P or IBGDA) based on per-rank availability.
//
// m = number of network backends (IBGDA, future: UCX, etc.)
// n = number of platform backends (CUDA, MUSA, etc.)
//
// DeviceOps  = bottom IR (n platform implementations)
// EpCommCtx  = top IR  (m network backends, unified routing)

#include <tent/device/ir/device_ops.cuh>
#include <tent/runtime/device_resources.h>

#ifdef MOONCAKE_EP_USE_MUSA
#include <tent/device/mtlink.cuh>
#include <tent/device/platform/musa/musa_ops.cuh>
#else
#include <tent/device/nvlink.cuh>
#include <tent/device/network/ibgda/ibgda_ops.cuh>
#include <tent/device/platform/cuda/cuda_ops.cuh>
#endif

namespace mooncake {
namespace tent {
namespace device {

struct EpCommCtx {
    DeviceOps* dops;
    void* local_base;  // symmetric memory base (mxa_buffer)

    P2PDeviceContext p2p_ctx;
#ifndef MOONCAKE_EP_USE_MUSA
    ibgda::IbGdaCtx ibgda_ctx;
#endif

    int rank;
    int num_ranks;
};

// ---------------------------------------------------------------------------
// Routing helpers
// ---------------------------------------------------------------------------

__device__ __forceinline__ bool ep_p2p_available(const EpCommCtx& ctx,
                                                  int dst_rank) {
#ifdef MOONCAKE_EP_USE_MUSA
    return mtlink::is_available(ctx.p2p_ctx, dst_rank);
#else
    return nvlink::is_available(ctx.p2p_ctx, dst_rank);
#endif
}

__device__ __forceinline__ void* ep_peer_ptr(const EpCommCtx& ctx,
                                              int dst_rank,
                                              const void* local_ptr) {
#ifdef MOONCAKE_EP_USE_MUSA
    return mtlink::peer_ptr(ctx.p2p_ctx, dst_rank, ctx.local_base, local_ptr);
#else
    return nvlink::peer_ptr(ctx.p2p_ctx, dst_rank, ctx.local_base, local_ptr);
#endif
}

// ---------------------------------------------------------------------------
// Data movement: ep_route_put
//
// Determines the routing for a remote-rank put and returns the destination
// pointer for warp-cooperative copy (UNROLLED_WARP_COPY).
//
// Returns the destination pointer to write to:
//   - local:  the original recv_ptr (caller does UNROLLED_WARP_COPY)
//   - P2P:    peer pointer (caller does UNROLLED_WARP_COPY)
//   - IBGDA:  nullptr (caller must stage data and call ep_put_ibgda)
//
// IMPORTANT: For the IBGDA path, the caller is responsible for:
//   1. Copying data to a staging buffer (if needed)
//   2. Calling ep_put_ibgda() to issue the RDMA write
// This is because the combine kernel needs to copy to a staging buffer
// before sending, while the dispatch kernel can send directly.
// ---------------------------------------------------------------------------

__device__ __forceinline__ void* ep_route_put(EpCommCtx& ctx,
                                               int dst_rank,
                                               void* recv_ptr) {
    if (dst_rank == ctx.rank) {
        return recv_ptr;  // local copy — caller does UNROLLED_WARP_COPY
    }

    if (ep_p2p_available(ctx, dst_rank)) {
        return ep_peer_ptr(ctx, dst_rank, recv_ptr);  // P2P — caller does UNROLLED_WARP_COPY
    }

    return nullptr;  // IBGDA — caller must stage + send
}

// Issue an IBGDA RDMA write. Only call this when ep_route_put returned nullptr.
__device__ __forceinline__ void ep_put_ibgda(EpCommCtx& ctx,
                                              int channel,
                                              void* recv_ptr,
                                              const void* send_ptr,
                                              size_t nbytes,
                                              int dst_rank,
                                              int lane_id = 0) {
#ifndef MOONCAKE_EP_USE_MUSA
    if (lane_id == 0) {
        ibgda::ibgda_put(&ctx.ibgda_ctx, channel, recv_ptr, send_ptr,
                         nbytes, dst_rank);
    }
#endif
}

// ---------------------------------------------------------------------------
// Signal / reduction
// ---------------------------------------------------------------------------

__device__ __forceinline__ void ep_signal(EpCommCtx& ctx,
                                           int dst_rank, int channel,
                                           void* sig_ptr, int32_t action) {
    if (dst_rank == ctx.rank) {
        // Local signal — kernel handles directly via st_na_release
        return;
    }

    if (ep_p2p_available(ctx, dst_rank)) {
#ifdef MOONCAKE_EP_USE_MUSA
        mtlink::mtlink_signal(ctx.dops, ctx.p2p_ctx, dst_rank,
                              ctx.local_base, sig_ptr, action);
#else
        nvlink::nvlink_signal(ctx.dops, ctx.p2p_ctx, dst_rank,
                              ctx.local_base, sig_ptr, action);
#endif
    } else {
#ifndef MOONCAKE_EP_USE_MUSA
        ibgda::ibgda_signal(&ctx.ibgda_ctx, channel, dst_rank,
                            static_cast<int64_t>(action));
#endif
    }
}

__device__ __forceinline__ void ep_red_add(EpCommCtx& ctx,
                                            int dst_rank, int channel,
                                            void* sym_ptr, int32_t val) {
    if (dst_rank == ctx.rank) {
        // Local — kernel handles directly via st_na_release
        return;
    }

    if (ep_p2p_available(ctx, dst_rank)) {
        // P2P path: release store (single-writer assumption)
#ifdef MOONCAKE_EP_USE_MUSA
        mtlink::mtlink_signal_add(ctx.dops, ctx.p2p_ctx, dst_rank,
                               ctx.local_base, sym_ptr, val);
#else
        nvlink::nvlink_signal_add(ctx.dops, ctx.p2p_ctx, dst_rank,
                               ctx.local_base, sym_ptr, val);
#endif
    } else {
        // IBGDA path: atomic add (RDMA needs atomic visibility)
#ifndef MOONCAKE_EP_USE_MUSA
        ibgda::ibgda_red_add(&ctx.ibgda_ctx, channel, sym_ptr,
                             static_cast<uint64_t>(val), dst_rank);
#endif
    }
}

}  // namespace device
}  // namespace tent
}  // namespace mooncake
