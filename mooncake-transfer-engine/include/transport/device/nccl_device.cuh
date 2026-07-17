// Copyright 2024 KVCache.AI
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

#ifndef USE_NCCL_DEVICE
#error "nccl_device.cuh requires USE_NCCL_DEVICE"
#endif

#include <cuda/atomic>
#include <nccl_device.h>

#include <cstddef>
#include <cstdint>

#include "transport/device/nccl_device_transport.h"

#if NCCL_VERSION_CODE < 23004
#error "Mooncake NCCL DeviceTransport requires NCCL 2.30.4 or newer"
#endif

// NCCL device code uses version-specific Device API definitions and layouts
// from nccl_device.h. Mooncake therefore requires these headers to exactly
// match the loaded runtime libnccl. After an NCCL upgrade, rebuild AOT kernels
// that include this header and invalidate and re-JIT cached NCCL Device API
// kernels before running.

namespace mooncake {
namespace device {
namespace detail {

struct NcclDeviceContextAccess {
    __device__ __forceinline__ static const ncclDevComm_t& comm(
        const NcclDeviceContext& ctx) {
        return *static_cast<const ncclDevComm_t*>(ctx.native_comm_);
    }

    __device__ __forceinline__ static ncclWindow_t window(
        const NcclDeviceContext& ctx) {
        return reinterpret_cast<ncclWindow_t>(
            const_cast<void*>(ctx.native_window_));
    }

    __device__ __forceinline__ static const char* localBase(
        const NcclDeviceContext& ctx) {
        return static_cast<const char*>(ctx.local_base_);
    }

    __device__ __forceinline__ static int rank(const NcclDeviceContext& ctx) {
        return ctx.rank_;
    }

    __device__ __forceinline__ static int ginContextCount(
        const NcclDeviceContext& ctx) {
        return ctx.gin_context_count_;
    }

    __device__ __forceinline__ static bool ginEnabled(
        const NcclDeviceContext& ctx) {
        return ctx.gin_enabled_;
    }

    __device__ __forceinline__ static bool lsaMultimemEnabled(
        const NcclDeviceContext& ctx) {
        return ctx.lsa_multimem_enabled_;
    }
};

__device__ __forceinline__ size_t
mc_nccl_pointer_offset(const NcclDeviceContext& ctx, const void* ptr) {
    return static_cast<size_t>(static_cast<const char*>(ptr) -
                               NcclDeviceContextAccess::localBase(ctx));
}

__device__ __forceinline__ int mc_nccl_gin_context(const NcclDeviceContext& ctx,
                                                   unsigned int channel) {
    const int count = NcclDeviceContextAccess::ginContextCount(ctx);
    return static_cast<int>(channel % static_cast<unsigned int>(count));
}

}  // namespace detail

// Device operations in this header are intentionally unchecked, matching the
// existing Mooncake and NCCL device APIs. The caller must provide a live
// context, a valid and reachable rank, the required NCCL resources, and pointer
// ranges wholly contained in the registration bound to ctx. Violating a
// precondition is undefined behavior. Hoist capability and route queries out of
// hot loops when the route is already known.

// Pointer and route queries are per-thread operations. peer must be a valid
// world rank.
__device__ __forceinline__ bool mc_nccl_lsa_available(
    const NcclDeviceContext& ctx, int peer) {
    const auto& comm = detail::NcclDeviceContextAccess::comm(ctx);
    return ncclTeamRankIsMember(ncclTeamLsa(comm), ncclTeamWorld(comm), peer);
}

__device__ __forceinline__ bool mc_nccl_gin_available(
    const NcclDeviceContext& ctx) {
    return detail::NcclDeviceContextAccess::ginEnabled(ctx);
}

__device__ __forceinline__ NcclDeviceRoute
mc_nccl_route(const NcclDeviceContext& ctx, int peer) {
    if (peer == detail::NcclDeviceContextAccess::rank(ctx))
        return NcclDeviceRoute::kLocal;
    if (mc_nccl_lsa_available(ctx, peer)) return NcclDeviceRoute::kLsa;
    return mc_nccl_gin_available(ctx) ? NcclDeviceRoute::kGin
                                      : NcclDeviceRoute::kUnavailable;
}

// Resolve the peer address corresponding to local_ptr. local_ptr must lie in
// the registration used to create ctx, and peer must be local or LSA reachable.
__device__ __forceinline__ void* mc_nccl_peer_ptr(const NcclDeviceContext& ctx,
                                                  int peer,
                                                  const void* local_ptr) {
    if (peer == detail::NcclDeviceContextAccess::rank(ctx))
        return const_cast<void*>(local_ptr);

    const size_t offset = detail::mc_nccl_pointer_offset(ctx, local_ptr);
    return ncclGetPeerPointer(detail::NcclDeviceContextAccess::window(ctx),
                              offset, peer);
}

__device__ __forceinline__ bool mc_nccl_multimem_available(
    const NcclDeviceContext& ctx) {
    return detail::NcclDeviceContextAccess::lsaMultimemEnabled(ctx);
}

// Return the LSA multicast pointer corresponding to local_ptr. The context must
// have been initialized with require_lsa_multimem=true.
__device__ __forceinline__ void* mc_nccl_multimem_ptr(
    const NcclDeviceContext& ctx, const void* local_ptr) {
    const size_t offset = detail::mc_nccl_pointer_offset(ctx, local_ptr);
    return ncclGetLsaMultimemPointer(
        detail::NcclDeviceContextAccess::window(ctx), offset,
        detail::NcclDeviceContextAccess::comm(ctx));
}

// Synchronize the local LSA team. Every thread in the CTA must call this
// convergently, and barrier_index must be below config.lsa_barrier_count.
// World/cross-LSA synchronization remains the caller's responsibility.
__device__ __forceinline__ void mc_nccl_lsa_barrier(
    const NcclDeviceContext& ctx, unsigned int barrier_index) {
    const auto& comm = detail::NcclDeviceContextAccess::comm(ctx);
    ncclLsaBarrierSession<ncclCoopCta> barrier{
        ncclCoopCta{}, comm, ncclTeamTagLsa{}, barrier_index,
        detail::NcclDeviceContextAccess::lsaMultimemEnabled(ctx)};
    barrier.sync(ncclCoopCta{}, cuda::memory_order_acq_rel);
}

// The GIN helpers below mirror Mooncake's lane-selected IBGDA call shape.
// Exactly lane 0 issues an NCCL ncclCoopThread operation; other lanes return.
// If a warp staged send_ptr, the caller must synchronize the warp before this
// call. mc_nccl_flush() makes sources issued on the context safe to reuse, but
// remote settlement requires mc_nccl_put_with_signal() and a receiver wait.
// GIN calls require GIN to be enabled, a valid nonlocal destination, and a
// nonnegative channel. Signal pointers must be naturally aligned uint64_t
// objects inside the registration.

__device__ __forceinline__ void mc_nccl_put(
    const NcclDeviceContext& ctx, int channel, int dst_rank, int qps_per_rank,
    const void* send_ptr, void* recv_ptr, uint32_t nbytes, int lane_id,
    bool rail = false, int sharing_mode = NCCL_GIN_RESOURCE_SHARING_GPU,
    int options = 0) {
    (void)qps_per_rank;
    if (lane_id != 0) return;

    const size_t src_offset = detail::mc_nccl_pointer_offset(ctx, send_ptr);
    const size_t dst_offset = detail::mc_nccl_pointer_offset(ctx, recv_ptr);
    const auto& comm = detail::NcclDeviceContextAccess::comm(ctx);
    const auto window = detail::NcclDeviceContextAccess::window(ctx);
    ncclGin gin{comm, detail::mc_nccl_gin_context(
                          ctx, static_cast<unsigned int>(channel)),
                static_cast<ncclGinResourceSharingMode>(sharing_mode)};
    const auto team = rail ? ncclTeamRail(comm) : ncclTeamWorld(comm);
    gin.put(team, dst_rank, window, dst_offset, window,
            src_offset, nbytes, ncclGin_None{}, ncclGin_None{},
            ncclCoopThread{}, ncclGin_None{}, cuda::thread_scope_thread,
            cuda::thread_scope_device, ncclGinOptFlagsDefault | options);
}

// Issue a GIN put and add completion_delta to a 64-bit destination signal only
// after this payload and preceding puts to the same peer/context have settled.
// recv_ptr and completion_ptr identify offsets in the peer's matching buffer.
__device__ __forceinline__ void mc_nccl_put_with_signal(
    const NcclDeviceContext& ctx, int channel, int dst_rank, int qps_per_rank,
    const void* send_ptr, void* recv_ptr, uint32_t nbytes,
    uint64_t* completion_ptr, uint64_t completion_delta, int lane_id) {
    (void)qps_per_rank;
    if (lane_id != 0) return;

    const size_t src_offset = detail::mc_nccl_pointer_offset(ctx, send_ptr);
    const size_t dst_offset = detail::mc_nccl_pointer_offset(ctx, recv_ptr);
    const size_t completion_offset =
        detail::mc_nccl_pointer_offset(ctx, completion_ptr);
    const auto& comm = detail::NcclDeviceContextAccess::comm(ctx);
    const auto window = detail::NcclDeviceContextAccess::window(ctx);
    ncclGin gin{comm, detail::mc_nccl_gin_context(
                          ctx, static_cast<unsigned int>(channel))};
    gin.put(ncclTeamWorld(comm), dst_rank, window, dst_offset, window,
            src_offset, nbytes,
            ncclGin_VASignalAdd{window, completion_offset, completion_delta},
            ncclGin_None{}, ncclCoopThread{});
}

// Add value to a 64-bit signal in the destination's matching buffer. This is
// explicitly increment semantics, not assignment. Local/LSA routes use a
// system-scope atomic; GIN uses a VA signal on the selected context.
__device__ __forceinline__ void mc_nccl_signal_add(
    const NcclDeviceContext& ctx, int dst_rank, int channel, int qps_per_rank,
    uint64_t* signal_ptr, uint64_t value, int lane_id, bool rail = false,
    int sharing_mode = NCCL_GIN_RESOURCE_SHARING_GPU) {
    (void)qps_per_rank;
    if (lane_id != 0) return;

    // A rail rank is indexed inside ncclTeamRail, whereas ctx.rank_ is a
    // world rank.  Comparing them would misclassify, for example, world rank
    // 1 sending to rail rank 1 as a local update and drop the cross-node
    // signal entirely.
    if (!rail && dst_rank == detail::NcclDeviceContextAccess::rank(ctx)) {
        cuda::atomic_ref<uint64_t, cuda::thread_scope_system> signal(
            *signal_ptr);
        signal.fetch_add(value, cuda::memory_order_release);
        return;
    }
    const auto& comm = detail::NcclDeviceContextAccess::comm(ctx);
    if (!rail && ncclTeamRankIsMember(ncclTeamLsa(comm), ncclTeamWorld(comm),
                             dst_rank)) {
        const size_t signal_offset =
            detail::mc_nccl_pointer_offset(ctx, signal_ptr);
        auto* target = static_cast<uint64_t*>(
            ncclGetPeerPointer(detail::NcclDeviceContextAccess::window(ctx),
                               signal_offset, dst_rank));
        cuda::atomic_ref<uint64_t, cuda::thread_scope_system> signal(*target);
        signal.fetch_add(value, cuda::memory_order_release);
        return;
    }

    const size_t signal_offset =
        detail::mc_nccl_pointer_offset(ctx, signal_ptr);
    const auto window = detail::NcclDeviceContextAccess::window(ctx);
    ncclGin gin{comm, detail::mc_nccl_gin_context(
                          ctx, static_cast<unsigned int>(channel)),
                static_cast<ncclGinResourceSharingMode>(sharing_mode)};
    const auto team = rail ? ncclTeamRail(comm) : ncclTeamWorld(comm);
    gin.signal(team, dst_rank,
               ncclGin_VASignalAdd{window, signal_offset, value},
               ncclCoopThread{}, ncclGin_None{}, cuda::thread_scope_thread,
               cuda::thread_scope_device, ncclGinOptFlagsDefault);
}

__device__ __forceinline__ void mc_nccl_flush(const NcclDeviceContext& ctx,
                                              int channel, int lane_id,
                                              int sharing_mode = NCCL_GIN_RESOURCE_SHARING_GPU) {
    if (lane_id != 0) return;
    const auto& comm = detail::NcclDeviceContextAccess::comm(ctx);
    ncclGin gin{comm, detail::mc_nccl_gin_context(
                          ctx, static_cast<unsigned int>(channel)),
                static_cast<ncclGinResourceSharingMode>(sharing_mode)};
    gin.flush(ncclCoopThread{});
}

__device__ __forceinline__ uint64_t mc_nccl_read_signal(
    const NcclDeviceContext& ctx, int channel, const uint64_t* signal_ptr,
    int sharing_mode = NCCL_GIN_RESOURCE_SHARING_GPU) {
    if (!mc_nccl_gin_available(ctx)) {
        auto& value = *const_cast<uint64_t*>(signal_ptr);
        cuda::atomic_ref<uint64_t, cuda::thread_scope_system> signal(value);
        return signal.load(cuda::memory_order_acquire);
    }

    const size_t signal_offset =
        detail::mc_nccl_pointer_offset(ctx, signal_ptr);
    const auto& comm = detail::NcclDeviceContextAccess::comm(ctx);
    ncclGin gin{comm, detail::mc_nccl_gin_context(
                          ctx, static_cast<unsigned int>(channel)),
                static_cast<ncclGinResourceSharingMode>(sharing_mode)};
    return gin.readSignal(detail::NcclDeviceContextAccess::window(ctx),
                          signal_offset);
}

// Only lane 0 waits. Callers that need a warp-wide dependency must synchronize
// after this function returns.
__device__ __forceinline__ void mc_nccl_wait_signal(
    const NcclDeviceContext& ctx, int channel, const uint64_t* signal_ptr,
    uint64_t least, int lane_id,
    int sharing_mode = NCCL_GIN_RESOURCE_SHARING_GPU) {
    if (lane_id != 0) return;
    if (!mc_nccl_gin_available(ctx)) {
        auto& value = *const_cast<uint64_t*>(signal_ptr);
        cuda::atomic_ref<uint64_t, cuda::thread_scope_system> signal(value);
        while (signal.load(cuda::memory_order_acquire) < least) {
        }
        return;
    }

    const size_t signal_offset =
        detail::mc_nccl_pointer_offset(ctx, signal_ptr);
    const auto& comm = detail::NcclDeviceContextAccess::comm(ctx);
    ncclGin gin{comm, detail::mc_nccl_gin_context(
                          ctx, static_cast<unsigned int>(channel)),
                static_cast<ncclGinResourceSharingMode>(sharing_mode)};
    gin.waitSignal(ncclCoopThread{},
                   detail::NcclDeviceContextAccess::window(ctx), signal_offset,
                   least);
}

}  // namespace device
}  // namespace mooncake
