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

#include "transport/device/device_transport.h"

#if NCCL_VERSION_CODE < 23004
#error "Mooncake NCCL DeviceTransport requires NCCL 2.30.4 or newer"
#endif

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

    __device__ __forceinline__ static size_t bufferBytes(
        const NcclDeviceContext& ctx) {
        return ctx.buffer_bytes_;
    }

    __device__ __forceinline__ static int rank(
        const NcclDeviceContext& ctx) {
        return ctx.rank_;
    }

    __device__ __forceinline__ static int numRanks(
        const NcclDeviceContext& ctx) {
        return ctx.num_ranks_;
    }

    __device__ __forceinline__ static int ginContextCount(
        const NcclDeviceContext& ctx) {
        return ctx.gin_context_count_;
    }

    __device__ __forceinline__ static int lsaBarrierCount(
        const NcclDeviceContext& ctx) {
        return ctx.lsa_barrier_count_;
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

__device__ __forceinline__ void mc_nccl_trap() {
    asm volatile("trap;");
}

__device__ __forceinline__ bool mc_nccl_pointer_in_buffer(
    const NcclDeviceContext& ctx, const void* ptr, size_t bytes = 1) {
    const uintptr_t base = reinterpret_cast<uintptr_t>(
        NcclDeviceContextAccess::localBase(ctx));
    const uintptr_t address = reinterpret_cast<uintptr_t>(ptr);
    const size_t extent = NcclDeviceContextAccess::bufferBytes(ctx);
    return address >= base && bytes <= extent &&
           address - base <= extent - bytes;
}

__device__ __forceinline__ size_t mc_nccl_pointer_offset(
    const NcclDeviceContext& ctx, const void* ptr, size_t bytes = 1) {
    if (!mc_nccl_pointer_in_buffer(ctx, ptr, bytes)) mc_nccl_trap();
    return static_cast<size_t>(
        static_cast<const char*>(ptr) -
        NcclDeviceContextAccess::localBase(ctx));
}

__device__ __forceinline__ int mc_nccl_gin_context(
    const NcclDeviceContext& ctx, unsigned int channel) {
    const int count = NcclDeviceContextAccess::ginContextCount(ctx);
    if (count <= 0) mc_nccl_trap();
    return static_cast<int>(channel % static_cast<unsigned int>(count));
}

}  // namespace detail

// Pointer and route queries are per-thread operations.
__device__ __forceinline__ bool mc_nccl_lsa_available(
    const NcclDeviceContext& ctx, int peer) {
    if (peer < 0 || peer >= detail::NcclDeviceContextAccess::numRanks(ctx))
        return false;
    const auto& comm = detail::NcclDeviceContextAccess::comm(ctx);
    return ncclTeamRankIsMember(ncclTeamLsa(comm), ncclTeamWorld(comm), peer);
}

__device__ __forceinline__ bool mc_nccl_gin_available(
    const NcclDeviceContext& ctx) {
    if (!detail::NcclDeviceContextAccess::ginEnabled(ctx)) return false;
    const auto& comm = detail::NcclDeviceContextAccess::comm(ctx);
    return comm.ginConnectionCount > 0 && comm.ginContextCount > 0;
}

__device__ __forceinline__ NcclDeviceRoute mc_nccl_route(
    const NcclDeviceContext& ctx, int peer) {
    if (peer < 0 || peer >= detail::NcclDeviceContextAccess::numRanks(ctx))
        return NcclDeviceRoute::kUnavailable;
    if (peer == detail::NcclDeviceContextAccess::rank(ctx))
        return NcclDeviceRoute::kLocal;
    if (mc_nccl_lsa_available(ctx, peer)) return NcclDeviceRoute::kLsa;
    return mc_nccl_gin_available(ctx) ? NcclDeviceRoute::kGin
                                      : NcclDeviceRoute::kUnavailable;
}

// Resolve the peer address corresponding to local_ptr. local_ptr must lie in
// the registration used to create ctx, and peer must be local or LSA reachable.
__device__ __forceinline__ void* mc_nccl_peer_ptr(
    const NcclDeviceContext& ctx, int peer, const void* local_ptr) {
    const NcclDeviceRoute route = mc_nccl_route(ctx, peer);
    if (route == NcclDeviceRoute::kLocal) return const_cast<void*>(local_ptr);
    if (route != NcclDeviceRoute::kLsa) return nullptr;

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
    if (!mc_nccl_multimem_available(ctx)) return nullptr;
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
    if (barrier_index >= static_cast<unsigned int>(
                             detail::NcclDeviceContextAccess::lsaBarrierCount(
                                 ctx))) {
        detail::mc_nccl_trap();
    }

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

__device__ __forceinline__ void mc_nccl_put(
    const NcclDeviceContext& ctx, int channel, int dst_rank,
    int qps_per_rank, const void* send_ptr, void* recv_ptr, uint32_t nbytes,
    int lane_id) {
    (void)qps_per_rank;
    if (lane_id != 0) return;
    if (dst_rank < 0 ||
        dst_rank >= detail::NcclDeviceContextAccess::numRanks(ctx) ||
        dst_rank == detail::NcclDeviceContextAccess::rank(ctx) ||
        !mc_nccl_gin_available(ctx)) {
        detail::mc_nccl_trap();
    }

    const size_t src_offset =
        detail::mc_nccl_pointer_offset(ctx, send_ptr, nbytes);
    const size_t dst_offset =
        detail::mc_nccl_pointer_offset(ctx, recv_ptr, nbytes);
    const auto& comm = detail::NcclDeviceContextAccess::comm(ctx);
    const auto window = detail::NcclDeviceContextAccess::window(ctx);
    ncclGin gin{comm, detail::mc_nccl_gin_context(
                          ctx, static_cast<unsigned int>(channel))};
    gin.put(ncclTeamWorld(comm), dst_rank, window, dst_offset, window,
            src_offset, nbytes, ncclGin_None{}, ncclGin_None{},
            ncclCoopThread{});
}

// Issue a GIN put and add completion_delta to a 64-bit destination signal only
// after this payload and preceding puts to the same peer/context have settled.
// recv_ptr and completion_ptr identify offsets in the peer's matching buffer.
__device__ __forceinline__ void mc_nccl_put_with_signal(
    const NcclDeviceContext& ctx, int channel, int dst_rank,
    int qps_per_rank, const void* send_ptr, void* recv_ptr, uint32_t nbytes,
    uint64_t* completion_ptr, uint64_t completion_delta, int lane_id) {
    (void)qps_per_rank;
    if (lane_id != 0) return;
    if (dst_rank < 0 ||
        dst_rank >= detail::NcclDeviceContextAccess::numRanks(ctx) ||
        dst_rank == detail::NcclDeviceContextAccess::rank(ctx) ||
        !mc_nccl_gin_available(ctx)) {
        detail::mc_nccl_trap();
    }

    const size_t src_offset =
        detail::mc_nccl_pointer_offset(ctx, send_ptr, nbytes);
    const size_t dst_offset =
        detail::mc_nccl_pointer_offset(ctx, recv_ptr, nbytes);
    const size_t completion_offset = detail::mc_nccl_pointer_offset(
        ctx, completion_ptr, sizeof(uint64_t));
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
    const NcclDeviceContext& ctx, int dst_rank, int channel,
    int qps_per_rank, uint64_t* signal_ptr, uint64_t value, int lane_id) {
    (void)qps_per_rank;
    if (lane_id != 0) return;

    const NcclDeviceRoute route = mc_nccl_route(ctx, dst_rank);
    if (route == NcclDeviceRoute::kLocal || route == NcclDeviceRoute::kLsa) {
        auto* target = static_cast<uint64_t*>(
            mc_nccl_peer_ptr(ctx, dst_rank, signal_ptr));
        cuda::atomic_ref<uint64_t, cuda::thread_scope_system> signal(*target);
        signal.fetch_add(value, cuda::memory_order_release);
        return;
    }
    if (route != NcclDeviceRoute::kGin) detail::mc_nccl_trap();

    const size_t signal_offset = detail::mc_nccl_pointer_offset(
        ctx, signal_ptr, sizeof(uint64_t));
    const auto& comm = detail::NcclDeviceContextAccess::comm(ctx);
    const auto window = detail::NcclDeviceContextAccess::window(ctx);
    ncclGin gin{comm, detail::mc_nccl_gin_context(
                          ctx, static_cast<unsigned int>(channel))};
    gin.signal(ncclTeamWorld(comm), dst_rank,
               ncclGin_VASignalAdd{window, signal_offset, value},
               ncclCoopThread{});
}

__device__ __forceinline__ void mc_nccl_flush(
    const NcclDeviceContext& ctx, int channel, int lane_id) {
    if (lane_id != 0 || !mc_nccl_gin_available(ctx)) return;
    const auto& comm = detail::NcclDeviceContextAccess::comm(ctx);
    ncclGin gin{comm, detail::mc_nccl_gin_context(
                          ctx, static_cast<unsigned int>(channel))};
    gin.flush(ncclCoopThread{});
}

__device__ __forceinline__ uint64_t mc_nccl_read_signal(
    const NcclDeviceContext& ctx, int channel,
    const uint64_t* signal_ptr) {
    const size_t signal_offset = detail::mc_nccl_pointer_offset(
        ctx, signal_ptr, sizeof(uint64_t));
    if (!mc_nccl_gin_available(ctx)) {
        auto& value = *const_cast<uint64_t*>(signal_ptr);
        cuda::atomic_ref<uint64_t, cuda::thread_scope_system> signal(value);
        return signal.load(cuda::memory_order_acquire);
    }

    const auto& comm = detail::NcclDeviceContextAccess::comm(ctx);
    ncclGin gin{comm, detail::mc_nccl_gin_context(
                          ctx, static_cast<unsigned int>(channel))};
    return gin.readSignal(detail::NcclDeviceContextAccess::window(ctx),
                          signal_offset);
}

// Only lane 0 waits. Callers that need a warp-wide dependency must synchronize
// after this function returns.
__device__ __forceinline__ void mc_nccl_wait_signal(
    const NcclDeviceContext& ctx, int channel, const uint64_t* signal_ptr,
    uint64_t least, int lane_id) {
    if (lane_id != 0) return;
    const size_t signal_offset = detail::mc_nccl_pointer_offset(
        ctx, signal_ptr, sizeof(uint64_t));
    if (!mc_nccl_gin_available(ctx)) {
        while (mc_nccl_read_signal(ctx, channel, signal_ptr) < least) {
        }
        return;
    }

    const auto& comm = detail::NcclDeviceContextAccess::comm(ctx);
    ncclGin gin{comm, detail::mc_nccl_gin_context(
                          ctx, static_cast<unsigned int>(channel))};
    gin.waitSignal(ncclCoopThread{},
                   detail::NcclDeviceContextAccess::window(ctx),
                   signal_offset, least);
}

}  // namespace device
}  // namespace mooncake
