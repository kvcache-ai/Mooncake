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

#ifndef USE_NCCL_DEVICE
#error "nccl_device.cuh requires USE_NCCL_DEVICE"
#endif

#include <cuda/atomic>
#include <nccl_device.h>

#include <cstddef>
#include <cstdint>
#include <type_traits>

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

enum class NcclGinTeam : uint8_t {
    kWorld = 0,
    kRail,
};

// Values intentionally match Mooncake EP's backend-neutral sharing-mode
// contract, not NCCL's enum values. Keep this to the GPU and CTA modes that
// exist in every supported NCCL release; EP does not require thread mode.
enum class NcclGinResourceSharing : uint8_t {
    kCta = 0,
    kGpu = 1,
};

namespace detail {

struct NcclDeviceContextAccess {
    __device__ __forceinline__ static const ncclDevComm_t& comm(
        const NcclDeviceContext& ctx) {
        static_assert(
            sizeof(ncclDevComm_t) <= NcclDeviceContext::kNativeCommCapacity,
            "Increase NcclDeviceContext communicator capacity");
        static_assert(
            alignof(ncclDevComm_t) <= NcclDeviceContext::kNativeCommAlignment,
            "Increase NcclDeviceContext communicator alignment");
        return *reinterpret_cast<const ncclDevComm_t*>(
            ctx.native_comm_storage_);
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

    __device__ __forceinline__ static bool ginConnectionsRailed(
        const NcclDeviceContext& ctx) {
        return ctx.gin_connections_railed_;
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

template <NcclGinTeam Team>
__device__ __forceinline__ ncclTeam
mc_nccl_gin_team(const ncclDevComm_t& comm) {
    if constexpr (Team == NcclGinTeam::kRail) {
        return ncclTeamRail(comm);
    } else {
        return ncclTeamWorld(comm);
    }
}

__device__ __forceinline__ ncclGinResourceSharingMode
mc_nccl_gin_sharing_mode(NcclGinResourceSharing mode) {
    switch (mode) {
        case NcclGinResourceSharing::kCta:
            return NCCL_GIN_RESOURCE_SHARING_CTA;
        case NcclGinResourceSharing::kGpu:
        default:
            return NCCL_GIN_RESOURCE_SHARING_GPU;
    }
}

#if NCCL_VERSION_CODE >= NCCL_VERSION(2, 30, 5)
using NcclStrongVaSignalAdd = ncclGin_StrongVASignalAdd;
#else
// NCCL 2.30.4 exposes only the legacy spelling, whose semantics are strong.
using NcclStrongVaSignalAdd = ncclGin_VASignalAdd;
#endif

}  // namespace detail

// Device operations in this header are intentionally unchecked, matching the
// existing Mooncake and NCCL device APIs. The caller must provide a live
// context, a valid and reachable rank, the required NCCL resources, and pointer
// ranges wholly contained in the registration bound to ctx. Violating a
// precondition is undefined behavior. Hoist capability and route queries out of
// hot loops when the route is already known.

// Pre-bound GIN state for hot device loops. Construct this once per issuing
// thread/warp and reuse it for every operation on the same logical channel.
// Unlike the lane-selected compatibility helpers below, these methods do not
// check a lane id: the caller must elect exactly the threads required by the
// selected NCCL cooperative operation.
class NcclGinHandle {
   public:
    __device__ __forceinline__
    NcclGinHandle(const NcclDeviceContext& ctx, unsigned int channel,
                  NcclGinResourceSharing sharing = NcclGinResourceSharing::kGpu)
        : comm_(detail::NcclDeviceContextAccess::comm(ctx)),
          gin_(comm_, detail::mc_nccl_gin_context(ctx, channel),
               detail::mc_nccl_gin_sharing_mode(sharing)),
          window_(detail::NcclDeviceContextAccess::window(ctx)),
          local_base_(detail::NcclDeviceContextAccess::localBase(ctx)),
          team_world_(ncclTeamWorld(comm_)),
          team_lsa_(ncclTeamLsa(comm_)),
          team_rail_(ncclTeamRail(comm_)) {}

    __device__ __forceinline__ int worldRank() const {
        return team_world_.rank;
    }

    __device__ __forceinline__ int lsaRank() const { return team_lsa_.rank; }

    __device__ __forceinline__ int railRank() const { return team_rail_.rank; }

    __device__ __forceinline__ bool worldRankInLsa(int peer) const {
        const int first = team_rail_.rank * team_lsa_.nRanks;
        return first <= peer && peer < first + team_lsa_.nRanks;
    }

    __device__ __forceinline__ int contextCount() const {
        return comm_.ginContextCount;
    }

    __device__ __forceinline__ size_t pointerOffset(const void* ptr) const {
        return static_cast<size_t>(static_cast<const char*>(ptr) - local_base_);
    }

    __device__ __forceinline__ void* worldPeerPointer(int peer,
                                                      const void* ptr) const {
        if (peer == team_world_.rank) return const_cast<void*>(ptr);
        return ncclGetPeerPointer(window_, pointerOffset(ptr), peer);
    }

    __device__ __forceinline__ void* lsaPeerPointer(int peer,
                                                    const void* ptr) const {
        if (peer == team_lsa_.rank) return const_cast<void*>(ptr);
        return ncclGetLsaPointer(window_, pointerOffset(ptr), peer);
    }

    template <NcclGinTeam Team>
    __device__ __forceinline__ void put(
        int dst_rank, const void* send_ptr, void* recv_ptr, uint32_t nbytes,
        uint32_t opt_flags = ncclGinOptFlagsDefault) const {
        gin_.put(team<Team>(), dst_rank, window_, pointerOffset(recv_ptr),
                 window_, pointerOffset(send_ptr), nbytes, ncclGin_None{},
                 ncclGin_None{}, ncclCoopThread{}, ncclGin_None{},
                 cuda::thread_scope_thread, cuda::thread_scope_device,
                 opt_flags);
    }

    template <NcclGinTeam Team, typename T>
    __device__ __forceinline__ void putValue(
        int dst_rank, T* recv_ptr, T value,
        uint32_t opt_flags = ncclGinOptFlagsDefault) const {
        static_assert(
            std::is_scalar<T>::value && std::is_trivially_copyable<T>::value,
            "NcclGinHandle::putValue requires a scalar");
        static_assert(sizeof(T) == 1 || sizeof(T) == 2 || sizeof(T) == 4 ||
                          sizeof(T) == 8,
                      "NcclGinHandle::putValue supports 1, 2, 4, or 8 bytes");
        gin_.putValue(team<Team>(), dst_rank, window_, pointerOffset(recv_ptr),
                      value, ncclGin_None{}, ncclCoopThread{}, ncclGin_None{},
                      cuda::thread_scope_thread, cuda::thread_scope_device,
                      opt_flags);
    }

    template <NcclGinTeam Team>
    __device__ __forceinline__ void signalAdd(int dst_rank,
                                              uint64_t* signal_ptr,
                                              uint64_t value) const {
        gin_.signal(team<Team>(), dst_rank,
                    detail::NcclStrongVaSignalAdd{
                        window_, pointerOffset(signal_ptr), value},
                    ncclCoopThread{});
    }

    __device__ __forceinline__ void resetSignal(uint64_t* signal_ptr) const {
        gin_.resetSignal(window_, pointerOffset(signal_ptr));
    }

    __device__ __forceinline__ uint64_t
    readSignal(const uint64_t* signal_ptr) const {
        return gin_.readSignal(window_, pointerOffset(signal_ptr));
    }

    __device__ __forceinline__ void flushWarp() const {
        gin_.flush(ncclCoopWarp{});
    }

    __device__ __forceinline__ void flushContextWarp(int context) const {
        ncclGin(comm_, context, NCCL_GIN_RESOURCE_SHARING_CTA)
            .flush(ncclCoopWarp{});
    }

    template <NcclGinTeam Team>
    __device__ __forceinline__ void signalIncContext0(int dst_rank,
                                                      int signal_id) const {
        ncclGin(comm_, 0, NCCL_GIN_RESOURCE_SHARING_CTA)
            .signal(team<Team>(), dst_rank,
                    ncclGin_SignalInc{static_cast<ncclGinSignal_t>(signal_id)},
                    ncclCoopThread{});
    }

    __device__ __forceinline__ uint64_t
    advanceSignalShadowContext0(int signal_id) const {
        ncclGin barrier(comm_, 0, NCCL_GIN_RESOURCE_SHARING_CTA);
        return ++(*barrier.getSignalShadowPtr(
            static_cast<ncclGinSignal_t>(signal_id)));
    }

    __device__ __forceinline__ uint64_t
    readSignalContext0(int signal_id) const {
        return ncclGin(comm_, 0, NCCL_GIN_RESOURCE_SHARING_CTA)
            .readSignal(static_cast<ncclGinSignal_t>(signal_id));
    }

   private:
    template <NcclGinTeam Team>
    __device__ __forceinline__ ncclTeam team() const {
        if constexpr (Team == NcclGinTeam::kRail) {
            return team_rail_;
        } else {
            return team_world_;
        }
    }

    const ncclDevComm_t& comm_;
    ncclGin gin_;
    ncclWindow_t window_;
    const char* local_base_;
    ncclTeam team_world_;
    ncclTeam team_lsa_;
    ncclTeam team_rail_;
};

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

// Return the number of GIN contexts configured for ctx. Like the other device
// helpers, this is unchecked: ctx must be live and GIN must be enabled.
__device__ __forceinline__ int mc_nccl_gin_context_count(
    const NcclDeviceContext& ctx) {
    return detail::NcclDeviceContextAccess::ginContextCount(ctx);
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

template <NcclGinTeam Team>
__device__ __forceinline__ void mc_nccl_put_team(
    const NcclDeviceContext& ctx, int channel, int dst_rank, int qps_per_rank,
    const void* send_ptr, void* recv_ptr, uint32_t nbytes, int lane_id,
    uint32_t opt_flags = ncclGinOptFlagsDefault,
    NcclGinResourceSharing sharing = NcclGinResourceSharing::kGpu) {
    (void)qps_per_rank;
    if (lane_id != 0) return;

    const size_t src_offset = detail::mc_nccl_pointer_offset(ctx, send_ptr);
    const size_t dst_offset = detail::mc_nccl_pointer_offset(ctx, recv_ptr);
    const auto& comm = detail::NcclDeviceContextAccess::comm(ctx);
    const auto window = detail::NcclDeviceContextAccess::window(ctx);
    ncclGin gin{
        comm,
        detail::mc_nccl_gin_context(ctx, static_cast<unsigned int>(channel)),
        detail::mc_nccl_gin_sharing_mode(sharing)};
    gin.put(detail::mc_nccl_gin_team<Team>(comm), dst_rank, window, dst_offset,
            window, src_offset, nbytes, ncclGin_None{}, ncclGin_None{},
            ncclCoopThread{}, ncclGin_None{}, cuda::thread_scope_thread,
            cuda::thread_scope_device, opt_flags);
}

__device__ __forceinline__ void mc_nccl_put(
    const NcclDeviceContext& ctx, int channel, int dst_rank, int qps_per_rank,
    const void* send_ptr, void* recv_ptr, uint32_t nbytes, int lane_id) {
    (void)qps_per_rank;
    if (lane_id != 0) return;

    const size_t src_offset = detail::mc_nccl_pointer_offset(ctx, send_ptr);
    const size_t dst_offset = detail::mc_nccl_pointer_offset(ctx, recv_ptr);
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
            detail::NcclStrongVaSignalAdd{window, completion_offset,
                                          completion_delta},
            ncclGin_None{}, ncclCoopThread{});
}

// Assign a 1-, 2-, 4-, or 8-byte scalar in the destination's matching
// registered buffer using GIN inline data. recv_ptr is interpreted as an offset
// in the peer's window; it is not a directly dereferenceable peer pointer.
// The destination must be naturally aligned for T and wholly contained in the
// symmetric registration bound to ctx. This operation does not signal remote
// completion; pair ordered traffic with a strong GIN signal and receiver wait.
template <NcclGinTeam Team, typename T>
__device__ __forceinline__ void mc_nccl_put_value_team(
    const NcclDeviceContext& ctx, int channel, int dst_rank, int qps_per_rank,
    T* recv_ptr, T value, int lane_id,
    uint32_t opt_flags = ncclGinOptFlagsDefault,
    NcclGinResourceSharing sharing = NcclGinResourceSharing::kGpu) {
    static_assert(std::is_scalar<T>::value,
                  "mc_nccl_put_value_team requires a scalar type");
    static_assert(std::is_trivially_copyable<T>::value,
                  "mc_nccl_put_value_team requires a trivially copyable type");
    static_assert(
        sizeof(T) == 1 || sizeof(T) == 2 || sizeof(T) == 4 || sizeof(T) == 8,
        "mc_nccl_put_value_team supports only 1, 2, 4, or 8 bytes");
    (void)qps_per_rank;
    if (lane_id != 0) return;

    const size_t dst_offset = detail::mc_nccl_pointer_offset(ctx, recv_ptr);
    const auto& comm = detail::NcclDeviceContextAccess::comm(ctx);
    const auto window = detail::NcclDeviceContextAccess::window(ctx);
    ncclGin gin{
        comm,
        detail::mc_nccl_gin_context(ctx, static_cast<unsigned int>(channel)),
        detail::mc_nccl_gin_sharing_mode(sharing)};
    gin.putValue(detail::mc_nccl_gin_team<Team>(comm), dst_rank, window,
                 dst_offset, value, ncclGin_None{}, ncclCoopThread{},
                 ncclGin_None{}, cuda::thread_scope_thread,
                 cuda::thread_scope_device, opt_flags);
}

template <typename T>
__device__ __forceinline__ void mc_nccl_put_value(const NcclDeviceContext& ctx,
                                                  int channel, int dst_rank,
                                                  int qps_per_rank, T* recv_ptr,
                                                  T value, int lane_id) {
    static_assert(std::is_scalar<T>::value,
                  "mc_nccl_put_value requires a scalar type");
    static_assert(std::is_trivially_copyable<T>::value,
                  "mc_nccl_put_value requires a trivially copyable type");
    static_assert(
        sizeof(T) == 1 || sizeof(T) == 2 || sizeof(T) == 4 || sizeof(T) == 8,
        "mc_nccl_put_value supports only 1, 2, 4, or 8 bytes");
    (void)qps_per_rank;
    if (lane_id != 0) return;

    const size_t dst_offset = detail::mc_nccl_pointer_offset(ctx, recv_ptr);
    const auto& comm = detail::NcclDeviceContextAccess::comm(ctx);
    const auto window = detail::NcclDeviceContextAccess::window(ctx);
    ncclGin gin{comm, detail::mc_nccl_gin_context(
                          ctx, static_cast<unsigned int>(channel))};
    gin.putValue(ncclTeamWorld(comm), dst_rank, window, dst_offset, value,
                 ncclGin_None{}, ncclCoopThread{});
}

// Add value to a GIN VA signal in the destination's matching buffer. Unlike
// mc_nccl_signal_add(), this helper never routes through local/LSA atomics.
// Signal visibility has strong semantics: it implies settlement of preceding
// puts issued to the same peer on this context. signal_ptr must identify a
// naturally aligned uint64_t wholly contained in the registration bound to ctx.
template <NcclGinTeam Team>
__device__ __forceinline__ void mc_nccl_gin_signal_add_team(
    const NcclDeviceContext& ctx, int dst_rank, int channel, int qps_per_rank,
    uint64_t* signal_ptr, uint64_t value, int lane_id,
    NcclGinResourceSharing sharing = NcclGinResourceSharing::kGpu) {
    (void)qps_per_rank;
    if (lane_id != 0) return;

    const size_t signal_offset =
        detail::mc_nccl_pointer_offset(ctx, signal_ptr);
    const auto& comm = detail::NcclDeviceContextAccess::comm(ctx);
    const auto window = detail::NcclDeviceContextAccess::window(ctx);
    ncclGin gin{
        comm,
        detail::mc_nccl_gin_context(ctx, static_cast<unsigned int>(channel)),
        detail::mc_nccl_gin_sharing_mode(sharing)};
    gin.signal(detail::mc_nccl_gin_team<Team>(comm), dst_rank,
               detail::NcclStrongVaSignalAdd{window, signal_offset, value},
               ncclCoopThread{});
}

// Increment one of the communicator-owned GIN signals. These signals avoid a
// symmetric-memory address calculation and are intended for compact control
// protocols such as a device barrier. signal_id must be smaller than the
// gin_signal_count requested when the device communicator was created.
template <NcclGinTeam Team>
__device__ __forceinline__ void mc_nccl_gin_signal_inc_team(
    const NcclDeviceContext& ctx, int dst_rank, int channel, int signal_id,
    int lane_id,
    NcclGinResourceSharing sharing = NcclGinResourceSharing::kGpu) {
    if (lane_id != 0) return;

    const auto& comm = detail::NcclDeviceContextAccess::comm(ctx);
    ncclGin gin{
        comm,
        detail::mc_nccl_gin_context(ctx, static_cast<unsigned int>(channel)),
        detail::mc_nccl_gin_sharing_mode(sharing)};
    gin.signal(detail::mc_nccl_gin_team<Team>(comm), dst_rank,
               ncclGin_SignalInc{static_cast<ncclGinSignal_t>(signal_id)},
               ncclCoopThread{});
}

// Advance the caller-managed shadow for a communicator-owned GIN signal and
// return the value that a matching receive must reach.
__device__ __forceinline__ uint64_t mc_nccl_gin_advance_signal_shadow(
    const NcclDeviceContext& ctx, int channel, int signal_id,
    NcclGinResourceSharing sharing = NcclGinResourceSharing::kGpu) {
    const auto& comm = detail::NcclDeviceContextAccess::comm(ctx);
    ncclGin gin{
        comm,
        detail::mc_nccl_gin_context(ctx, static_cast<unsigned int>(channel)),
        detail::mc_nccl_gin_sharing_mode(sharing)};
    return ++(*gin.getSignalShadowPtr(static_cast<ncclGinSignal_t>(signal_id)));
}

__device__ __forceinline__ uint64_t mc_nccl_gin_read_signal_id(
    const NcclDeviceContext& ctx, int channel, int signal_id,
    NcclGinResourceSharing sharing = NcclGinResourceSharing::kGpu) {
    const auto& comm = detail::NcclDeviceContextAccess::comm(ctx);
    ncclGin gin{
        comm,
        detail::mc_nccl_gin_context(ctx, static_cast<unsigned int>(channel)),
        detail::mc_nccl_gin_sharing_mode(sharing)};
    return gin.readSignal(static_cast<ncclGinSignal_t>(signal_id));
}

__device__ __forceinline__ void mc_nccl_gin_signal_add(
    const NcclDeviceContext& ctx, int dst_rank, int channel, int qps_per_rank,
    uint64_t* signal_ptr, uint64_t value, int lane_id) {
    (void)qps_per_rank;
    if (lane_id != 0) return;

    const size_t signal_offset =
        detail::mc_nccl_pointer_offset(ctx, signal_ptr);
    const auto& comm = detail::NcclDeviceContextAccess::comm(ctx);
    const auto window = detail::NcclDeviceContextAccess::window(ctx);
    ncclGin gin{comm, detail::mc_nccl_gin_context(
                          ctx, static_cast<unsigned int>(channel))};
    gin.signal(ncclTeamWorld(comm), dst_rank,
               detail::NcclStrongVaSignalAdd{window, signal_offset, value},
               ncclCoopThread{});
}

// Reset a local GIN VA signal through the context that owns it. The caller must
// ensure that no remote signal operation can race this reset. Only lane 0 acts.
__device__ __forceinline__ void mc_nccl_gin_reset_signal(
    const NcclDeviceContext& ctx, int channel, uint64_t* signal_ptr,
    int lane_id,
    NcclGinResourceSharing sharing = NcclGinResourceSharing::kGpu) {
    if (lane_id != 0) return;

    const size_t signal_offset =
        detail::mc_nccl_pointer_offset(ctx, signal_ptr);
    const auto& comm = detail::NcclDeviceContextAccess::comm(ctx);
    const auto window = detail::NcclDeviceContextAccess::window(ctx);
    ncclGin gin{
        comm,
        detail::mc_nccl_gin_context(ctx, static_cast<unsigned int>(channel)),
        detail::mc_nccl_gin_sharing_mode(sharing)};
    gin.resetSignal(window, signal_offset);
}

// Add value to a 64-bit signal in the destination's matching buffer. This is
// explicitly increment semantics, not assignment. Local/LSA routes use a
// system-scope atomic; GIN uses a VA signal on the selected context.
__device__ __forceinline__ void mc_nccl_signal_add(
    const NcclDeviceContext& ctx, int dst_rank, int channel, int qps_per_rank,
    uint64_t* signal_ptr, uint64_t value, int lane_id) {
    (void)qps_per_rank;
    if (lane_id != 0) return;

    if (dst_rank == detail::NcclDeviceContextAccess::rank(ctx)) {
        cuda::atomic_ref<uint64_t, cuda::thread_scope_system> signal(
            *signal_ptr);
        signal.fetch_add(value, cuda::memory_order_release);
        return;
    }
    const auto& comm = detail::NcclDeviceContextAccess::comm(ctx);
    if (ncclTeamRankIsMember(ncclTeamLsa(comm), ncclTeamWorld(comm),
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

    mc_nccl_gin_signal_add(ctx, dst_rank, channel, qps_per_rank, signal_ptr,
                           value, 0);
}

__device__ __forceinline__ void mc_nccl_flush(
    const NcclDeviceContext& ctx, int channel, int lane_id,
    NcclGinResourceSharing sharing = NcclGinResourceSharing::kGpu) {
    if (lane_id != 0) return;
    const auto& comm = detail::NcclDeviceContextAccess::comm(ctx);
    ncclGin gin{
        comm,
        detail::mc_nccl_gin_context(ctx, static_cast<unsigned int>(channel)),
        detail::mc_nccl_gin_sharing_mode(sharing)};
    gin.flush(ncclCoopThread{});
}

__device__ __forceinline__ void mc_nccl_flush_warp(
    const NcclDeviceContext& ctx, int channel,
    NcclGinResourceSharing sharing = NcclGinResourceSharing::kCta) {
    const auto& comm = detail::NcclDeviceContextAccess::comm(ctx);
    ncclGin gin{
        comm,
        detail::mc_nccl_gin_context(ctx, static_cast<unsigned int>(channel)),
        detail::mc_nccl_gin_sharing_mode(sharing)};
    gin.flush(ncclCoopWarp{});
}

// Read a GIN VA signal from the local matching window. This helper never falls
// back to an ordinary CUDA atomic load. signal_ptr must be an aligned uint64_t
// inside the registration bound to ctx, and channel must select the same GIN
// context used by the remote producer.
__device__ __forceinline__ uint64_t mc_nccl_gin_read_signal(
    const NcclDeviceContext& ctx, int channel, const uint64_t* signal_ptr,
    NcclGinResourceSharing sharing = NcclGinResourceSharing::kGpu) {
    const size_t signal_offset =
        detail::mc_nccl_pointer_offset(ctx, signal_ptr);
    const auto& comm = detail::NcclDeviceContextAccess::comm(ctx);
    ncclGin gin{
        comm,
        detail::mc_nccl_gin_context(ctx, static_cast<unsigned int>(channel)),
        detail::mc_nccl_gin_sharing_mode(sharing)};
    return gin.readSignal(detail::NcclDeviceContextAccess::window(ctx),
                          signal_offset);
}

__device__ __forceinline__ uint64_t mc_nccl_read_signal(
    const NcclDeviceContext& ctx, int channel, const uint64_t* signal_ptr) {
    if (!mc_nccl_gin_available(ctx)) {
        auto& value = *const_cast<uint64_t*>(signal_ptr);
        cuda::atomic_ref<uint64_t, cuda::thread_scope_system> signal(value);
        return signal.load(cuda::memory_order_acquire);
    }

    return mc_nccl_gin_read_signal(ctx, channel, signal_ptr);
}

// Wait on a GIN VA signal in the local matching window. This helper never
// falls back to an ordinary CUDA atomic load. Only lane 0 waits; callers that
// need a warp-wide dependency must synchronize after it returns. signal_ptr
// and channel have the same preconditions as mc_nccl_gin_read_signal().
__device__ __forceinline__ void mc_nccl_gin_wait_signal(
    const NcclDeviceContext& ctx, int channel, const uint64_t* signal_ptr,
    uint64_t least, int lane_id,
    NcclGinResourceSharing sharing = NcclGinResourceSharing::kGpu) {
    if (lane_id != 0) return;
    const size_t signal_offset =
        detail::mc_nccl_pointer_offset(ctx, signal_ptr);
    const auto& comm = detail::NcclDeviceContextAccess::comm(ctx);
    ncclGin gin{
        comm,
        detail::mc_nccl_gin_context(ctx, static_cast<unsigned int>(channel)),
        detail::mc_nccl_gin_sharing_mode(sharing)};
    gin.waitSignal(ncclCoopThread{},
                   detail::NcclDeviceContextAccess::window(ctx), signal_offset,
                   least);
}

// Only lane 0 waits. Callers that need a warp-wide dependency must synchronize
// after this function returns.
__device__ __forceinline__ void mc_nccl_wait_signal(
    const NcclDeviceContext& ctx, int channel, const uint64_t* signal_ptr,
    uint64_t least, int lane_id) {
    if (lane_id != 0) return;
    if (!mc_nccl_gin_available(ctx)) {
        auto& value = *const_cast<uint64_t*>(signal_ptr);
        cuda::atomic_ref<uint64_t, cuda::thread_scope_system> signal(value);
        while (signal.load(cuda::memory_order_acquire) < least) {
        }
        return;
    }

    mc_nccl_gin_wait_signal(ctx, channel, signal_ptr, least, 0);
}

}  // namespace device
}  // namespace mooncake
