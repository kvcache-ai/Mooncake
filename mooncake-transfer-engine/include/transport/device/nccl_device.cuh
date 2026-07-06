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

#include "transport/device/device_transport.h"

namespace mooncake {
namespace device {

__device__ __forceinline__ bool mc_nccl_lsa_available(
    const NcclDeviceContext& ctx, int peer) {
    return ncclTeamRankIsMember(ncclTeamLsa(ctx.comm),
                                ncclTeamWorld(ctx.comm), peer);
}

__device__ __forceinline__ bool mc_nccl_gin_available(
    const NcclDeviceContext& ctx) {
    return ctx.comm.ginConnectionCount > 0 && ctx.comm.ginContextCount > 0;
}

__device__ __forceinline__ int mc_nccl_gin_context(
    const NcclDeviceContext& ctx, unsigned int key) {
    return ctx.comm.ginContextCount == 0
               ? 0
               : static_cast<int>(key % ctx.comm.ginContextCount);
}

// Return the peer pointer for a rank in the local LSA team. The caller must
// first check mc_nccl_lsa_available().
__device__ __forceinline__ void* mc_nccl_lsa_ptr(
    const NcclDeviceContext&, ncclWindow_t window, size_t offset, int peer) {
    return ncclGetPeerPointer(window, offset, peer);
}

// Post a thread-cooperative GIN put. The source may be reused only after the
// same context has been flushed; remote settlement requires a signal or a
// higher-level acknowledgement protocol.
__device__ __forceinline__ void mc_nccl_gin_put(
    const NcclDeviceContext& ctx, int context_index, int peer,
    ncclWindow_t dst_window, size_t dst_offset, ncclWindow_t src_window,
    size_t src_offset, size_t bytes) {
    ncclGin gin(ctx.comm, context_index);
    gin.put(ncclTeamWorld(ctx.comm), peer, dst_window, dst_offset, src_window,
            src_offset, bytes, ncclGin_None{}, ncclGin_None{},
            ncclCoopThread{});
}

__device__ __forceinline__ void mc_nccl_gin_put_signal(
    const NcclDeviceContext& ctx, int context_index, int peer,
    ncclWindow_t dst_window, size_t dst_offset, ncclWindow_t src_window,
    size_t src_offset, size_t bytes, ncclGinSignal_t signal_index) {
    ncclGin gin(ctx.comm, context_index);
    gin.put(ncclTeamWorld(ctx.comm), peer, dst_window, dst_offset, src_window,
            src_offset, bytes, ncclGin_StrongSignalInc{signal_index},
            ncclGin_None{}, ncclCoopThread{});
}

__device__ __forceinline__ void mc_nccl_gin_signal(
    const NcclDeviceContext& ctx, int context_index, int peer,
    ncclGinSignal_t signal_index) {
    ncclGin gin(ctx.comm, context_index);
    gin.signal(ncclTeamWorld(ctx.comm), peer,
               ncclGin_StrongSignalInc{signal_index}, ncclCoopThread{});
}

__device__ __forceinline__ void mc_nccl_gin_flush(
    const NcclDeviceContext& ctx, int context_index) {
    ncclGin gin(ctx.comm, context_index);
    gin.flush(ncclCoopThread{});
}

__device__ __forceinline__ void mc_nccl_gin_wait_signal(
    const NcclDeviceContext& ctx, int context_index,
    ncclGinSignal_t signal_index, uint64_t value) {
    ncclGin gin(ctx.comm, context_index);
    gin.waitSignal(ncclCoopThread{}, signal_index, value);
}

}  // namespace device
}  // namespace mooncake
