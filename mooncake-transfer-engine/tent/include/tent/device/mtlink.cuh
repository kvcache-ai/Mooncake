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

#ifndef TENT_DEVICE_MTLINK_CUH_
#define TENT_DEVICE_MTLINK_CUH_

#include <cstddef>
#include <cstdint>

#include <tent/runtime/device_resources.h>
#include <tent/device/ir/device_ops.cuh>

namespace mooncake {
namespace tent {
namespace device {
namespace mtlink {

// ---------------------------------------------------------------------------
// Availability & address computation
// ---------------------------------------------------------------------------

__device__ __forceinline__ bool is_available(const MtLinkDeviceContext& ctx,
                                             int dst_rank) {
    return ctx.available != nullptr && ctx.peer_ptrs != nullptr &&
           dst_rank >= 0 && dst_rank < ctx.num_ranks &&
           ctx.available[dst_rank] != 0 && ctx.peer_ptrs[dst_rank] != nullptr;
}

__device__ __forceinline__ void* peer_ptr(const MtLinkDeviceContext& ctx,
                                          int dst_rank, const void* local_base,
                                          const void* local_ptr) {
    const auto offset = reinterpret_cast<const char*>(local_ptr) -
                        reinterpret_cast<const char*>(local_base);
    return reinterpret_cast<char*>(ctx.peer_ptrs[dst_rank]) + offset;
}

// ---------------------------------------------------------------------------
// EpCommOps — MTLink P2P communication primitives
//
// These use DeviceOps function pointers for memory ordering, which are
// populated by the platform backend (musa_ops.cuh for MUSA).  This avoids
// CUDA PTX inline ASM and keeps the code portable.
// ---------------------------------------------------------------------------

__device__ __forceinline__ void mtlink_put(DeviceOps* dops,
                                           const MtLinkDeviceContext& ctx,
                                           int dst_rank,
                                           const void* local_base,
                                           void* recv, const void* send,
                                           size_t n) {
    void* peer_dst = peer_ptr(ctx, dst_rank, local_base, recv);
    // store_release = copy + __threadfence_system(), ensures P2P visibility
    dops->store_release(peer_dst, send, n);
}

__device__ __forceinline__ void mtlink_signal(DeviceOps* dops,
                                              const MtLinkDeviceContext& ctx,
                                              int dst_rank,
                                              const void* local_base,
                                              void* sig, int32_t action) {
    void* peer_sig = peer_ptr(ctx, dst_rank, local_base, sig);
    // Release store to peer signal address (matches original st_na_release)
    dops->store_release_32(peer_sig, static_cast<uint32_t>(action));
}

__device__ __forceinline__ void mtlink_wait_signal(DeviceOps* dops,
                                                   void* sig,
                                                   uint64_t expected) {
    // Spin until local signal reaches expected value
    dops->spin_wait_ne(sig, static_cast<uint32_t>(0));
    // For 64-bit signals, use atomic_load_acquire
    uint64_t val = 0;
    do {
        dops->fence_acq_rel();
        val = dops->atomic_load_acquire(sig);
    } while (val != expected);
}

__device__ __forceinline__ void mtlink_wait_signal_32(DeviceOps* dops,
                                                      void* sig,
                                                      uint32_t expected) {
    dops->spin_wait_eq(sig, expected);
}

__device__ __forceinline__ void mtlink_red_add(DeviceOps* dops,
                                               const MtLinkDeviceContext& ctx,
                                               int dst_rank,
                                               const void* local_base,
                                               void* sym, int32_t val) {
    void* peer_sym = peer_ptr(ctx, dst_rank, local_base, sym);
    // P2P path uses release store (matches original st_na_release).
    // IBGDA path uses atomic add; that's handled separately in ep_red_add.
    dops->store_release_32(peer_sym, static_cast<uint32_t>(val));
}

__device__ __forceinline__ void mtlink_flush(DeviceOps* dops) {
    dops->fence_acq_rel();
}

}  // namespace mtlink
}  // namespace device
}  // namespace tent
}  // namespace mooncake

#endif  // TENT_DEVICE_MTLINK_CUH_
