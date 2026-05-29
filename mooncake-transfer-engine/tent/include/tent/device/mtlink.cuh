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

// Deprecated: use <tent/device/p2p_ops.cuh> instead.
// This header provides backward-compatible wrappers.

#include <tent/device/p2p_ops.cuh>

namespace mooncake {
namespace tent {
namespace device {
namespace mtlink {

__device__ __forceinline__ bool is_available(const P2PDeviceContext& ctx,
                                             int dst_rank) {
    return p2p::is_available(ctx, dst_rank);
}

__device__ __forceinline__ void* peer_ptr(const P2PDeviceContext& ctx,
                                          int dst_rank, const void* local_base,
                                          const void* local_ptr) {
    return p2p::peer_ptr(ctx, dst_rank, local_base, local_ptr);
}

__device__ __forceinline__ void mtlink_put(DeviceOps* dops,
                                           const P2PDeviceContext& ctx,
                                           int dst_rank,
                                           const void* local_base,
                                           void* recv, const void* send,
                                           size_t n) {
    p2p::p2p_put(dops, ctx, dst_rank, local_base, recv, send, n);
}

__device__ __forceinline__ void mtlink_signal(DeviceOps* dops,
                                              const P2PDeviceContext& ctx,
                                              int dst_rank,
                                              const void* local_base,
                                              void* sig, int32_t action) {
    p2p::p2p_signal(dops, ctx, dst_rank, local_base, sig, action);
}

__device__ __forceinline__ void mtlink_wait_signal(DeviceOps* dops,
                                                   void* sig,
                                                   uint64_t expected) {
    p2p::p2p_wait_signal(dops, sig, expected);
}

__device__ __forceinline__ void mtlink_wait_signal_32(DeviceOps* dops,
                                                      void* sig,
                                                      uint32_t expected) {
    p2p::p2p_wait_signal_32(dops, sig, expected);
}

__device__ __forceinline__ void mtlink_signal_add(DeviceOps* dops,
                                               const P2PDeviceContext& ctx,
                                               int dst_rank,
                                               const void* local_base,
                                               void* sym, int32_t val) {
    p2p::p2p_signal_add(dops, ctx, dst_rank, local_base, sym, val);
}

__device__ __forceinline__ void mtlink_flush(DeviceOps* dops) {
    p2p::p2p_flush(dops);
}

}  // namespace mtlink
}  // namespace device
}  // namespace tent
}  // namespace mooncake

#endif  // TENT_DEVICE_MTLINK_CUH_
