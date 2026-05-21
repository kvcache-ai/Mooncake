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

#ifndef TENT_DEVICE_NVLINK_CUH_
#define TENT_DEVICE_NVLINK_CUH_

#include <cstddef>
#include <cstdint>

#include <tent/runtime/device_resources.h>

namespace mooncake {
namespace tent {
namespace device {
namespace nvlink {

__device__ __forceinline__ bool is_available(const NvLinkDeviceContext& ctx,
                                             int dst_rank) {
    return ctx.available != nullptr && ctx.peer_ptrs != nullptr &&
           dst_rank >= 0 && dst_rank < ctx.num_ranks &&
           ctx.available[dst_rank] != 0 && ctx.peer_ptrs[dst_rank] != nullptr;
}

__device__ __forceinline__ void* peer_ptr(const NvLinkDeviceContext& ctx,
                                          int dst_rank, const void* local_base,
                                          const void* local_ptr) {
    const auto offset = reinterpret_cast<const char*>(local_ptr) -
                        reinterpret_cast<const char*>(local_base);
    return reinterpret_cast<char*>(ctx.peer_ptrs[dst_rank]) + offset;
}

}  // namespace nvlink
}  // namespace device
}  // namespace tent
}  // namespace mooncake

#endif  // TENT_DEVICE_NVLINK_CUH_
