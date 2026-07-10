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

#include "transport/device/comm_device.cuh"
#include "transport/device/nccl_device.cuh"

#include <cstdint>

namespace mooncake {
namespace device {
namespace compatibility_test {

struct NcclOps {
    using Context = NcclDeviceContext;

    __device__ __forceinline__ static void put(
        const Context& ctx, int channel, int peer, int qps_per_rank,
        const void* source, void* destination, uint32_t bytes, int lane) {
        mc_nccl_put(ctx, channel, peer, qps_per_rank, source, destination,
                    bytes, lane);
    }
};

struct IbgdaOps {
    using Context = CommCtx;

    __device__ __forceinline__ static void put(
        const Context& ctx, int channel, int peer, int qps_per_rank,
        const void* source, void* destination, uint32_t bytes, int lane) {
        mc_rdma_put(ctx, channel, peer, qps_per_rank, source, destination,
                    bytes, lane);
    }
};

template <typename Ops>
__device__ __forceinline__ void exchange(
    const typename Ops::Context& ctx, int channel, int peer, int qps_per_rank,
    const void* source, void* destination, uint32_t bytes, int lane) {
    Ops::put(ctx, channel, peer, qps_per_rank, source, destination, bytes,
             lane);
}

// These kernels are compile probes. They deliberately share the same templated
// operation and are never executed, so the test needs neither GPUs nor NICs.
__global__ void instantiateNccl(
    NcclOps::Context ctx, int channel, int peer, int qps_per_rank,
    const void* source, void* destination, uint32_t bytes, int lane) {
    exchange<NcclOps>(ctx, channel, peer, qps_per_rank, source, destination,
                      bytes, lane);
}

__global__ void instantiateIbgda(
    IbgdaOps::Context ctx, int channel, int peer, int qps_per_rank,
    const void* source, void* destination, uint32_t bytes, int lane) {
    exchange<IbgdaOps>(ctx, channel, peer, qps_per_rank, source, destination,
                       bytes, lane);
}

}  // namespace compatibility_test
}  // namespace device
}  // namespace mooncake
