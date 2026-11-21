#ifndef MOONCAKE_PG_DEVICE_COMM_DEVICE_COLLECTIVE_DEVICE_COLLECTIVE_LOCAL_CUH
#define MOONCAKE_PG_DEVICE_COMM_DEVICE_COLLECTIVE_DEVICE_COLLECTIVE_LOCAL_CUH

#include <cstdint>

#include <cooperative_groups.h>

#include "common_types.h"
#include "device_comm/device_collective/device_collective_types.cuh"
#include "device_comm/device_primitives/value_primitives.cuh"

namespace mooncake {
namespace detail {

// The first CTA copies the entire input.
__device__ __forceinline__ void copyLocalCollectiveInput(
    const void* source, void* destination, uint64_t bytes,
    cooperative_groups::thread_block block) {
    if (blockIdx.x != 0 || bytes == 0 || source == destination) return;
    copyValuesTo(static_cast<const uint8_t*>(source), bytes, block,
                 static_cast<uint8_t*>(destination));
}

}  // namespace detail

// Execute a collective with only the local rank participating. All algorithms
// share these implementations. Keep the implementation simple and readable
// since local-only performance is not critical.
__device__ __forceinline__ void executeLocalOnlyCollective(
    const AllReduceRequest& request, InGroupRank,
    cooperative_groups::thread_block block) {
    const uint64_t bytes = request.count * elementSize(request.datatype);
    detail::copyLocalCollectiveInput(request.send_buffer, request.recv_buffer,
                                     bytes, block);
}

__device__ __forceinline__ void executeLocalOnlyCollective(
    const AllGatherRequest& request, InGroupRank self_rank,
    cooperative_groups::thread_block block) {
    const uint64_t bytes = request.count * elementSize(request.datatype);
    auto* destination = static_cast<uint8_t*>(request.recv_buffer) +
                        static_cast<uint64_t>(self_rank) * bytes;
    detail::copyLocalCollectiveInput(request.send_buffer, destination, bytes,
                                     block);
}

}  // namespace mooncake

#endif  // MOONCAKE_PG_DEVICE_COMM_DEVICE_COLLECTIVE_DEVICE_COLLECTIVE_LOCAL_CUH
