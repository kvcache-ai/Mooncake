#ifndef MOONCAKE_PG_DEVICE_COMM_DEVICE_COLLECTIVE_ALGORITHMS_STAR_ALGORITHM_CUH
#define MOONCAKE_PG_DEVICE_COMM_DEVICE_COLLECTIVE_ALGORITHMS_STAR_ALGORITHM_CUH

#include <cstdint>

#include <cuda_alike.h>

#include "common_types.h"
#include "device_comm/device_collective/protocols/simple/simple_types.cuh"
#include "error_types.h"

namespace mooncake {

// Bound each transfer so successive chunks can overlap.
inline constexpr uint64_t kMaxStarChunkBytes = 512ull * 1024;  // 512 KiB

// Workspace bindings for the shared DeviceGroupView. The root is supplied
// by each request.
struct StarPlan {
    DevicePlanStatus status = DevicePlanStatus::Unavailable;
    SimpleWorkspace workspace;
};

// Local device state owned by StarAlgorithm and shared by its kernels.
struct alignas(256) StarDeviceState {
    StarPlan plan;
    CollectiveRuntimeBindings collective;
    // Borrowed state from the communicator's Simple protocol.
    SimpleState* simple = nullptr;
};

PGResult<void> launchStarGatherKernel(const GatherRequest& request,
                                      StarDeviceState* state,
                                      cudaStream_t stream);
PGResult<void> launchStarScatterKernel(const ScatterRequest& request,
                                       StarDeviceState* state,
                                       cudaStream_t stream);
PGResult<void> launchStarBroadcastKernel(const BroadcastRequest& request,
                                         StarDeviceState* state,
                                         cudaStream_t stream);
PGResult<void> launchStarReduceKernel(const ReduceRequest& request,
                                      StarDeviceState* state,
                                      cudaStream_t stream);

}  // namespace mooncake

#endif  // MOONCAKE_PG_DEVICE_COMM_DEVICE_COLLECTIVE_ALGORITHMS_STAR_ALGORITHM_CUH
