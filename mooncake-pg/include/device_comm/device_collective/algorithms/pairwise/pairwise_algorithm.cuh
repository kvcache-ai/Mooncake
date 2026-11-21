#ifndef MOONCAKE_PG_DEVICE_COMM_DEVICE_COLLECTIVE_ALGORITHMS_PAIRWISE_ALGORITHM_CUH
#define MOONCAKE_PG_DEVICE_COMM_DEVICE_COLLECTIVE_ALGORITHMS_PAIRWISE_ALGORITHM_CUH

#include <cstdint>

#include <cuda_alike.h>

#include "common_types.h"
#include "device_comm/device_collective/protocols/simple/simple_types.cuh"
#include "error_types.h"

namespace mooncake {

// Bound each transfer so successive chunks can overlap.
inline constexpr uint64_t kMaxPairwiseChunkBytes = 512ull * 1024;  // 512 KiB

// Workspace bindings for the shared DeviceGroupView. Kernels select each
// round's peer from the current membership.
struct PairwisePlan {
    DevicePlanStatus status = DevicePlanStatus::Unavailable;
    SimpleWorkspace workspace;
};

// Local device state owned by PairwiseAlgorithm and shared by its kernels.
struct alignas(256) PairwiseDeviceState {
    PairwisePlan plan;
    CollectiveRuntimeBindings collective;
    // Borrowed state from the communicator's Simple protocol.
    SimpleState* simple = nullptr;
};

PGResult<void> launchPairwiseAllToAllKernel(const AllToAllRequest& request,
                                            PairwiseDeviceState* state,
                                            cudaStream_t stream);

}  // namespace mooncake

#endif  // MOONCAKE_PG_DEVICE_COMM_DEVICE_COLLECTIVE_ALGORITHMS_PAIRWISE_ALGORITHM_CUH
