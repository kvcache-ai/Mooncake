#ifndef MOONCAKE_PG_DEVICE_COMM_DEVICE_COLLECTIVE_ALGORITHMS_RING_ALGORITHM_CUH
#define MOONCAKE_PG_DEVICE_COMM_DEVICE_COLLECTIVE_ALGORITHMS_RING_ALGORITHM_CUH

#include <cstdint>

#include <cuda_alike.h>

#include "common_types.h"
#include "device_comm/device_collective/protocols/simple/simple_types.cuh"
#include "error_types.h"

namespace mooncake {

// An oversized chunk reduces pipeline overlap, so a cap is applied here.
inline constexpr uint64_t kMaxRingChunkBytes = 512ull * 1024;  // 512 KiB

// Ring topology and workspace bindings for the shared DeviceGroupView.
// The runtime publishes this plan through a device control update.
struct RingPlan {
    DevicePlanStatus status = DevicePlanStatus::Unavailable;

    SimpleWorkspace workspace;

    // Active indices in DeviceGroupView
    uint32_t predecessor_index = 0;
    uint32_t successor_index = 0;
};

// Local device state owned by RingAlgorithm and shared by its kernels.
struct alignas(256) RingDeviceState {
    RingPlan plan;
    CollectiveRuntimeBindings collective;
    // Borrowed state from the communicator's Simple protocol.
    SimpleState* simple = nullptr;
};

PGResult<void> launchRingAllReduceKernel(const AllReduceRequest& request,
                                         RingDeviceState* state,
                                         cudaStream_t stream);
PGResult<void> launchRingAllGatherKernel(const AllGatherRequest& request,
                                         RingDeviceState* state,
                                         cudaStream_t stream);
PGResult<void> launchRingReduceScatterKernel(
    const ReduceScatterRequest& request, RingDeviceState* state,
    cudaStream_t stream);
PGResult<void> launchRingBroadcastKernel(const BroadcastRequest& request,
                                         RingDeviceState* state,
                                         cudaStream_t stream);
PGResult<void> launchRingReduceKernel(const ReduceRequest& request,
                                      RingDeviceState* state,
                                      cudaStream_t stream);
PGResult<void> launchRingBarrierKernel(const BarrierRequest& request,
                                       RingDeviceState* state,
                                       cudaStream_t stream);

}  // namespace mooncake

#endif  // MOONCAKE_PG_DEVICE_COMM_DEVICE_COLLECTIVE_ALGORITHMS_RING_ALGORITHM_CUH
