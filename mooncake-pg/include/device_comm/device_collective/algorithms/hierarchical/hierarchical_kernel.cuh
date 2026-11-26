#pragma once

#include "device_comm/device_collective/algorithms/one_shot/one_shot_algorithm.cuh"
#include "device_comm/device_collective/protocols/simple/simple_types.cuh"

namespace mooncake {

struct HierarchicalKernelPlan {
    DevicePlanStatus status = DevicePlanStatus::Unavailable;
    int32_t local_leader = -1;
    uint32_t remote_leader = 0;
    SimpleWorkspace workspace;
    OneShotBufferLayout layout;
    LLPacket* packets = nullptr;
    uint64_t packet_offsets[kMaxNumRanks] = {};
};

struct HierarchicalKernelState {
    HierarchicalKernelPlan plan;
    CollectiveRuntimeBindings collective;
    LLState* ll = nullptr;
    const SimpleState* simple = nullptr;
};

PGResult<void> launchHierarchicalSumKernel(const AllReduceRequest& request,
                                           HierarchicalKernelState* state,
                                           cudaStream_t stream);

}  // namespace mooncake
