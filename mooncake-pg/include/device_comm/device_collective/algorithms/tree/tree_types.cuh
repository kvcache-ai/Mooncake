#ifndef MOONCAKE_PG_DEVICE_COMM_DEVICE_COLLECTIVE_ALGORITHMS_TREE_TYPES_CUH
#define MOONCAKE_PG_DEVICE_COMM_DEVICE_COLLECTIVE_ALGORITHMS_TREE_TYPES_CUH

#include "device_comm/device_collective/algorithms/tree/tree_geometry.h"
#include "device_comm/device_collective/protocols/simple/simple_types.cuh"

namespace mooncake {

static_assert(kTreePipelineDepth == kSimplePipelineDepth);
static_assert(kTreePayloadAlignment == kSimplePayloadAlignment);

struct TreeTopology {
    CollectivePeer parent;
    CollectivePeer children[kTreeChildCount];
    uint32_t parent_child_slot = kInvalidTreeSlot;
};

struct TreeAllReducePlan {
    DevicePlanStatus status = DevicePlanStatus::Unavailable;
    uint64_t view_epoch = kInvalidViewEpoch;
    SimpleWorkspace workspace;
    int32_t self_active_index = -1;
    uint32_t participant_count = 0;
    uint32_t tree_count = 0;
    TreeTopology topologies[kTreeTopologyCapacity] = {};
    // Union across topologies, excluding self. First version uses one tree.
    CollectivePeer remote_peers[kTreeTopologyCapacity * 3] = {};
    uint32_t peer_count = 0;

    [[nodiscard]] __device__ __forceinline__ RemotePeerList
    remotePeers() const {
        return {remote_peers, peer_count};
    }
};

struct alignas(256) TreeAllReduceDeviceState {
    TreeAllReducePlan plan;
    CollectiveRuntimeBindings collective;
    SimpleBindings* simple = nullptr;
};

cudaError_t launchTreeAllReduceKernel(const AllReduceRequest& request,
                                      TreeAllReduceDeviceState* state,
                                      cudaStream_t stream);

}  // namespace mooncake

#endif  // MOONCAKE_PG_DEVICE_COMM_DEVICE_COLLECTIVE_ALGORITHMS_TREE_TYPES_CUH
