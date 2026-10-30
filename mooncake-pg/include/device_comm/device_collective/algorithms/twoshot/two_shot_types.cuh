#ifndef MOONCAKE_PG_DEVICE_COMM_DEVICE_COLLECTIVE_ALGORITHMS_TWO_SHOT_TYPES_CUH
#define MOONCAKE_PG_DEVICE_COMM_DEVICE_COLLECTIVE_ALGORITHMS_TWO_SHOT_TYPES_CUH

#include "device_comm/device_collective/protocols/simple/simple_types.cuh"

namespace mooncake {

inline constexpr uint64_t kMaxTwoShotChunkBytes = 256ull * 1024;

struct TwoShotAllReducePlan {
    DevicePlanStatus status = DevicePlanStatus::Unavailable;
    uint64_t view_epoch = kInvalidViewEpoch;
    SimpleWorkspace workspace;
    int32_t self_active_index = -1;
    uint32_t participant_count = 0;
    // Active order with self removed. Payload ownership uses active indices;
    // protocol signals continue to use stable in-group ranks.
    CollectivePeer remote_peers[kMaxNumRanks - 1] = {};

    [[nodiscard]] __device__ __forceinline__ RemotePeerList
    remotePeers() const {
        return {remote_peers,
                participant_count == 0 ? 0 : participant_count - 1};
    }

    [[nodiscard]] __device__ __forceinline__ const CollectivePeer&
    remoteParticipant(uint32_t index) const {
        return remote_peers[index < static_cast<uint32_t>(self_active_index)
                                ? index
                                : index - 1];
    }
};

struct alignas(256) TwoShotAllReduceDeviceState {
    TwoShotAllReducePlan plan;
    CollectiveRuntimeBindings collective;
    SimpleBindings* simple = nullptr;
};

cudaError_t launchTwoShotAllReduceKernel(const AllReduceRequest& request,
                                         TwoShotAllReduceDeviceState* state,
                                         cudaStream_t stream);

}  // namespace mooncake

#endif  // MOONCAKE_PG_DEVICE_COMM_DEVICE_COLLECTIVE_ALGORITHMS_TWO_SHOT_TYPES_CUH
