#ifndef MOONCAKE_PG_DEVICE_COMM_DEVICE_COLLECTIVE_ALGORITHMS_RING_TYPES_CUH
#define MOONCAKE_PG_DEVICE_COMM_DEVICE_COLLECTIVE_ALGORITHMS_RING_TYPES_CUH

#include <cstdint>

#include <cuda_alike.h>

#include "common_types.h"
#include "device_comm/device_collective/protocols/simple/simple_types.cuh"

namespace mooncake {

// An oversized chunk reduces pipeline overlap, so a cap is applied here.

inline constexpr uint64_t kMaxRingChunkBytes = 512ull * 1024;  // 512 KiB

// Complete published resource and topology binding read by a Ring AllReduce
// kernel. A control update replaces the Plan while algorithm execution is
// quiescent, so a kernel needs only the state pointer and its per-invocation
// request.
struct RingAllReducePlan {
    DevicePlanStatus status = DevicePlanStatus::Unavailable;
    uint64_t view_epoch = kInvalidViewEpoch;

    SimpleWorkspace workspace;

    int32_t self_active_index = -1;
    uint32_t participant_count = 0;
    // Predecessor first, successor last. A two-rank Ring has one remote peer.
    CollectivePeer remote_peers[2] = {};
    uint32_t peer_count = 0;

    [[nodiscard]] __device__ __forceinline__ const CollectivePeer& predecessor()
        const {
        return remote_peers[0];
    }

    [[nodiscard]] __device__ __forceinline__ const CollectivePeer& successor()
        const {
        return remote_peers[peer_count - 1];
    }

    [[nodiscard]] __device__ __forceinline__ RemotePeerList
    remotePeers() const {
        return {remote_peers, peer_count};
    }
};

// Ordinary device memory owned by one Ring AllReduce algorithm instance. None
// of this state is registered or published to peers.
struct alignas(256) RingAllReduceDeviceState {
    RingAllReducePlan plan;
    CollectiveRuntimeBindings collective;
    // Borrowed Simple bindings; progress is shared with other Simple
    // algorithms.
    SimpleBindings* simple = nullptr;
};

cudaError_t launchRingAllReduceKernel(const AllReduceRequest& request,
                                      RingAllReduceDeviceState* state,
                                      cudaStream_t stream);

}  // namespace mooncake

#endif  // MOONCAKE_PG_DEVICE_COMM_DEVICE_COLLECTIVE_ALGORITHMS_RING_TYPES_CUH
