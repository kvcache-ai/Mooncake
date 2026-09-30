#ifndef MOONCAKE_PG_DEVICE_COMM_DEVICE_COLLECTIVE_ALGORITHMS_ONE_SHOT_TYPES_CUH
#define MOONCAKE_PG_DEVICE_COMM_DEVICE_COLLECTIVE_ALGORITHMS_ONE_SHOT_TYPES_CUH

#include "device_comm/device_collective/device_collective_types.cuh"
#include "device_comm/device_collective/protocols/ll/ll_types.cuh"

namespace mooncake {

inline constexpr int kOneShotMaxThreads = 512;
inline constexpr uint32_t kOneShotMaxChannels = 16;
static_assert(kOneShotMaxChannels <= kMaxDeviceCollectiveChannels);

// Each channel owns [slot][sender][packet], with two slots per sender.
struct OneShotBufferLayout {
    static constexpr uint32_t kChunkBytes = 4 * 1024;
    static constexpr uint32_t kSlots = 2;
    uint32_t max_group_size = 0;

    [[nodiscard]] __host__ __device__ constexpr uint64_t packetIndex(
        uint32_t channel, uint32_t slot, InGroupRank sender,
        uint64_t packet_index = 0) const {
        return ((uint64_t{channel} * kSlots + slot) * max_group_size +
                static_cast<uint32_t>(sender)) *
                   LLPacket::packetCount(kChunkBytes) +
               packet_index;
    }

    [[nodiscard]] __host__ __device__ constexpr uint64_t channelPacketBytes()
        const {
        return uint64_t{kSlots} * max_group_size *
               LLPacket::storageBytes(kChunkBytes);
    }

    [[nodiscard]] __host__ __device__ constexpr uint64_t packetBytes() const {
        return kOneShotMaxChannels * channelPacketBytes();
    }
};

struct OneShotAllReducePlan {
    DevicePlanStatus status = DevicePlanStatus::Unavailable;
    uint64_t view_epoch = kInvalidViewEpoch;
    OneShotBufferLayout layout;
    LLPacket* packets = nullptr;
    InGroupRank self_rank = kInvalidInGroupRank;
    uint32_t self_active_index = 0;
    uint32_t participant_count = 0;
    // Active-rank order with self removed; offsets address one-shot packets.
    CollectivePeer remote_peers[kMaxNumRanks - 1] = {};

    [[nodiscard]] __device__ __forceinline__ RemotePeerList
    remotePeers() const {
        return {remote_peers,
                participant_count == 0 ? 0 : participant_count - 1};
    }

    // Map a non-local active index into remote_peers.
    [[nodiscard]] __device__ __forceinline__ const CollectivePeer&
    remoteParticipant(uint32_t active_index) const {
        return remote_peers[active_index < self_active_index
                                ? active_index
                                : active_index - 1];
    }
};

struct alignas(64) OneShotAllReduceDeviceState {
    OneShotAllReducePlan plan;
    CollectiveRuntimeBindings collective;
    // Borrowed control bindings and progress from the communicator's LL
    // resources.
    LLState* ll = nullptr;
};

cudaError_t launchOneShotAllReduceKernel(const AllReduceRequest& request,
                                         OneShotAllReduceDeviceState* state,
                                         cudaStream_t stream);

}  // namespace mooncake

#endif  // MOONCAKE_PG_DEVICE_COMM_DEVICE_COLLECTIVE_ALGORITHMS_ONE_SHOT_TYPES_CUH
