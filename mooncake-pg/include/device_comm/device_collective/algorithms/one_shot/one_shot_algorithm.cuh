#ifndef MOONCAKE_PG_DEVICE_COMM_DEVICE_COLLECTIVE_ALGORITHMS_ONE_SHOT_ALGORITHM_CUH
#define MOONCAKE_PG_DEVICE_COMM_DEVICE_COLLECTIVE_ALGORITHMS_ONE_SHOT_ALGORITHM_CUH

#include "device_comm/device_collective/device_collective_types.cuh"
#include "device_comm/device_collective/protocols/ll/ll_types.cuh"
#include "error_types.h"

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

struct OneShotPlan {
    DevicePlanStatus status = DevicePlanStatus::Unavailable;
    OneShotBufferLayout layout;
    LLPacket* packets = nullptr;
    // Remote packet-buffer offsets, indexed by DeviceGroupView active index.
    uint64_t packet_offsets[kMaxNumRanks] = {};
};

// Local device state owned by OneShotAlgorithm and shared by its kernels.
struct alignas(64) OneShotDeviceState {
    OneShotPlan plan;
    CollectiveRuntimeBindings collective;
    // Borrowed state from the communicator's LL protocol.
    LLState* ll = nullptr;
};

PGResult<void> launchOneShotAllReduceKernel(const AllReduceRequest& request,
                                            OneShotDeviceState* state,
                                            cudaStream_t stream);
PGResult<void> launchOneShotAllGatherKernel(const AllGatherRequest& request,
                                            OneShotDeviceState* state,
                                            cudaStream_t stream);

}  // namespace mooncake

#endif  // MOONCAKE_PG_DEVICE_COMM_DEVICE_COLLECTIVE_ALGORITHMS_ONE_SHOT_ALGORITHM_CUH
