#ifndef MOONCAKE_PG_DEVICE_COMM_DEVICE_COLLECTIVE_PROTOCOLS_RING_TYPES_CUH
#define MOONCAKE_PG_DEVICE_COMM_DEVICE_COLLECTIVE_PROTOCOLS_RING_TYPES_CUH

#include <cstdint>

#include <cuda_alike.h>

#include "common_types.h"
#include "device_comm/device_collective/device_collective_types.cuh"

namespace mooncake {

inline constexpr uint32_t kRingPipelineSlots = 2;
static_assert(kRingPipelineSlots >= 2);
inline constexpr uint64_t kRingBufferBytes = 16ull << 20;
inline constexpr uint64_t kRingStagingBytes = kRingBufferBytes;
static_assert(kRingBufferBytes <= kDeviceCollectiveBufferCapacity);

// Defines the [kind][channel][signaling rank][slot] byte offsets within one
// Ring protocol instance's signal slice. The slice belongs to the rank
// receiving the signal; the rank dimension identifies the peer writing it.
struct RingSignalLayout {
    uint32_t max_group_size = 0;
    uint64_t payload_ready_offset = 0;
    uint64_t payload_consumed_offset = 0;
    uint32_t total_signal_count = 0;

    [[nodiscard]] static RingSignalLayout make(
        uint32_t max_group_size) noexcept {
        const uint32_t recv_buffer_ready_count =
            kMaxDeviceCollectiveChannels * max_group_size;
        const uint32_t payload_ready_count =
            kMaxDeviceCollectiveChannels * max_group_size *
            kRingPipelineSlots;
        const uint32_t payload_consumed_count = payload_ready_count;

        const uint64_t payload_ready_offset =
            static_cast<uint64_t>(recv_buffer_ready_count) * sizeof(uint64_t);
        const uint64_t payload_consumed_offset =
            payload_ready_offset +
            static_cast<uint64_t>(payload_ready_count) * sizeof(uint64_t);
        const uint32_t total_signal_count =
            recv_buffer_ready_count + payload_ready_count +
            payload_consumed_count;

        return RingSignalLayout{
            .max_group_size = max_group_size,
            .payload_ready_offset = payload_ready_offset,
            .payload_consumed_offset = payload_consumed_offset,
            .total_signal_count = total_signal_count,
        };
    }

    [[nodiscard]] __device__ __forceinline__ uint64_t recvBufferReadyOffset(
        uint64_t region_offset, uint32_t channel_index,
        InGroupRank signaling_rank) const {
        return channelSignalingRankOffset(region_offset, channel_index,
                                          signaling_rank);
    }

    [[nodiscard]] __device__ __forceinline__ uint64_t payloadReadyOffset(
        uint64_t region_offset, uint32_t channel_index,
        InGroupRank signaling_rank, uint32_t payload_slot) const {
        return pipelinedOffset(region_offset + payload_ready_offset,
                               channel_index, signaling_rank, payload_slot);
    }

    [[nodiscard]] __device__ __forceinline__ uint64_t payloadConsumedOffset(
        uint64_t region_offset, uint32_t channel_index,
        InGroupRank signaling_rank, uint32_t payload_slot) const {
        return pipelinedOffset(region_offset + payload_consumed_offset,
                               channel_index, signaling_rank, payload_slot);
    }

   private:
    [[nodiscard]] __device__ __forceinline__ uint64_t
    channelSignalingRankOffset(
        uint64_t region_offset, uint32_t channel_index,
        InGroupRank signaling_rank) const {
        const uint64_t channel_signaling_rank_index =
            static_cast<uint64_t>(channel_index) * max_group_size +
            static_cast<uint32_t>(signaling_rank);
        return region_offset +
               channel_signaling_rank_index * sizeof(uint64_t);
    }

    [[nodiscard]] __device__ __forceinline__ uint64_t pipelinedOffset(
        uint64_t region_offset, uint32_t channel_index,
        InGroupRank signaling_rank, uint32_t payload_slot) const {
        const uint64_t channel_signaling_rank_index =
            static_cast<uint64_t>(channel_index) * max_group_size +
            static_cast<uint32_t>(signaling_rank);
        return region_offset +
               (channel_signaling_rank_index * kRingPipelineSlots +
                payload_slot) *
                   sizeof(uint64_t);
    }
};

struct RingPeerTarget {
    GlobalRank global_rank = kInvalidGlobalRank;
    InGroupRank in_group_rank = kInvalidInGroupRank;
    uint64_t buffer_offset = 0;
    uint64_t signal_offset = 0;
};

// Complete published resource and topology binding read by a Ring AllReduce
// kernel. Rolling sequence state lives next to it in RingAllReduceDeviceState.
// publish() replaces the Plan while protocol execution is quiescent, so a
// kernel needs only the state pointer and its per-invocation request.
struct RingAllReducePlan {
    const DeviceTransferHandle* transfer_handle = nullptr;
    DeviceCollectiveInvocationState* invocation_state = nullptr;
    DeviceCollectiveRecoveryMailbox* recovery_mailbox = nullptr;
    uint64_t timeout_ticks = 0;

    // Local registered-memory bindings.
    uint64_t buffer_offset = 0;
    uint64_t buffer_size = 0;
    uint64_t signal_offset = 0;
    RingSignalLayout signal_layout;

    // Optional local staging for a successor route without a direct mapping.
    uint64_t staging_offset = 0;
    uint64_t staging_size = 0;

    InGroupRank self_rank = kInvalidInGroupRank;
    int32_t self_active_index = -1;
    uint32_t participant_count = 0;
    RingPeerTarget predecessor;
    RingPeerTarget successor;
};

using RingAllReducePlanSlot = DevicePlanSlot<RingAllReducePlan>;

// Ordinary device memory owned by one Ring AllReduce protocol instance. None
// of this state is registered or published to peers.
struct alignas(256) RingAllReduceDeviceState {
    RingAllReducePlanSlot plan;
    uint64_t next_step_sequences[kMaxDeviceCollectiveChannels *
                                 kRingPipelineSlots] = {};
    uint64_t
        next_recv_buffer_ready_sequences[kMaxDeviceCollectiveChannels] = {};
};

struct RingAllReduceKernelArgs {
    const void* send_buffer = nullptr;
    void* recv_buffer = nullptr;
    uint64_t count = 0;
    DataType datatype = DataType::Float32;
    ReduceOp op = ReduceOp::Sum;
    int32_t* failed_ranks_hint = nullptr;
};

cudaError_t launchRingAllReduceKernel(
    const RingAllReduceKernelArgs& request, RingAllReduceDeviceState* state,
    uint32_t channel_count, cudaStream_t stream);

}  // namespace mooncake

#endif  // MOONCAKE_PG_DEVICE_COMM_DEVICE_COLLECTIVE_PROTOCOLS_RING_TYPES_CUH
