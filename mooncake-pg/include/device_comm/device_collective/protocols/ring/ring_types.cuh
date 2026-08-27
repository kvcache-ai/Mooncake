#ifndef MOONCAKE_PG_DEVICE_COMM_DEVICE_COLLECTIVE_PROTOCOLS_RING_TYPES_CUH
#define MOONCAKE_PG_DEVICE_COMM_DEVICE_COLLECTIVE_PROTOCOLS_RING_TYPES_CUH

#include <cstdint>

#include <cuda_alike.h>

#include "common_types.h"
#include "device_comm/device_collective/device_collective_types.cuh"

namespace mooncake {

inline constexpr uint32_t kRingPipelineSlots = 2;
static_assert(kRingPipelineSlots >= 2);
// Keep every dynamically sized payload slot on the 16-byte ValuePack path.
inline constexpr uint64_t kRingPayloadAlignment = 16;
// Payload slot size is derived by evenly dividing the shared workspace. An
// oversized slot reduces Ring pipeline overlap, so a cap is applied here.
inline constexpr uint64_t kMaxRingPayloadSlotSize = 512ull * 1024;  // 512 KiB
static_assert(kMaxRingPayloadSlotSize % kRingPayloadAlignment == 0);

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
            kMaxDeviceCollectiveChannels * max_group_size * kRingPipelineSlots;
        const uint32_t payload_consumed_count = payload_ready_count;

        const uint64_t payload_ready_offset =
            static_cast<uint64_t>(recv_buffer_ready_count) * sizeof(uint64_t);
        const uint64_t payload_consumed_offset =
            payload_ready_offset +
            static_cast<uint64_t>(payload_ready_count) * sizeof(uint64_t);
        const uint32_t total_signal_count = recv_buffer_ready_count +
                                            payload_ready_count +
                                            payload_consumed_count;

        return RingSignalLayout{
            .max_group_size = max_group_size,
            .payload_ready_offset = payload_ready_offset,
            .payload_consumed_offset = payload_consumed_offset,
            .total_signal_count = total_signal_count,
        };
    }

    [[nodiscard]] __device__ __forceinline__ uint64_t
    recvBufferReadyOffset(uint64_t region_offset, uint32_t channel_index,
                          InGroupRank signaling_rank) const {
        return region_offset +
               channelSignalingRankByteOffset(channel_index, signaling_rank);
    }

    [[nodiscard]] __device__ __forceinline__ const uint64_t* recvBufferReadyPtr(
        const uint64_t* region_ptr, uint32_t channel_index,
        InGroupRank signaling_rank) const {
        return reinterpret_cast<const uint64_t*>(
            reinterpret_cast<const char*>(region_ptr) +
            channelSignalingRankByteOffset(channel_index, signaling_rank));
    }

    [[nodiscard]] __device__ __forceinline__ uint64_t payloadReadyOffset(
        uint64_t region_offset, uint32_t channel_index,
        InGroupRank signaling_rank, uint32_t payload_slot) const {
        return region_offset +
               pipelinedByteOffset(payload_ready_offset, channel_index,
                                   signaling_rank, payload_slot);
    }

    [[nodiscard]] __device__ __forceinline__ const uint64_t* payloadReadyPtr(
        const uint64_t* region_ptr, uint32_t channel_index,
        InGroupRank signaling_rank, uint32_t payload_slot) const {
        return reinterpret_cast<const uint64_t*>(
            reinterpret_cast<const char*>(region_ptr) +
            pipelinedByteOffset(payload_ready_offset, channel_index,
                                signaling_rank, payload_slot));
    }

    [[nodiscard]] __device__ __forceinline__ uint64_t payloadConsumedOffset(
        uint64_t region_offset, uint32_t channel_index,
        InGroupRank signaling_rank, uint32_t payload_slot) const {
        return region_offset +
               pipelinedByteOffset(payload_consumed_offset, channel_index,
                                   signaling_rank, payload_slot);
    }

    [[nodiscard]] __device__ __forceinline__ const uint64_t* payloadConsumedPtr(
        const uint64_t* region_ptr, uint32_t channel_index,
        InGroupRank signaling_rank, uint32_t payload_slot) const {
        return reinterpret_cast<const uint64_t*>(
            reinterpret_cast<const char*>(region_ptr) +
            pipelinedByteOffset(payload_consumed_offset, channel_index,
                                signaling_rank, payload_slot));
    }

   private:
    [[nodiscard]] __device__ __forceinline__ uint64_t
    channelSignalingRankByteOffset(uint32_t channel_index,
                                   InGroupRank signaling_rank) const {
        const uint64_t channel_signaling_rank_index =
            static_cast<uint64_t>(channel_index) * max_group_size +
            static_cast<uint32_t>(signaling_rank);
        return channel_signaling_rank_index * sizeof(uint64_t);
    }

    [[nodiscard]] __device__ __forceinline__ uint64_t pipelinedByteOffset(
        uint64_t kind_offset, uint32_t channel_index,
        InGroupRank signaling_rank, uint32_t payload_slot) const {
        const uint64_t channel_signaling_rank_index =
            static_cast<uint64_t>(channel_index) * max_group_size +
            static_cast<uint32_t>(signaling_rank);
        return kind_offset +
               (channel_signaling_rank_index * kRingPipelineSlots +
                payload_slot) *
                   sizeof(uint64_t);
    }
};

struct RingPeerTarget {
    GlobalRank global_rank = kInvalidGlobalRank;
    InGroupRank in_group_rank = kInvalidInGroupRank;
    // Each offset is relative to this peer's independently published region.
    uint64_t buffer_offset = 0;
    uint64_t signal_offset = 0;
    // Exact remote word used to publish this rank's view epoch to the peer.
    uint64_t view_epoch_signal_offset = 0;
};

// Complete published resource and topology binding read by a Ring AllReduce
// kernel. Rolling sequence state lives next to it in RingAllReduceDeviceState.
// A control update replaces the Plan while protocol execution is quiescent, so
// a kernel needs only the state pointer and its per-invocation request.
struct RingAllReducePlan {
    const DeviceTransferHandle* transfer_handle = nullptr;
    uint64_t timeout_ticks = 0;
    uint64_t view_epoch = kInvalidViewEpoch;

    // Local bindings are concrete addresses. Peer bindings remain offsets in
    // RingPeerTarget because each rank publishes an independent region base.
    char* buffer_ptr = nullptr;
    uint64_t buffer_size = 0;
    const uint64_t* signal_ptr = nullptr;
    RingSignalLayout signal_layout;

    // Optional source binding in DTS's separate local staging allocation. It
    // is never published to peers.
    char* staging_ptr = nullptr;
    uint64_t staging_size = 0;

    InGroupRank self_rank = kInvalidInGroupRank;
    int32_t self_active_index = -1;
    uint32_t participant_count = 0;
    RingPeerTarget predecessor;
    RingPeerTarget successor;
};

using RingAllReducePlanSlot = PlanSlot<RingAllReducePlan>;

// Ordinary device memory owned by one Ring AllReduce protocol instance. None
// of this state is registered or published to peers.
struct alignas(256) RingAllReduceDeviceState {
    RingAllReducePlanSlot plan;
    // Runtime-owned local slice; word i is written only by InGroupRank i.
    const uint64_t* view_epoch_signals = nullptr;
    InvocationState* invocation_state = nullptr;
    ControlMailbox* control_mailbox = nullptr;
    uint64_t next_step_sequences[kMaxDeviceCollectiveChannels *
                                 kRingPipelineSlots] = {};
    uint64_t next_recv_buffer_ready_sequences[kMaxDeviceCollectiveChannels] =
        {};
};

struct RingAllReduceKernelArgs {
    const void* send_buffer = nullptr;
    void* recv_buffer = nullptr;
    uint64_t count = 0;
    DataType datatype = DataType::Float32;
    ReduceOp op = ReduceOp::Sum;
    int32_t* failed_ranks_hint = nullptr;
};

cudaError_t launchRingAllReduceKernel(const RingAllReduceKernelArgs& request,
                                      RingAllReduceDeviceState* state,
                                      cudaStream_t stream);

}  // namespace mooncake

#endif  // MOONCAKE_PG_DEVICE_COMM_DEVICE_COLLECTIVE_PROTOCOLS_RING_TYPES_CUH
