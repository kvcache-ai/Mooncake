#ifndef MOONCAKE_PG_DEVICE_COMM_DEVICE_COLLECTIVE_PROTOCOLS_SIMPLE_TYPES_CUH
#define MOONCAKE_PG_DEVICE_COMM_DEVICE_COLLECTIVE_PROTOCOLS_SIMPLE_TYPES_CUH

#include "device_comm/device_collective/device_collective_types.cuh"

namespace mooncake {

inline constexpr uint32_t kSimplePipelineDepth = 2;
inline constexpr uint64_t kSimplePayloadAlignment = 16;

// Host-bound workspace shared by the channels of one invocation.
struct SimpleWorkspace {
    char* buffer = nullptr;
    uint64_t bytes = 0;
    char* staging = nullptr;
};

struct SimpleBufferLayout {
    uint64_t chunk_bytes = 0;

    [[nodiscard]] __device__ __forceinline__ static SimpleBufferLayout make(
        uint64_t channel_bytes, uint64_t max_chunk_bytes) {
        const uint64_t available = channel_bytes / kSimplePipelineDepth;
        const uint64_t bounded =
            available < max_chunk_bytes ? available : max_chunk_bytes;
        return {bounded - bounded % kSimplePayloadAlignment};
    }

    [[nodiscard]] __device__ __forceinline__ uint64_t bufferBytes() const {
        return chunk_bytes * kSimplePipelineDepth;
    }
};

// Persistent progress for one (channel, peer) within a communicator.
struct SimplePeerProgress {
    uint64_t send_cursor = 0;
    uint64_t recv_cursor = 0;
    uint64_t buffer_ready_sequence = 0;
};

// Defines the [kind][channel][signaling rank][slot] byte offsets within one
// pipeline instance's signal slice.
struct SimplePipelineSignalLayout {
    uint32_t max_group_size = 0;
    uint64_t payload_ready_offset = 0;
    uint64_t payload_consumed_offset = 0;
    uint32_t total_signal_count = 0;

    [[nodiscard]] static SimplePipelineSignalLayout make(
        uint32_t max_group_size) noexcept {
        const uint32_t recv_buffer_ready_count =
            kMaxDeviceCollectiveChannels * max_group_size;
        const uint32_t payload_ready_count = kMaxDeviceCollectiveChannels *
                                             max_group_size *
                                             kSimplePipelineDepth;
        const uint32_t payload_consumed_count = payload_ready_count;

        const uint64_t payload_ready_offset =
            static_cast<uint64_t>(recv_buffer_ready_count) * sizeof(uint64_t);
        const uint64_t payload_consumed_offset =
            payload_ready_offset +
            static_cast<uint64_t>(payload_ready_count) * sizeof(uint64_t);
        const uint32_t total_signal_count = recv_buffer_ready_count +
                                            payload_ready_count +
                                            payload_consumed_count;

        return SimplePipelineSignalLayout{
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
        uint64_t base_offset, uint32_t channel_index,
        InGroupRank signaling_rank, uint32_t payload_slot) const {
        const uint64_t channel_signaling_rank_index =
            static_cast<uint64_t>(channel_index) * max_group_size +
            static_cast<uint32_t>(signaling_rank);
        return base_offset +
               (channel_signaling_rank_index * kSimplePipelineDepth +
                payload_slot) *
                   sizeof(uint64_t);
    }
};

// Protocol-owned bindings, indexed by stable InGroupRank.
struct SimplePeer {
    GlobalRank global_rank = kInvalidGlobalRank;
    uint64_t signal_offset = 0;
};

// Host-published Simple bindings for the current View. Progress lives in a
// separate [channel][peer] array, reset when the View changes.
struct SimpleBindings {
    const DeviceTransferHandle* transfer_handle = nullptr;
    InGroupRank self_rank = kInvalidInGroupRank;
    const uint64_t* signals = nullptr;
    SimplePipelineSignalLayout signal_layout;
    SimplePeerProgress* peer_progress = nullptr;
    // Indexed by InGroupRank.
    SimplePeer peer_bindings[kMaxNumRanks] = {};
};

}  // namespace mooncake

#endif  // MOONCAKE_PG_DEVICE_COMM_DEVICE_COLLECTIVE_PROTOCOLS_SIMPLE_TYPES_CUH
