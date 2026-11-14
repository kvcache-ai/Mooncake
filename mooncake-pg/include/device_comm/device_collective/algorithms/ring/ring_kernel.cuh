#ifndef MOONCAKE_PG_DEVICE_COMM_DEVICE_COLLECTIVE_ALGORITHMS_RING_KERNEL_CUH
#define MOONCAKE_PG_DEVICE_COMM_DEVICE_COLLECTIVE_ALGORITHMS_RING_KERNEL_CUH

#include "device_comm/device_collective/algorithms/ring/ring_algorithm.cuh"
#include "device_comm/device_utils/device_utils.cuh"

namespace mooncake {

// Keep enough data per channel per step to amortize signaling.
inline constexpr uint64_t kMinRingBytesPerChannelStep = 8ull << 10;

inline constexpr int kRingBlockThreads = 512;

// One channel's contiguous input range, in the caller's units: elements for
// typed operands, or bytes for byte-copy operations.
struct RingChannelPartition {
    uint64_t offset;
    uint64_t count;

    // Choose the active channel count based on step size and available
    // workspace. step_bytes is estimated before channel and chunk partitioning.
    [[nodiscard]] __device__ __forceinline__ static uint32_t chooseChannelCount(
        uint64_t step_bytes, uint64_t workspace_bytes) {
        // Launch is plan-independent. Each CTA derives the same active channel
        // prefix after the startup leader has applied the latest Plan.
        uint32_t channel_count = kMaxDeviceCollectiveChannels;
        while (channel_count > 1) {
            const bool step_is_too_small =
                step_bytes / channel_count < kMinRingBytesPerChannelStep;
            const auto buffers = SimpleBufferLayout::make(
                workspace_bytes / channel_count, kMaxRingChunkBytes);
            if (!step_is_too_small && buffers.chunk_bytes != 0) break;
            channel_count /= 2;
        }
        return channel_count;
    }

    // With C = channel_count, each channel gets count / C units. The first
    // count % C channels each get one additional unit.
    //
    // For count = 10 and C = 3:
    //
    //             channel 0  channel 1  channel 2
    // offset              0          4          7
    // count               4          3          3
    [[nodiscard]] __device__ __forceinline__ static RingChannelPartition make(
        uint64_t count, uint32_t channel, uint32_t channel_count) {
        const uint64_t per_channel = count / channel_count;
        const uint64_t extra = count % channel_count;
        // Include the extra units assigned to earlier channels in the offset.
        return {static_cast<uint64_t>(channel) * per_channel +
                    minimum(channel, extra),
                per_channel + (channel < extra ? 1 : 0)};
    }
};

// Move distance positions backward from start_index, wrapping at
// participant_count. Each column follows one starting index; every row covers
// all indices.
//
// For participant_count = 4:
//
//             start 0  start 1  start 2  start 3
// distance 0  index 0  index 1  index 2  index 3
// distance 1  index 3  index 0  index 1  index 2
// distance 2  index 2  index 3  index 0  index 1
// distance 3  index 1  index 2  index 3  index 0
[[nodiscard]] __device__ __forceinline__ uint32_t ringIndexAtDistance(
    int32_t start_index, uint32_t participant_count, uint64_t distance) {
    int64_t index =
        static_cast<int64_t>(start_index) - static_cast<int64_t>(distance);
    index %= static_cast<int64_t>(participant_count);
    if (index < 0) index += participant_count;
    return static_cast<uint32_t>(index);
}

}  // namespace mooncake

#endif  // MOONCAKE_PG_DEVICE_COMM_DEVICE_COLLECTIVE_ALGORITHMS_RING_KERNEL_CUH
