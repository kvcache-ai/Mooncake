#ifndef MOONCAKE_PG_DEVICE_COMM_DEVICE_COLLECTIVE_ALGORITHMS_PAIRWISE_KERNEL_CUH
#define MOONCAKE_PG_DEVICE_COMM_DEVICE_COLLECTIVE_ALGORITHMS_PAIRWISE_KERNEL_CUH

#include "device_comm/device_collective/algorithms/pairwise/pairwise_algorithm.cuh"
#include "device_comm/device_utils/device_utils.cuh"

namespace mooncake {

// Keep enough data per channel to amortize signaling each peer.
inline constexpr uint64_t kMinPairwiseBytesPerChannel = 8ull << 10;
inline constexpr int kPairwiseBlockThreads = 512;

// One channel's contiguous range, in elements or bytes as chosen by the caller.
struct PairwiseChannelPartition {
    uint64_t offset;
    uint64_t count;

    [[nodiscard]] __device__ __forceinline__ static uint32_t chooseChannelCount(
        uint64_t peer_bytes, uint64_t workspace_bytes) {
        uint32_t channel_count = kMaxDeviceCollectiveChannels;
        while (channel_count > 1) {
            const auto buffers = SimpleBufferLayout::make(
                workspace_bytes / channel_count, kMaxPairwiseChunkBytes);
            if (peer_bytes / channel_count >= kMinPairwiseBytesPerChannel &&
                buffers.chunk_bytes != 0)
                break;
            channel_count /= 2;
        }
        return channel_count;
    }

    // The first count % channel_count channels receive one extra unit.
    [[nodiscard]] __device__ __forceinline__ static PairwiseChannelPartition
    make(uint64_t count, uint32_t channel, uint32_t channel_count) {
        const uint64_t per_channel = count / channel_count;
        const uint64_t extra = count % channel_count;
        return {uint64_t{channel} * per_channel + minimum(channel, extra),
                per_channel + (channel < extra ? 1 : 0)};
    }
};

// Return this round's peer for Pairwise AllToAll. Rank a sends its input slot b
// to rank b, and receives b's input slot a into its output slot b. These buffer
// slots use in-group ranks; the schedule below uses active indices.
//
// With P participants, assign the pair (i, j) to round (i + j) % P. Swapping
// i and j gives the same round, so both participants choose each other. For a
// fixed i, increasing j from 0 to P - 1 cycles through all P rounds once.
// Solving for the peer in round r gives j = (r + P - i) % P. When j == i,
// the caller performs the local copy.
//
// For P = 4, the pairs of active indices are:
//   round 0: 0 (local), 1 <-> 3, 2 (local)
//   round 1: 0 <-> 1, 2 <-> 3
//   round 2: 0 <-> 2, 1 (local), 3 (local)
//   round 3: 0 <-> 3, 1 <-> 2
//
// At rank a, sending to b reads input slot b; receiving from b writes output
// slot b. With in-place buffers these are the same memory, so the caller sends
// each chunk through Simple before receiving into that chunk's location.
[[nodiscard]] __device__ __forceinline__ uint32_t
pairwisePeerAtRound(const DeviceGroupView& view, uint32_t round) {
    return (round + view.participant_count -
            static_cast<uint32_t>(view.self_active_index)) %
           view.participant_count;
}

}  // namespace mooncake

#endif  // MOONCAKE_PG_DEVICE_COMM_DEVICE_COLLECTIVE_ALGORITHMS_PAIRWISE_KERNEL_CUH
