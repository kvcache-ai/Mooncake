#include "device_comm/device_collective/algorithms/ring/ring_kernel.cuh"

#include <cstdint>

#include "device_comm/device_collective/device_collective_kernel.cuh"
#include "device_comm/device_collective/protocols/simple/simple_primitives.cuh"
#include "device_comm/device_utils/device_utils.cuh"

namespace mooncake {
namespace {

// Each participant contributes rank_bytes bytes. The active index selects
// the contributor; its in-group rank selects the output slot.
struct AllGatherLayout {
    const uint8_t* input;
    uint8_t* output;
    const DeviceGroupView& view;
    uint64_t rank_bytes;
    RingChannelPartition channel;
    uint64_t chunk_capacity;

    [[nodiscard]] __device__ __forceinline__ uint8_t* destination(
        uint32_t active_index, uint64_t offset) const {
        PG_ASSERT(active_index < view.participant_count);
        const auto rank = view.participants[active_index].in_group_rank;
        PG_ASSERT(rank >= 0);
        return output + static_cast<uint64_t>(rank) * rank_bytes + offset;
    }

    [[nodiscard]] __device__ __forceinline__ CollectiveChunk<uint8_t> chunk(
        uint32_t active_index, uint64_t chunk_index) const {
        const uint64_t chunk_begin = chunk_index * chunk_capacity;
        PG_ASSERT(chunk_begin < channel.count);
        const uint64_t offset = channel.offset + chunk_begin;
        return {
            .source = input + offset,
            .destination = destination(active_index, offset),
            .count = minimum(chunk_capacity, channel.count - chunk_begin),
        };
    }

    [[nodiscard]] __device__ __forceinline__ uint64_t chunkCount() const {
        return ceilDiv(channel.count, chunk_capacity);
    }
};

[[nodiscard]] __device__ __forceinline__ CollectiveResult
runRingRound(const DeviceGroupView& view, SimplePrimitives<uint8_t>& primitives,
             const AllGatherLayout& layout, uint64_t chunk_index,
             cooperative_groups::thread_block block) {
    const uint64_t ring_steps = view.participant_count - 1;
    // Each rank injects its own contribution and stores it in local output.
    const uint32_t first_active_index =
        ringIndexAtDistance(view.self_active_index, view.participant_count, 0);
    const auto first_chunk = layout.chunk(first_active_index, chunk_index);
    PG_COLL_TRY(primitives.copySend(first_chunk, block));

    for (uint64_t distance = 1; distance < ring_steps; ++distance) {
        const uint32_t active_index = ringIndexAtDistance(
            view.self_active_index, view.participant_count, distance);
        const auto chunk = layout.chunk(active_index, chunk_index);
        PG_COLL_TRY(primitives.recvCopySend(chunk, block));
    }

    const uint32_t last_active_index = ringIndexAtDistance(
        view.self_active_index, view.participant_count, ring_steps);
    const auto last_chunk = layout.chunk(last_active_index, chunk_index);
    return primitives.recvCopy(last_chunk, block);
}

[[nodiscard]] __device__ __forceinline__ CollectiveResult
runRingChannel(const AllGatherRequest& request, uint64_t rank_bytes,
               RingDeviceState* state, uint32_t channel,
               cooperative_groups::thread_block block) {
    // Empty inputs still pass through shared startup and completion, but does
    // not form addresses from the potentially null user buffers.
    if (rank_bytes == 0) return {};

    const auto& plan = state->plan;
    const auto& view = *state->collective.group_view;
    const uint32_t channel_count = RingChannelPartition::chooseChannelCount(
        rank_bytes, plan.workspace.bytes);
    if (channel >= channel_count) return {};
    const auto partition =
        RingChannelPartition::make(rank_bytes, channel, channel_count);

    const auto* input = static_cast<const uint8_t*>(request.send_buffer);
    auto* output = static_cast<uint8_t*>(request.recv_buffer);

    PG_ASSERT(state->simple);
    auto primitives = SimplePrimitives<uint8_t>::bind(
        *state->simple, plan.workspace, view, plan.predecessor_index,
        plan.successor_index, channel, channel_count, kMaxRingChunkBytes,
        state->collective.timeout_ticks);

    const AllGatherLayout layout{
        .input = input,
        .output = output,
        .view = view,
        .rank_bytes = rank_bytes,
        .channel = partition,
        .chunk_capacity = primitives.chunkElements(),
    };

    PG_COLL_TRY(primitives.begin(block));
    const uint64_t chunk_count = layout.chunkCount();
    // Each round circulates the chunk at this index from every participant.
    for (uint64_t chunk_index = 0; chunk_index < chunk_count; ++chunk_index) {
        PG_COLL_TRY(runRingRound(view, primitives, layout, chunk_index, block));
    }
    return primitives.finish(block);
}

__global__ __launch_bounds__(kRingBlockThreads, 1) void ringAllGatherKernel(
    AllGatherRequest request, uint64_t rank_bytes, RingDeviceState* state) {
    PG_ASSERT(state);
    runCollectiveInvocation<CollectiveExecution::MultiCTA>(
        request, &state->plan, state->collective,
        [&](cooperative_groups::thread_block block) {
            return runRingChannel(request, rank_bytes, state, blockIdx.x,
                                  block);
        });
}

}  // namespace

PGResult<void> launchRingAllGatherKernel(const AllGatherRequest& request,
                                         RingDeviceState* state,
                                         cudaStream_t stream) {
    const uint64_t rank_bytes = request.count * elementSize(request.datatype);
    const dim3 grid(kMaxDeviceCollectiveChannels);
    ringAllGatherKernel<<<grid, kRingBlockThreads, 0, stream>>>(
        request, rank_bytes, state);
    PG_TRY_CUDA(cudaGetLastError());
    return {};
}

}  // namespace mooncake
