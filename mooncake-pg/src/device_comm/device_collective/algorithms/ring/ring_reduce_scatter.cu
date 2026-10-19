#include "device_comm/device_collective/algorithms/ring/ring_kernel.cuh"

#include <cstdint>

#include "device_comm/device_collective/device_collective_kernel.cuh"
#include "device_comm/device_collective/protocols/simple/simple_primitives.cuh"
#include "device_comm/device_utils/device_utils.cuh"

namespace mooncake {
namespace {

// Input segments are indexed by in-group rank; output contains only this
// rank's result. Ring traversal uses active indices to select each recipient.
template <typename T>
struct ReduceScatterLayout {
    const T* input;
    T* output;
    const DeviceGroupView& view;
    uint64_t rank_elements;
    RingChannelPartition channel;
    uint64_t chunk_capacity;

    [[nodiscard]] __device__ __forceinline__ CollectiveChunk<T> chunk(
        uint32_t active_index, uint64_t chunk_index) const {
        PG_ASSERT(active_index < view.participant_count);
        const auto rank = view.participants[active_index].in_group_rank;
        PG_ASSERT(rank >= 0);
        const uint64_t chunk_begin = chunk_index * chunk_capacity;
        PG_ASSERT(chunk_begin < channel.count);
        const uint64_t offset = channel.offset + chunk_begin;
        return {
            .source =
                input + static_cast<uint64_t>(rank) * rank_elements + offset,
            // Only the final reduction writes to this destination.
            .destination = output + offset,
            .count = minimum(chunk_capacity, channel.count - chunk_begin),
        };
    }

    [[nodiscard]] __device__ __forceinline__ uint64_t chunkCount() const {
        return ceilDiv(channel.count, chunk_capacity);
    }
};

template <typename T, ReduceOp Op>
[[nodiscard]] __device__ __forceinline__ CollectiveResult
runRingRound(const DeviceGroupView& view, SimplePrimitives<T>& primitives,
             const ReduceScatterLayout<T>& layout, uint64_t chunk_index,
             cooperative_groups::thread_block block) {
    const auto [send_op, reduce_op, final_op] =
        makeRingReductionOps<T, Op>(view.participant_count);
    // Inject the preceding participant's segment so that P - 1 receives
    // complete this rank's result.
    const uint32_t first_active_index =
        ringIndexAtDistance(view.self_active_index, view.participant_count, 1);
    const auto first_chunk = layout.chunk(first_active_index, chunk_index);
    PG_COLL_TRY(primitives.transformSend(first_chunk, block, send_op));

    for (uint64_t distance = 2; distance < view.participant_count; ++distance) {
        const uint32_t active_index = ringIndexAtDistance(
            view.self_active_index, view.participant_count, distance);
        const auto chunk = layout.chunk(active_index, chunk_index);
        PG_COLL_TRY(primitives.recvTransformSend(chunk, block, reduce_op));
    }

    // The last receive is at distance P and belongs to this rank. In-place
    // output overwrites its input segment only after the final reduction.
    const auto completed_chunk =
        layout.chunk(view.self_active_index, chunk_index);
    return primitives.recvTransformCopy(completed_chunk, block, final_op);
}

template <typename T, ReduceOp Op>
[[nodiscard]] __device__ __forceinline__ CollectiveResult
runRingChannel(const ReduceScatterRequest& request, RingDeviceState* state,
               uint32_t channel, cooperative_groups::thread_block block) {
    // Empty requests still execute shared startup and completion.
    if (request.count == 0) return {};

    const auto& plan = state->plan;
    const auto& view = *state->collective.group_view;
    const uint32_t channel_count = RingChannelPartition::chooseChannelCount(
        request.count * sizeof(T), plan.workspace.bytes);
    if (channel >= channel_count) return {};
    const auto partition =
        RingChannelPartition::make(request.count, channel, channel_count);

    PG_ASSERT(state->simple);
    auto primitives = SimplePrimitives<T>::bind(
        *state->simple, plan.workspace, view, plan.predecessor_index,
        plan.successor_index, channel, channel_count, kMaxRingChunkBytes,
        state->collective.timeout_ticks);
    const ReduceScatterLayout<T> layout{
        .input = static_cast<const T*>(request.send_buffer),
        .output = static_cast<T*>(request.recv_buffer),
        .view = view,
        .rank_elements = request.count,
        .channel = partition,
        .chunk_capacity = primitives.chunkElements(),
    };

    PG_COLL_TRY(primitives.begin(block));
    const uint64_t chunk_count = layout.chunkCount();
    for (uint64_t chunk_index = 0; chunk_index < chunk_count; ++chunk_index) {
        PG_COLL_TRY(
            runRingRound<T, Op>(view, primitives, layout, chunk_index, block));
    }
    return primitives.finish(block);
}

template <typename T, ReduceOp Op>
__global__ __launch_bounds__(kRingBlockThreads, 1) void ringReduceScatterKernel(
    ReduceScatterRequest request, RingDeviceState* state) {
    PG_ASSERT(state);
    runCollectiveInvocation<CollectiveExecution::MultiCTA>(
        request, &state->plan, state->collective,
        [&](cooperative_groups::thread_block block) {
            return runRingChannel<T, Op>(request, state, blockIdx.x, block);
        });
}

template <typename T, ReduceOp Op>
PGResult<void> launchKernel(const ReduceScatterRequest& request,
                            RingDeviceState* state, cudaStream_t stream) {
    const dim3 grid(kMaxDeviceCollectiveChannels);
    ringReduceScatterKernel<T, Op>
        <<<grid, kRingBlockThreads, 0, stream>>>(request, state);
    PG_TRY_CUDA(cudaGetLastError());
    return {};
}

}  // namespace

PGResult<void> launchRingReduceScatterKernel(
    const ReduceScatterRequest& request, RingDeviceState* state,
    cudaStream_t stream) {
    PG_DISPATCH_REDUCTION(launchKernel, request, state, stream);
}

}  // namespace mooncake
