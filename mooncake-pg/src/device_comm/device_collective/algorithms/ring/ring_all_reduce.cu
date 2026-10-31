#include "device_comm/device_collective/algorithms/ring/ring_kernel.cuh"

#include <cstdint>

#include <cooperative_groups.h>

#include "pg_assert.h"
#include "device_comm/device_collective/device_collective_kernel.cuh"
#include "device_comm/device_collective/device_collective_types.cuh"
#include "device_comm/device_collective/protocols/simple/simple_primitives.cuh"
#include "device_comm/device_utils/device_utils.cuh"

namespace mooncake {
namespace {

// Ring maps each chunk to local input/output operands. Simple receives
// the resulting addresses without knowing the Ring's shard or channel layout.
template <typename T>
struct AllReduceLayout {
    const T* input = nullptr;
    T* output = nullptr;
    uint64_t channel_elements = 0;
    uint64_t shard_element_capacity = 0;
    uint64_t chunk_element_capacity = 0;

    [[nodiscard]] __device__ __forceinline__ CollectiveChunk<T> chunk(
        uint32_t shard_index, uint64_t chunk_index) const {
        // With S = shard_element_capacity, shard i covers the channel-relative
        // interval [i * S, min((i + 1) * S, channel_elements)).
        const uint64_t shard_begin =
            static_cast<uint64_t>(shard_index) * shard_element_capacity;
        const uint64_t shard_elements =
            shard_begin < channel_elements
                ? minimum(shard_element_capacity,
                          channel_elements - shard_begin)
                : 0;

        // With K = chunk_element_capacity, chunk j covers the shard-relative
        // interval [j * K, min((j + 1) * K, shard_elements)).
        const uint64_t chunk_begin = chunk_index * chunk_element_capacity;
        const uint64_t count =
            chunk_begin < shard_elements
                ? minimum(chunk_element_capacity, shard_elements - chunk_begin)
                : 0;

        // Short shards still execute the common message schedule. Keep the
        // base pointers for an empty chunk instead of forming addresses beyond
        // the channel's input/output ranges.
        if (count == 0) {
            return CollectiveChunk<T>{
                .source = input,
                .destination = output,
                .count = 0,
            };
        }

        const uint64_t offset = shard_begin + chunk_begin;
        return CollectiveChunk<T>{
            .source = input + offset,
            .destination = output + offset,
            .count = count,
        };
    }

    [[nodiscard]] __device__ __forceinline__ uint64_t chunkCount() const {
        // Count chunks from the common shard capacity, rather than one shard's
        // clipped length, so every rank executes the same number of actions.
        return ceilDiv(shard_element_capacity, chunk_element_capacity);
    }
};

template <typename T, ReduceOp Op>
[[nodiscard]] __device__ __forceinline__ CollectiveResult
runRingRound(const DeviceGroupView& view, SimplePrimitives<T>& primitives,
             const AllReduceLayout<T>& layout, uint64_t chunk_index,
             cooperative_groups::thread_block block) {
    const uint64_t ring_steps = view.participant_count - 1;
    // Start reduce-scatter by injecting this rank's contribution for its own
    // shard into the ring.
    const uint32_t first_shard_index =
        ringIndexAtDistance(view.self_active_index, view.participant_count, 0);
    const auto first_chunk = layout.chunk(first_shard_index, chunk_index);
    PG_COLL_TRY(primitives.send(first_chunk, block));

    // Each intermediate reduce-scatter step receives a partially reduced
    // shard, adds this rank's contribution, and forwards the new partial.
    for (uint64_t distance = 1; distance < ring_steps; ++distance) {
        const uint32_t shard_index = ringIndexAtDistance(
            view.self_active_index, view.participant_count, distance);
        const auto chunk = layout.chunk(shard_index, chunk_index);
        PG_COLL_TRY(primitives.template recvReduceSend<Op>(chunk, block));
    }

    // The final reduce-scatter receive completes one shard. Store the reduced
    // value in local output and forward it as the first all-gather send.
    const uint32_t completed_shard_index = ringIndexAtDistance(
        view.self_active_index, view.participant_count, ring_steps);
    const auto completed_chunk =
        layout.chunk(completed_shard_index, chunk_index);
    PG_COLL_TRY(
        primitives.template recvReduceCopySend<Op>(completed_chunk, block));

    // The remaining all-gather steps receive fully reduced shards, copy them
    // into local output, and forward them without another reduction.
    for (uint64_t distance = 0; distance + 1 < ring_steps; ++distance) {
        const uint32_t shard_index = ringIndexAtDistance(
            view.self_active_index, view.participant_count, distance);
        const auto chunk = layout.chunk(shard_index, chunk_index);
        PG_COLL_TRY(primitives.recvCopySend(chunk, block));
    }

    // Receive the last fully reduced shard without forwarding it.
    const uint32_t last_shard_index = ringIndexAtDistance(
        view.self_active_index, view.participant_count, ring_steps - 1);
    const auto last_chunk = layout.chunk(last_shard_index, chunk_index);
    return primitives.recvCopy(last_chunk, block);
}

template <typename T, ReduceOp Op>
[[nodiscard]] __device__ __forceinline__ CollectiveResult
runRingChannel(const AllReduceRequest& request, RingDeviceState* state,
               uint32_t channel, cooperative_groups::thread_block block) {
    // Recovery may update this host-constructed Plan between Graph replays.
    const auto& plan = state->plan;
    const auto& view = *state->collective.group_view;
    if (request.count == 0) {
        return {};
    }

    const uint32_t channel_count = RingChannelPartition::chooseChannelCount(
        request.count * sizeof(T) / view.participant_count,
        plan.workspace.bytes);
    if (channel >= channel_count) {
        // The fixed launch capacity keeps host submission independent of the
        // Plan image. Inactive CTAs still participate in invocation completion.
        return {};
    }
    const auto partition =
        RingChannelPartition::make(request.count, channel, channel_count);

    const auto* input =
        static_cast<const T*>(request.send_buffer) + partition.offset;
    auto* output = static_cast<T*>(request.recv_buffer) + partition.offset;

    PG_ASSERT(state->simple);
    auto primitives = SimplePrimitives<T>::bind(
        *state->simple, plan.workspace, view, plan.predecessor_index,
        plan.successor_index, channel, channel_count, kMaxRingChunkBytes,
        state->collective.timeout_ticks);

    // full request -> channel -> ring shard -> chunk
    const AllReduceLayout<T> layout{
        .input = input,
        .output = output,
        .channel_elements = partition.count,
        .shard_element_capacity =
            ceilDiv(partition.count, view.participant_count),
        .chunk_element_capacity = primitives.chunkElements(),
    };

    PG_COLL_TRY(primitives.begin(block));

    const uint64_t chunk_count = layout.chunkCount();

    // Each round handles the same chunk index in every shard. Complete the
    // reduce-scatter and all-gather traversal before advancing to the next
    // chunk index.
    for (uint64_t chunk_index = 0; chunk_index < chunk_count; ++chunk_index) {
        PG_COLL_TRY(
            runRingRound<T, Op>(view, primitives, layout, chunk_index, block));
    }

    return primitives.finish(block);
}

template <typename T, ReduceOp Op>
__global__ __launch_bounds__(kRingBlockThreads, 1) void ringAllReduceKernel(
    AllReduceRequest request, RingDeviceState* state) {
    PG_ASSERT(state);
    runCollectiveInvocation<CollectiveExecution::MultiCTA>(
        request, &state->plan, state->collective,
        [&](cooperative_groups::thread_block block) {
            return runRingChannel<T, Op>(request, state, blockIdx.x, block);
        });
}

template <typename T, ReduceOp Op>
PGResult<void> launchKernel(const AllReduceRequest& request,
                            RingDeviceState* state, cudaStream_t stream) {
    const dim3 grid(kMaxDeviceCollectiveChannels);
    ringAllReduceKernel<T, Op>
        <<<grid, kRingBlockThreads, 0, stream>>>(request, state);
    PG_TRY_CUDA(cudaGetLastError());
    return {};
}

template <typename T>
PGResult<void> launchForType(const AllReduceRequest& request,
                             RingDeviceState* state, cudaStream_t stream) {
    switch (request.op) {
        case ReduceOp::Sum:
            return launchKernel<T, ReduceOp::Sum>(request, state, stream);
        case ReduceOp::Product:
            return launchKernel<T, ReduceOp::Product>(request, state, stream);
        case ReduceOp::Min:
            return launchKernel<T, ReduceOp::Min>(request, state, stream);
        case ReduceOp::Max:
            return launchKernel<T, ReduceOp::Max>(request, state, stream);
        default:
            return makePGError(
                PGErrorCode::NotSupported,
                "Ring AllReduce reduction operation is not supported");
    }
}

}  // namespace

PGResult<void> launchRingAllReduceKernel(const AllReduceRequest& request,
                                         RingDeviceState* state,
                                         cudaStream_t stream) {
    switch (request.datatype) {
        case DataType::Float16:
            return launchForType<__half>(request, state, stream);
        case DataType::Uint8:
            return launchForType<uint8_t>(request, state, stream);
        case DataType::Int8:
            return launchForType<int8_t>(request, state, stream);
        case DataType::Int16:
            return launchForType<int16_t>(request, state, stream);
        case DataType::Int32:
            return launchForType<int32_t>(request, state, stream);
        case DataType::Int64:
            return launchForType<int64_t>(request, state, stream);
        case DataType::Bfloat16:
            return launchForType<__nv_bfloat16>(request, state, stream);
        case DataType::Float32:
            return launchForType<float>(request, state, stream);
        case DataType::Float64:
            return launchForType<double>(request, state, stream);
        case DataType::Bool:
            return launchForType<bool>(request, state, stream);
        default:
            return makePGError(PGErrorCode::NotSupported,
                               "Ring AllReduce datatype is not supported");
    }
}

}  // namespace mooncake
